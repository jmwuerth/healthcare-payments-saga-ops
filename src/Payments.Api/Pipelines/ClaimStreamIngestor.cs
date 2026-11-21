using System.Buffers;
using System.Diagnostics;
using System.IO.Pipelines;
using System.Text.Json;
using System.Threading.Channels;

namespace Payments.Api.Pipelines;

public sealed record ClaimPaymentCommand(
    Guid ClaimId,
    Guid PayerId,
    string ProviderNpi,
    decimal Amount,
    string TraceId);

/// <summary>
/// High-throughput, non-blocking batch ingestor.
/// Streams raw NDJSON/EDI claim bytes via System.IO.Pipelines using ReadOnlySequence/Span
/// to avoid intermediate string heap allocations, then hands off to a Bounded Channel
/// with backpressure (BoundedChannelFullMode.Wait).
/// </summary>
public sealed class ClaimStreamIngestor(ChannelWriter<ClaimPaymentCommand> channelWriter)
{
    public static readonly ActivitySource Telemetry = new("COMPANYNAME.HealthcarePayments.Ingestor");

    public async Task<int> IngestStreamAsync(Stream networkStream, CancellationToken ct)
    {
        using var activity = Telemetry.StartActivity("IngestClaimBatchStream");
        var traceId = activity?.TraceId.ToString() ?? ActivityTraceId.CreateRandom().ToString();

        var pipe = new Pipe();
        int ingestedCount = 0;

        // Producer task: fills PipeWriter from network stream without blocking threads
        Task writingTask = FillPipeAsync(networkStream, pipe.Writer, ct);

        // Consumer task: slices newline-delimited frames on the stack and pushes to Channel
        Task readingTask = ReadPipeAsync(pipe.Reader, traceId, () => Interlocked.Increment(ref ingestedCount), ct);

        await Task.WhenAll(writingTask, readingTask);
        activity?.SetTag("claims.ingested", ingestedCount);
        return ingestedCount;
    }

    private static async Task FillPipeAsync(Stream stream, PipeWriter writer, CancellationToken ct)
    {
        const int minimumBufferSize = 4096;
        try
        {
            while (!ct.IsCancellationRequested)
            {
                Memory<byte> memory = writer.GetMemory(minimumBufferSize);
                int bytesRead = await stream.ReadAsync(memory, ct);
                if (bytesRead == 0) break;

                writer.Advance(bytesRead);
                FlushResult result = await writer.FlushAsync(ct);
                if (result.IsCompleted) break;
            }
        }
        finally
        {
            await writer.CompleteAsync();
        }
    }

    private async Task ReadPipeAsync(
        PipeReader reader,
        string traceId,
        Action onItemEnqueued,
        CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                ReadResult result = await reader.ReadAsync(ct);
                ReadOnlySequence<byte> buffer = result.Buffer;

                while (TryReadLine(ref buffer, out ReadOnlySequence<byte> line))
                {
                    var cmd = ParseZeroAllocJson(line, traceId);
                    if (cmd is not null)
                    {
                        // Non-blocking async backpressure: suspends task without pinning OS threads
                        await channelWriter.WriteAsync(cmd, ct);
                        onItemEnqueued();
                    }
                }

                reader.AdvanceTo(buffer.Start, buffer.End);
                if (result.IsCompleted) break;
            }
        }
        finally
        {
            await reader.CompleteAsync();
        }
    }

    private static bool TryReadLine(ref ReadOnlySequence<byte> buffer, out ReadOnlySequence<byte> line)
    {
        SequencePosition? position = buffer.PositionOf((byte)'\n');
        if (position is null)
        {
            line = default;
            return false;
        }

        line = buffer.Slice(0, position.Value);
        buffer = buffer.Slice(buffer.GetPosition(1, position.Value));
        return true;
    }

    private static ClaimPaymentCommand? ParseZeroAllocJson(ReadOnlySequence<byte> line, string traceId)
    {
        if (line.IsEmpty) return null;

        // Utf8JsonReader operates directly over ReadOnlySequence<byte> without string allocation
        var jsonReader = new Utf8JsonReader(line);
        Guid claimId = Guid.Empty, payerId = Guid.Empty;
        string npi = string.Empty;
        decimal amount = 0m;

        while (jsonReader.Read())
        {
            if (jsonReader.TokenType != JsonTokenType.PropertyName) continue;

            if (jsonReader.ValueTextEquals("claimId"u8) && jsonReader.Read())
                claimId = jsonReader.GetGuid();
            else if (jsonReader.ValueTextEquals("payerId"u8) && jsonReader.Read())
                payerId = jsonReader.GetGuid();
            else if (jsonReader.ValueTextEquals("providerNpi"u8) && jsonReader.Read())
                npi = jsonReader.GetString() ?? string.Empty;
            else if (jsonReader.ValueTextEquals("amount"u8) && jsonReader.Read())
                amount = jsonReader.GetDecimal();
        }

        return claimId == Guid.Empty ? null : new ClaimPaymentCommand(claimId, payerId, npi, amount, traceId);
    }
}
