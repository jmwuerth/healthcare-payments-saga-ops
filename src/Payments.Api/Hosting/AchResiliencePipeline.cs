using Microsoft.Extensions.Http.Resilience;
using Polly;

namespace Payments.Api.Hosting;

/// <summary>
/// Single source of truth for the ACH egress resilience pipeline, shared by the API
/// registration and the resilience tests so both exercise identical settings.
/// </summary>
public static class AchResiliencePipeline
{
    public static void Configure(ResiliencePipelineBuilder<HttpResponseMessage> builder)
    {
        // Retry first (outermost): decorrelated jitter avoids synchronized retry storms.
        builder.AddRetry(new HttpRetryStrategyOptions
        {
            MaxRetryAttempts = 3,
            BackoffType = DelayBackoffType.Exponential,
            UseJitter = true,
            Delay = TimeSpan.FromMilliseconds(200),
            ShouldHandle = new PredicateBuilder<HttpResponseMessage>()
                .HandleResult(r => (int)r.StatusCode >= 500)
                .Handle<HttpRequestException>()
                .Handle<TaskCanceledException>()
        });

        // Circuit breaker inside the retry loop so each attempt is sampled.
        // MinimumThroughput is lowered from the 100-request HTTP default so a short
        // outage of a low-volume ACH endpoint still trips the breaker.
        builder.AddCircuitBreaker(new HttpCircuitBreakerStrategyOptions
        {
            FailureRatio = 0.5,
            MinimumThroughput = 4,
            SamplingDuration = TimeSpan.FromSeconds(10),
            BreakDuration = TimeSpan.FromSeconds(5)
        });

        // Innermost: bounds every individual attempt, including hung sockets.
        builder.AddTimeout(TimeSpan.FromSeconds(2));
    }
}
