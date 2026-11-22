// exhaustMap Double-Click Guard + Async Polling
import { Component, DestroyRef, inject, signal } from '@angular/core';
import { HttpClient, HttpErrorResponse } from '@angular/common/http';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { Subject, catchError, EMPTY, exhaustMap, tap } from 'rxjs';

export interface SagaResultDto {
  sagaId: string;
  claimId: string;
  amount: number;
  state: number;
  traceId: string;
  failureReason?: string;
}

@Component({
  selector: 'app-claim-settlement',
  standalone: true,
  template: `
    <div class="settlement-card">
      <h3>Healthcare Provider Claim Settlement</h3>
      <p class="idempotency-badge" title="Client-generated Idempotency Key (ClaimId)">
        Idempotency Key: <code>{{ claimId().slice(0, 8) }}</code>
      </p>
      <button class="primary-action" (click)="submitClicks$.next()" [disabled]="submitting()">
        {{ submitting() ? 'Executing Saga...' : 'Settle Provider Claim ($12,500)' }}
      </button>
      @if (statusBanner()) {
        <p class="status"
           [class.status-success]="statusTone() === 'success'"
           [class.status-error]="statusTone() === 'error'">
          {{ statusBanner() }}
        </p>
      }
    </div>
  `,
  styles: [`
    .settlement-card {
      max-width: 28rem;
      padding: 1.25rem;
      border: 1px solid #d0d7de;
      border-radius: 8px;
      background: #ffffff;
      font-family: system-ui, -apple-system, "Segoe UI", sans-serif;
    }
    .settlement-card h3 {
      margin: 0 0 0.75rem;
      font-size: 1.05rem;
      color: #1f2328;
    }
    .idempotency-badge {
      display: inline-block;
      margin: 0 0 1rem;
      padding: 0.25rem 0.6rem;
      border: 1px solid #d0d7de;
      border-radius: 999px;
      background: #f6f8fa;
      font-size: 0.8rem;
      color: #57606a;
    }
    .idempotency-badge code {
      font-family: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, monospace;
      font-size: 0.8rem;
      color: #1f2328;
    }
    .primary-action {
      width: 100%;
      padding: 0.65rem 1rem;
      border: 0;
      border-radius: 6px;
      background: #1f6feb;
      color: #ffffff;
      font-size: 0.95rem;
      font-weight: 600;
      cursor: pointer;
      transition: background-color 0.15s ease;
    }
    .primary-action:hover:not(:disabled) { background: #1a5fd0; }
    .primary-action:disabled { background: #8c959f; cursor: progress; }
    .status {
      margin: 1rem 0 0;
      padding: 0.65rem 0.8rem;
      border: 1px solid;
      border-radius: 6px;
      font-size: 0.85rem;
    }
    .status-success { border-color: #1a7f37; background: #dafbe1; color: #1a7f37; }
    .status-error { border-color: #cf222e; background: #ffebe9; color: #cf222e; }
  `]
})
export class ClaimSettlementComponent {
  private readonly http = inject(HttpClient);
  private readonly destroyRef = inject(DestroyRef);

  // Client-generated Idempotency Key prevents duplicate payments across network retries
  readonly claimId = signal(crypto.randomUUID());
  readonly submitting = signal(false);
  readonly statusBanner = signal<string | null>(null);
  readonly statusTone = signal<'success' | 'error' | null>(null);
  readonly submitClicks$ = new Subject<void>();

  constructor() {
    this.submitClicks$.pipe(
      // exhaustMap drops duplicate button clicks while a payment Saga is in-flight
      exhaustMap(() => {
        this.submitting.set(true);
        return this.http.post<SagaResultDto>('/api/claims/settle', {
          claimId: this.claimId(),
          payerId: '11111111-1111-1111-1111-111111111111',
          providerNpi: '1234567890',
          amount: 12500.00,
          traceId: crypto.randomUUID()
        }).pipe(
          tap(res => {
            this.statusBanner.set(`Settled! SagaId=${res.sagaId} (Trace: ${res.traceId})`);
            this.statusTone.set('success');
            this.claimId.set(crypto.randomUUID()); // Rotate idempotency key only on 200 OK
            this.submitting.set(false);
          }),
          catchError((err: HttpErrorResponse) => {
            this.statusBanner.set(
              err.status === 409
                ? `Saga Compensated (409): ${err.error?.failureReason ?? 'Conflict'}`
                : `Settlement Failed (${err.status}): ${err.error?.failureReason ?? err.message}`
            );
            this.statusTone.set('error'); // Key is NOT rotated: retry stays idempotent
            this.submitting.set(false);
            return EMPTY;
          })
        );
      }),
      takeUntilDestroyed(this.destroyRef)
    ).subscribe();
  }
}
