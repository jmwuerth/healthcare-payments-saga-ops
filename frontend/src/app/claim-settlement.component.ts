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
      <button (click)="submitClicks$.next()" [disabled]="submitting()">
        {{ submitting() ? 'Executing Saga...' : 'Settle Provider Claim ($12,500)' }}
      </button>
      @if (statusBanner()) {
        <p class="status">{{ statusBanner() }}</p>
      }
    </div>
  `
})
export class ClaimSettlementComponent {
  private readonly http = inject(HttpClient);
  private readonly destroyRef = inject(DestroyRef);

  // Client-generated Idempotency Key prevents duplicate payments across network retries
  private claimId = crypto.randomUUID();
  readonly submitting = signal(false);
  readonly statusBanner = signal<string | null>(null);
  readonly submitClicks$ = new Subject<void>();

  constructor() {
    this.submitClicks$.pipe(
      // exhaustMap drops duplicate button clicks while a payment Saga is in-flight
      exhaustMap(() => {
        this.submitting.set(true);
        return this.http.post<SagaResultDto>('/api/claims/settle', {
          claimId: this.claimId,
          payerId: '11111111-1111-1111-1111-111111111111',
          providerNpi: '1234567890',
          amount: 12500.00,
          traceId: crypto.randomUUID()
        }).pipe(
          tap(res => {
            this.statusBanner.set(`Settled! SagaId=${res.sagaId} (Trace: ${res.traceId})`);
            this.claimId = crypto.randomUUID(); // Rotate idempotency key for next claim
            this.submitting.set(false);
          }),
          catchError((err: HttpErrorResponse) => {
            this.statusBanner.set(`Saga Compensated (409): ${err.error?.failureReason ?? 'Conflict'}`);
            this.submitting.set(false);
            return EMPTY;
          })
        );
      }),
      takeUntilDestroyed(this.destroyRef)
    ).subscribe();
  }
}
