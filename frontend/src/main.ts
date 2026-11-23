import { bootstrapApplication } from '@angular/platform-browser';
import { provideHttpClient } from '@angular/common/http';
import { ClaimSettlementComponent } from './app/claim-settlement.component';

bootstrapApplication(ClaimSettlementComponent, {
  providers: [provideHttpClient()]
}).catch((err) => console.error(err));
