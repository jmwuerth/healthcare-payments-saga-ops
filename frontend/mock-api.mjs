import { createServer } from 'node:http';
import { randomUUID } from 'node:crypto';

const PORT = Number(process.env.PORT ?? 3000);
const DELAY_MS = 600;
const force409 = process.argv.includes('--409');

const server = createServer((req, res) => {
  const sendJson = (status, payload) => {
    res.writeHead(status, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(payload));
  };

  if (req.method === 'OPTIONS') {
    res.writeHead(204, {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type, X-Mock-Status',
    });
    res.end();
    return;
  }

  if (req.method !== 'POST' || req.url?.split('?')[0] !== '/api/claims/settle') {
    sendJson(404, { failureReason: 'Not found' });
    return;
  }

  let raw = '';
  req.setEncoding('utf8');
  req.on('data', (chunk) => {
    raw += chunk;
  });
  req.on('end', () => {
    let body = {};
    try {
      body = raw ? JSON.parse(raw) : {};
    } catch {
      sendJson(400, { failureReason: 'Invalid JSON body' });
      return;
    }

    const wants409 = force409 || req.headers['x-mock-status'] === '409';

    setTimeout(() => {
      if (wants409) {
        sendJson(409, {
          sagaId: randomUUID(),
          claimId: body.claimId,
          amount: body.amount,
          state: 3,
          traceId: body.traceId,
          failureReason: 'Mock clearinghouse rejected ACH settlement; reservation compensated.',
        });
        return;
      }

      sendJson(200, {
        sagaId: randomUUID(),
        claimId: body.claimId,
        amount: body.amount,
        state: 2,
        traceId: body.traceId,
      });
    }, DELAY_MS);
  });
});

server.listen(PORT, () => {
  console.log(
    `[mock-api] listening on http://localhost:${PORT} (delay ${DELAY_MS}ms${force409 ? ', forcing 409' : ''})`
  );
});
