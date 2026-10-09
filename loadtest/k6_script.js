import http from 'k6/http';
import { check } from 'k6';
export const options = { vus: 50, duration: '60s',
  thresholds: { http_req_duration: ['p(99)<200'], http_req_failed: ['rate<0.01'] } };
const KEY = __ENV.API_KEY || 'dev-key-123';
export default function () {
  const r = http.post('http://localhost:8000/v1/usage-events',
    JSON.stringify({ event_id: `k6-${__VU}-${__ITER}`, customer_id: 'acme',
      product_id: 'design-suite', metric: 'cloud_credits', quantity: 1,
      timestamp: '2026-10-09T10:00:00Z' }),
    { headers: { 'Content-Type': 'application/json', 'X-API-Key': KEY } });
  check(r, { '202': (x) => x.status === 202 });
}
