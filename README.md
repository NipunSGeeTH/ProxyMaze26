# ProxyMaze'26 — Torch Labs Engineering Challenge

Real-time proxy monitoring HTTP API built with FastAPI + asyncio + httpx.

---

## Quick Start

```bash
chmod +x setup.sh run.sh
./setup.sh          # creates venv and installs deps
./run.sh            # starts server on :8000
```

Or manually:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

---

## Architecture

Everything lives in `main.py` — a single FastAPI application.

| Component | Description |
|-----------|-------------|
| `_proxies` | In-memory dict of proxy records (id → record) |
| `_alerts` | Append-only list of ALL alerts (active + resolved) |
| `_active_alert` | Reference to the single currently-active alert, or `None` |
| `_webhooks` | Registered raw JSON webhook endpoints |
| `_integrations` | Registered Slack / Discord integrations |
| `_monitoring_loop()` | Background asyncio task — probes all proxies every `check_interval_seconds` |
| `_run_check_cycle()` | Probes proxies concurrently, updates state, fires/resolves alerts |
| `_do_deliver()` | Posts to a webhook URL, retries on 500/502/503/504 |

---

## Endpoints

### Chapter 01 — Health
```
GET /health
→ 200  {"status": "ok"}
```

### Chapter 02 — Set Config
```
POST /config
Body: {"check_interval_seconds": 15, "request_timeout_ms": 3000}
→ 200  (echoes active config)
```

### Chapter 03 — Get Config
```
GET /config
→ 200  {"check_interval_seconds": 15, "request_timeout_ms": 3000}
```

### Chapter 04 — Add Proxies
```
POST /proxies
Body: {"proxies": ["https://..."], "replace": true}
→ 201  {"accepted": 2, "proxies": [...]}
```
- `replace: true` clears the pool first
- New proxies start as `pending`; they transition on the next probe cycle

### Chapter 05 — Pool Overview
```
GET /proxies
→ 200  {"total":10,"up":7,"down":3,"failure_rate":0.3,"proxies":[...]}
```

### Chapter 06 — Single Proxy
```
GET /proxies/{id}
→ 200  {id, url, status, last_checked_at, consecutive_failures,
         total_checks, uptime_percentage, history}
→ 404  if unknown
```

### Chapter 07 — Proxy History
```
GET /proxies/{id}/history
→ 200  [{"checked_at": "...", "status": "up"}, ...]
→ 404  if unknown
```

### Chapter 08 — Clear Pool
```
DELETE /proxies
→ 204  (pool cleared; alerts preserved)
```

### Chapter 09 — All Alerts
```
GET /alerts
→ 200  [{alert_id, status, failure_rate, total_proxies, failed_proxies,
          failed_proxy_ids, threshold, fired_at, resolved_at, message}]
```

### Chapter 10 — Register Webhook
```
POST /webhooks
Body: {"url": "https://receiver.example/hook"}
→ 201  {"webhook_id": "wh-...", "url": "..."}
```

### Chapter 11 — Register Integration
```
POST /integrations
Body: {"type":"slack","webhook_url":"...","username":"ProxyWatch","events":["alert.fired","alert.resolved"]}
→ 201  {"integration_id":"...","type":"slack","webhook_url":"..."}
```

### Chapter 12 — Metrics
```
GET /metrics
→ 200  {total_checks, current_pool_size, active_alerts, total_alerts, webhook_deliveries}
```

---

## Behavioral Rules

| Rule | Implementation |
|------|----------------|
| Continuous background monitoring | `_monitoring_loop()` asyncio task started in lifespan |
| Real HTTP probes only | `probe_proxy()` uses `httpx.AsyncClient` GET |
| 2xx within timeout → up | Checked in `probe_proxy()` |
| timeout / refusal / 5xx → down | Exception catch + status code check |
| Threshold = 0.20 | `THRESHOLD = 0.20` constant |
| At most one active alert | `_active_alert` guard in `_run_check_cycle()` |
| New `alert_id` after resolution | `uuid.uuid4().hex[:8]` minted per breach |
| Webhook retry on 5xx | Exponential back-off in `_do_deliver()` |
| Exactly one delivery per transition | One task per webhook per event; stops on first success |
| Unknown JSON fields accepted | `request.json()` — extra fields are simply ignored |
| Config changes take effect within 0.5 s | Loop polls interval in 0.5 s slices |
| Alert history survives `DELETE /proxies` | `_alerts` is never touched by the delete handler |

---

## Scoring Summary

| Category | Points |
|----------|--------|
| Service bootstrap and configuration | 10 |
| Proxy pool ingestion and background monitoring | 45 |
| Single failure behavior | 30 |
| Threshold breach alerts and webhook delivery | 90 |
| Alert resolution | 20 |
| Re-breach lifecycle integrity | 30 |
| Pool operations and observability | 25 |
| **Core Total** | **250** |
| Slack integration (bonus) | +10 |
| Discord integration (bonus) | +10 |
| **Maximum** | **270** |
| **Passing Score** | **186** |

---

## Proxy ID Rule

```
https://proxy-provider.example/proxy/px-101
                                        ↓
                                      px-101
```

The last path segment of the URL is the proxy id — deterministic and consistent across
all endpoints and webhook payloads.

---

## Alert Lifecycle

```
Normal ──(rate ≥ 0.20)──► Active Alert ──(rate < 0.20)──► Resolved ──(rate ≥ 0.20)──► New Alert
```

- At most **one** active alert at any time
- Each breach mints a fresh `alert_id`; the previous resolved alert stays in the archive
- `alert.fired` and `alert.resolved` webhooks fire in-order, guaranteed by sequential check cycles
