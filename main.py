"""
ProxyMaze'26 — Torch Labs Engineering Challenge
Real-time proxy monitoring HTTP API.

Endpoints:
  GET  /health
  POST /config
  GET  /config
  POST /proxies
  GET  /proxies
  GET  /proxies/{id}
  GET  /proxies/{id}/history
  DELETE /proxies
  GET  /alerts
  POST /webhooks
  POST /integrations
  GET  /metrics
"""
from __future__ import annotations

import asyncio
import copy
import logging
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse

# ─── Logging Configuration ────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ─── Constants ────────────────────────────────────────────────────────────────
THRESHOLD = 0.20

# ─── Global In-Memory State ───────────────────────────────────────────────────
_config: Dict[str, Any] = {
    "check_interval_seconds": 30,
    "request_timeout_ms": 5000,
}

_proxies: Dict[str, Dict] = {}        # proxy_id -> record
_alerts: List[Dict] = []              # ALL alerts (active + resolved), never deleted
_active_alert: Optional[Dict] = None  # the single currently-active alert (or None)
_webhooks: List[Dict] = []            # registered raw JSON webhooks
_integrations: List[Dict] = []        # registered Slack / Discord integrations
_metrics: Dict[str, int] = {
    "total_checks": 0,
    "webhook_deliveries": 0,
}

_lock = asyncio.Lock()                # single asyncio lock guards all state above


# ─── Helpers ──────────────────────────────────────────────────────────────────

def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def proxy_id_from_url(url: str) -> str:
    """Last path segment of a URL is the proxy id."""
    return url.rstrip("/").split("/")[-1]


def make_proxy(pid: str, url: str) -> Dict:
    return {
        "id": pid,
        "url": url,
        "status": "pending",
        "last_checked_at": None,
        "consecutive_failures": 0,
        "total_checks": 0,
        "history": [],
    }


def proxy_summary(p: Dict) -> Dict:
    """Minimum fields for GET /proxies list entries."""
    return {
        "id": p["id"],
        "url": p["url"],
        "status": p["status"],
        "last_checked_at": p["last_checked_at"],
        "consecutive_failures": p["consecutive_failures"],
    }


def proxy_detail(p: Dict) -> Dict:
    """Full fields for GET /proxies/{id}."""
    total = p["total_checks"]
    up_checks = sum(1 for h in p["history"] if h["status"] == "up")
    uptime_pct = round((up_checks / total) * 100, 1) if total > 0 else 0.0
    return {
        "id": p["id"],
        "url": p["url"],
        "status": p["status"],
        "last_checked_at": p["last_checked_at"],
        "consecutive_failures": p["consecutive_failures"],
        "total_checks": total,
        "uptime_percentage": uptime_pct,
        "history": list(p["history"]),
    }


# ─── HTTP Probing ─────────────────────────────────────────────────────────────

async def probe_proxy(url: str, timeout_ms: int) -> bool:
    """
    Return True (up) if a 2xx response arrives within timeout_ms.
    Return False (down) on timeout, connection error, refusal, or any 5xx.
    """
    timeout_s = timeout_ms / 1000.0
    try:
        async with httpx.AsyncClient(timeout=timeout_s, follow_redirects=True) as client:
            resp = await client.get(url)
            is_up = 200 <= resp.status_code < 300 and not (500 <= resp.status_code < 600)
            logger.info(f"[PROBE] URL: {url} | Status: {resp.status_code} | Result: {'UP' if is_up else 'DOWN'}")
            if 500 <= resp.status_code < 600:
                return False
            return 200 <= resp.status_code < 300
    except Exception as e:
        logger.error(f"[PROBE_ERROR] URL: {url} | Error: {str(e)}")
        return False


# ─── Webhook Delivery (with retry) ───────────────────────────────────────────

async def _do_deliver(url: str, payload: Dict) -> None:
    """
    POST JSON payload to url.
    Retry with exponential back-off on 500 / 502 / 503 / 504.
    Counts one successful delivery in _metrics.
    """
    global _metrics
    headers = {"Content-Type": "application/json"}
    attempt = 0
    while True:
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.post(url, json=payload, headers=headers)
            logger.info(f"[WEBHOOK] Attempt {attempt + 1} | URL: {url} | Status: {resp.status_code} | Event: {payload.get('event', 'unknown')}")
            if resp.status_code in (500, 502, 503, 504):
                delay = min(2 ** attempt, 64)
                logger.warning(f"[WEBHOOK_RETRY] URL: {url} | Status: {resp.status_code} | Retry in {delay}s")
                await asyncio.sleep(delay)
                attempt += 1
                continue
            # Any other status code (including 4xx) → treat as accepted / final
            async with _lock:
                _metrics["webhook_deliveries"] += 1
            logger.info(f"[WEBHOOK_SUCCESS] URL: {url} | Status: {resp.status_code}")
            return
        except Exception as e:
            delay = min(2 ** attempt, 64)
            logger.error(f"[WEBHOOK_ERROR] Attempt {attempt + 1} | URL: {url} | Error: {str(e)} | Retry in {delay}s")
            await asyncio.sleep(delay)
            attempt += 1
            if attempt >= 20:
                logger.error(f"[WEBHOOK_FAILED] URL: {url} | Failed after 20 retries")
                return  # give up after many retries


def _fire(url: str, payload: Dict) -> None:
    """Schedule a delivery task without blocking the caller."""
    asyncio.create_task(_do_deliver(url, payload))


# ─── Alert Notification Dispatch ─────────────────────────────────────────────

async def _notify_fired(alert: Dict) -> None:
    """Send alert.fired to all raw webhooks and formatted integrations."""
    logger.warning(f"[ALERT_FIRED] Alert ID: {alert['alert_id']} | Failure Rate: {alert['failure_rate']:.2%} | Failed Proxies: {alert['failed_proxies']}")
    raw_payload = {
        "event": "alert.fired",
        "alert_id": alert["alert_id"],
        "fired_at": alert["fired_at"],
        "failure_rate": alert["failure_rate"],
        "total_proxies": alert["total_proxies"],
        "failed_proxies": alert["failed_proxies"],
        "failed_proxy_ids": alert["failed_proxy_ids"],
        "threshold": alert["threshold"],
        "message": alert["message"],
    }
    async with _lock:
        whs = list(_webhooks)
        integs = list(_integrations)

    logger.info(f"[NOTIFY_FIRED] Sending to {len(whs)} webhooks + {len(integs)} integrations")
    for wh in whs:
        logger.debug(f"[NOTIFY_FIRED] Dispatching to webhook: {wh['url']}")
        _fire(wh["url"], raw_payload)

    for integ in integs:
        if "alert.fired" not in integ.get("events", []):
            continue
        if integ["type"] == "slack":
            logger.debug(f"[NOTIFY_FIRED] Dispatching to Slack integration: {integ['integration_id']}")
            _fire(integ["webhook_url"], _build_slack_fired(integ, alert))
        elif integ["type"] == "discord":
            logger.debug(f"[NOTIFY_FIRED] Dispatching to Discord integration: {integ['integration_id']}")
            _fire(integ["webhook_url"], _build_discord_fired(alert))


async def _notify_resolved(alert: Dict) -> None:
    """Send alert.resolved to all raw webhooks and formatted integrations."""
    logger.info(f"[ALERT_RESOLVED] Alert ID: {alert['alert_id']} | Resolved At: {alert['resolved_at']}")
    raw_payload = {
        "event": "alert.resolved",
        "alert_id": alert["alert_id"],
        "resolved_at": alert["resolved_at"],
    }
    async with _lock:
        whs = list(_webhooks)
        integs = list(_integrations)

    logger.info(f"[NOTIFY_RESOLVED] Sending to {len(whs)} webhooks + {len(integs)} integrations")
    for wh in whs:
        logger.debug(f"[NOTIFY_RESOLVED] Dispatching to webhook: {wh['url']}")
        _fire(wh["url"], raw_payload)

    for integ in integs:
        if "alert.resolved" not in integ.get("events", []):
            continue
        if integ["type"] == "slack":
            logger.debug(f"[NOTIFY_RESOLVED] Dispatching to Slack integration: {integ['integration_id']}")
            _fire(integ["webhook_url"], _build_slack_resolved(integ, alert))
        elif integ["type"] == "discord":
            logger.debug(f"[NOTIFY_RESOLVED] Dispatching to Discord integration: {integ['integration_id']}")
            _fire(integ["webhook_url"], _build_discord_resolved(alert))


# ─── Slack Payload Builders ───────────────────────────────────────────────────

def _iso_to_unix(iso: str) -> int:
    return int(datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp())


def _build_slack_fired(integ: Dict, alert: Dict) -> Dict:
    username = integ.get("username") or "ProxyWatch"
    failed_ids_str = ", ".join(alert["failed_proxy_ids"]) if alert["failed_proxy_ids"] else "None"
    return {
        "username": username,
        "text": (
            f":rotating_light: *ALERT FIRED* — Proxy pool failure rate "
            f"{alert['failure_rate']:.1%} exceeded threshold {alert['threshold']:.0%}"
        ),
        "attachments": [{
            "color": "#FF3333",
            "fields": [
                {"title": "Alert ID",       "value": alert["alert_id"],                  "short": True},
                {"title": "Failure Rate",   "value": f"{alert['failure_rate']:.2%}",     "short": True},
                {"title": "Failed Proxies", "value": str(alert["failed_proxies"]),       "short": True},
                {"title": "Threshold",      "value": str(alert["threshold"]),            "short": True},
                {"title": "Failed IDs",     "value": failed_ids_str,                    "short": False},
                {"title": "Fired At",       "value": alert["fired_at"],                 "short": True},
            ],
            "footer": "ProxyMaze • Torch Labs",
            "ts": _iso_to_unix(alert["fired_at"]),
        }],
    }


def _build_slack_resolved(integ: Dict, alert: Dict) -> Dict:
    username = integ.get("username") or "ProxyWatch"
    failed_ids_str = ", ".join(alert["failed_proxy_ids"]) if alert["failed_proxy_ids"] else "None"
    ts_str = alert.get("resolved_at") or alert["fired_at"]
    return {
        "username": username,
        "text": (
            f":white_check_mark: *ALERT RESOLVED* — Pool failure rate "
            f"recovered below threshold. Alert: {alert['alert_id']}"
        ),
        "attachments": [{
            "color": "#33BB55",
            "fields": [
                {"title": "Alert ID",       "value": alert["alert_id"],                  "short": True},
                {"title": "Failure Rate",   "value": f"{alert['failure_rate']:.2%}",     "short": True},
                {"title": "Failed Proxies", "value": str(alert["failed_proxies"]),       "short": True},
                {"title": "Threshold",      "value": str(alert["threshold"]),            "short": True},
                {"title": "Failed IDs",     "value": failed_ids_str,                    "short": False},
                {"title": "Fired At",       "value": alert["fired_at"],                 "short": True},
            ],
            "footer": "ProxyMaze • Torch Labs",
            "ts": _iso_to_unix(ts_str),
        }],
    }


# ─── Discord Payload Builders ─────────────────────────────────────────────────

def _build_discord_fired(alert: Dict) -> Dict:
    failed_ids_str = ", ".join(alert["failed_proxy_ids"]) if alert["failed_proxy_ids"] else "None"
    return {
        "embeds": [{
            "title": "🚨 Proxy Alert Fired",
            "description": (
                f"Pool failure rate **{alert['failure_rate']:.1%}** exceeded "
                f"threshold **{alert['threshold']:.0%}**"
            ),
            "color": 16711680,   # #FF0000 — red
            "fields": [
                {"name": "Alert ID",       "value": alert["alert_id"],              "inline": True},
                {"name": "Failure Rate",   "value": f"{alert['failure_rate']:.2%}", "inline": True},
                {"name": "Failed Proxies", "value": str(alert["failed_proxies"]),   "inline": True},
                {"name": "Threshold",      "value": str(alert["threshold"]),        "inline": True},
                {"name": "Failed IDs",     "value": failed_ids_str,                "inline": False},
            ],
            "footer": {"text": "ProxyMaze • Torch Labs"},
        }]
    }


def _build_discord_resolved(alert: Dict) -> Dict:
    failed_ids_str = ", ".join(alert["failed_proxy_ids"]) if alert["failed_proxy_ids"] else "None"
    return {
        "embeds": [{
            "title": "✅ Proxy Alert Resolved",
            "description": "Pool failure rate recovered below threshold.",
            "color": 3394611,    # #33BB33 — green
            "fields": [
                {"name": "Alert ID",       "value": alert["alert_id"],              "inline": True},
                {"name": "Failure Rate",   "value": f"{alert['failure_rate']:.2%}", "inline": True},
                {"name": "Failed Proxies", "value": str(alert["failed_proxies"]),   "inline": True},
                {"name": "Threshold",      "value": str(alert["threshold"]),        "inline": True},
                {"name": "Failed IDs",     "value": failed_ids_str,                "inline": False},
            ],
            "footer": {"text": "ProxyMaze • Torch Labs"},
        }]
    }


# ─── Monitoring Loop ──────────────────────────────────────────────────────────

async def _run_check_cycle() -> None:
    """
    Probe every proxy in the pool concurrently.
    After all probes complete, evaluate alert state and fire / resolve as needed.
    """
    global _active_alert
    logger.info(f"[CHECK_CYCLE_START] Starting new check cycle")

    # Snapshot URLs and timeout while holding the lock (no I/O inside lock).
    async with _lock:
        if not _proxies:
            logger.debug("[CHECK_CYCLE] No proxies to check")
            return
        proxy_items = [(pid, p["url"]) for pid, p in _proxies.items()]
        timeout_ms = _config["request_timeout_ms"]
    
    logger.info(f"[CHECK_CYCLE] Checking {len(proxy_items)} proxies with timeout {timeout_ms}ms")

    # ── Probe all proxies concurrently (outside the lock) ──────────────────
    probe_tasks = {
        pid: asyncio.create_task(probe_proxy(url, timeout_ms))
        for pid, url in proxy_items
    }
    raw = await asyncio.gather(*probe_tasks.values(), return_exceptions=True)
    results: Dict[str, bool] = {
        pid: (r is True)   # exceptions → False (down)
        for pid, r in zip(probe_tasks.keys(), raw)
    }

    checked_at = utc_now()
    alert_to_fire: Optional[Dict] = None
    alert_to_resolve: Optional[Dict] = None

    # ── Update proxy records and evaluate alert state ──────────────────────
    async with _lock:
        for pid, is_up in results.items():
            if pid not in _proxies:
                continue                      # proxy was removed mid-cycle
            p = _proxies[pid]
            p["status"] = "up" if is_up else "down"
            p["last_checked_at"] = checked_at
            p["total_checks"] += 1
            if is_up:
                p["consecutive_failures"] = 0
            else:
                p["consecutive_failures"] += 1
            p["history"].append({"checked_at": checked_at, "status": p["status"]})
            _metrics["total_checks"] += 1
            logger.debug(f"[CHECK_CYCLE] Proxy {pid}: {p['status'].upper()} | Consecutive Failures: {p['consecutive_failures']}")

        total = len(_proxies)
        if total == 0:
            return

        down_ids = [pid for pid, p in _proxies.items() if p["status"] == "down"]
        down_count = len(down_ids)
        failure_rate = round(down_count / total, 6)

        if failure_rate >= THRESHOLD and _active_alert is None:
            # ── Fire new alert ────────────────────────────────────────────
            new_alert: Dict = {
                "alert_id": f"alert-{uuid.uuid4().hex[:8]}",
                "status": "active",
                "failure_rate": failure_rate,
                "total_proxies": total,
                "failed_proxies": down_count,
                "failed_proxy_ids": list(down_ids),
                "threshold": THRESHOLD,
                "fired_at": checked_at,
                "resolved_at": None,
                "message": "Proxy pool failure rate exceeded threshold",
            }
            _alerts.append(new_alert)
            _active_alert = new_alert
            alert_to_fire = copy.deepcopy(new_alert)
            logger.warning(f"[ALERT_STATE] FIRING NEW ALERT: {new_alert['alert_id']} | Failure Rate: {failure_rate:.2%}")

        elif failure_rate < THRESHOLD and _active_alert is not None:
            # ── Resolve existing alert ────────────────────────────────────
            _active_alert["status"] = "resolved"
            _active_alert["resolved_at"] = checked_at
            alert_to_resolve = copy.deepcopy(_active_alert)
            logger.info(f"[ALERT_STATE] RESOLVING ALERT: {_active_alert['alert_id']} | Failure Rate: {failure_rate:.2%}")
            _active_alert = None

    # ── Dispatch webhooks outside the lock ────────────────────────────────
    if alert_to_fire:
        logger.info(f"[CHECK_CYCLE_END] CHECK COMPLETED | Total: {total} | Down: {down_count} | Failure Rate: {failure_rate:.2%} | ALERT FIRING QUEUED")
        asyncio.create_task(_notify_fired(alert_to_fire))
    elif alert_to_resolve:
        logger.info(f"[CHECK_CYCLE_END] CHECK COMPLETED | Total: {total} | Down: {down_count} | Failure Rate: {failure_rate:.2%} | ALERT RESOLVING QUEUED")
        asyncio.create_task(_notify_resolved(alert_to_resolve))
    else:
        logger.info(f"[CHECK_CYCLE_END] CHECK COMPLETED | Total: {total} | Down: {down_count} | Failure Rate: {failure_rate:.2%} | No alert state change")


async def _monitoring_loop() -> None:
    """
    Continuously check proxies at the configured cadence.
    Config changes take effect within 0.5 s (the polling slice size).
    """
    while True:
        interval = _config["check_interval_seconds"]
        elapsed = 0.0
        while elapsed < interval:
            chunk = min(0.5, interval - elapsed)
            await asyncio.sleep(chunk)
            elapsed += chunk
            new_interval = _config["check_interval_seconds"]
            if new_interval < interval:
                # Shorter interval requested → restart countdown immediately
                interval = new_interval
                elapsed = 0.0
        await _run_check_cycle()


# ─── App Lifespan ─────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(_app: FastAPI):
    task = asyncio.create_task(_monitoring_loop())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


app = FastAPI(title="ProxyMaze '26", version="1.0.0", lifespan=lifespan)


# ══════════════════════════════════════════════════════════════════════════════
# CHAPTER 01 — GET /health
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/health")
async def health():
    logger.info("[REQUEST] GET /health")
    response = {"status": "ok"}
    logger.info(f"[RESPONSE] GET /health | Status: 200 | Body: {response}")
    return response


# ══════════════════════════════════════════════════════════════════════════════
# CHAPTER 02 — POST /config
# ══════════════════════════════════════════════════════════════════════════════
@app.post("/config")
async def set_config(request: Request):
    logger.info(f"[REQUEST] POST /config")
    try:
        body = await request.json()
        logger.info(f"[REQUEST_BODY] POST /config | Body: {body}")
    except Exception as e:
        logger.error(f"[REQUEST_ERROR] POST /config | Error: Invalid JSON - {str(e)}")
        raise HTTPException(status_code=400, detail="Invalid JSON")

    async with _lock:
        if "check_interval_seconds" in body:
            _config["check_interval_seconds"] = int(body["check_interval_seconds"])
            logger.info(f"[CONFIG_UPDATE] check_interval_seconds = {_config['check_interval_seconds']}")
        if "request_timeout_ms" in body:
            _config["request_timeout_ms"] = int(body["request_timeout_ms"])
            logger.info(f"[CONFIG_UPDATE] request_timeout_ms = {_config['request_timeout_ms']}")
        current = dict(_config)

    logger.info(f"[RESPONSE] POST /config | Status: 200 | Body: {current}")
    return JSONResponse(content=current)


# ══════════════════════════════════════════════════════════════════════════════
# CHAPTER 03 — GET /config
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/config")
async def get_config():
    logger.info("[REQUEST] GET /config")
    async with _lock:
        config = dict(_config)
    logger.info(f"[RESPONSE] GET /config | Status: 200 | Body: {config}")
    return config


# ══════════════════════════════════════════════════════════════════════════════
# CHAPTER 04 — POST /proxies
# ══════════════════════════════════════════════════════════════════════════════
@app.post("/proxies", status_code=201)
async def add_proxies(request: Request):
    logger.info("[REQUEST] POST /proxies")
    try:
        body = await request.json()
        logger.info(f"[REQUEST_BODY] POST /proxies | Body: {body}")
    except Exception as e:
        logger.error(f"[REQUEST_ERROR] POST /proxies | Error: Invalid JSON - {str(e)}")
        raise HTTPException(status_code=400, detail="Invalid JSON")

    proxy_urls: List[str] = body.get("proxies", [])
    replace: bool = bool(body.get("replace", False))
    logger.info(f"[POST_PROXIES] Adding {len(proxy_urls)} proxies | Replace: {replace}")

    async with _lock:
        if replace:
            logger.info(f"[POST_PROXIES] Clearing existing proxies (count: {len(_proxies)})")
            _proxies.clear()

        accepted_list = []
        for url in proxy_urls:
            pid = proxy_id_from_url(url)
            if pid not in _proxies:
                _proxies[pid] = make_proxy(pid, url)
                logger.info(f"[POST_PROXIES] Added new proxy: {pid} -> {url}")
            else:
                logger.info(f"[POST_PROXIES] Proxy already exists: {pid}")
            accepted_list.append({
                "id": pid,
                "url": _proxies[pid]["url"],
                "status": _proxies[pid]["status"],
            })

    response = {"accepted": len(accepted_list), "proxies": accepted_list}
    logger.info(f"[RESPONSE] POST /proxies | Status: 201 | Accepted: {len(accepted_list)}")
    return JSONResponse(
        status_code=201,
        content=response,
    )


# ══════════════════════════════════════════════════════════════════════════════
# CHAPTER 05 — GET /proxies
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/proxies")
async def get_proxies():
    logger.info("[REQUEST] GET /proxies")
    async with _lock:
        total = len(_proxies)
        up_count = sum(1 for p in _proxies.values() if p["status"] == "up")
        down_count = sum(1 for p in _proxies.values() if p["status"] == "down")
        failure_rate = round(down_count / total, 6) if total > 0 else 0.0
        proxies_out = [proxy_summary(p) for p in _proxies.values()]

    response = {
        "total": total,
        "up": up_count,
        "down": down_count,
        "failure_rate": failure_rate,
        "proxies": proxies_out,
    }
    logger.info(f"[RESPONSE] GET /proxies | Status: 200 | Total: {total} | Up: {up_count} | Down: {down_count} | Failure Rate: {failure_rate:.2%}")
    return response


# ══════════════════════════════════════════════════════════════════════════════
# CHAPTER 06 — GET /proxies/{id}
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/proxies/{proxy_id}")
async def get_proxy(proxy_id: str):
    logger.info(f"[REQUEST] GET /proxies/{proxy_id}")
    async with _lock:
        if proxy_id not in _proxies:
            logger.warning(f"[RESPONSE] GET /proxies/{proxy_id} | Status: 404 | Proxy not found")
            raise HTTPException(status_code=404, detail="Proxy not found")
        detail = proxy_detail(_proxies[proxy_id])
    logger.info(f"[RESPONSE] GET /proxies/{proxy_id} | Status: 200 | Proxy: {proxy_id}")
    return detail


# ══════════════════════════════════════════════════════════════════════════════
# CHAPTER 07 — GET /proxies/{id}/history
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/proxies/{proxy_id}/history")
async def get_proxy_history(proxy_id: str):
    logger.info(f"[REQUEST] GET /proxies/{proxy_id}/history")
    async with _lock:
        if proxy_id not in _proxies:
            logger.warning(f"[RESPONSE] GET /proxies/{proxy_id}/history | Status: 404 | Proxy not found")
            raise HTTPException(status_code=404, detail="Proxy not found")
        history = list(_proxies[proxy_id]["history"])
    logger.info(f"[RESPONSE] GET /proxies/{proxy_id}/history | Status: 200 | History entries: {len(history)}")
    return history


# ══════════════════════════════════════════════════════════════════════════════
# CHAPTER 08 — DELETE /proxies
# ══════════════════════════════════════════════════════════════════════════════
@app.delete("/proxies")
async def delete_proxies():
    logger.info("[REQUEST] DELETE /proxies")
    async with _lock:
        deleted_count = len(_proxies)
        _proxies.clear()
        # _alerts and _active_alert are intentionally preserved
    logger.info(f"[RESPONSE] DELETE /proxies | Status: 204 | Deleted proxies: {deleted_count}")
    return Response(status_code=204)


# ══════════════════════════════════════════════════════════════════════════════
# CHAPTER 09 — GET /alerts
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/alerts")
async def get_alerts():
    logger.info("[REQUEST] GET /alerts")
    async with _lock:
        alerts = list(_alerts)
    logger.info(f"[RESPONSE] GET /alerts | Status: 200 | Total alerts: {len(alerts)}")
    return alerts


# ══════════════════════════════════════════════════════════════════════════════
# CHAPTER 10 — POST /webhooks
# ══════════════════════════════════════════════════════════════════════════════
@app.post("/webhooks", status_code=201)
async def register_webhook(request: Request):
    logger.info("[REQUEST] POST /webhooks")
    try:
        body = await request.json()
        logger.info(f"[REQUEST_BODY] POST /webhooks | Body: {body}")
    except Exception as e:
        logger.error(f"[REQUEST_ERROR] POST /webhooks | Error: Invalid JSON - {str(e)}")
        raise HTTPException(status_code=400, detail="Invalid JSON")

    url = body.get("url")
    if not url:
        logger.error(f"[REQUEST_ERROR] POST /webhooks | Error: 'url' field is required")
        raise HTTPException(status_code=400, detail="'url' field is required")

    wh_id = f"wh-{uuid.uuid4().hex[:8]}"
    webhook = {"webhook_id": wh_id, "url": url}

    async with _lock:
        _webhooks.append(webhook)

    logger.info(f"[RESPONSE] POST /webhooks | Status: 201 | Webhook ID: {wh_id} | URL: {url}")
    return JSONResponse(status_code=201, content=webhook)


# ══════════════════════════════════════════════════════════════════════════════
# CHAPTER 11 — POST /integrations
# ══════════════════════════════════════════════════════════════════════════════
@app.post("/integrations", status_code=201)
async def register_integration(request: Request):
    logger.info("[REQUEST] POST /integrations")
    try:
        body = await request.json()
        logger.info(f"[REQUEST_BODY] POST /integrations | Body: {body}")
    except Exception as e:
        logger.error(f"[REQUEST_ERROR] POST /integrations | Error: Invalid JSON - {str(e)}")
        raise HTTPException(status_code=400, detail="Invalid JSON")

    integ_type = body.get("type", "")
    if integ_type not in ("slack", "discord"):
        logger.error(f"[REQUEST_ERROR] POST /integrations | Error: 'type' must be 'slack' or 'discord', got '{integ_type}'")
        raise HTTPException(status_code=400, detail="'type' must be 'slack' or 'discord'")

    webhook_url = body.get("webhook_url", "")
    if not webhook_url:
        logger.error(f"[REQUEST_ERROR] POST /integrations | Error: 'webhook_url' is required")
        raise HTTPException(status_code=400, detail="'webhook_url' is required")

    integ_id = f"integ-{uuid.uuid4().hex[:8]}"
    integ = {
        "integration_id": integ_id,
        "type": integ_type,
        "webhook_url": webhook_url,
        "username": body.get("username") or "ProxyWatch",
        "events": body.get("events", ["alert.fired", "alert.resolved"]),
    }

    async with _lock:
        _integrations.append(integ)

    logger.info(f"[RESPONSE] POST /integrations | Status: 201 | Integration ID: {integ_id} | Type: {integ_type}")
    return JSONResponse(
        status_code=201,
        content={"integration_id": integ_id, "type": integ_type, "webhook_url": webhook_url},
    )


# ══════════════════════════════════════════════════════════════════════════════
# CHAPTER 12 — GET /metrics
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/metrics")
async def get_metrics():
    logger.info("[REQUEST] GET /metrics")
    async with _lock:
        metrics = {
            "total_checks": _metrics["total_checks"],
            "current_pool_size": len(_proxies),
            "active_alerts": 1 if _active_alert is not None else 0,
            "total_alerts": len(_alerts),
            "webhook_deliveries": _metrics["webhook_deliveries"],
        }
    logger.info(f"[RESPONSE] GET /metrics | Status: 200 | Total Checks: {metrics['total_checks']} | Pool Size: {metrics['current_pool_size']} | Active Alerts: {metrics['active_alerts']}")
    return metrics


# ══════════════════════════════════════════════════════════════════════════════
# TEST HELPER — GET /mock/{status_code}/{proxy_id}
#
# Returns the given HTTP status code so the test suite can use self-hosted
# URLs that are always reachable (no dependency on httpbin.org or any
# external service).
#
# Examples:
#   GET /mock/200/px-101  →  200 OK      (proxy will be classified "up")
#   GET /mock/500/px-bad  →  500 Error   (proxy will be classified "down")
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/mock/{status_code}/{proxy_id}")
async def mock_probe(status_code: int, proxy_id: str):
    return Response(
        content=f'{{"mock":"{proxy_id}","status":{status_code}}}',
        status_code=status_code,
        media_type="application/json",
    )