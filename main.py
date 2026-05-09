"""ProxyMaze'26 — black-box HTTP API for proxy pool monitoring."""
from __future__ import annotations

import asyncio
import json
import os
import secrets
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import urlparse

import httpx
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse


# ---------------- helpers ----------------

def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def now_unix() -> int:
    return int(time.time())


def proxy_id_from_url(url: str) -> str:
    path = urlparse(url).path or ""
    seg = path.rstrip("/").split("/")[-1]
    return seg or url


def short_id(prefix: str) -> str:
    return f"{prefix}-{secrets.token_hex(4)}"


# ---------------- state ----------------

class State:
    def __init__(self) -> None:
        self.lock = asyncio.Lock()

        # config
        self.check_interval_seconds: int = 15
        self.request_timeout_ms: int = 3000

        # proxies: id -> dict
        self.proxies: dict[str, dict[str, Any]] = {}

        # alerts: list (active + resolved). At most one active at a time.
        self.alerts: list[dict[str, Any]] = []
        self.active_alert_id: Optional[str] = None

        # webhooks (generic): id -> {url}
        self.webhooks: dict[str, dict[str, Any]] = {}

        # integrations (slack/discord): id -> {type, webhook_url, username, events}
        self.integrations: dict[str, dict[str, Any]] = {}

        # metrics
        self.total_checks: int = 0
        self.webhook_deliveries: int = 0  # successful deliveries

        # delivery dedup: (target_id, alert_id, event) -> True once delivered
        self.delivered: set[tuple[str, str, str]] = set()

        # delivery queues per target (FIFO, sequential)
        self.delivery_queues: dict[str, asyncio.Queue] = {}
        self.delivery_workers: dict[str, asyncio.Task] = {}


STATE = State()


# ---------------- proxy probing ----------------

async def probe_one(client: httpx.AsyncClient, proxy: dict[str, Any], timeout_s: float) -> bool:
    """Probe a proxy URL. Return True if up (2xx within timeout), False otherwise."""
    url = proxy["url"]
    try:
        resp = await client.get(url, timeout=timeout_s, follow_redirects=True)
        return 200 <= resp.status_code < 300
    except Exception:
        return False


def transition_proxy(proxy: dict[str, Any], up: bool, ts: str) -> None:
    proxy["status"] = "up" if up else "down"
    proxy["last_checked_at"] = ts
    proxy["total_checks"] = proxy.get("total_checks", 0) + 1
    if up:
        proxy["consecutive_failures"] = 0
        proxy["successful_checks"] = proxy.get("successful_checks", 0) + 1
    else:
        proxy["consecutive_failures"] = proxy.get("consecutive_failures", 0) + 1
    h = proxy.setdefault("history", [])
    h.append({"checked_at": ts, "status": proxy["status"]})
    if len(h) > 1000:
        del h[: len(h) - 1000]


async def run_check_round() -> None:
    """One check round: probe every proxy in parallel, then evaluate alerts."""
    async with STATE.lock:
        snapshot = list(STATE.proxies.values())
        timeout_s = max(0.05, STATE.request_timeout_ms / 1000.0)

    if not snapshot:
        # still evaluate alerts (no breach when pool is empty -> resolve any active)
        await evaluate_alerts()
        return

    limits = httpx.Limits(max_connections=200, max_keepalive_connections=50)
    async with httpx.AsyncClient(limits=limits, verify=False) as client:
        results = await asyncio.gather(
            *[probe_one(client, p, timeout_s) for p in snapshot],
            return_exceptions=True,
        )

    ts = now_iso()
    async with STATE.lock:
        for p, ok in zip(snapshot, results):
            up = bool(ok) if not isinstance(ok, Exception) else False
            cur = STATE.proxies.get(p["id"])
            if cur is None:
                continue  # proxy removed mid-round
            transition_proxy(cur, up, ts)
            STATE.total_checks += 1

    await evaluate_alerts()


def compute_pool_stats() -> dict[str, Any]:
    proxies = list(STATE.proxies.values())
    total = len(proxies)
    up = sum(1 for p in proxies if p["status"] == "up")
    down = sum(1 for p in proxies if p["status"] == "down")
    failure_rate = (down / total) if total > 0 else 0.0
    failed_ids = sorted(p["id"] for p in proxies if p["status"] == "down")
    return {
        "total": total,
        "up": up,
        "down": down,
        "failure_rate": failure_rate,
        "failed_ids": failed_ids,
    }


THRESHOLD = 0.20


async def evaluate_alerts() -> None:
    """Check current pool state vs threshold; fire/resolve alerts as needed."""
    async with STATE.lock:
        stats = compute_pool_stats()
        rate = stats["failure_rate"]

        active = None
        if STATE.active_alert_id:
            for a in STATE.alerts:
                if a["alert_id"] == STATE.active_alert_id:
                    active = a
                    break

        events_to_dispatch: list[dict[str, Any]] = []

        if rate >= THRESHOLD and stats["total"] > 0:
            if active is None:
                # fire new alert
                alert_id = short_id("alert")
                fired_at = now_iso()
                alert = {
                    "alert_id": alert_id,
                    "status": "active",
                    "failure_rate": rate,
                    "total_proxies": stats["total"],
                    "failed_proxies": stats["down"],
                    "failed_proxy_ids": stats["failed_ids"],
                    "threshold": THRESHOLD,
                    "fired_at": fired_at,
                    "resolved_at": None,
                    "message": "Proxy pool failure rate exceeded threshold",
                }
                STATE.alerts.append(alert)
                STATE.active_alert_id = alert_id
                events_to_dispatch.append({
                    "event": "alert.fired",
                    "alert": dict(alert),
                })
            else:
                # update active alert stats to current state
                active["failure_rate"] = rate
                active["total_proxies"] = stats["total"]
                active["failed_proxies"] = stats["down"]
                active["failed_proxy_ids"] = stats["failed_ids"]
        else:
            if active is not None:
                # resolve: keep last-active stats frozen (>= 0.20 invariant)
                active["status"] = "resolved"
                active["resolved_at"] = now_iso()
                STATE.active_alert_id = None
                events_to_dispatch.append({
                    "event": "alert.resolved",
                    "alert": dict(active),
                })

    for ev in events_to_dispatch:
        await enqueue_event(ev)


# ---------------- monitoring loop ----------------

_monitor_task: Optional[asyncio.Task] = None


async def monitor_loop() -> None:
    last_run = 0.0
    while True:
        now = time.monotonic()
        interval = max(1, STATE.check_interval_seconds)
        if now - last_run >= interval:
            last_run = now
            try:
                await run_check_round()
            except Exception as e:
                print("[monitor] error:", e)
        try:
            await asyncio.sleep(0.25)
        except asyncio.CancelledError:
            return


# ---------------- webhook delivery ----------------

async def enqueue_event(ev: dict[str, Any]) -> None:
    """Fan-out an alert event to every registered receiver and integration."""
    targets: list[tuple[str, str, str]] = []  # (target_id, kind, url)
    async with STATE.lock:
        for wid, w in STATE.webhooks.items():
            targets.append((f"wh:{wid}", "generic", w["url"]))
        for iid, integ in STATE.integrations.items():
            evs = integ.get("events") or ["alert.fired", "alert.resolved"]
            if ev["event"] not in evs:
                continue
            kind = integ["type"]  # slack | discord
            targets.append((f"int:{iid}", kind, integ["webhook_url"]))

    print(f"[enqueue] event={ev['event']} alert_id={ev['alert']['alert_id']} fanout_targets={len(targets)}", flush=True)
    for target_id, kind, url in targets:
        print(f"[enqueue]   -> target={target_id} kind={kind} url={url}", flush=True)
        async with STATE.lock:
            q = STATE.delivery_queues.get(target_id)
            if q is None:
                q = asyncio.Queue()
                STATE.delivery_queues[target_id] = q
                STATE.delivery_workers[target_id] = asyncio.create_task(
                    delivery_worker(target_id, q)
                )
        await q.put({"event": ev, "kind": kind, "url": url})


def build_generic_payload(ev: dict[str, Any]) -> dict[str, Any]:
    a = ev["alert"]
    if ev["event"] == "alert.fired":
        return {
            "event": "alert.fired",
            "alert_id": a["alert_id"],
            "fired_at": a["fired_at"],
            "failure_rate": a["failure_rate"],
            "total_proxies": a["total_proxies"],
            "failed_proxies": a["failed_proxies"],
            "failed_proxy_ids": a["failed_proxy_ids"],
            "threshold": a["threshold"],
            "message": a["message"],
        }
    return {
        "event": "alert.resolved",
        "alert_id": a["alert_id"],
        "resolved_at": a["resolved_at"],
    }


def build_slack_payload(ev: dict[str, Any], integ: dict[str, Any]) -> dict[str, Any]:
    a = ev["alert"]
    fired = ev["event"] == "alert.fired"
    color = "#D7263D" if fired else "#2ECC71"
    text = (
        f"Proxy pool breach — failure rate {a['failure_rate']:.2%}"
        if fired
        else f"Proxy pool recovered — alert {a['alert_id']} resolved"
    )
    return {
        "username": integ.get("username") or "ProxyMaze",
        "text": text,
        "attachments": [
            {
                "color": color,
                "fields": [
                    {"title": "Alert ID", "value": a["alert_id"], "short": True},
                    {"title": "Failure Rate", "value": f"{a['failure_rate']:.2%}", "short": True},
                    {"title": "Failed Proxies", "value": str(a["failed_proxies"]), "short": True},
                    {"title": "Total Proxies", "value": str(a["total_proxies"]), "short": True},
                    {"title": "Threshold", "value": f"{a['threshold']:.2f}", "short": True},
                    {"title": "Failed IDs", "value": ", ".join(a["failed_proxy_ids"]) or "-", "short": False},
                    {"title": "Fired At", "value": a["fired_at"], "short": True},
                ],
                "footer": "ProxyMaze",
                "ts": now_unix(),
            }
        ],
    }


def build_discord_payload(ev: dict[str, Any], integ: dict[str, Any]) -> dict[str, Any]:
    a = ev["alert"]
    fired = ev["event"] == "alert.fired"
    color_int = 0xD7263D if fired else 0x2ECC71
    title = "Proxy Pool Breach" if fired else "Proxy Pool Recovered"
    description = (
        f"Failure rate reached {a['failure_rate']:.2%} (threshold {a['threshold']:.2f})."
        if fired
        else f"Alert {a['alert_id']} resolved at {a['resolved_at']}."
    )
    payload = {
        "username": integ.get("username") or "ProxyMaze",
        "embeds": [
            {
                "title": title,
                "description": description,
                "color": color_int,
                "fields": [
                    {"name": "Alert ID", "value": a["alert_id"], "inline": True},
                    {"name": "Failure Rate", "value": f"{a['failure_rate']:.2%}", "inline": True},
                    {"name": "Failed Proxies", "value": str(a["failed_proxies"]), "inline": True},
                    {"name": "Total Proxies", "value": str(a["total_proxies"]), "inline": True},
                    {"name": "Threshold", "value": f"{a['threshold']:.2f}", "inline": True},
                    {"name": "Failed IDs", "value": ", ".join(a["failed_proxy_ids"]) or "-", "inline": False},
                ],
                "footer": {"text": "ProxyMaze monitoring"},
            }
        ],
    }
    return payload


async def delivery_worker(target_id: str, q: asyncio.Queue) -> None:
    """Sequentially deliver events to a single target. Retries on 5xx and network errors."""
    print(f"[worker] started target={target_id}", flush=True)
    while True:
        item = await q.get()
        ev = item["event"]
        kind = item["kind"]
        url = item["url"]
        a = ev["alert"]
        dedup_key = (target_id, a["alert_id"], ev["event"])
        if dedup_key in STATE.delivered:
            print(f"[worker] dedup-skip target={target_id} key={dedup_key}", flush=True)
            q.task_done()
            continue

        if kind == "slack":
            iid = target_id.split(":", 1)[1]
            integ = STATE.integrations.get(iid, {})
            payload = build_slack_payload(ev, integ)
        elif kind == "discord":
            iid = target_id.split(":", 1)[1]
            integ = STATE.integrations.get(iid, {})
            payload = build_discord_payload(ev, integ)
        else:
            payload = build_generic_payload(ev)

        print(f"[worker] deliver target={target_id} kind={kind} url={url} event={ev['event']} alert={a['alert_id']}", flush=True)

        # retry loop — preserves POST method across 3xx redirects
        backoff = 0.25
        attempts = 0
        deadline = time.time() + 600
        while time.time() < deadline:
            attempts += 1
            try:
                async with httpx.AsyncClient(timeout=8.0, verify=False, follow_redirects=False) as client:
                    target_url = url
                    headers = {
                        "Content-Type": "application/json",
                        "User-Agent": "ProxyMaze/1.0 (+https://proxymaze.jaindu.me)",
                    }
                    redirects = 0
                    while True:
                        resp = await client.post(target_url, json=payload, headers=headers)
                        if resp.status_code in (301, 302, 303, 307, 308) and "location" in resp.headers and redirects < 5:
                            new_loc = resp.headers["location"]
                            if new_loc.startswith("/"):
                                from urllib.parse import urlparse, urlunparse
                                p = urlparse(target_url)
                                new_loc = urlunparse((p.scheme, p.netloc, new_loc, "", "", ""))
                            print(f"[worker]   redirect {resp.status_code} {target_url} -> {new_loc}", flush=True)
                            target_url = new_loc
                            redirects += 1
                            continue
                        break
                print(f"[worker]   attempt={attempts} status={resp.status_code} target={target_id} final_url={target_url}", flush=True)
                if resp.status_code in (500, 502, 503, 504):
                    await asyncio.sleep(min(backoff, 2.0))
                    backoff = min(backoff * 1.5, 2.0)
                    continue
                if 200 <= resp.status_code < 300:
                    STATE.delivered.add(dedup_key)
                    STATE.webhook_deliveries += 1
                    print(f"[worker]   ok delivered target={target_id} alert={a['alert_id']} event={ev['event']}", flush=True)
                if 400 <= resp.status_code < 500:
                    STATE.delivered.add(dedup_key)
                    print(f"[worker]   4xx-give-up target={target_id} status={resp.status_code}", flush=True)
                break
            except Exception as e:
                print(f"[worker]   attempt={attempts} EXC target={target_id} type={type(e).__name__} msg={e}", flush=True)
                await asyncio.sleep(min(backoff, 2.0))
                backoff = min(backoff * 1.5, 2.0)
                continue
        q.task_done()


# ---------------- FastAPI app ----------------

app = FastAPI(title="ProxyMaze", default_response_class=JSONResponse)


@app.on_event("startup")
async def _on_startup() -> None:
    global _monitor_task
    if _monitor_task is None or _monitor_task.done():
        _monitor_task = asyncio.create_task(monitor_loop())


@app.on_event("shutdown")
async def _on_shutdown() -> None:
    global _monitor_task
    if _monitor_task:
        _monitor_task.cancel()


async def parse_json(request: Request) -> dict[str, Any]:
    try:
        body = await request.body()
        if not body:
            return {}
        data = json.loads(body)
        if not isinstance(data, dict):
            raise HTTPException(status_code=400, detail="JSON body must be an object")
        return data
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Malformed JSON")


# Chapter 01: GET /health
@app.get("/health")
async def get_health():
    return {"status": "ok"}


# Chapter 02: POST /config
@app.post("/config")
async def post_config(request: Request):
    data = await parse_json(request)
    interval = data.get("check_interval_seconds")
    timeout = data.get("request_timeout_ms")
    if not isinstance(interval, (int, float)) or interval <= 0:
        raise HTTPException(status_code=400, detail="check_interval_seconds must be a positive number")
    if not isinstance(timeout, (int, float)) or timeout <= 0:
        raise HTTPException(status_code=400, detail="request_timeout_ms must be a positive number")
    async with STATE.lock:
        STATE.check_interval_seconds = int(interval)
        STATE.request_timeout_ms = int(timeout)
    return {
        "check_interval_seconds": STATE.check_interval_seconds,
        "request_timeout_ms": STATE.request_timeout_ms,
    }


# Chapter 03: GET /config
@app.get("/config")
async def get_config():
    return {
        "check_interval_seconds": STATE.check_interval_seconds,
        "request_timeout_ms": STATE.request_timeout_ms,
    }


# Chapter 04: POST /proxies
@app.post("/proxies", status_code=201)
async def post_proxies(request: Request):
    data = await parse_json(request)
    proxies = data.get("proxies")
    replace = bool(data.get("replace", False))
    if not isinstance(proxies, list) or not all(isinstance(u, str) for u in proxies):
        raise HTTPException(status_code=400, detail="proxies must be an array of strings")

    async with STATE.lock:
        if replace:
            STATE.proxies.clear()
        accepted = 0
        result = []
        seen = set()
        for url in proxies:
            pid = proxy_id_from_url(url)
            if not pid:
                continue
            accepted += 1
            if pid in seen:
                continue
            seen.add(pid)
            if pid not in STATE.proxies:
                STATE.proxies[pid] = {
                    "id": pid,
                    "url": url,
                    "status": "pending",
                    "last_checked_at": None,
                    "consecutive_failures": 0,
                    "total_checks": 0,
                    "successful_checks": 0,
                    "history": [],
                }
            else:
                STATE.proxies[pid]["url"] = url
            result.append({
                "id": pid,
                "url": STATE.proxies[pid]["url"],
                "status": STATE.proxies[pid]["status"],
            })
    return {"accepted": accepted, "proxies": result}


# Chapter 05: GET /proxies
@app.get("/proxies")
async def get_proxies():
    async with STATE.lock:
        proxies = list(STATE.proxies.values())
        stats = compute_pool_stats()
        out = []
        for p in proxies:
            out.append({
                "id": p["id"],
                "url": p["url"],
                "status": p["status"],
                "last_checked_at": p["last_checked_at"],
                "consecutive_failures": p["consecutive_failures"],
            })
    return {
        "total": stats["total"],
        "up": stats["up"],
        "down": stats["down"],
        "failure_rate": stats["failure_rate"],
        "proxies": out,
    }


# Chapter 06: GET /proxies/{id}
@app.get("/proxies/{pid}")
async def get_proxy(pid: str):
    async with STATE.lock:
        p = STATE.proxies.get(pid)
        if p is None:
            raise HTTPException(status_code=404, detail="Not Found")
        total = p.get("total_checks", 0)
        success = p.get("successful_checks", 0)
        uptime = (success / total * 100.0) if total > 0 else 0.0
        return {
            "id": p["id"],
            "url": p["url"],
            "status": p["status"],
            "last_checked_at": p["last_checked_at"],
            "consecutive_failures": p["consecutive_failures"],
            "total_checks": total,
            "uptime_percentage": round(uptime, 1),
            "history": list(p.get("history", [])),
        }


# Chapter 07: GET /proxies/{id}/history
@app.get("/proxies/{pid}/history")
async def get_proxy_history(pid: str):
    async with STATE.lock:
        p = STATE.proxies.get(pid)
        if p is None:
            raise HTTPException(status_code=404, detail="Not Found")
        return list(p.get("history", []))


# Chapter 08: DELETE /proxies
@app.delete("/proxies", status_code=204)
async def delete_proxies():
    async with STATE.lock:
        STATE.proxies.clear()
        # active alert may auto-resolve on next eval (empty pool -> rate 0)
    # trigger evaluation to clear active alert if pool now empty
    await evaluate_alerts()
    return Response(status_code=204)


# Chapter 09: GET /alerts
@app.get("/alerts")
async def get_alerts():
    async with STATE.lock:
        # For active alert, refresh stats to current state to satisfy consistency rule
        if STATE.active_alert_id:
            stats = compute_pool_stats()
            for a in STATE.alerts:
                if a["alert_id"] == STATE.active_alert_id and a["status"] == "active":
                    a["failure_rate"] = stats["failure_rate"]
                    a["total_proxies"] = stats["total"]
                    a["failed_proxies"] = stats["down"]
                    a["failed_proxy_ids"] = stats["failed_ids"]
                    break
        return [dict(a) for a in STATE.alerts]


# Chapter 10: POST /webhooks
@app.post("/webhooks", status_code=201)
async def post_webhooks(request: Request):
    data = await parse_json(request)
    url = data.get("url")
    if not isinstance(url, str) or not url:
        raise HTTPException(status_code=400, detail="url is required")
    async with STATE.lock:
        wid = short_id("wh")
        STATE.webhooks[wid] = {"url": url}
    print(f"[webhook-register] id={wid} url={url}", flush=True)
    return {"webhook_id": wid, "url": url}


# Chapter 11: POST /integrations
@app.post("/integrations")
async def post_integrations(request: Request):
    data = await parse_json(request)
    typ = data.get("type")
    webhook_url = data.get("webhook_url")
    username = data.get("username")
    events = data.get("events")
    if typ not in ("slack", "discord"):
        raise HTTPException(status_code=400, detail="type must be slack or discord")
    if not isinstance(webhook_url, str) or not webhook_url:
        raise HTTPException(status_code=400, detail="webhook_url is required")
    if events is None:
        events = ["alert.fired", "alert.resolved"]
    if not isinstance(events, list):
        raise HTTPException(status_code=400, detail="events must be an array")
    async with STATE.lock:
        iid = short_id("int")
        STATE.integrations[iid] = {
            "type": typ,
            "webhook_url": webhook_url,
            "username": username or "ProxyMaze",
            "events": events,
        }
    print(f"[integration-register] id={iid} type={typ} url={webhook_url} events={events}", flush=True)
    return JSONResponse(
        status_code=201,
        content={
            "integration_id": iid,
            "type": typ,
            "webhook_url": webhook_url,
            "username": STATE.integrations[iid]["username"],
            "events": events,
        },
    )


# Chapter 12: GET /metrics
@app.get("/metrics")
async def get_metrics():
    async with STATE.lock:
        active = 1 if STATE.active_alert_id else 0
        total_alerts = len(STATE.alerts)
        return {
            "total_checks": STATE.total_checks,
            "current_pool_size": len(STATE.proxies),
            "active_alerts": active,
            "total_alerts": total_alerts,
            "webhook_deliveries": STATE.webhook_deliveries,
        }


# Root
@app.get("/")
async def root():
    return {"service": "ProxyMaze", "status": "ok"}


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, log_level="info")
