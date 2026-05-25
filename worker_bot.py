# -*- coding: utf-8 -*-
"""
╔══════════════════════════════════════════════════════════════════╗
║  WORKER BOT — Price Tracker                                      ║
║  GeckoTerminal + DexScreener | Trailing Sell | FastAPI           ║
║  Deploy on ANY free hosting: Render / Railway / Fly.io / Koyeb  ║
╚══════════════════════════════════════════════════════════════════╝

DEPLOY IN 2 MINUTES:
  Set 3 environment variables:
    MASTER_BOT_URL  = https://your-master-bot.yourdomain.com
    SHARED_SECRET   = you-secret-code
    WORKER_NAME     = worker1   (unique name for each worker)

  Then run:  python worker_bot.py
  It auto-registers with master on startup.
"""

import asyncio
import logging
import os
import time
import statistics
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from contextlib import asynccontextmanager

import aiohttp
from fastapi import FastAPI, Request, HTTPException, Header
from fastapi.responses import JSONResponse
import uvicorn

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ═══════════════════════════════════════════════════════════════════
#  LOGGING
# ═══════════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("worker_bot")

# ═══════════════════════════════════════════════════════════════════
#  ENVIRONMENT
# ═══════════════════════════════════════════════════════════════════
MASTER_BOT_URL  = os.getenv("MASTER_BOT_URL",  "http://localhost:8080")
SHARED_SECRET   = os.getenv("SHARED_SECRET",   "you-secret-code")
WORKER_NAME     = os.getenv("WORKER_NAME",     "worker1")
WORKER_PORT     = int(os.getenv("WORKER_PORT", "8081"))

# ═══════════════════════════════════════════════════════════════════
#  API STATS (in-memory, reported to master on each update)
# ═══════════════════════════════════════════════════════════════════
api_stats = {
    "gecko_terminal": {"success": 0, "fail": 0, "last_error": ""},
    "dexscreener":    {"success": 0, "fail": 0, "last_error": ""},
}
start_time = time.time()

# ═══════════════════════════════════════════════════════════════════
#  PRICE FETCHING
# ═══════════════════════════════════════════════════════════════════
_http_session: Optional[aiohttp.ClientSession] = None
_price_cache:  Dict[str, Tuple[float, float]] = {}  # ca → (price, timestamp)
PRICE_CACHE_TTL = 25  # seconds

async def get_session() -> aiohttp.ClientSession:
    global _http_session
    if _http_session is None or _http_session.closed:
        _http_session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=10),
            headers={"User-Agent": f"WorkerBot/{WORKER_NAME}"}
        )
    return _http_session

async def fetch_price_gecko(ca: str, session: aiohttp.ClientSession) -> Optional[float]:
    """GeckoTerminal — primary price source (IP rate limited, one per worker IP)."""
    endpoints = [
        f"https://api.geckoterminal.com/api/v2/networks/solana/pools/{ca}",
        f"https://api.geckoterminal.com/api/v2/networks/solana/tokens/{ca}",
    ]
    for ep in endpoints:
        for attempt in range(2):
            try:
                async with session.get(ep, timeout=aiohttp.ClientTimeout(total=8)) as r:
                    if r.status == 200:
                        d = await r.json()
                        # Pool endpoint
                        price = None
                        if "data" in d and "attributes" in d.get("data", {}):
                            price = float(d["data"]["attributes"].get("base_token_price_usd") or 0)
                        # Token endpoint → find top pool
                        if not price and "data" in d:
                            pools_url = f"https://api.geckoterminal.com/api/v2/networks/solana/tokens/{ca}/pools?page=1"
                            async with session.get(pools_url) as pr:
                                if pr.status == 200:
                                    pd = await pr.json()
                                    pool_list = pd.get("data", [])
                                    if pool_list:
                                        pool_addr = pool_list[0]["id"].split("_")[-1]
                                        pool_url2 = f"https://api.geckoterminal.com/api/v2/networks/solana/pools/{pool_addr}"
                                        async with session.get(pool_url2) as pp:
                                            if pp.status == 200:
                                                ppd = await pp.json()
                                                price = float(
                                                    ppd.get("data", {}).get("attributes", {})
                                                       .get("base_token_price_usd") or 0)
                        if price and price > 0:
                            api_stats["gecko_terminal"]["success"] += 1
                            return price
                    elif r.status == 429:
                        api_stats["gecko_terminal"]["fail"] += 1
                        api_stats["gecko_terminal"]["last_error"] = "Rate limited (429)"
                        await asyncio.sleep(5)
                    else:
                        api_stats["gecko_terminal"]["fail"] += 1
                        api_stats["gecko_terminal"]["last_error"] = f"HTTP {r.status}"
            except asyncio.TimeoutError:
                api_stats["gecko_terminal"]["fail"] += 1
                api_stats["gecko_terminal"]["last_error"] = "Timeout"
                if attempt == 0:
                    await asyncio.sleep(2)
            except Exception as e:
                api_stats["gecko_terminal"]["fail"] += 1
                api_stats["gecko_terminal"]["last_error"] = str(e)[:80]
                if attempt == 0:
                    await asyncio.sleep(2)
    return None

async def fetch_price_dex(ca: str, session: aiohttp.ClientSession) -> Optional[float]:
    """DexScreener — fallback price source."""
    endpoints = [
        f"https://api.dexscreener.com/latest/dex/tokens/{ca}",
        f"https://api.dexscreener.com/latest/dex/pairs/solana/{ca}",
    ]
    for ep in endpoints:
        for attempt in range(2):
            try:
                async with session.get(ep, timeout=aiohttp.ClientTimeout(total=8)) as r:
                    if r.status == 200:
                        d = await r.json()
                        pairs = d.get("pairs") or ([d.get("pair")] if d.get("pair") else [])
                        for pair in (pairs or []):
                            if pair and "priceUsd" in pair and pair["priceUsd"]:
                                price = float(pair["priceUsd"])
                                if price > 0:
                                    api_stats["dexscreener"]["success"] += 1
                                    return price
                    elif r.status == 429:
                        api_stats["dexscreener"]["fail"] += 1
                        api_stats["dexscreener"]["last_error"] = "Rate limited (429)"
                        await asyncio.sleep(5)
            except asyncio.TimeoutError:
                api_stats["dexscreener"]["fail"] += 1
                api_stats["dexscreener"]["last_error"] = "Timeout"
                if attempt == 0:
                    await asyncio.sleep(2)
            except Exception as e:
                api_stats["dexscreener"]["fail"] += 1
                api_stats["dexscreener"]["last_error"] = str(e)[:80]
    return None

async def get_price(ca: str) -> Optional[float]:
    """Get price with cache. GeckoTerminal first, DexScreener fallback."""
    # Check cache
    if ca in _price_cache:
        cached_price, cached_ts = _price_cache[ca]
        if time.time() - cached_ts < PRICE_CACHE_TTL:
            return cached_price

    session = await get_session()
    price   = await fetch_price_gecko(ca, session)
    if not price:
        price = await fetch_price_dex(ca, session)
    if price and price > 0:
        _price_cache[ca] = (price, time.time())
        # Report API stats to master
        asyncio.create_task(report_api_stats())
    return price

# ═══════════════════════════════════════════════════════════════════
#  TRAILING SELL CALCULATIONS
# ═══════════════════════════════════════════════════════════════════

def calc_pct(entry: float, current: float) -> float:
    if entry <= 0:
        return 0.0
    return ((current - entry) / entry) * 100.0

def calc_trailing_stop(job: dict) -> float:
    """
    Calculate current trailing stop price based on mode.
    Returns the price at which tracking should stop.
    """
    cfg        = job["trailing_config"]
    mode       = cfg.get("mode", 2)
    entry      = job["entry_price"]
    peak       = job["_peak_price"]
    sl_pct     = cfg.get("sl_pct", 50.0)
    trail_pct  = cfg.get("trail_pct", 80.0)
    tiers      = cfg.get("tiers", [])
    atr_mult   = cfg.get("atr_mult", 3.0)
    prices     = job["_price_history"]
    peak_pct   = calc_pct(entry, peak)

    # Hard floor (always active regardless of mode)
    hard_floor = entry * (1.0 - sl_pct / 100.0)

    if mode == 1:
        # Fixed stop loss — never moves
        return hard_floor

    elif mode == 2:
        # Percentage trailing from peak
        # stop = peak × (1 - trail_pct/100)
        if peak <= entry:
            return hard_floor  # haven't pumped yet, use hard floor
        trail_stop = peak * (1.0 - trail_pct / 100.0)
        return max(trail_stop, hard_floor)

    elif mode == 3:
        # Step ratchet — tier-based floors
        if not tiers:
            return hard_floor
        # Sort tiers by at_pct descending, find highest tier hit
        sorted_tiers = sorted(tiers, key=lambda t: t["at_pct"], reverse=True)
        for tier in sorted_tiers:
            if peak_pct >= tier["at_pct"]:
                floor_price = entry * (1.0 + tier["floor_pct"] / 100.0)
                return max(floor_price, hard_floor)
        return hard_floor

    elif mode == 4:
        # ATR trailing
        atr_period = cfg.get("atr_period", 10)
        if len(prices) < atr_period + 1:
            # Not enough data yet, fall back to mode 2
            if peak <= entry:
                return hard_floor
            trail_stop = peak * (1.0 - trail_pct / 100.0)
            return max(trail_stop, hard_floor)
        recent = prices[-(atr_period + 1):]
        moves  = [abs(recent[i] - recent[i-1]) for i in range(1, len(recent))]
        atr    = statistics.mean(moves) if moves else 0
        if atr <= 0 or peak <= entry:
            return hard_floor
        atr_stop = peak - (atr_mult * atr)
        return max(atr_stop, hard_floor)

    return hard_floor

def should_remove_token(job: dict, current_price: float) -> Tuple[bool, str]:
    """
    Returns (should_remove, reason).
    reason: 'stop_loss' | 'trailing_stop' | 'max_gain'
    """
    entry     = job["entry_price"]
    cfg       = job["trailing_config"]
    sl_pct    = cfg.get("sl_pct", 50.0)
    current_pct = calc_pct(entry, current_price)
    peak_pct    = calc_pct(entry, job["_peak_price"])

    # Hard cap: max 20000%
    if current_pct >= 20000.0:
        return True, "max_gain"

    # Hard floor: -sl_pct% from entry
    if current_pct <= -sl_pct:
        return True, "stop_loss"

    # Trailing stop (only kicks in once price has moved above entry)
    trailing_stop = calc_trailing_stop(job)
    if job["_peak_price"] > entry and current_price <= trailing_stop:
        reason = "trailing_stop"
        return True, reason

    return False, ""

# ═══════════════════════════════════════════════════════════════════
#  JOB MANAGER
# ═══════════════════════════════════════════════════════════════════
_jobs:       Dict[str, dict]     = {}   # job_id → job state
_job_tasks:  Dict[str, asyncio.Task] = {}
_jobs_lock   = asyncio.Lock()

def _init_job_state(job: dict) -> dict:
    """Enrich incoming job with tracking state."""
    entry = float(job.get("entry_price", 0))
    return {
        **job,
        "entry_price":    entry,
        "_peak_price":    entry,
        "_price_history": [],
        "_started_at":    time.time(),
        "_last_tick":     0,
    }

async def start_job(job: dict):
    job_id = job["job_id"]
    async with _jobs_lock:
        if job_id in _jobs:
            logger.info(f"[JOB] {job_id} already running, skipping duplicate")
            return
        _jobs[job_id] = _init_job_state(job)

    task = asyncio.create_task(_tracking_loop(job_id))
    _job_tasks[job_id] = task
    logger.info(f"[JOB START] {job_id} | CA: {job['ca'][:12]}... | "
                f"interval: {job.get('interval_sec',60)}s | mode: {job['trailing_config']['mode']}")

async def cancel_job(job_id: str):
    async with _jobs_lock:
        _jobs.pop(job_id, None)
    task = _job_tasks.pop(job_id, None)
    if task and not task.done():
        task.cancel()
    logger.info(f"[JOB CANCEL] {job_id}")

async def _tracking_loop(job_id: str):
    """Main price tracking loop for one token."""
    try:
        while True:
            async with _jobs_lock:
                job = _jobs.get(job_id)
            if not job:
                break

            interval = int(job.get("interval_sec", 60))
            await asyncio.sleep(interval)

            async with _jobs_lock:
                job = _jobs.get(job_id)
            if not job:
                break

            ca    = job["ca"]
            entry = job["entry_price"]

            # Fetch current price
            price = await get_price(ca)
            if not price or price <= 0:
                logger.warning(f"[PRICE FAIL] {job_id} — no price from any API")
                await report_error(job_id, "gecko_terminal", "No price from any API")
                continue

            # Update entry price on first successful read if entry was 0
            async with _jobs_lock:
                if _jobs[job_id]["entry_price"] <= 0:
                    _jobs[job_id]["entry_price"] = price
                    entry = price
                    logger.info(f"[ENTRY SET] {job_id} entry_price={price:.8f}")

                _jobs[job_id]["_price_history"].append(price)
                # Keep last 50 prices for ATR
                if len(_jobs[job_id]["_price_history"]) > 50:
                    _jobs[job_id]["_price_history"].pop(0)

                # Update peak
                if price > _jobs[job_id]["_peak_price"]:
                    _jobs[job_id]["_peak_price"] = price

                job = _jobs[job_id]

            # Calculate metrics
            current_pct    = calc_pct(entry, price)
            peak_pct       = calc_pct(entry, job["_peak_price"])
            trailing_stop  = calc_trailing_stop(job)

            # Check if should remove
            remove, reason = should_remove_token(job, price)

            if remove:
                duration_min = int((time.time() - job["_started_at"]) / 60)
                await report_stop_triggered(job_id, job, price, current_pct, peak_pct, reason, duration_min)
                await cancel_job(job_id)
                break
            else:
                await report_price_update(job_id, job, price, current_pct, peak_pct, trailing_stop)

    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error(f"[TRACKING ERROR] {job_id}: {e}")
        await report_error(job_id, "tracking", str(e))

# ═══════════════════════════════════════════════════════════════════
#  MASTER BOT REPORTING
# ═══════════════════════════════════════════════════════════════════
async def _post_master(endpoint: str, data: dict, retries: int = 2):
    session = await get_session()
    url = f"{MASTER_BOT_URL.rstrip('/')}/{endpoint.lstrip('/')}"
    for attempt in range(retries):
        try:
            async with session.post(
                url, json=data,
                headers={"X-Secret": SHARED_SECRET, "Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                if r.status == 200:
                    return True
                logger.warning(f"[MASTER POST] {endpoint} → HTTP {r.status}")
        except Exception as e:
            logger.error(f"[MASTER POST ERROR] {endpoint} attempt {attempt+1}: {e}")
            if attempt < retries - 1:
                await asyncio.sleep(3)
    return False

async def report_price_update(job_id: str, job: dict, price: float,
                               pct: float, peak_pct: float, trailing_stop: float):
    await _post_master("/price_update", {
        "job_id":               job_id,
        "worker_name":          WORKER_NAME,
        "ca":                   job["ca"],
        "source":               job["source"],
        "strategy_id":          job["strategy_id"],
        "current_price":        price,
        "pct_from_entry":       round(pct, 4),
        "peak_pct":             round(peak_pct, 4),
        "trailing_stop_price":  round(trailing_stop, 10),
        "timestamp":            datetime.utcnow().isoformat(),
    })

async def report_stop_triggered(job_id: str, job: dict, price: float,
                                  final_pct: float, peak_pct: float,
                                  reason: str, duration_min: int):
    logger.info(f"[STOP] {job_id} | reason={reason} | final={final_pct:+.2f}% | peak={peak_pct:+.2f}%")
    await _post_master("/stop_triggered", {
        "job_id":           job_id,
        "worker_name":      WORKER_NAME,
        "ca":               job["ca"],
        "source":           job["source"],
        "strategy_id":      job["strategy_id"],
        "final_price":      price,
        "final_pct":        round(final_pct, 4),
        "peak_pct":         round(peak_pct, 4),
        "reason":           reason,
        "duration_minutes": duration_min,
    })

async def report_error(job_id: str, api_name: str, error_msg: str):
    await _post_master("/worker_error", {
        "job_id":       job_id,
        "worker_name":  WORKER_NAME,
        "api_name":     api_name,
        "error_msg":    error_msg[:300],
    })

async def report_api_stats():
    """Periodically push API stats to master."""
    await _post_master("/worker_error", {
        "job_id":       "_stats",
        "worker_name":  WORKER_NAME,
        "api_name":     "gecko_terminal",
        "error_msg":    api_stats["gecko_terminal"]["last_error"],
    }) if api_stats["gecko_terminal"]["last_error"] else None

async def register_with_master():
    """Register this worker with master on startup. Retries until success."""
    worker_url = os.getenv("WORKER_PUBLIC_URL", f"http://localhost:{WORKER_PORT}")
    for attempt in range(30):
        try:
            session = await get_session()
            async with session.post(
                f"{MASTER_BOT_URL.rstrip('/')}/register",
                json={"worker_name": WORKER_NAME, "worker_url": worker_url},
                headers={"X-Secret": SHARED_SECRET},
                timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                if r.status == 200:
                    logger.info(f"✅ Registered with master as '{WORKER_NAME}' @ {worker_url}")
                    return
                logger.warning(f"Master returned {r.status} on register attempt {attempt+1}")
        except Exception as e:
            logger.warning(f"Register attempt {attempt+1} failed: {e}")
        await asyncio.sleep(min(10 * (attempt + 1), 60))
    logger.error("❌ Could not register with master after 30 attempts")

# ═══════════════════════════════════════════════════════════════════
#  FASTAPI HTTP SERVER
# ═══════════════════════════════════════════════════════════════════
app = FastAPI(title=f"Worker Bot: {WORKER_NAME}")

def verify_secret(x_secret: Optional[str] = Header(None)):
    if x_secret != SHARED_SECRET:
        raise HTTPException(status_code=403, detail="Invalid secret")

@app.post("/job")
async def endpoint_job(request: Request, x_secret: Optional[str] = Header(None)):
    verify_secret(x_secret)
    job = await request.json()
    if not job.get("job_id") or not job.get("ca"):
        raise HTTPException(status_code=400, detail="Missing job_id or ca")
    asyncio.create_task(start_job(job))
    return JSONResponse({"status": "started", "job_id": job["job_id"]})

@app.post("/cancel")
async def endpoint_cancel(request: Request, x_secret: Optional[str] = Header(None)):
    verify_secret(x_secret)
    body = await request.json()
    job_id = body.get("job_id", "")
    await cancel_job(job_id)
    return JSONResponse({"status": "cancelled", "job_id": job_id})

@app.get("/health")
async def endpoint_health():
    active_jobs = len(_jobs)
    return JSONResponse({
        "status":         "ok",
        "worker_name":    WORKER_NAME,
        "active_jobs":    active_jobs,
        "uptime_seconds": int(time.time() - start_time),
        "gecko_success":  api_stats["gecko_terminal"]["success"],
        "gecko_fail":     api_stats["gecko_terminal"]["fail"],
        "gecko_last_err": api_stats["gecko_terminal"]["last_error"],
        "dex_success":    api_stats["dexscreener"]["success"],
        "dex_fail":       api_stats["dexscreener"]["fail"],
        "master_url":     MASTER_BOT_URL,
    })

@app.get("/jobs")
async def endpoint_jobs(x_secret: Optional[str] = Header(None)):
    verify_secret(x_secret)
    jobs_summary = []
    async with _jobs_lock:
        for job_id, job in _jobs.items():
            entry = job["entry_price"]
            peak  = job["_peak_price"]
            jobs_summary.append({
                "job_id":     job_id,
                "ca":         job["ca"],
                "source":     job["source"],
                "strategy_id":job["strategy_id"],
                "entry_price":entry,
                "peak_price": peak,
                "peak_pct":   round(calc_pct(entry, peak), 2),
                "readings":   len(job["_price_history"]),
                "age_min":    int((time.time() - job["_started_at"]) / 60),
            })
    return JSONResponse({"worker": WORKER_NAME, "jobs": jobs_summary})

# ═══════════════════════════════════════════════════════════════════
#  STARTUP / SHUTDOWN
# ═══════════════════════════════════════════════════════════════════
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("=" * 60)
    logger.info(f"  WORKER BOT: {WORKER_NAME}")
    logger.info(f"  Master URL: {MASTER_BOT_URL}")
    logger.info(f"  Port:       {WORKER_PORT}")
    logger.info(f"  Secret:     {SHARED_SECRET[:8]}...")
    logger.info("=" * 60)
    asyncio.create_task(register_with_master())
    yield
    # Clean up
    for task in _job_tasks.values():
        if not task.done():
            task.cancel()
    if _http_session and not _http_session.closed:
        await _http_session.close()
    logger.info("Worker bot shutdown complete")

app.router.lifespan_context = lifespan

# ═══════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    uvicorn.run(
        "worker_bot:app",
        host="0.0.0.0",
        port=WORKER_PORT,
        reload=False,
        log_level="info",
    )
