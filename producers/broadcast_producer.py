#!/usr/bin/env python3
# Simple broadcast producer: pull today's telecasts by sport -> Kafka (once daily)
# producers/broadcast_producer.py

import os
import time
import json
import logging
from typing import Dict, List, Optional, Set
from datetime import datetime, timedelta, timezone

from common import (
    producer, delivery_report, HEADERS,
    http_get, safe_json
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
TOPIC = "soccer.broadcast"

# ---- config (env) -----------------------------------------------------------
SPORT_NAME = os.getenv("BROADCAST_SPORT_NAME", "Soccer")   # e.g., Soccer

# Run schedule
# - BROADCAST_RUN_AT: "HH:MM" daily time (default 00:10)
# - BROADCAST_USE_UTC: "1" to schedule by UTC (default), "0" to use local system time
# - BROADCAST_RUN_ONCE: "1" to run once and exit (useful for ad-hoc/manual runs)
RUN_AT        = os.getenv("BROADCAST_RUN_AT", "00:10")
USE_UTC       = os.getenv("BROADCAST_USE_UTC", "1") == "1"
RUN_ONCE      = os.getenv("BROADCAST_RUN_ONCE", "0") == "1"

# Optional comma-separated allow lists; leave empty to disable filtering
COUNTRIES   = [s.strip() for s in os.getenv("BROADCAST_COUNTRIES", "").split(",") if s.strip()]
CHANNEL_IDS = {s.strip() for s in os.getenv("BROADCAST_CHANNEL_IDS", "").split(",") if s.strip()}
CHANNELS    = {s.strip().lower() for s in os.getenv("BROADCAST_CHANNELS", "").split(",") if s.strip()}

# Simple de-dupe across cycles (prevents repeats flooding Kafka across the same day)
MAX_SEEN = int(os.getenv("BROADCAST_MAX_SEEN", "5000"))
_seen_keys: List[str] = []     # ring buffer order
_seen_set:  Set[str]  = set()  # membership

# ---- helpers ----------------------------------------------------------------

def _arr(j: Optional[Dict]) -> List[Dict]:
    """Return list-like payload for tv filter endpoints."""
    if not isinstance(j, dict):
        return []
    for k in ("broadcasts", "telecast", "results", "list", "filter"):
        v = j.get(k)
        if isinstance(v, list):  return v
        if isinstance(v, dict):  return [v]
    data = j.get("data")
    if isinstance(data, dict):
        for k in ("broadcasts", "telecast", "results", "list"):
            v = data.get(k)
            if isinstance(v, list):  return v
            if isinstance(v, dict):  return [v]
    return []

def fetch_sport_today() -> List[Dict]:
    """Fetch today's telecasts for the sport."""
    u = f"https://www.thesportsdb.com/api/v2/json/filter/tv/sport/{SPORT_NAME}"
    r = http_get(u, headers=HEADERS, max_retries=2)
    rows = _arr(safe_json(r))
    logging.info(f"[broadcast] tv/sport {SPORT_NAME} -> {len(rows)} rows")
    return rows or []

def _allowed(b: Dict) -> bool:
    """Optional country/channel filtering."""
    if COUNTRIES and (b.get("strCountry") not in COUNTRIES):
        return False
    if CHANNEL_IDS and (str(b.get("idChannel") or "") not in CHANNEL_IDS):
        return False
    if CHANNELS and (str(b.get("strChannel") or "").lower() not in CHANNELS):
        return False
    return True

def _remember(key: str) -> bool:
    """Return True if new; remember key with a simple ring buffer."""
    if not key:
        return True
    if key in _seen_set:
        return False
    _seen_set.add(key)
    _seen_keys.append(key)
    if len(_seen_keys) > MAX_SEEN:
        old = _seen_keys.pop(0)
        _seen_set.discard(old)
    return True

def _parse_hhmm(hhmm: str) -> tuple[int, int]:
    hh, mm = hhmm.strip().split(":")
    return int(hh), int(mm)

def _now_dt():
    return datetime.now(timezone.utc) if USE_UTC else datetime.now()

def _next_run_dt() -> datetime:
    """Compute the next datetime to run (today at HH:MM, else tomorrow)."""
    hh, mm = _parse_hhmm(RUN_AT)
    now = _now_dt()
    target = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return target

# ---- main loop ---------------------------------------------------------------

def run_once() -> int:
    """Returns number of produced messages."""
    rows = fetch_sport_today()
    produced = 0
    for b in rows:
        if not _allowed(b):
            continue
        payload = b
        key = payload["id"]
        if not _remember(key):
            continue
        producer.produce(TOPIC, key=key, value=json.dumps(payload), callback=delivery_report)
        produced += 1
    if produced:
        producer.flush()
    logging.info(f"[broadcast] produced {produced} rows to {TOPIC}")
    return produced

def run():
    while True:
        try:
            tz_label = "UTC" if USE_UTC else "local"
            logging.info(f"[broadcast] daily run starting (schedule {RUN_AT} {tz_label})")
            run_once()
        except Exception as e:
            logging.exception(f"[broadcast] run failed: {e}")

        if RUN_ONCE:
            logging.info("[broadcast] BROADCAST_RUN_ONCE=1 -> exiting after one run")
            break

        nxt = _next_run_dt()
        now = _now_dt()
        sleep_s = max(1, int((nxt - now).total_seconds()))
        logging.info(f"[broadcast] next run at {nxt.isoformat()} ({tz_label}); sleeping {sleep_s//3600}h {(sleep_s%3600)//60}m")
        time.sleep(sleep_s)

if __name__ == "__main__":
    run()
