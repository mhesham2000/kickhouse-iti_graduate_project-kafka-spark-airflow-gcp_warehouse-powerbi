#!/usr/bin/env python3
# producers/event_producer.py

import os
import json, logging, time
from typing import List, Dict, Any, Optional

from common import (
    producer, delivery_report, HEADERS, SEASON, LEAGUE_IDS,
    REFDATA_POLL_SEC, http_get, safe_json
)

TOPIC = "soccer.event"
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ---- seasons (multi) ---------------------------------------------------------
# Accept SEASONS="2025-2026,2024-2025" or fall back to SEASON
_raw_seasons = os.getenv("SEASONS") or os.getenv("SEASON") or ""
SEASONS = [s.strip() for s in _raw_seasons.split(",") if s.strip()]
if not SEASONS:
    raise RuntimeError("Set SEASON=2025-2026 or SEASONS=2025-2026,2024-2025")

# ---- pacing config (env) ----------------------------------------------------
# Requests per minute for lookup/event calls (default 50 rpm -> ~1.2s/request)
LOOKUP_RPM = int(os.getenv("EVENT_LOOKUP_RPM", "50"))
# If you want to limit how many events you process per run (0 = no limit)
MAX_EVENTS_PER_RUN = int(os.getenv("EVENT_MAX_EVENTS_PER_RUN", "0"))
# If you want the producer to exit after one pass (run-once backfill)
RUN_ONCE = os.getenv("EVENT_RUN_ONCE", "0") == "1"

# Derived min interval between lookups (seconds)
_MIN_INTERVAL = 60.0 / max(LOOKUP_RPM, 1)

def _pacer_factory():
    last = [0.0]
    def wait():
        now = time.perf_counter()
        delta = now - last[0]
        if delta < _MIN_INTERVAL:
            time.sleep(_MIN_INTERVAL - delta)
        last[0] = time.perf_counter()
    return wait

_pace_lookup = _pacer_factory()

def _extract_arr(j: Dict[str, Any]):
    if not isinstance(j, dict):
        return []
    for k in ("lookup", "events", "event"):
        v = j.get(k)
        if isinstance(v, list):
            return v
        if isinstance(v, dict):
            return [v]
    return []

def fetch_event_ids() -> List[str]:
    ids: List[str] = []
    total_added = 0
    for league_id in LEAGUE_IDS:
        for season in SEASONS:
            u = f"https://www.thesportsdb.com/api/v2/json/schedule/league/{league_id}/{season}"
            try:
                r = http_get(u, headers=HEADERS)
                sch = (r.json().get("schedule") or [])
                count_before = len(ids)
                for e in sch:
                    if e.get("idEvent"):
                        ids.append(str(e["idEvent"]))
                added = len(ids) - count_before
                logging.info(f"[event] schedule league={league_id} season={season} -> {added} ids")
                total_added += added
            except Exception as e:
                logging.warning(f"[event] schedule {league_id}/{season} -> {e}")

    # de-dupe / preserve order across leagues & seasons
    seen, out = set(), []
    for x in ids:
        if x and x not in seen:
            seen.add(x)
            out.append(x)

    logging.info(f"[event] collected {len(out)} unique ids across {len(LEAGUE_IDS)} leagues and {len(SEASONS)} season(s)")
    return out

def fetch_event_detail(event_id: str) -> Optional[Dict[str, Any]]:
    u = f"https://www.thesportsdb.com/api/v2/json/lookup/event/{event_id}"
    try:
        r = http_get(u, headers=HEADERS)
        arr = _extract_arr(safe_json(r) or {})
        if arr:
            return arr[0]
        logging.info(f"[event] v2 lookup empty for {event_id}")
    except Exception as e:
        logging.warning(f"[event] {u} -> {e}")
    return None

def run():
    while True:
        ids = fetch_event_ids()
        if MAX_EVENTS_PER_RUN > 0:
            ids = ids[:MAX_EVENTS_PER_RUN]

        total = len(ids)
        sent = 0

        logging.info(f"[event] starting lookup for {total} event ids "
                     f"at ~{LOOKUP_RPM} req/min (interval ~{_MIN_INTERVAL:.2f}s)")

        for i, eid in enumerate(ids, start=1):
            _pace_lookup()  # enforce rate limit before each lookup call

            ed = fetch_event_detail(eid)
            if not ed:
                continue

            payload = ed
            producer.produce(
                TOPIC,
                key=str(payload["idEvent"]),
                value=json.dumps(payload),
                callback=delivery_report
            )
            sent += 1

            # Flush periodically to keep the buffer small
            if sent % 200 == 0:
                producer.flush()
                logging.info(f"[event] progress: {sent}/{total} produced")

        if sent:
            producer.flush()
        logging.info(f"[event] produced {sent} rows (looked up {total})")

        if RUN_ONCE:
            logging.info("[event] RUN_ONCE=1 -> exiting after one pass")
            break

        time.sleep(REFDATA_POLL_SEC)

if __name__ == "__main__":
    run()
