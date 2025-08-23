#!/usr/bin/env python3
# producers/event_details_producer.py
# Fetch event_stats/timeline/highlights/lineup for the SAME event set used by event_producer:
#   leagues × seasons from /schedule/league/{league}/{season}
# Then call the four lookup endpoints for each idEvent and publish to Kafka.

import os
import json
import time
import logging
from typing import Dict, List, Optional, Any

from common import (
    producer,
    delivery_report,
    HEADERS,
    LEAGUE_IDS,
    http_get,
    safe_json,
    REFDATA_POLL_SEC,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ──────────────────── Topics ────────────────────
TOPIC_STATS      = "soccer.event.stats"
TOPIC_TIMELINE   = "soccer.event.timeline"
TOPIC_HIGHLIGHTS = "soccer.event.highlights"
TOPIC_LINEUP     = "soccer.event.lineup"

# ──────────────────── Seasons (match event_producer) ────────────────────
_raw_seasons = os.getenv("SEASONS") or os.getenv("SEASON") or ""
SEASONS = [s.strip() for s in _raw_seasons.split(",") if s.strip()]
if not SEASONS:
    raise RuntimeError("Set SEASON=2025-2026 or SEASONS=2025-2026,2024-2025")

# ──────────────────── Pacing / Run Mode (keep your env names) ───────────
REQS_PER_MIN  = int(os.getenv("EVENT_STATS_RPM", "150"))     # total HTTP calls/min (across all 4 lookups)
RUN_ONCE      = os.getenv("EVENT_STATS_RUN_ONCE", "1") == "1"
MAX_EVENTS    = int(os.getenv("EVENT_STATS_MAX_EVENTS", "0"))  # 0 = no cap

_MIN_INTERVAL = 60.0 / max(REQS_PER_MIN, 1)

def _pacer_factory():
    last = [0.0]
    def wait():
        now = time.perf_counter()
        delta = now - last[0]
        if delta < _MIN_INTERVAL:
            time.sleep(_MIN_INTERVAL - delta)
        last[0] = time.perf_counter()
    return wait

_pace = _pacer_factory()

# ──────────────────── Helpers ────────────────────
def _arr(j: Optional[Dict]) -> List[Dict]:
    """Extract list-like payloads from varied v2 shapes."""
    if not isinstance(j, dict):
        return []
    for k in ("lookup","stats","timeline","highlights","lineup","results","list","event","data","schedule"):
        v = j.get(k)
        if isinstance(v, list):
            return v
        if isinstance(v, dict):
            return [v]
    return []

def fetch_event_ids_all_seasons() -> List[str]:
    """
    Match event_producer: iterate league × season over /schedule/league/{league}/{season}.
    Return de-duped list of idEvent strings (order preserved).
    """
    ids: List[str] = []
    for league_id in LEAGUE_IDS:
        for season in SEASONS:
            url = f"https://www.thesportsdb.com/api/v2/json/schedule/league/{league_id}/{season}"
            try:
                _pace()
                resp = http_get(url, headers=HEADERS)
                schedule = safe_json(resp).get("schedule", []) or []
                before = len(ids)
                for e in schedule:
                    eid = str(e.get("idEvent") or "").strip()
                    if eid:
                        ids.append(eid)
                logging.info(f"[details] schedule league={league_id} season={season} → +{len(ids)-before} ids")
            except Exception as exc:
                logging.warning(f"[details] schedule {league_id}/{season} → {exc}")

    # de-dupe preserving order
    seen, out = set(), []
    for eid in ids:
        if eid and eid not in seen:
            seen.add(eid)
            out.append(eid)

    logging.info(f"[details] collected {len(out)} unique ids across {len(LEAGUE_IDS)} leagues × {len(SEASONS)} season(s)")
    return out

def fetch_event_stats_bundle(event_id: str) -> Dict[str, Dict[str, Any]]:
    """
    Fetch four endpoints for one event id; returns raw JSON dicts keyed by type.
    """
    endpoints = {
        "event_stats":      f"https://www.thesportsdb.com/api/v2/json/lookup/event_stats/{event_id}",
        "event_timeline":   f"https://www.thesportsdb.com/api/v2/json/lookup/event_timeline/{event_id}",
        "event_highlights": f"https://www.thesportsdb.com/api/v2/json/lookup/event_highlights/{event_id}",
        "event_lineup":     f"https://www.thesportsdb.com/api/v2/json/lookup/event_lineup/{event_id}",
    }
    bundle: Dict[str, Dict[str, Any]] = {}
    for key, url in endpoints.items():
        try:
            _pace()
            resp = http_get(url, headers=HEADERS)
            bundle[key] = safe_json(resp) or {}
        except Exception as exc:
            logging.warning(f"[details] {key} {event_id} → {exc}")
            bundle[key] = {}
    return bundle

def produce_event_stats(event_id: str, data: Dict[str, Dict[str, Any]]) -> None:
    """
    Split the bundle into individual records and produce to Kafka topics.
    """
    ts = time.time()

    # 1) event_stats
    for rec in _arr(data.get("event_stats")):
        payload = {"idEvent": event_id, **rec, "ingested_at": ts}
        producer.produce(TOPIC_STATS, key=event_id, value=json.dumps(payload), callback=delivery_report)

    # 2) event_timeline
    for rec in _arr(data.get("event_timeline")):
        payload = {"idEvent": event_id, **rec, "ingested_at": ts}
        producer.produce(TOPIC_TIMELINE, key=event_id, value=json.dumps(payload), callback=delivery_report)

    # 3) event_highlights
    for rec in _arr(data.get("event_highlights")):
        payload = {"idEvent": event_id, **rec, "ingested_at": ts}
        producer.produce(TOPIC_HIGHLIGHTS, key=event_id, value=json.dumps(payload), callback=delivery_report)

    # 4) event_lineup
    for rec in _arr(data.get("event_lineup")):
        payload = {"idEvent": event_id, **rec, "ingested_at": ts}
        producer.produce(TOPIC_LINEUP, key=event_id, value=json.dumps(payload), callback=delivery_report)

def run_once():
    ids = fetch_event_ids_all_seasons()
    if MAX_EVENTS > 0:
        ids = ids[:MAX_EVENTS]
    logging.info(f"[details] processing {len(ids)} events (four lookups per event; pacing ~{REQS_PER_MIN}/min total)")

    produced = 0
    for i, eid in enumerate(ids, start=1):
        bundle = fetch_event_stats_bundle(eid)
        produce_event_stats(eid, bundle)

        # flush every N events to keep buffer small; here flush by minute-equivalent
        if i % 200 == 0:
            producer.flush()
            logging.info(f"[details] progress {i}/{len(ids)}")

        produced += 1

    producer.flush()
    logging.info(f"[details] finished producing bundles for {produced} events")

def main():
    try:
        run_once()
    except Exception as e:
        logging.exception(f"[details] run failed: {e}")

    if RUN_ONCE:
        logging.info("[details] EVENT_STATS_RUN_ONCE=1 → exiting")
    else:
        time.sleep(REFDATA_POLL_SEC)
        main()

if __name__ == "__main__":
    main()
