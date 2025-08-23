#!/usr/bin/env python3
# producers/event_lookup_producer.py
"""
Poll livescore → extract idEvent → call lookup/event/{id} → produce full event docs.

Topic: soccer.live.event.lookup   (key = idEvent, value = JSON event doc)
"""

import time
import json
import logging
from typing import Dict, Set, Optional

from common import (
    producer, delivery_report, HEADERS,
    LEAGUE_IDS, LIVESCORE_POLL_SEC, http_get, safe_json
)
import os

# Pacing for lookup calls (requests per minute)
EVENT_LOOKUP_RPM = int(os.getenv("EVENT_LOOKUP_RPM", "50"))
LOOKUP_SLEEP = max(0.0, 60.0 / EVENT_LOOKUP_RPM)

# Don’t hammer the same idEvent repeatedly; cache for a short window
DEDUP_TTL_SEC = int(os.getenv("LIVE_EVENT_DEDUP_TTL_SEC", "300"))

TOPIC_OUT = "soccer.live.event.lookup"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


class SeenCache:
    """Simple TTL cache for idEvent -> last_seen_epoch."""
    def __init__(self, ttl: int):
        self.ttl = ttl
        self._seen: Dict[str, float] = {}

    def mark(self, key: str):
        self._seen[key] = time.time()

    def should_skip(self, key: str) -> bool:
        ts = self._seen.get(key)
        if ts is None:
            return False
        return (time.time() - ts) < self.ttl

    def gc(self):
        now = time.time()
        to_del = [k for k, ts in self._seen.items() if (now - ts) >= self.ttl]
        for k in to_del:
            self._seen.pop(k, None)


def fetch_livescore_ids() -> Set[str]:
    """Return a set of idEvent values found across all configured leagues."""
    event_ids: Set[str] = set()
    total_rows = 0

    for league_id in LEAGUE_IDS:
        url = f"https://www.thesportsdb.com/api/v2/json/livescore/{league_id}"
        try:
            resp = http_get(url, headers=HEADERS)
        except Exception as e:
            logging.error(f"[livescore] league {league_id}: {e}")
            continue

        data = safe_json(resp) or {}
        rows = data.get("livescore") or []
        total_rows += len(rows)
        for row in rows:
            ev = str(row.get("idEvent") or "").strip()
            if ev:
                event_ids.add(ev)

    logging.info(f"[livescore] scanned rows={total_rows}, unique event ids={len(event_ids)}")
    return event_ids


def lookup_event(id_event: str) -> Optional[Dict]:
    """
    Return the event detail document for an id_event (or None).

    v2 shapes we’ve seen:
      { "lookup":  [ {..event..} ] }
      { "events":  [ {..event..} ] }
      { "event":    {..event..}     }
    """
    url = f"https://www.thesportsdb.com/api/v2/json/lookup/event/{id_event}"
    try:
        resp = http_get(url, headers=HEADERS)
    except Exception as e:
        logging.warning(f"[lookup] {id_event}: {e}")
        return None

    payload = safe_json(resp) or {}

    doc = None
    if isinstance(payload.get("lookup"), list) and payload["lookup"]:
        doc = payload["lookup"][0]
    elif isinstance(payload.get("events"), list) and payload["events"]:
        doc = payload["events"][0]
    elif isinstance(payload.get("event"), dict):
        doc = payload["event"]

    if not isinstance(doc, dict):
        logging.warning(f"[lookup] {id_event}: unexpected body keys={list(payload.keys())}")
        return None

    # Normalize & stamp
    out = dict(doc)
    out["idEvent"] = str(out.get("idEvent") or id_event)
    out["ingested_at"] = time.time()
    return out


def produce_event(doc: Dict):
    key = str(doc.get("idEvent") or "")
    if not key:
        return
    producer.produce(
        TOPIC_OUT,
        key=key,
        value=json.dumps(doc, ensure_ascii=False),
        callback=delivery_report
    )


def main_loop():
    seen = SeenCache(ttl=DEDUP_TTL_SEC)

    while True:
        try:
            ids = fetch_livescore_ids()
            produced = 0

            for id_event in ids:
                if seen.should_skip(id_event):
                    continue

                doc = lookup_event(id_event)
                if doc:
                    produce_event(doc)
                    produced += 1
                    seen.mark(id_event)

                if LOOKUP_SLEEP > 0:
                    time.sleep(LOOKUP_SLEEP)

            producer.flush()
            seen.gc()
            logging.info(f"[live_event_lookup] produced={produced} events (ids={len(ids)})")
        except Exception as e:
            logging.exception(f"[live_event_lookup] loop error: {e}")

        time.sleep(LIVESCORE_POLL_SEC)


if __name__ == "__main__":
    main_loop()
