#!/usr/bin/env python3
# producers/schedule_producer.py

import time, json, logging
from common import (
    producer, delivery_report, HEADERS, SEASON, SCHEDULE_POLL_SEC,
    LEAGUE_IDS, http_get
)

TOPIC = "soccer.schedule"
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def fetch_and_produce():
    total = 0
    for league_id in LEAGUE_IDS:
        url = f"https://www.thesportsdb.com/api/v2/json/schedule/league/{league_id}/{SEASON}"
        try:
            resp = http_get(url, headers=HEADERS)
        except Exception as e:
            logging.error(f"[schedule] {league_id}/{SEASON} -> {e}")
            continue

        events = resp.json().get("schedule") or []
        logging.info(f"[schedule] {league_id}/{SEASON} -> {len(events)} events")

        for evt in events:
            evt["ingested_at"] = time.time()
            producer.produce(
                TOPIC,
                key=str(evt.get("idEvent")),
                value=json.dumps(evt),
                callback=delivery_report
            )
        producer.flush()
        total += len(events)

    logging.info(f"[schedule] Produced total {total} events")

if __name__ == "__main__":
    while True:
        fetch_and_produce()
        time.sleep(SCHEDULE_POLL_SEC)
