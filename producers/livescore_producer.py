#!/usr/bin/env python3
# producers/livescore_producer.py

import time, json, logging
from common import (
    producer, delivery_report, HEADERS, LIVESCORE_POLL_SEC,
    LEAGUE_IDS, http_get
)

TOPIC = "soccer.live_score"
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def fetch_and_produce():
    total = 0
    for league_id in LEAGUE_IDS:
        url = f"https://www.thesportsdb.com/api/v2/json/livescore/{league_id}"
        try:
            r = http_get(url, headers=HEADERS)
        except Exception as e:
            logging.error(f"[livescore] {league_id} -> {e}")
            continue

        scores = r.json().get("livescore", []) or []
        for sc in scores:
            sc["ingested_at"] = time.time()
            producer.produce(
                TOPIC,
                key=str(sc.get("idLiveScore") or sc.get("idEvent")),
                value=json.dumps(sc),
                callback=delivery_report
            )
        producer.flush()
        total += len(scores)
    logging.info(f"[livescore] Produced {total} rows across {len(LEAGUE_IDS)} leagues")

if __name__ == "__main__":
    while True:
        fetch_and_produce()
        time.sleep(LIVESCORE_POLL_SEC)
