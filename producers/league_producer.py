#!/usr/bin/env python3
# producers/league_producer.py
#
# • Looks up every league ID from common.LEAGUE_IDS via /lookup/league/{id}
# • On first run (or when strCurrentSeason changes) produces to topic "soccer.league"
# • Keeps a small JSON state so it only emits when season changes.
#
# Env knobs (same as before):
#   LEAGUE_RUN_AT="HH:MM" (default 02:15)      – daily cron-like schedule
#   LEAGUE_USE_UTC=1 (default)                 – schedule in UTC; set 0 for local
#   LEAGUE_RUN_ONCE=1                          – run once then exit (for Airflow/Batch)
#   LEAGUE_PRODUCE_BASELINE=1 (default)        – emit all leagues on first run
#   LEAGUE_STATE_PATH=state/league_current_season.json
#
# All IDs are taken verbatim from LEAGUE_IDS in common.py / .env

import os
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional

from common import (
    producer, delivery_report, LEAGUE_IDS, http_get,
    HEADERS, safe_json
)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")

TOPIC = "soccer.league"

# -------- scheduling & state --------------------------------------------------

RUN_AT   = os.getenv("LEAGUE_RUN_AT", "02:15")             # HH:MM
USE_UTC  = os.getenv("LEAGUE_USE_UTC", "1") == "1"
RUN_ONCE = os.getenv("LEAGUE_RUN_ONCE", "0") == "1"

STATE_PATH = Path(os.getenv("LEAGUE_STATE_PATH",
                            "state/league_current_season.json"))
PRODUCE_BASELINE = os.getenv("LEAGUE_PRODUCE_BASELINE", "1") == "1"

# -----------------------------------------------------------------------------


def _extract_arr(j: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Unwrap common shapes from the SportsDB v2 response."""
    if not isinstance(j, dict):
        return []
    for k in ("lookup", "league", "leagues"):
        v = j.get(k)
        if isinstance(v, list):
            return v
        if isinstance(v, dict):
            return [v]
    data = j.get("data")
    if isinstance(data, dict):
        for k in ("lookup", "league", "leagues"):
            v = data.get(k)
            if isinstance(v, list):
                return v
            if isinstance(v, dict):
                return [v]
    return []


def fetch_league_obj(league_id: int) -> Optional[Dict[str, Any]]:
    """Return dict from API or None when empty/error."""
    url = f"https://www.thesportsdb.com/api/v2/json/lookup/league/{league_id}"
    logging.debug(f"[league] GET {url}")
    try:
        r = http_get(url, headers=HEADERS)
        arr = _extract_arr(safe_json(r) or {})
        if arr:
            return arr[0]
        logging.warning(f"[league] empty response for id={league_id}")
    except Exception as e:
        logging.warning(f"[league] lookup failed id={league_id}: {e}")
    return None


# -------------------- state helpers ------------------------------------------


def _load_state() -> Dict[str, str]:
    if STATE_PATH.exists():
        try:
            with STATE_PATH.open() as f:
                return json.load(f)
        except Exception as e:
            logging.warning(f"[league] state load error: {e}")
    return {}


def _save_state(state: Dict[str, str]):
    try:
        STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = STATE_PATH.with_suffix(".tmp")
        with tmp.open("w") as f:
            json.dump(state, f)
        tmp.replace(STATE_PATH)
    except Exception as e:
        logging.warning(f"[league] state save error: {e}")


# ---------------------- scheduling helpers -----------------------------------


def _parse_hhmm(hhmm: str) -> tuple[int, int]:
    h, m = hhmm.strip().split(":")
    return int(h), int(m)


def _now() -> datetime:
    return datetime.now(timezone.utc) if USE_UTC else datetime.now()


def _next_run() -> datetime:
    h, m = _parse_hhmm(RUN_AT)
    now = _now()
    nxt = now.replace(hour=h, minute=m, second=0, microsecond=0)
    if nxt <= now:
        nxt += timedelta(days=1)
    return nxt


# ---------------------- main logic -------------------------------------------


def run_once() -> int:
    """
    Returns number of produced Kafka messages.
    Produces when:
      • first ever run (baseline) and PRODUCE_BASELINE=1
      • strCurrentSeason changed since last run
    """
    state = _load_state()
    baseline = (not state)
    produced = 0

    for lid in LEAGUE_IDS:
        d = fetch_league_obj(lid)
        if not d:
            continue

        curr_season = (d.get("strCurrentSeason") or "").strip()
        prev_season = state.get(str(lid), "")

        if baseline and not PRODUCE_BASELINE:
            logging.info(f"[league] skip baseline id={lid}")
            continue

        if baseline or curr_season != prev_season:
            payload = d
            producer.produce(
                TOPIC,
                key=str(lid),
                value=json.dumps(payload),
                callback=delivery_report,
            )
            produced += 1
            state[str(lid)] = curr_season
            logging.info(
                f"[league] produced id={lid} "
                f"({'baseline' if baseline else f'season {prev_season}→{curr_season}'})"
            )
        else:
            logging.debug(f"[league] no change id={lid}")

    if produced:
        producer.flush()
        _save_state(state)
    return produced


def run():
    while True:
        try:
            cnt = run_once()
            logging.info(f"[league] run complete. produced={cnt}")
        except Exception:
            logging.exception("[league] fatal error during run")

        if RUN_ONCE:
            break

        sleep_s = max(1, int((_next_run() - _now()).total_seconds()))
        logging.info(
            f"[league] sleep {sleep_s//3600}h {(sleep_s%3600)//60}m "
            f"(next run { _next_run().isoformat() })"
        )
        time.sleep(sleep_s)


if __name__ == "__main__":
    run()
