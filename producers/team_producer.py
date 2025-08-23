#!/usr/bin/env python3
# producers/team_producer.py
#
# 1) v2 list teams by league -> team IDs  (supports {"list": [...]} or {"team(s)":[...]})
# 2) v2 lookup team by ID   -> full record
#
# Output topic: soccer.team

import json
import logging
import os
import time
from typing import List, Dict, Any, Optional

from common import (
    producer, delivery_report, LEAGUE_IDS, REFDATA_POLL_SEC,
    http_get, HEADERS, safe_json
)

TOPIC = "soccer.team"
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

DETAIL_PAUSE_SEC = float(os.getenv("TEAM_DETAIL_SLEEP", "0.25"))  # gentle pacing

# ---- helpers ----------------------------------------------------------------

def unique_ordered(seq: List[str]) -> List[str]:
    seen, out = set(), []
    for x in seq:
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out

def _extract_array_payload(j: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Handles the shapes we've seen across v2:
      { "list": [...] }           # v2 list/teams
      { "lookup": [...] }         # v2 lookup/team
      { "team": [...] } or { "teams": [...] }
      { "data": { ...same keys... } }
    """
    if not isinstance(j, dict):
        return []

    for k in ("list", "lookup", "team", "teams"):
        v = j.get(k)
        if isinstance(v, list):
            return v
        if isinstance(v, dict):
            return [v]

    data = j.get("data")
    if isinstance(data, dict):
        for k in ("list", "lookup", "team", "teams"):
            v = data.get(k)
            if isinstance(v, list):
                return v
            if isinstance(v, dict):
                return [v]

    return []

def list_team_ids_by_league(league_id: int) -> List[str]:
    """
    v2:  /api/v2/json/list/teams/{league_id}
         -> { "list": [ { "idTeam": "...", ... }, ... ] } (or "teams"/"team")
    """
    url_v2 = f"https://www.thesportsdb.com/api/v2/json/list/teams/{league_id}"
    try:
        r = http_get(url_v2, headers=HEADERS)
        j = safe_json(r) or {}
        items = _extract_array_payload(j)
        ids = [str(it.get("idTeam")) for it in items if it.get("idTeam")]
        if ids:
            return unique_ordered(ids)
        else:
            logging.info(f"[team] v2 list empty for league {league_id}; payload head={str(j)[:200]}")
    except Exception as e:
        logging.warning(f"[team] list v2 {league_id} -> {e}")

    return []

def fetch_team_detail(team_id: str) -> Optional[Dict[str, Any]]:
    url_v2 = f"https://www.thesportsdb.com/api/v2/json/lookup/team/{team_id}"
    try:
        r = http_get(url_v2, headers=HEADERS)
        arr = _extract_array_payload(safe_json(r) or {})
        if arr:
            return arr[0]
        logging.info(f"[team] v2 lookup empty for {team_id}")
    except Exception as e:
        logging.warning(f"[team] lookup v2 {team_id} -> {e}")

    return None



# ---- main loop --------------------------------------------------------------

def run():
    while True:
        sent = 0
        for lid in LEAGUE_IDS:
            team_ids = list_team_ids_by_league(lid)
            logging.info(f"[team] league {lid} -> {len(team_ids)} team ids")
            for tid in team_ids:
                td = fetch_team_detail(tid)
                if not td:
                    continue
                payload = td
                if not payload.get("idTeam"):
                    continue
                producer.produce(
                    TOPIC,
                    key=str(payload["idTeam"]),
                    value=json.dumps(payload),
                    callback=delivery_report
                )
                sent += 1
                time.sleep(DETAIL_PAUSE_SEC)  # avoid bursty hits to the API

        if sent:
            producer.flush()
        logging.info(f"[team] produced {sent} rows")
        time.sleep(REFDATA_POLL_SEC)

if __name__ == "__main__":
    run()
