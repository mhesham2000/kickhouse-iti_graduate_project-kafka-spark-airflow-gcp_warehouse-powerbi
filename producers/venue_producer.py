#!/usr/bin/env python3
# producers/venue_producer.py
#
# v2-only implementation.
# Strategy:
#   1) From schedules: collect venue IDs if present (idVenue/idStadium).
#   2) From teams: list -> lookup team -> collect idStadium/idVenue.
#   3) Lookup venues by ID using v2 endpoints and produce.
# NOTE: Name-based fallback (v1 search) has been removed.

import json, logging, time
from typing import Set, Dict, Any, Optional, List
from common import (
    producer, delivery_report, HEADERS, SEASON, LEAGUE_IDS, REFDATA_POLL_SEC,
    http_get, safe_json
)

TOPIC = "soccer.venue"
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

DETAIL_PAUSE_SEC = 0.2  # gentle pacing

def arr(obj: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not isinstance(obj, dict):
        return []
    for k in ("schedule", "list", "lookup", "venues", "venue", "teams", "team"):
        v = obj.get(k)
        if isinstance(v, list):
            return v
        if isinstance(v, dict):
            return [v]
    data = obj.get("data") if isinstance(obj, dict) else None
    if isinstance(data, dict):
        for k in ("schedule", "list", "lookup", "venues", "venue", "teams", "team"):
            v = data.get(k)
            if isinstance(v, list):
                return v
            if isinstance(v, dict):
                return [v]
    return []

def collect_candidate_venue_ids() -> Set[str]:
    venue_ids: Set[str] = set()

    # A) from schedules (v2) -> try idVenue/idStadium if available
    for league_id in LEAGUE_IDS:
        u = f"https://www.thesportsdb.com/api/v2/json/schedule/league/{league_id}/{SEASON}"
        try:
            r = http_get(u, headers=HEADERS)
            j = safe_json(r) or {}
            for ev in arr(j):
                for key in ("idVenue", "idStadium"):
                    vid = ev.get(key)
                    if vid:
                        venue_ids.add(str(vid))
        except Exception as e:
            logging.warning(f"[venue] schedule {league_id} -> {e}")

    # B) from teams: v2 list -> v2 lookup/team -> idStadium/idVenue
    for league_id in LEAGUE_IDS:
        list_url = f"https://www.thesportsdb.com/api/v2/json/list/teams/{league_id}"
        try:
            r = http_get(list_url, headers=HEADERS)
            j = safe_json(r) or {}
            team_ids = [t.get("idTeam") for t in arr(j) if t.get("idTeam")]
            for tid in team_ids:
                lu = f"https://www.thesportsdb.com/api/v2/json/lookup/team/{tid}"
                try:
                    rr = http_get(lu, headers=HEADERS)
                    jj = safe_json(rr) or {}
                    for t in arr(jj):
                        vid = t.get("idStadium") or t.get("idVenue")
                        if vid:
                            venue_ids.add(str(vid))
                    time.sleep(DETAIL_PAUSE_SEC)
                except Exception as e:
                    logging.warning(f"[venue] team lookup {tid} -> {e}")
        except Exception as e:
            logging.warning(f"[venue] team list {league_id} -> {e}")

    return venue_ids

def fetch_venue_by_id(venue_id: str) -> Optional[Dict[str, Any]]:
    urls = [
        f"https://www.thesportsdb.com/api/v2/json/lookup/venue/{venue_id}",
        f"https://www.thesportsdb.com/api/v2/json/lookup/stadium/{venue_id}",
    ]
    for u in urls:
        try:
            r = http_get(u, headers=HEADERS)
            j = safe_json(r)
            if not j:
                continue
            got = arr(j)
            if got:
                return got[0]
        except Exception as e:
            logging.debug(f"[venue] lookup id {venue_id} via {u} -> {e}")
    return None

def run():
    while True:
        ids = collect_candidate_venue_ids()
        sent = 0

        for vid in sorted(ids):
            v = fetch_venue_by_id(vid)
            if not v:
                continue
            payload = v
            k = str(payload.get("idVenue") or vid)
            if not k:
                continue
            producer.produce(TOPIC, key=k, value=json.dumps(payload), callback=delivery_report)
            sent += 1
            time.sleep(DETAIL_PAUSE_SEC)

        if sent:
            producer.flush()
        logging.info(f"[venue] produced {sent} rows")
        time.sleep(REFDATA_POLL_SEC)

if __name__ == "__main__":
    run()
