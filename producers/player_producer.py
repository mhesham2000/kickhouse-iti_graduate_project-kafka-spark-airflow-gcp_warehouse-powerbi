#!/usr/bin/env python3
# producers/player_producer.py
#
# Strategy:
#   1) For each league in LEAGUE_IDS -> list team IDs (v2: /list/teams/{league_id})
#   2) For each team ID -> list players (v2: /list/players/{team_id})
#   3) OPTIONAL: for each player -> lookup full detail (v2: /lookup/player/{idPlayer})
#   4) Produce one record per player (key=idPlayer) to topic "soccer.player"
#
# Pacing:
#   - All HTTP calls are throttled to PLAYER_RPM req/min (default 30 rpm -> ~2.0s/request)
#
# Run behavior (env):
#   - PLAYER_RUN_ONCE=1 (default) -> run one pass (good for backfills), then exit
#   - PLAYER_RUN_ONCE=0 -> sleep REFDATA_POLL_SEC and repeat (low-frequency refresh)
#
# Volume controls (env):
#   - PLAYER_RPM=30                 # requests/minute across ALL HTTP calls
#   - PLAYER_FETCH_DETAIL=1         # also call lookup/player/{id}
#   - PLAYER_MAX_LEAGUES=0          # 0=all; else limit number of leagues processed this run
#   - PLAYER_MAX_TEAMS_PER_RUN=0    # 0=all; else cap teams processed this run
#   - PLAYER_MAX_PLAYERS_PER_TEAM=0 # 0=all; else cap players per team
#   - PLAYER_MAX_PLAYERS_PER_RUN=0  # 0=all; else global cap of players produced per run
#
# Notes:
#   - Topic "soccer.player" should be compacted (latest-by-key) if you want a single current row per player.

import os
import json
import time
import logging
from typing import Dict, List, Optional, Tuple, Set

from common import (
    producer, delivery_report, LEAGUE_IDS, HEADERS,
    http_get, safe_json, REFDATA_POLL_SEC
)

TOPIC = "soccer.player"
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ---- pacing / run config -----------------------------------------------------

PLAYER_RPM = int(os.getenv("PLAYER_RPM", "30"))  # req/min across all HTTP calls
_MIN_INTERVAL = 60.0 / max(PLAYER_RPM, 1)

PLAYER_RUN_ONCE = os.getenv("PLAYER_RUN_ONCE", "1") == "1"
PLAYER_FETCH_DETAIL = os.getenv("PLAYER_FETCH_DETAIL", "1") == "1"

PLAYER_MAX_LEAGUES = int(os.getenv("PLAYER_MAX_LEAGUES", "0"))
PLAYER_MAX_TEAMS_PER_RUN = int(os.getenv("PLAYER_MAX_TEAMS_PER_RUN", "0"))
PLAYER_MAX_PLAYERS_PER_TEAM = int(os.getenv("PLAYER_MAX_PLAYERS_PER_TEAM", "0"))
PLAYER_MAX_PLAYERS_PER_RUN = int(os.getenv("PLAYER_MAX_PLAYERS_PER_RUN", "0"))

FLUSH_EVERY = int(os.getenv("PLAYER_FLUSH_EVERY", "500"))  # flush Kafka every N produced

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

# ---- helpers -----------------------------------------------------------------

def _arr(j: Optional[Dict]) -> List[Dict]:
    """Return list-like payload for common v2 shapes."""
    if not isinstance(j, dict):
        return []
    for k in ("list", "lookup", "team", "teams", "player", "players", "results", "data"):
        v = j.get(k)
        if isinstance(v, list):  return v
        if isinstance(v, dict):  return [v]
    data = j.get("data")
    if isinstance(data, dict):
        for k in ("list", "lookup", "team", "teams", "player", "players", "results"):
            v = data.get(k)
            if isinstance(v, list):  return v
            if isinstance(v, dict):  return [v]
    return []

def list_team_ids_by_league(league_id: int) -> List[str]:
    """v2: GET /api/v2/json/list/teams/{league_id} -> extract idTeam list"""
    url = f"https://www.thesportsdb.com/api/v2/json/list/teams/{league_id}"
    try:
        _pace()
        r = http_get(url, headers=HEADERS)
        items = _arr(safe_json(r) or {})
        ids = [str(it.get("idTeam")) for it in items if it.get("idTeam")]
        logging.info(f"[player] league {league_id}: found {len(ids)} teams")
        return ids
    except Exception as e:
        logging.warning(f"[player] list teams {league_id} -> {e}")
        return []

def list_players_by_team(team_id: str) -> List[Dict]:
    """v2: GET /api/v2/json/list/players/{team_id} -> 'list' of players (lite)"""
    url = f"https://www.thesportsdb.com/api/v2/json/list/players/{team_id}"
    try:
        _pace()
        r = http_get(url, headers=HEADERS)
        items = _arr(safe_json(r) or {})
        # Keep only items that have idPlayer
        players = [p for p in items if p.get("idPlayer")]
        logging.info(f"[player] team {team_id}: {len(players)} players")
        return players
    except Exception as e:
        logging.warning(f"[player] list players {team_id} -> {e}")
        return []

def lookup_player(player_id: str) -> Optional[Dict]:
    """v2: GET /api/v2/json/lookup/player/{player_id} -> 'lookup' array"""
    url = f"https://www.thesportsdb.com/api/v2/json/lookup/player/{player_id}"
    try:
        _pace()
        r = http_get(url, headers=HEADERS)
        arr = _arr(safe_json(r) or {})
        return arr[0] if arr else None
    except Exception as e:
        logging.warning(f"[player] lookup {player_id} -> {e}")
        return None

def build_payload(team_id: str, list_row: Dict, lookup_row: Optional[Dict]) -> Dict:
    """
    Compose a stable payload that includes:
      - idPlayer (key)
      - idTeam (source team id used to list)
      - list_row: raw row from list/players (lite)
      - lookup_player: full detail (optional)
    """
    return {
        "idPlayer": str(list_row.get("idPlayer")),
        "idTeam": str(team_id),
        "list_player": list_row,
        "lookup_player": lookup_row or {},
        "ingested_at": time.time(),
    }

# ---- main --------------------------------------------------------------------

def run_once() -> None:
    produced = 0
    total_teams_scanned = 0
    total_players_seen = 0

    leagues = LEAGUE_IDS[:PLAYER_MAX_LEAGUES] if PLAYER_MAX_LEAGUES > 0 else LEAGUE_IDS
    logging.info(f"[player] starting league scan (count={len(leagues)}), rate ~{PLAYER_RPM} req/min")

    # Discover team IDs across leagues
    team_ids: List[str] = []
    for lid in leagues:
        tids = list_team_ids_by_league(lid)
        if tids:
            team_ids.extend(tids)

    # Deduplicate team IDs (preserve order)
    seen_t, ordered_teams = set(), []
    for tid in team_ids:
        if tid and tid not in seen_t:
            seen_t.add(tid)
            ordered_teams.append(tid)

    if PLAYER_MAX_TEAMS_PER_RUN > 0:
        ordered_teams = ordered_teams[:PLAYER_MAX_TEAMS_PER_RUN]

    logging.info(f"[player] total unique teams to scan: {len(ordered_teams)}")

    # Loop teams -> players
    seen_players: Set[str] = set()
    for team_id in ordered_teams:
        total_teams_scanned += 1
        players = list_players_by_team(team_id)
        if PLAYER_MAX_PLAYERS_PER_TEAM > 0:
            players = players[:PLAYER_MAX_PLAYERS_PER_TEAM]

        for p in players:
            pid = str(p.get("idPlayer") or "")
            if not pid or pid in seen_players:
                continue

            # Global cap
            if PLAYER_MAX_PLAYERS_PER_RUN > 0 and total_players_seen >= PLAYER_MAX_PLAYERS_PER_RUN:
                logging.info(f"[player] reached PLAYER_MAX_PLAYERS_PER_RUN={PLAYER_MAX_PLAYERS_PER_RUN}; stopping")
                break

            seen_players.add(pid)
            total_players_seen += 1

            detail = lookup_player(pid) if PLAYER_FETCH_DETAIL else None
            payload = build_payload(team_id, p, detail)

            producer.produce(
                TOPIC,
                key=pid,
                value=json.dumps(payload),
                callback=delivery_report
            )
            produced += 1

            if produced % FLUSH_EVERY == 0:
                producer.flush()
                logging.info(f"[player] progress: produced={produced} players "
                             f"(teams_scanned={total_teams_scanned}, players_seen={total_players_seen})")

        # If we broke out due to global cap, exit outer loops
        if PLAYER_MAX_PLAYERS_PER_RUN > 0 and total_players_seen >= PLAYER_MAX_PLAYERS_PER_RUN:
            break

    if produced:
        producer.flush()

    logging.info(f"[player] finished. produced={produced}, teams_scanned={total_teams_scanned}, "
                 f"players_seen={total_players_seen}")

def main():
    try:
        run_once()
    except Exception as e:
        logging.exception(f"[player] run failed: {e}")

    if PLAYER_RUN_ONCE:
        logging.info("[player] PLAYER_RUN_ONCE=1 -> exiting after one pass")
        return

    # Low-frequency refresh (reference data rarely changes)
    logging.info(f"[player] sleeping {REFDATA_POLL_SEC}s before next run")
    time.sleep(REFDATA_POLL_SEC)
    main()

if __name__ == "__main__":
    main()
