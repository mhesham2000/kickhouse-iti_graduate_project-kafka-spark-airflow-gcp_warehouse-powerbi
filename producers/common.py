#!/usr/bin/env python3
# producers/common.py
import os
import time
import logging
import random
from typing import Optional, Dict, Any
from dotenv import load_dotenv
from confluent_kafka import Producer
import httpx

# ─── load your .env ───────────────────────────────────────────────────────────
load_dotenv(dotenv_path="config/.env")

# ─── Kafka connection settings ───────────────────────────────────────────────
KAFKA_BOOTSTRAP        = os.environ["KAFKA_BOOTSTRAP"]
KAFKA_SECURITY         = os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
KAFKA_SASL_MECHANISM   = os.environ.get("KAFKA_SASL_MECHANISM",   "")
KAFKA_SASL_USERNAME    = os.environ.get("KAFKA_SASL_USERNAME",    "")
KAFKA_SASL_PASSWORD    = os.environ.get("KAFKA_SASL_PASSWORD",    "")

producer_conf: Dict[str, Any] = {
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "security.protocol": KAFKA_SECURITY,
    "client.id":         "clickpipe-producer"
}

if KAFKA_SECURITY.startswith("SASL"):
    producer_conf.update({
        "sasl.mechanisms": KAFKA_SASL_MECHANISM,
        "sasl.username":   KAFKA_SASL_USERNAME,
        "sasl.password":   KAFKA_SASL_PASSWORD,
    })

producer = Producer(producer_conf)


def delivery_report(err, msg):
    if err:
        logging.error(f"Delivery failed for key={msg.key()}: {err}")
    else:
        logging.info(f"Delivered to {msg.topic()}[{msg.partition()}] at offset {msg.offset()}")


# ─── rest of your common utilities (unchanged) ───────────────────────────────

# API key for TheSportsDB v2
API_KEY = os.environ.get("SPORTSDB_API_KEY", "").strip()
HEADERS: Dict[str, str] = {"X-API-Key": API_KEY} if API_KEY else {}

# League IDs you care about
LEAGUE_IDS = [4792, 5188, 5185, 5180, 5016, 5604, 5054, 5281, 4513, 4404, 4569, 4328, 4335, 4668, 4480, 4738, 4639]

SEASON           = os.environ.get("SEASON") or ""
SEASONS           = os.environ.get("SEASONS") or ""
SCHEDULE_POLL_SEC = int(os.getenv("SCHEDULE_POLL_SEC", 3600))
LIVESCORE_POLL_SEC= int(os.getenv("LIVESCORE_POLL_SEC", 120))
BROADCAST_POLL_SEC= int(os.getenv("BROADCAST_POLL_SEC", 180))
REFDATA_POLL_SEC  = int(os.getenv("REFDATA_POLL_SEC", 43200))

_http = httpx.Client(timeout=30)


def http_get(url: str, headers: Optional[Dict[str, str]] = None, max_retries: int = 3) -> httpx.Response:
    backoff = 1.0
    last_exc: Optional[Exception] = None
    h = headers if headers is not None else HEADERS

    for _ in range(max_retries):
        try:
            r = _http.get(url, headers=h)
            if r.status_code == 404:
                r.raise_for_status()
            if r.status_code == 429:
                sleep_for = backoff + random.uniform(0, 0.5)
                logging.warning(f"429 from {url}; backing off {sleep_for:.1f}s")
                time.sleep(sleep_for)
                backoff *= 2
                continue
            r.raise_for_status()
            return r
        except Exception as e:
            last_exc = e
            sleep_for = backoff + random.uniform(0, 0.5)
            logging.warning(f"GET {url} failed ({e}); retrying in {sleep_for:.1f}s")
            time.sleep(sleep_for)
            backoff *= 2

    raise last_exc  # type: ignore


def safe_json(resp: httpx.Response) -> Optional[Dict[str, Any]]:
    try:
        return resp.json()
    except Exception:
        logging.warning(f"Non-JSON or empty body from {resp.request.url}")
        return None


def slugify(s: str) -> str:
    import re
    return re.sub(r'[^a-z0-9]+', '_', (s or "").strip().lower()).strip('_')
