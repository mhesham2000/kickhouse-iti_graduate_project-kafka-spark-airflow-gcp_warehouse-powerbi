#!/usr/bin/env python3
# producers/create_topics.py
import sys
import argparse
import os
from dotenv import load_dotenv
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException

# ─── load .env ───────────────────────────────────────────────────────────────
load_dotenv(dotenv_path="config/.env")

# ─── Kafka connection settings ───────────────────────────────────────────────
KAFKA_BOOTSTRAP      = os.environ["KAFKA_BOOTSTRAP"]
KAFKA_SECURITY       = os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
KAFKA_SASL_MECHANISM = os.environ.get("KAFKA_SASL_MECHANISM", "")
KAFKA_SASL_USERNAME  = os.environ.get("KAFKA_SASL_USERNAME", "")
KAFKA_SASL_PASSWORD  = os.environ.get("KAFKA_SASL_PASSWORD", "")

def parse_args():
    ap = argparse.ArgumentParser(description="Create Kafka topics for clickpipe soccer pipeline")
    ap.add_argument("--rf",      type=int, default=1, help="Replication factor")
    ap.add_argument("--timeout", type=int, default=10, help="Admin call timeout (s)")
    ap.add_argument("--dim-policy",
                    choices=["compact", "delete"],
                    default="compact",
                    help="Cleanup policy for dimension topics")
    return ap.parse_args()


def main():
    args = parse_args()

    admin_conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "security.protocol": KAFKA_SECURITY,
    }
    if KAFKA_SECURITY.startswith("SASL"):
        admin_conf.update({
            "sasl.mechanisms": KAFKA_SASL_MECHANISM,
            "sasl.username":   KAFKA_SASL_USERNAME,
            "sasl.password":   KAFKA_SASL_PASSWORD,
        })

    admin = AdminClient(admin_conf)

    # Use *either* compact or delete, not both
    ref_cleanup = "compact" if args.dim_policy == "compact" else "delete"

    # ─── define topics ────────────────────────────────────────────────────────
    topics_spec = [
        # transient streams
        {
            "name": "soccer.live_score",
            "partitions": 6, "rf": args.rf,
            "config": {
                "retention.ms":  "43200000",   # 12h
                "cleanup.policy":"delete",
                "segment.ms":    "3600000"
            }
        },
        {
            "name": "soccer.schedule",
            "partitions": 3, "rf": args.rf,
            "config": {
                "retention.ms":  "604800000",  # 7d
                "cleanup.policy":"delete",
                "segment.ms":    "86400000"
            }
        },
        {
            "name": "soccer.broadcast",
            "partitions": 3, "rf": args.rf,
            "config": {
                "retention.ms":  "86400000",   # 24h
                "cleanup.policy":"delete",
                "segment.ms":    "3600000"
            }
        },

        # NEW: live event lookup (treat as reference by idEvent → compact)
        {
            "name": "soccer.live.event.lookup",
            "partitions": 3, "rf": args.rf,
            "config": {
                "cleanup.policy": ref_cleanup,  # default compact
                "min.cleanable.dirty.ratio": "0.1",
                "segment.ms": "86400000",
                "delete.retention.ms": "86400000"
            }
        },

        # split-out stats (reference by key)
        *[
            {
                "name": f"soccer.event.{sub}",
                "partitions": 3, "rf": args.rf,
                "config": {
                    "cleanup.policy": ref_cleanup,
                    "min.cleanable.dirty.ratio": "0.1",
                    "segment.ms": "86400000",
                    "delete.retention.ms": "86400000"
                }
            }
            for sub in ("stats", "timeline", "highlights", "lineup")
        ],

        # other dimensions
        *[
            {
                "name": f"soccer.{dim}",
                "partitions": 3, "rf": args.rf,
                "config": {
                    "cleanup.policy": ref_cleanup,
                    "min.cleanable.dirty.ratio": "0.1",
                    "segment.ms": "86400000",
                    "delete.retention.ms": "86400000"
                }
            }
            for dim in ("team", "league", "venue", "event", "player")
        ],
    ]

    # validated/rejected for each
    base = [
        "broadcast","event",
        "event.stats","event.timeline","event.highlights","event.lineup",
        "schedule","live_score","team","league","venue","player",
        "live.event.lookup"  # ← NEW
    ]
    for t in base:
        for kind in ("validated","rejected"):
            topics_spec.append({
                "name": f"{kind}.soccer.{t}",
                "partitions": 3, "rf": args.rf,
                "config": {
                    "retention.ms":  "604800000",  # 7d
                    "cleanup.policy":"delete"
                }
            })

    # ─── check / create ───────────────────────────────────────────────────────
    try:
        md = admin.list_topics(timeout=args.timeout)
    except KafkaException as e:
        print(f"ERROR: cannot reach Kafka: {e}", file=sys.stderr)
        sys.exit(1)

    existing = set(md.topics.keys())
    to_create = []
    for spec in topics_spec:
        if spec["name"] not in existing:
            print(f"→ will create {spec['name']}")
            to_create.append(NewTopic(
                topic=spec["name"],
                num_partitions=spec["partitions"],
                replication_factor=spec["rf"],
                config=spec["config"],
            ))
        else:
            print(f"✔ already exists: {spec['name']}")

    if to_create:
        futures = admin.create_topics(to_create, request_timeout=args.timeout)
        had_err = False
        for name, f in futures.items():
            try:
                f.result()
                print(f"✅ created {name}")
            except Exception as e:
                print(f"❌ failed {name}: {e}", file=sys.stderr)
                had_err = True
        if had_err:
            sys.exit(2)
    else:
        print("Nothing to do.")

if __name__ == "__main__":
    main()
