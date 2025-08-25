# TheSportsDB Streaming Stack (Kafka → Spark → Airflow/Grafana)

**Date:** 2025-08-23

This repo ingests **TheSportsDB v2** soccer data into Kafka via Python producers, validates/filters with a **Spark Structured Streaming** job, and orchestrates + monitors with **Airflow + Grafana**.

---

## 🧭 Repo Map (key parts)

```
.
├─ Dockerfile                         # Custom Spark image (bitnami/spark base)
├─ docker-compose.yaml                # Airflow + Kafka + Spark + Grafana stack
├─ .env                               # Global env (copied/mounted by services)
├─ config/.env                        # Loaded by producers/common.py
├─ producers/                         # Python producers (imported by Airflow)
│  ├─ common.py                       # Kafka client, HTTP client, env knobs
│  ├─ create_topics.py                # Admin client topic provisioning
│  ├─ livescore_producer.py           # → soccer.live_score (2m cadence via DAG)
│  ├─ broadcast_producer.py           # → soccer.broadcast (2m cadence via DAG)
│  ├─ schedule_producer.py            # → soccer.schedule (nightly)
│  ├─ event_producer.py               # → soccer.event (nightly)
│  ├─ event_lookup_producer.py        # → soccer.live.event.lookup (2m)
│  ├─ event_details_producer.py       # → soccer.event.(stats|timeline|...)
│  ├─ team_producer.py                # → soccer.team (nightly)
│  ├─ player_producer.py              # → soccer.player (nightly)
│  └─ venue_producer.py               # → soccer.venue (nightly)
├─ spark/
│  └─ jobs/
│     ├─ validate_json.py             # streaming validator/router
│     └─ spark_submit_command.txt     # last working spark-submit
└─ airflow/
   ├─ Dockerfile                      # Custom Airflow image
   ├─ requirements.txt                # Airflow extras (kafka, httpx, etc.)
   ├─ dags/
   │  ├─ live_score_dag.py            # 2m PythonOperator
   │  ├─ broadcast_dag.py             # 2m PythonOperator
   │  ├─ event_lookup_dag.py          # 2m; live score → lookup
   │  ├─ scheduale_dag.py             # 00:00 Africa/Cairo
   │  ├─ event_dag.py                 # 00:00 Africa/Cairo
   │  ├─ event_details_dag.py         # 00:00 Africa/Cairo
   │  ├─ league_dag.py                # 00:00 Africa/Cairo
   │  ├─ team_dag.py                  # 00:00 Africa/Cairo
   │  ├─ player_dag.py                # 00:00 Africa/Cairo
   │  ├─ venue_dag.py                 # 00:00 Africa/Cairo
   │  ├─ save_invalid_topics_as_paquert.py # 06:00 save rejected.* → Parquet
   │  └─ data/kafka_invalid/          # daily Parquet dumps
   └─ scripts/
      └─ consume_kafka.py             # reads rejected.* and writes Parquet
```

> **Ports**: Airflow UI → `http://localhost:9050`, Grafana → `http://localhost:3000`

---

## 1) Kafka + Python Producers (Section 1)

**Topics (base)**

- `soccer.live_score`, `soccer.broadcast`, `soccer.schedule`, `soccer.event`, `soccer.live.event.lookup`
- `soccer.event.stats`, `soccer.event.timeline`, `soccer.event.highlights`, `soccer.event.lineup`
- `soccer.team`, `soccer.player`, `soccer.venue`, `soccer.league`

Downstream Spark writes to mirrors: `validated.soccer.*` and `rejected.soccer.*`.

**Environment** (loaded from `config/.env` by `producers/common.py`):

- Kafka: `KAFKA_BOOTSTRAP`, `KAFKA_SECURITY_PROTOCOL`, `KAFKA_SASL_*` (if used)
- SportsDB: `SPORTSDB_API_KEY`, cadence knobs: `LIVESCORE_POLL_SEC`, `SCHEDULE_POLL_SEC`, `REFDATA_POLL_SEC`, etc.
- Scope: `LEAGUE_IDS`, `SEASON` (single) or `SEASONS` (comma‑sep)

**Create topics**

```bash
# inside repo root (with config/.env set)
python -m producers.create_topics --dim-policy compact
```

---

## 2) Spark — JSON Validation & Routing (Section 2)

**What it does**

- Consumes `soccer.*` (excluding `validated.*`/`rejected.*`), validates JSON into strict schemas, computes PKs, payload hashes, sport filters, and routes to **`validated.soccer.<name>`** or **`rejected.soccer.<name>`**. Two writeStreams total.

**Run it (your two supported ways)**

**A. Inside the `spark-master` container (interactive, your workflow)**

```bash
docker compose up -d   # ensure cluster is up
docker exec -it spark-master bash

# paste the command from spark/jobs/spark_submit_command.txt
/opt/bitnami/spark/bin/spark-submit   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.2,org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.4.2   /opt/bitnami/spark/app/validate_json.py
```

> The job is streaming (runs forever). Keep the shell open or detach the TTY.

**B. Detached container (optional)**

Build the custom Spark image and run streaming validator in the background:

```bash
docker build -t yourorg/spark-custom:3.4.2 -f Dockerfile .

docker run -d --name spark-validator --restart unless-stopped   -e KAFKA_BOOTSTRAP="$KAFKA_BOOTSTRAP"   -e CHK_ROOT="/opt/bitnami/spark/checkpoints/validator"   -v "$(pwd)/spark/jobs:/opt/bitnami/spark/app:ro"   yourorg/spark-custom:3.4.2   spark-submit     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.2,org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.4.2     /opt/bitnami/spark/app/validate_json.py
```

---

## 3) Airflow + Grafana Monitoring (Section 3)

**Airflow service**

- Built from `./airflow/Dockerfile` (custom image with providers & deps)
- Exposed at **`http://localhost:9050`**
- Mounts producers at `/opt/airflow/producers` (DAGs import them in‑proc)
- Starts with `airflow standalone` (auto‑generates an admin password)

**Start + get password (your note)**

```bash
docker compose up
# then, in another terminal
docker exec -it airflow bash
cat simpl   # press <TAB> twice to auto-complete the file name, then Enter
# the password will appear
```

**DAGs**

- **2‑minute loops**: `broadcast_dag`, `live_score_dag`, `event_lookup_dag` (task order: live_score → live_lookup)  Each uses **PythonOperator** and emails failures to `shinetym@gmail.com`.
- **Nightly (00:00 Africa/Cairo)**: `venue_proucer_daily_dag`, `team_proucer_daily_dag`, `schedual_proucer_daily_dag`, `player_proucer_daily_dag`, `league_proucer_daily_dag`, `event_proucer_daily_dag`, `event_stats_daily_dag`.
- **Triage (06:00)**: `save_rejected_topics_as_parquet_daily` — **BashOperator** running `python /opt/airflow/scripts/consume_kafka.py`, producing Parquet under `airflow/dags/data/kafka_invalid/` for Grafana/analysis.

**Grafana**

- Comes up via compose on **`http://localhost:3000`**. SMTP creds/alerts are configured in compose (redact real values before commit if needed).
- Point panels to the daily Parquet (`/opt/airflow/dags/data/kafka_invalid/*.parquet`) or to Prometheus if you enable exporters.

---

## 4) Quick Start

```bash
# 1) Build images
docker build -t yourorg/spark-custom:3.4.2 -f Dockerfile .
docker build -t thesportsdb-airflow:latest -f ./airflow/Dockerfile ./airflow

# 2) Configure env
cp config/.env.example config/.env   # if you keep an example file
# set SPORTSDB_API_KEY, KAFKA_BOOTSTRAP, SASL creds, LEAGUE_IDS, SEASON/SEASONS

# 3) Boot the stack
docker compose up -d

# 4) Create Kafka topics (once)
python -m producers.create_topics --dim-policy compact

# 5) Start Spark validator (choose A or B above)

# 6) Open Airflow (http://localhost:9050) → enable DAGs

# 7) Watch rejected Parquet (daily)
ls airflow/dags/data/kafka_invalid
```

---

## 5) Frequently Used Commands

```bash
# kcat tail
kcat -b "$KAFKA_BOOTSTRAP" -t soccer.live_score -C -o -5

# Airflow logs (container)
docker logs -f airflow

# Spark validator logs (if using detached container)
docker logs -f spark-validator
```

---

## 6) Notes & Safety

- Do **not** commit real secrets (`SPORTSDB_API_KEY`, SMTP, SASL passwords). Use `.env` files and compose secrets.- If both `live_score_dag` and `event_lookup_dag` are enabled, you might **double‑call** live score — prefer the combined DAG (`event_lookup_dag`).

---

## 7) Credits

- TheSportsDB v2 API
- Apache Kafka / Spark / Airflow / Grafana

---

## 8) Implementation — Kafka TLS/SASL secrets on **GCP VM**

The steps below main purpose is to rebuild TLS material from zero and stand up one working **SASL_SSL** listener that `kcat` / ClickPipe can reach **directly via your own VM's public static IP**. The VM has to have a **reachable public static IP!** In my case, I used a GCP VM as an example. Adjust to your case accordingly

**ClickPipes service** from ClickHouse Cloud needs the following setup in order to connect to your Kafka broker properly and subscribe to the topics. So make sure you get it right! **A technically better alternative** is to use a message broker service directly from any cloud provider instead of the local dockerized Kafka services and setting up their certificates + exposing the IPs//ports, but where's the fun and learning experience in that? :'D

Run each numbered block in order. Stop after any step that errors and keep the exact output.

### 🗒️ Assumptions

- **Public static IP** of your GCP VM → export as `$GCP_PUBLIC_IP` (e.g., `34.41.123.167`)
- **External TLS port** exposed by the broker → `$KAFKA_SSL_PORT` (e.g., `9094`)
- **One password** used for keystore/truststore/PKCS12 while testing → `m3troctyKafka2025` (change later)
- All TLS files live in `./secrets/` (already volume-mounted by `docker-compose.yml`)

```bash
export TLS_DIR=$(pwd)/secrets
mkdir -p "$TLS_DIR"
cd "$TLS_DIR"

export GCP_PUBLIC_IP=34.41.123.167     # <-- set yours
export KAFKA_SSL_PORT=9094             # <-- set yours
```

### 1) Create a tiny CA (key + cert)

```bash
# CA private key
openssl genrsa -out tsdb-ca.key 4096

# Self-signed CA certificate (10 years)
openssl req -x509 -new -nodes -key tsdb-ca.key   -sha256 -days 3650   -subj "/C=US/L=NYC/O=Metrocity/CN=TheSportsDB-Local-CA"   -out tsdb-ca.pem
```

### 2) Generate **broker key + CSR** with **SAN = IP:${GCP_PUBLIC_IP}**

> Use an IP SAN (not DNS) because clients will connect to your VM’s **IP**. If you later attach a DNS name, add both entries like `subjectAltName = IP:… , DNS:broker.example.com`.

```bash
# 2.1 openssl config snippet with IP SAN
cat > san.cnf <<'EOF'
[ req ]
distinguished_name = dn
req_extensions     = san
prompt             = no
[ dn ]
CN = __WILL_BE_REPLACED__
O  = Metrocity
L  = NYC
C  = US
[ san ]
subjectAltName = IP:__IP__
EOF

# replace placeholders with your values
sed -i "s/__WILL_BE_REPLACED__/${GCP_PUBLIC_IP}/" san.cnf
sed -i "s/__IP__/${GCP_PUBLIC_IP}/" san.cnf

# 2.2 Private key for broker
openssl genrsa -out broker.key 4096

# 2.3 CSR
openssl req -new -key broker.key -out broker.csr -config san.cnf
```

### 3) Sign the CSR with the CA

```bash
openssl x509 -req   -in  broker.csr   -CA   tsdb-ca.pem   -CAkey tsdb-ca.key   -CAcreateserial   -out  broker.pem   -days 825   -sha256   -extfile san.cnf   -extensions san
```

Build the **full chain** most clients expect:

```bash
cat broker.pem tsdb-ca.pem > broker-full.pem
```

### 4) Build **keystore** (`kafka.keystore.jks`) and **truststore** (`kafka.truststore.jks`)

**4.1 Keystore** (contains broker **key**, leaf cert & CA)

```bash
# Convert to PKCS12
openssl pkcs12 -export   -inkey broker.key   -in  broker.pem   -certfile tsdb-ca.pem   -name broker   -out broker.p12   -passout pass:m3troctyKafka2025

# Import PKCS12 → JKS
keytool -importkeystore   -destkeystore kafka.keystore.jks   -deststorepass m3troctyKafka2025   -srckeystore  broker.p12   -srcstoretype PKCS12   -srcstorepass m3troctyKafka2025   -alias broker   -noprompt
```

**4.2 Truststore** (contains only your CA)

```bash
keytool -import   -alias tsdb-ca   -file tsdb-ca.pem   -keystore kafka.truststore.jks   -storepass m3troctyKafka2025   -noprompt
```

**4.3 Sanity**

```bash
keytool -list -v -keystore kafka.keystore.jks   -storepass m3troctyKafka2025 | grep Alias
keytool -list -v -keystore kafka.truststore.jks -storepass m3troctyKafka2025 | grep Alias
# expect: keystore alias 'broker', truststore alias 'tsdb-ca'
```

### 5) Point the **broker** at the new files

Ensure your compose/env sets (example):

```yaml
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT,EXTERNAL:SASL_SSL
KAFKA_ADVERTISED_LISTENERS: INTERNAL://broker:9092,EXTERNAL://${GCP_PUBLIC_IP}:${KAFKA_SSL_PORT}
KAFKA_LISTENERS: INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:${KAFKA_SSL_PORT}
KAFKA_SSL_KEYSTORE_LOCATION:   /etc/kafka/secrets/kafka.keystore.jks
KAFKA_SSL_KEYSTORE_PASSWORD:   m3troctyKafka2025
KAFKA_SSL_TRUSTSTORE_LOCATION: /etc/kafka/secrets/kafka.truststore.jks
KAFKA_SSL_TRUSTSTORE_PASSWORD: m3troctyKafka2025
KAFKA_SSL_CLIENT_AUTH: required
KAFKA_SASL_ENABLED_MECHANISMS: PLAIN
KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL: PLAIN
KAFKA_AUTHORIZER_CLASS_NAME: kafka.security.authorizer.AclAuthorizer
```

Mount the directory **read‑only**:

```yaml
- ./secrets:/etc/kafka/secrets:ro
```

> Replace the `${...}` placeholders with your values or interpolate them from the environment.

### 6) Restart only the broker

```bash
docker compose stop broker
docker compose up -d broker
```

Wait ~10 s, then check logs:

```bash
docker compose logs -f broker | egrep -i 'TLS|SASL|LISTENER|READY'
```

You should see the EXTERNAL listener binding on `0.0.0.0:$KAFKA_SSL_PORT` and advertising `$GCP_PUBLIC_IP:$KAFKA_SSL_PORT`.

### 7) Test locally with OpenSSL

```bash
openssl s_client -connect 127.0.0.1:${KAFKA_SSL_PORT}   -CAfile secrets/tsdb-ca.pem -verify_return_error -quiet
# Ctrl-C to exit if no errors printed
```

### 8) Test remotely with `kcat`

```bash
kcat -b ${GCP_PUBLIC_IP}:${KAFKA_SSL_PORT}      -X security.protocol=SASL_SSL      -X sasl.mechanisms=PLAIN      -X sasl.username=clickpipe      -X sasl.password=clickpipe-secret      -X ssl.ca.location=secrets/tsdb-ca.pem      -L | head
```

If you see cluster metadata, TLS + SASL works. If you get certificate errors, ensure the **SAN contains `IP:${GCP_PUBLIC_IP}`** and that you pointed `ssl.ca.location` to **the CA** (`tsdb-ca.pem`), not the leaf.

### 9) Create kafka_server_jaas.conf file at project root directory
Use the same SASL password you used in the previous steps, in my case it's `clickpipe-secret`. Following is an example of how it looks like:
```
KafkaServer {
  org.apache.kafka.common.security.plain.PlainLoginModule required
    user_clickpipe="clickpipe-secret";
};
```

### 10) Configure ClickPipe (or other clients)

- **Bootstrap:** `${GCP_PUBLIC_IP}:${KAFKA_SSL_PORT}`
- **Protocol:** `SASL_SSL`
- **Username / Password:** `clickpipe` / `clickpipe-secret`
- **Root CA:** upload `tsdb-ca.pem` (just the CA, *not* broker.pem)
- Leave client-cert/key blank unless you enable mutual TLS.


---

### 11) ClickHouse Warehouse Setup — DDLs, MVs & One‑Time Backfill

This section shows how to stand up the **DW schema** and seed it from the `default.raw_*` staging tables populated by ClickPipes. It uses the DDLs included in this repo: `ClickHouse Data Warehouse Creation + Backfill DDLs.txt` (create DB/tables/MVs/view + backfill statements).

### A. Preconditions
- ClickPipes (or your ingest job) writes **validated** rows into `default.raw_*` tables (soccer-only).
- Your ClickHouse server/Cloud endpoint and credentials are ready.
- You are OK with **ReplacingMergeTree** in staging and DW tables where we want **latest state per key**.

### B. Run the DDLs (creates DB/tables/MVs/view)
You can run the entire file in one go via **clickhouse-client** or via the **HTTP** interface.

**Option 1 — clickhouse-client (secure connection)**
```bash
# rename the DDL file if you like; here we use the file as-is
CLICKHOUSE_HOST="<your-hostname>"    # e.g. <cluster>.clickhouse.cloud
CLICKHOUSE_PORT="9440"               # TLS port (cloud default)
CLICKHOUSE_USER="<user>"
CLICKHOUSE_PASSWORD="<password>"

clickhouse client \
  --host "$CLICKHOUSE_HOST" \
  --secure \
  --port "$CLICKHOUSE_PORT" \
  --user "$CLICKHOUSE_USER" \
  --password "$CLICKHOUSE_PASSWORD" \
  --multiquery < "ClickHouse Data Warehouse Creation + Backfill DDLs.txt"
```

**Option 2 — HTTP (curl)**
```bash
CLICKHOUSE_HTTP="https://<your-hostname>:443"   # adjust port if needed
curl -sS -u "$CLICKHOUSE_USER:$CLICKHOUSE_PASSWORD" \
  --data-binary @"ClickHouse Data Warehouse Creation + Backfill DDLs.txt" \
  "$CLICKHOUSE_HTTP/?database=default&enable_sql_parser=1"
```

> The script will: create `dw` database; create **dimensions** (Type‑1 SCD via ReplacingMergeTree on `updated_at`), **facts**, **materialized views** from `default.raw_*`, a **hub view** (`dw.v_fact_event_latest`), and **one-time backfills**.

### C. Modeling highlights (what the script implements)
- **Dimensions**: `dw.dim_league`, `dw.dim_team`, `dw.dim_player`, `dw.dim_venue`, `dw.dim_channel` (Type‑1 SCD with `ReplacingMergeTree(updated_at)`; deterministic `*_sk` via `cityHash64` on natural keys).
- **Facts**: event, snapshot, stat, timeline, lineup, broadcast, highlight (append-friendly MergeTrees; `fact_event` uses a **SharedReplacingMergeTree** so the **latest** row per event wins by `updated_at`).
- **Views**: `dw.v_fact_event_latest` provides **latest state per event** using `argMax` by `updated_at`, exposing `scheduled_date` for calendar joins.
- **MVs**: all `dw.mv_*` stream **soccer-only** rows from `default.raw_*` into the DW tables. (If you later change SELECT lists, drop & recreate the respective MV.)

### D. One‑time backfill
The DDL file includes **INSERT … SELECT** statements to seed the DW from existing `default.raw_*` (soccer scope). Run the whole file once; future updates will arrive via the MVs automatically.

### E. Verifications
```sql
-- counts after backfill
SELECT count() FROM dw.dim_league;
SELECT count() FROM dw.dim_team;
SELECT count() FROM dw.fact_event;

-- latest-per-event sanity
SELECT idEvent, home_score, away_score, updated_at_latest
FROM dw.v_fact_event_latest
ORDER BY updated_at_latest DESC
LIMIT 10;

-- soccer-only constraint example
SELECT DISTINCT lowerUTF8(trim(strSport)) FROM default.raw_event;
```

### F. Power BI DirectQuery targets
For fresh visuals, point DirectQuery to:
- `dw.v_fact_event_latest` (latest factual state)
- `dw.dim_league`, `dw.dim_team`, `dw.dim_player`, `dw.dim_venue`, `dw.dim_channel`
For historical deep dives, use `dw.fact_event_snapshot`, `dw.fact_event_stat`, etc.

### G. Why ReplacingMergeTree here?
- In **staging** (raw) and in **dw.fact_event**/**dims**, `ReplacingMergeTree` (with proper `ORDER BY` and a version column like `updated_at`) lets merges keep the **latest state per key** without expensive batch dedup jobs—ideal for live scores and evolving event details.
- Use `FINAL` judiciously on very small tables; prefer **views/MVs** (like `v_fact_event_latest`) for “latest” semantics on bigger ones.
