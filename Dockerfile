# ────────────────────────────────────────────────────────────
# Spark 3.4.2 + JSON-only Kafka Streaming
# ────────────────────────────────────────────────────────────
FROM bitnami/spark:3.4.2

USER root
RUN mkdir -p /opt/bitnami/spark/app /opt/bitnami/spark/jars-ext

# ───── Your job(s) go here ─────
COPY spark/jobs/ /opt/bitnami/spark/app/

USER 1001
