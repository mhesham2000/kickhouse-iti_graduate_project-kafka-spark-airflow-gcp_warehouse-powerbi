import os
from datetime import datetime
import pandas as pd
from kafka import KafkaConsumer
import pyarrow as pa
import pyarrow.parquet as pq

BROKER = "34.41.123.167:9094"
TOPICS = [
    "rejected.soccer.event",
    "rejected.soccer.event.highlights",
    "rejected.soccer.venue",
    "rejected.soccer.league",
    "rejected.soccer.broadcast",
    "rejected.soccer.event.lineup",
    "rejected.soccer.event.stats",
    "rejected.soccer.event.timeline",
    "rejected.soccer.live.event.lookup",
    "rejected.soccer.live_score",
    "rejected.soccer.player",
    "rejected.soccer.schedule",
    "rejected.soccer.team"


]
OUTPUT_DIR = "/opt/airflow/dags/data/kafka_invalid"
USERNAME = "clickpipe"
PASSWORD = "clickpipe-secret"

def consume_and_save():
    date_str = datetime.now().strftime("%Y-%m-%d")
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    all_data = []
    for topic in TOPICS:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=BROKER,
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="PLAIN",
            sasl_plain_username=USERNAME,
            sasl_plain_password=PASSWORD,
            auto_offset_reset='earliest',
            consumer_timeout_ms=10000
        )
        for msg in consumer:
            all_data.append({
                "topic": topic,
                "event_time": datetime.utcnow().isoformat(),
                "message": msg.value.decode("utf-8")
            })
        consumer.close()

    if all_data:
        df = pd.DataFrame(all_data)
        table = pa.Table.from_pandas(df)
        file_path = os.path.join(OUTPUT_DIR, f"{date_str}.parquet")
        pq.write_table(table, file_path)
        print(f"Saved {len(df)} records to {file_path}")
    else:
        print("No data found for today.")

if __name__ == "__main__":
    consume_and_save()
