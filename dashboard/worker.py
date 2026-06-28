import json
import logging
import os
import sqlite3
import time
from pathlib import Path
from typing import Any

from kafka import KafkaConsumer
from kafka.serializer import Deserializer

#  Config
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "fraud-alerts"
DB_PATH = Path(__file__).parent / "alerts.db"
MAX_ROWS = 50_000  # keep DB from growing indefinitely

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("worker")

class JSONDeserializer(Deserializer):
    def deserialize(self, _: Any, data: bytes) -> dict[str, Any]:
        return json.loads(data.decode("utf-8"))



def init_db() -> None:
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")  # better concurrent read perf
    conn.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            rule      TEXT    NOT NULL,
            user_id   TEXT    NOT NULL,
            alert_ts  TEXT    NOT NULL,
            details   TEXT    NOT NULL,   -- JSON string
            received  TEXT    NOT NULL    -- when the worker received it
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_rule ON alerts(rule)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_alert_ts ON alerts(alert_ts)")
    conn.commit()
    conn.close()
    log.info("Database ready: %s", DB_PATH)


def insert_batch(rows: list[tuple[str, str, str, str, str]]) -> None:
    """Insert a batch of alerts and prune old rows if needed."""
    conn = sqlite3.connect(str(DB_PATH))
    conn.executemany(
        "INSERT INTO alerts (rule, user_id, alert_ts, details, received) "
        "VALUES (?, ?, ?, ?, ?)",
        rows,
    )
    # prune
    count = conn.execute("SELECT COUNT(*) FROM alerts").fetchone()[0]
    if count > MAX_ROWS:
        excess = count - MAX_ROWS + 500
        conn.execute(f"DELETE FROM alerts WHERE id IN (SELECT id FROM alerts ORDER BY id LIMIT {excess})")
    conn.commit()
    conn.close()



def main():
    init_db()

    log.info("Connecting to Kafka at %s / %s", KAFKA_BOOTSTRAP, TOPIC)
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=JSONDeserializer(),
        auto_offset_reset="latest",
        group_id="dashboard-worker",
        enable_auto_commit=True,
    )
    log.info("Connected, now consuming messages")

    batch = []
    total = 0
    last_flush = time.time()

    try:
        for msg in consumer:
            alert = msg.value
            received = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            batch.append((
                alert.get("rule", "?"),
                alert.get("user_id", "?"),
                alert.get("alert_ts", ""),
                alert.get("details", "{}"),
                received,
            ))
            total += 1

            # flush every 100 msgs or every 2 seconds
            if len(batch) >= 100 or (batch and time.time() - last_flush > 2):
                insert_batch(batch)
                if total % 500 == 0:
                    log.info("Inserted total %s alerts in DB", total)
                batch.clear()
                last_flush = time.time()

    except KeyboardInterrupt:
        log.info("Shutting down")
    finally:
        if batch:
            insert_batch(batch)
        consumer.close()
        log.info("Worker stopped, %s alerts written", total)


if __name__ == "__main__":
    main()
