import json
import logging
import random
import time
from datetime import datetime, timezone
from itertools import count

from faker import Faker
from kafka import KafkaProducer

import fraud_rules

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s %(asctime)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("producer")

#  Config
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "transactions"

USERS = [f"u{i}" for i in range(1000, 2000)]
METHODS = ["credit_card", "debit_card", "paypal", "crypto"]
CURRENCIES = ["EUR", "USD", "GBP"]

SLEEP_MIN = 0.01
SLEEP_MAX = 0.10

#  Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
    linger_ms=20,
    batch_size=32 * 1024,
    acks=1,
)

fake = Faker()
_tx_seq = count(1)

#  transaction builder


def _now():
    return datetime.now(timezone.utc).isoformat(
        timespec="milliseconds"
    ).replace("+00:00", "Z")


def build_tx(**overrides):
    """Return a transaction dict.  Callers override fields by name."""
    return {
        "user_id":       overrides.get("user_id", random.choice(USERS)),
        "transaction_id": f"t-{next(_tx_seq):07d}",
        "amount":         overrides.get("amount", round(random.uniform(5, 1000), 2)),
        "currency":       random.choice(CURRENCIES),
        "timestamp":      _now(),
        "location":       overrides.get("location", fake.city()),
        "method":         overrides.get("method", random.choice(METHODS)),
    }

#  Main Loop


def run():
    log.info("Producer started => topic: transactions")
    sent = 0
    legit = 0
    fraud = 0

    try:
        while True:
            batch, pattern = fraud_rules.generate_batch(build_tx, USERS)

            for e in batch:
                producer.send(TOPIC, key=e["user_id"], value=e)
                sent += 1
                if pattern == "legitimate":
                    legit += 1
                else:
                    fraud += 1

            if sent % 100 == 0:
                log.info("%s sent | %s legit | %s fraud", f"{sent:,}", legit, fraud)

            time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))

    except KeyboardInterrupt:
        log.warning("Shutting down ...")
    finally:
        producer.flush(timeout=10)
        producer.close(timeout=5)
        log.info("FINAL => %s sent | %s legit | %s fraud", f"{sent:,}", legit, fraud)
        log.info("Producer stopped")


if __name__ == "__main__":
    run()
