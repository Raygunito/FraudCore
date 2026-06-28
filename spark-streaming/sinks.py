import logging
from typing import List, Tuple

from pyspark.sql import DataFrame
from pyspark.sql.functions import struct, to_json
from pyspark.sql.streaming import StreamingQuery

log = logging.getLogger("spark-streaming")


def start_sinks(alerts: DataFrame, kafka_bootstrap: str) -> List[Tuple[str, StreamingQuery]]:
    """Start console, Parquet, and Kafka sinks.  Returns a list of (name, query) tuples."""
    log.info("Starting output sinks ")
    queries = []

    # Console
    q_console = (
        alerts.writeStream.outputMode("append")
        .format("console")
        .option("truncate", "false")
        .option("numRows", "50")
        .trigger(processingTime="15 seconds")
        .start()
    )
    queries.append(("console", q_console))
    log.info("Console sink started")

    # Parquet
    q_parquet = (
        alerts.writeStream.outputMode("append")
        .format("parquet")
        .option("path", "/data/alerts/")
        .option("checkpointLocation", "/data/checkpoints/parquet/")
        .trigger(processingTime="15 seconds")
        .start()
    )
    queries.append(("parquet", q_parquet))
    log.info("Parquet sink started : /data/alerts/")

    # Kafka
    q_kafka = (
        alerts.select(to_json(struct("*")).alias("value"))
        .writeStream.outputMode("append")
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("topic", "fraud-alerts")
        .option("checkpointLocation", "/data/checkpoints/kafka/")
        .trigger(processingTime="5 seconds")
        .start()
    )
    queries.append(("kafka", q_kafka))
    log.info("Kafka sink started : topic 'fraud-alerts'")

    return queries
