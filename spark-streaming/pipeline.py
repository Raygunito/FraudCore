import logging
from typing import Tuple

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.functions import (
    approx_count_distinct,
    col,
    count,
    current_timestamp,
    from_json,
    lit,
    struct,
    sum as _sum,
    to_json,
    to_timestamp,
    window,
)
from pyspark.sql.types import StructType, TimestampType

log = logging.getLogger("spark-streaming")


def _alert(
    rule_name: str,
    user_id_col: Column,
    alert_ts_col: Column,
    payload_struct: Column,
) -> Tuple[Column, Column, Column, Column]:
    """Return a 4-tuple of Column expressions for the unified alert schema.

    Parameters
    ----------
    rule_name : str
        Human-readable rule identifier (e.g. "HIGH_VALUE").
    user_id_col : Column
        The user involved.
    alert_ts_col : Column
        Timestamp when the alert was raised.
    payload_struct : Column
        A ``struct`` column : serialised to a JSON string in the final output.
    """
    return (
        lit(rule_name).alias("rule"),
        user_id_col.alias("user_id"),
        alert_ts_col.alias("alert_ts"),
        to_json(payload_struct).alias("details"),
    )


def build_pipeline(
    spark: SparkSession,
    kafka_bootstrap: str,
    txn_schema: StructType,
    watermark_duration: str,
    high_value_min: float,
    rapid_count_min: int,
    geo_city_min: int,
    repeated_high_min: int,
    crypto_amount_min: float,
    velocity_sum_min: float,
    window_1min: str,
    window_2min: str,
) -> DataFrame:
    """Create and return the fraud-detection streaming DataFrame."""
    log.info("Reading from Kafka topic 'transactions' at %s", kafka_bootstrap)

    # Read from Kafka
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", "transactions")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Parse JSON & add typed event-time
    parsed = (
        raw.select(from_json(col("value").cast("string"), txn_schema).alias("data"))
        .select("data.*")
        .withColumn(
            "ts",
            to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX").cast(
                TimestampType()
            ),
        )
        .drop("timestamp")
    )

    wm = parsed.withWatermark("ts", watermark_duration)

    # Rule 1 : HIGH_VALUE
    rule1 = wm.filter(col("amount") > high_value_min).select(
        *_alert(
            "HIGH_VALUE",
            col("user_id"),
            current_timestamp(),
            struct(
                col("amount"),
                col("transaction_id"),
                col("location"),
                col("method"),
                col("ts").alias("event_time"),
            ),
        )
    )

    # Rule 2 : RAPID_ACTIVITY
    rule2_agg = (
        wm.groupBy(window(col("ts"), window_1min), col("user_id"))
        .agg(count("*").alias("tx_count"))
        .filter(col("tx_count") > rapid_count_min)
    )
    rule2 = rule2_agg.select(
        *_alert(
            "RAPID_ACTIVITY",
            col("user_id"),
            col("window.end"),
            struct(
                col("tx_count"),
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
            ),
        )
    )

    # Rule 3 : GEO_ANOMALY
    rule3_agg = (
        wm.groupBy(window(col("ts"), window_1min), col("user_id"))
        .agg(approx_count_distinct("location").alias("city_count"))
        .filter(col("city_count") > geo_city_min)
    )
    rule3 = rule3_agg.select(
        *_alert(
            "GEO_ANOMALY",
            col("user_id"),
            col("window.end"),
            struct(
                col("city_count"),
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
            ),
        )
    )

    # Rule 4 : REPEATED_HIGH_VALUE
    rule4_agg = (
        wm.filter(col("amount") > high_value_min)
        .groupBy(window(col("ts"), window_2min), col("user_id"))
        .agg(count("*").alias("high_count"))
        .filter(col("high_count") >= repeated_high_min)
    )
    rule4 = rule4_agg.select(
        *_alert(
            "REPEATED_HIGH_VALUE",
            col("user_id"),
            col("window.end"),
            struct(
                col("high_count"),
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
            ),
        )
    )

    # Rule 5 : CRYPTO_RISK
    rule5 = wm.filter(
        (col("method") == "crypto") & (col("amount") > crypto_amount_min)
    ).select(
        *_alert(
            "CRYPTO_RISK",
            col("user_id"),
            current_timestamp(),
            struct(
                col("amount"),
                col("transaction_id"),
                col("location"),
                col("ts").alias("event_time"),
            ),
        )
    )

    # Rule 6 : VELOCITY_SPIKE
    rule6_agg = (
        wm.groupBy(window(col("ts"), window_1min), col("user_id"))
        .agg(_sum("amount").alias("total_spent"))
        .filter(col("total_spent") > velocity_sum_min)
    )
    rule6 = rule6_agg.select(
        *_alert(
            "VELOCITY_SPIKE",
            col("user_id"),
            col("window.end"),
            struct(
                col("total_spent"),
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
            ),
        )
    )

    alerts = (
        rule1.unionByName(rule2)
        .unionByName(rule3)
        .unionByName(rule4)
        .unionByName(rule5)
        .unionByName(rule6)
    )

    log.info("Pipeline has been built")
    return alerts
