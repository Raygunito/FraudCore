import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import (DoubleType,StringType,StructField,StructType,)

from pipeline import build_pipeline
from sinks import start_sinks

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s %(asctime)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("spark-streaming")

# replace with localhost if running outside of Docker
KAFKA_BOOTSTRAP = "kafka-broker:29092"

# Rule thresholds
HIGH_VALUE_MIN = 1000.0       
RAPID_COUNT_MIN = 3           
GEO_CITY_MIN = 1              
REPEATED_HIGH_MIN = 3         
CRYPTO_AMOUNT_MIN = 500.0     
VELOCITY_SUM_MIN = 5000.0     

WINDOW_1MIN = "1 minute"
WINDOW_2MIN = "2 minutes"
WATERMARK_DURATION = "1 minute"

# Transaction schema (timestamp is StringType cause of JSON but we parse to TimestampType downstream)
TXN_SCHEMA = StructType(
    [
        StructField("user_id", StringType()),
        StructField("transaction_id", StringType()),
        StructField("amount", DoubleType()),
        StructField("currency", StringType()),
        StructField("timestamp", StringType()),
        StructField("location", StringType()),
        StructField("method", StringType()),
    ]
)

def main() -> None:
    log.info("Starting Spark Streaming")

    spark = (
        SparkSession.builder.appName("FraudCore-Streaming")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.schemaInference", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log.info("SparkSession created  (app: %s)", spark.sparkContext.appName)

    queries = []
    try:
        alerts = build_pipeline(
            spark=spark,
            kafka_bootstrap=KAFKA_BOOTSTRAP,
            txn_schema=TXN_SCHEMA,
            watermark_duration=WATERMARK_DURATION,
            high_value_min=HIGH_VALUE_MIN,
            rapid_count_min=RAPID_COUNT_MIN,
            geo_city_min=GEO_CITY_MIN,
            repeated_high_min=REPEATED_HIGH_MIN,
            crypto_amount_min=CRYPTO_AMOUNT_MIN,
            velocity_sum_min=VELOCITY_SUM_MIN,
            window_1min=WINDOW_1MIN,
            window_2min=WINDOW_2MIN,
        )
        
        queries = start_sinks(alerts, kafka_bootstrap=KAFKA_BOOTSTRAP)
        log.info("All sinks running.")
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        log.warning("Interrupted by user, now stopping")
    except Exception:
        log.exception("Fatal error in streaming pipeline")
        sys.exit(1)
    finally:
        for name, q in queries:
            try:
                q.stop()
                log.info("Stopped %s sink", name)
            except Exception:
                pass
        spark.stop()
        log.info("Spark stopped")


if __name__ == "__main__":
    main()
