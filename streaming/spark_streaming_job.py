"""PySpark Structured Streaming job consuming from Kafka and writing to multiple sinks."""
import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, count, avg, sum as spark_sum,
    current_timestamp, to_timestamp, lit
)
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

from transforms.data_cleaner import clean_events
from transforms.feature_extractor import extract_features
from sinks.s3_sink import write_to_s3
from sinks.postgres_sink import write_to_postgres
from sinks.kafka_sink import write_to_kafka

logger = logging.getLogger(__name__)

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("page_url", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("country", StringType(), True),
    StructField("content_id", StringType(), True),
    StructField("duration_ms", LongType(), True),
])


def create_spark_session(app_name: str = "RealTimeEventPipeline") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "12")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints")
        .getOrCreate()
    )


def run_streaming_job(
    kafka_brokers: str,
    input_topic: str,
    output_s3_path: str,
    output_topic: str,
    checkpoint_location: str,
):
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    logger.info(f"Starting Spark Structured Streaming | brokers={kafka_brokers} | topic={input_topic}")

    # Read from Kafka
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers)
        .option("subscribe", input_topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 10000)
        .load()
    )

    # Deserialize and parse events
    events = (
        raw_stream
        .selectExpr("CAST(value AS STRING) as json_value", "timestamp as kafka_timestamp")
        .withColumn("data", from_json(col("json_value"), EVENT_SCHEMA))
        .select("data.*", "kafka_timestamp")
        .withColumn("event_time", to_timestamp(col("timestamp")))
    )

    # Clean and validate
    clean_stream = clean_events(events)

    # Feature extraction
    enriched_stream = extract_features(clean_stream)

    # Windowed aggregations (5-minute tumbling window)
    agg_stream = (
        enriched_stream
        .withWatermark("event_time", "10 minutes")
        .groupBy(
            window(col("event_time"), "5 minutes"),
            col("event_type"),
            col("device_type"),
            col("country"),
        )
        .agg(
            count("event_id").alias("event_count"),
            avg("duration_ms").alias("avg_duration_ms"),
            spark_sum("duration_ms").alias("total_duration_ms"),
        )
    )

    # Write to S3 (Parquet, micro-batch)
    s3_query = write_to_s3(
        enriched_stream,
        path=output_s3_path,
        checkpoint_path=f"{checkpoint_location}/s3",
        trigger_interval="60 seconds",
    )

    # Write aggregations to PostgreSQL
    pg_query = write_to_postgres(
        agg_stream,
        table="event_aggregations",
        checkpoint_path=f"{checkpoint_location}/postgres",
    )

    # Write enriched events to output Kafka topic
    kafka_query = write_to_kafka(
        enriched_stream,
        topic=output_topic,
        brokers=kafka_brokers,
        checkpoint_path=f"{checkpoint_location}/kafka_out",
    )

    logger.info("All streaming queries started. Awaiting termination...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PySpark Kafka Streaming Job")
    parser.add_argument("--kafka-brokers", default="localhost:9092")
    parser.add_argument("--input-topic", default="raw-events")
    parser.add_argument("--output-s3", default="s3://my-bucket/streaming/events/")
    parser.add_argument("--output-topic", default="processed-events")
    parser.add_argument("--checkpoint", default="s3://my-bucket/checkpoints/")
    args = parser.parse_args()

    run_streaming_job(
        kafka_brokers=args.kafka_brokers,
        input_topic=args.input_topic,
        output_s3_path=args.output_s3,
        output_topic=args.output_topic,
        checkpoint_location=args.checkpoint,
    )
