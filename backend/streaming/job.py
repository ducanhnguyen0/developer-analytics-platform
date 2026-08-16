import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp

from backend.config import settings
from backend.streaming.schema import GITHUB_EVENT_SCHEMA
from backend.streaming.aggregations import (
    aggregate_by_event_type,
    aggregate_by_repo,
    aggregate_by_actor,
)
from backend.storage.mongo_client import (
    upsert_event_type_aggregates,
    upsert_repo_aggregates,
    upsert_actor_aggregates,
    record_repos_first_seen,
)


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName(settings.SPARK_APP_NAME)
        .config("spark.sql.shuffle.partitions", "4")  # for small cluster/local run no need for the 200 default
        .config('spark.driver.extraJavaOptions', '-Duser.timezone=UTC')
        .config('spark.executor.extraJavaOptions', '-Duser.timezone=UTC')
        .config('spark.sql.session.timeZone', 'UTC')
        .getOrCreate()
    )


def read_parsed_stream(spark: SparkSession):
    """Reads raw JSON bytes from Kafka and parses them into typed columns
    using GITHUB_EVENT_SCHEMA."""

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", ",".join(settings.KAFKA_BOOTSTRAP_SERVERS))
        .option("subscribe", settings.KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = (
        raw_stream
        .selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), GITHUB_EVENT_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_time", to_timestamp(col("created_at")))
    )
    return parsed


def _write_event_type_batch(batch_df, batch_id: int):
    rows = [row.asDict() for row in batch_df.collect()]
    written = upsert_event_type_aggregates(rows)
    logger.info("[batch %s] event_type_stats: %d row(s) written", batch_id, written)


def _write_repo_batch(batch_df, batch_id: int):
    rows = [row.asDict() for row in batch_df.collect()]
    written = upsert_repo_aggregates(rows)
    repo_names = [row["repo_name"] for row in rows]
    new_repos = record_repos_first_seen(repo_names, seen_at=datetime.now(timezone.utc))
    logger.info("[batch %s] repo_stats: %d row(s) written, %d newly-seen repo(s)",
                batch_id, written, new_repos)


def _write_actor_batch(batch_df, batch_id: int):
    rows = [row.asDict() for row in batch_df.collect()]
    written = upsert_actor_aggregates(rows)
    logger.info("[batch %s] actor_stats: %d row(s) written", batch_id, written)


def run():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    parsed_events = read_parsed_stream(spark)

    event_type_agg = aggregate_by_event_type(parsed_events)
    repo_agg = aggregate_by_repo(parsed_events)
    actor_agg = aggregate_by_actor(parsed_events)

    query1 = (
        event_type_agg.writeStream
        .outputMode("update")
        .foreachBatch(_write_event_type_batch)
        .option("checkpointLocation", f"{settings.SPARK_CHECKPOINT_DIR}/event_type_agg")
        .trigger(processingTime=settings.SPARK_TRIGGER_INTERVAL)
        .start()
    )

    query2 = (
        repo_agg.writeStream
        .outputMode("update")
        .foreachBatch(_write_repo_batch)
        .option("checkpointLocation", f"{settings.SPARK_CHECKPOINT_DIR}/repo_agg")
        .trigger(processingTime=settings.SPARK_TRIGGER_INTERVAL)
        .start()
    )

    query3 = (
        actor_agg.writeStream
        .outputMode("update")
        .foreachBatch(_write_actor_batch)
        .option("checkpointLocation", f"{settings.SPARK_CHECKPOINT_DIR}/actor_agg")
        .trigger(processingTime=settings.SPARK_TRIGGER_INTERVAL)
        .start()
    )

    logger.info("Streaming job started (3 queries: event_type, repo, actor). Waiting for data...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    run()
