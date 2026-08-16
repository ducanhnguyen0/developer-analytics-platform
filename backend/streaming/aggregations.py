from pyspark.sql import DataFrame
from pyspark.sql.functions import window, col, count, approx_count_distinct

from backend.config import settings


def aggregate_by_event_type(parsed_events: DataFrame) -> DataFrame:
    """parsed_events must have columns: type (string), repo_name (string),
    event_time (timestamp)."""

    return (
        parsed_events
        .withWatermark("event_time", settings.SPARK_WATERMARK_DELAY)
        .groupBy(
            window(col("event_time"), settings.SPARK_WINDOW_DURATION),
            col("type").alias("event_type"),
        )
        .agg(
            count("*").alias("event_count"),
            approx_count_distinct("repo_name").alias("distinct_repo_count"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("event_type"),
            col("event_count"),
            col("distinct_repo_count"),
        )
    )


def aggregate_by_repo(parsed_events: DataFrame) -> DataFrame:
    """parsed_events must have columns: repo_name (string), event_time (timestamp)."""

    return (
        parsed_events
        .withWatermark("event_time", settings.SPARK_WATERMARK_DELAY)
        .groupBy(
            window(col("event_time"), settings.SPARK_WINDOW_DURATION),
            col("repo_name"),
        )
        .agg(count("*").alias("event_count"))
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("repo_name"),
            col("event_count"),
        )
    )


def aggregate_by_actor(parsed_events: DataFrame) -> DataFrame:
    """parsed_events must have columns: actor_login (string), event_time (timestamp)."""
    
    return (
        parsed_events
        .withWatermark("event_time", settings.SPARK_WATERMARK_DELAY)
        .groupBy(
            window(col("event_time"), settings.SPARK_WINDOW_DURATION),
            col("actor_login"),
        )
        .agg(count("*").alias("event_count"))
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("actor_login"),
            col("event_count"),
        )
    )
