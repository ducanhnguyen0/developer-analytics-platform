import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv(usecwd=True))  # loads variables from a .env file in the project root if present


# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
).split(",")

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "github-events")


# MongoDB
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "dev_analytics")
MONGO_COLLECTION_RAW = os.getenv("MONGO_COLLECTION_RAW", "raw_events")
MONGO_COLLECTION_EVENT_TYPE_STATS = os.getenv("MONGO_COLLECTION_EVENT_TYPE_STATS", "event_type_stats")
MONGO_COLLECTION_REPO_STATS = os.getenv("MONGO_COLLECTION_REPO_STATS", "repo_stats")
MONGO_COLLECTION_ACTOR_STATS = os.getenv("MONGO_COLLECTION_ACTOR_STATS", "actor_stats")
MONGO_COLLECTION_REPO_FIRST_SEEN = os.getenv("MONGO_COLLECTION_REPO_FIRST_SEEN", "repo_first_seen")
MONGO_SHARD_KEY_FIELD = os.getenv("MONGO_SHARD_KEY_FIELD", "event_type")


# GitHub Events API
GITHUB_EVENTS_URL = os.getenv("GITHUB_EVENTS_URL", "https://api.github.com/events")
GITHUB_POLL_INTERVAL_SECONDS = int(os.getenv("GITHUB_POLL_INTERVAL_SECONDS", "60"))
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")


# Spark Structured Streaming
SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "developer-analytics-platform")
SPARK_WINDOW_DURATION = os.getenv("SPARK_WINDOW_DURATION", "1 minute")
SPARK_WATERMARK_DELAY = os.getenv("SPARK_WATERMARK_DELAY", "2 minutes")
SPARK_TRIGGER_INTERVAL = os.getenv("SPARK_TRIGGER_INTERVAL", "30 seconds")
SPARK_CHECKPOINT_DIR = os.getenv("SPARK_CHECKPOINT_DIR", "/tmp/dap-checkpoints")


# Dashboard
DASHBOARD_REFRESH_SECONDS = int(os.getenv("DASHBOARD_REFRESH_SECONDS", "5"))
DASHBOARD_LOOKBACK_MINUTES = int(os.getenv("DASHBOARD_LOOKBACK_MINUTES", "30"))
