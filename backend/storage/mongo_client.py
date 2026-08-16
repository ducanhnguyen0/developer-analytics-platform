from functools import lru_cache
from datetime import timezone
from pymongo import MongoClient, UpdateOne
from pymongo.collection import Collection
from pymongo.database import Database

from backend.config import settings


@lru_cache(maxsize=1)
def get_client() -> MongoClient:
    """A single cached client is reused across the process."""
    return MongoClient(
        settings.MONGO_URI,
        tz_aware=True,
        tzinfo=timezone.utc,
    )


def get_db() -> Database:
    return get_client()[settings.MONGO_DB_NAME]


def get_event_type_stats_collection() -> Collection:
    return get_db()[settings.MONGO_COLLECTION_EVENT_TYPE_STATS]


def get_repo_stats_collection() -> Collection:
    return get_db()[settings.MONGO_COLLECTION_REPO_STATS]


def get_actor_stats_collection() -> Collection:
    return get_db()[settings.MONGO_COLLECTION_ACTOR_STATS]


def get_repo_first_seen_collection() -> Collection:
    return get_db()[settings.MONGO_COLLECTION_REPO_FIRST_SEEN]


def _upsert(collection: Collection, rows: list[dict], *key_fields: str, on_insert_only: bool = False) -> int:
    """Idempotently writes with upsert operation."""

    if not rows:
        return 0
    operator = "$setOnInsert" if on_insert_only else "$set"
    operations = [
        UpdateOne(
            {field: row[field] for field in key_fields}, 
            {operator: row}, 
            upsert=True
        ) for row in rows
    ]
    result = collection.bulk_write(operations, ordered=False)
    if on_insert_only:
        return result.upserted_count
    return result.upserted_count + result.modified_count


def upsert_event_type_aggregates(rows: list[dict]) -> int:
    """Idempotently writes windowed event-type aggregates."""
    return _upsert(get_event_type_stats_collection(), rows, "window_start", "event_type")

def upsert_repo_aggregates(rows: list[dict]) -> int:
    """Same idempotent-upsert pattern, keyed on (window_start, repo_name)."""
    return _upsert(get_repo_stats_collection(), rows, "window_start", "repo_name")

def upsert_actor_aggregates(rows: list[dict]) -> int:
    """Same idempotent-upsert pattern, keyed on (window_start, actor_login)."""
    return _upsert(get_actor_stats_collection(), rows, "window_start", "actor_login")

def record_repos_first_seen(repo_names: list[str], seen_at) -> int:
    """Records the first time each repo appears, WITHOUT overwriting an
    existing first_seen_at on repeat sightings."""
    
    rows = [{"repo_name": name, "first_seen_at": seen_at} for name in set(repo_names)]
    return _upsert(get_repo_first_seen_collection(), rows, "repo_name", on_insert_only=True)
