import logging

from pymongo import ASCENDING, DESCENDING

from backend.storage.mongo_client import (
    get_event_type_stats_collection,
    get_repo_stats_collection,
    get_actor_stats_collection,
    get_repo_first_seen_collection,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def create_indexes():
    event_type_stats = get_event_type_stats_collection()
    event_type_stats.create_index([("window_start", ASCENDING)], name="window_idx")

    repo_stats = get_repo_stats_collection()
    repo_stats.create_index([("window_start", ASCENDING)], name="window_idx")
    repo_stats.create_index([("repo_name", ASCENDING)], name="repo_idx")

    actor_stats = get_actor_stats_collection()
    actor_stats.create_index([("window_start", ASCENDING)], name="window_idx")
    actor_stats.create_index([("actor_login", ASCENDING)], name="actor_idx")

    repo_first_seen = get_repo_first_seen_collection()
    repo_first_seen.create_index([("first_seen_at", DESCENDING)], name="first_seen_idx")
    repo_first_seen.create_index([("repo_name", ASCENDING)], name="repo_idx", unique=True)

    logger.info("event_type_stats indexes: %s", list(event_type_stats.index_information().keys()))
    logger.info("repo_stats indexes: %s", list(repo_stats.index_information().keys()))
    logger.info("actor_stats indexes: %s", list(actor_stats.index_information().keys()))
    logger.info("repo_first_seen indexes: %s", list(repo_first_seen.index_information().keys()))


if __name__ == "__main__":
    create_indexes()
