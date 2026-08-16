import json
import logging

from kafka import KafkaProducer
from kafka.errors import KafkaError

from backend.config import settings
from backend.producer.github_client import GitHubEvent, GitHubEventsClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _serialize_event(event: GitHubEvent) -> dict:
    """Trim the raw GitHub payload down to get specific fields."""
    
    return {
        "id": event.id,
        "type": event.type,
        "actor_login": event.actor_login,
        "repo_name": event.repo_name,
        "created_at": event.created_at,
    }


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",  # wait for all in-sync replicas — favors durability
        retries=5,
        linger_ms=50,  # small batching window, still near-real-time
        api_version=(3, 5, 0)
    )


def run():
    producer = build_producer()
    client = GitHubEventsClient()

    published_count = 0
    try:
        for event in client.stream():
            payload = _serialize_event(event)
            try:
                future = producer.send(settings.KAFKA_TOPIC, key=event.type, value=payload)
                future.get(timeout=10)  # block briefly to surface send errors early
                published_count += 1
                if published_count % 10 == 0:
                    logger.info("Published %d events so far (latest: %s on %s)",
                                published_count, event.type, event.repo_name)
            except KafkaError as exc:
                logger.error("Failed to publish event %s: %s", event.id, exc)
    except KeyboardInterrupt:
        logger.info("Shutting down producer (published %d events total).", published_count)
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    run()
