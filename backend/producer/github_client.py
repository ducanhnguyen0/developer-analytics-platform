import logging
import time
from dataclasses import dataclass
from typing import Iterator, Optional

import requests

from backend.config import settings

logger = logging.getLogger(__name__)


@dataclass
class GitHubEvent:
    id: str
    type: str
    actor_login: str
    repo_name: str
    created_at: str
    raw: dict  # full original payload


class GitHubEventsClient:
    """Polls the GitHub public Events API and yields new events since the
    last poll, respecting the API's rate limit via conditional requests."""

    def __init__(self, poll_interval_seconds: Optional[int] = None):
        self.url = settings.GITHUB_EVENTS_URL
        self.poll_interval = poll_interval_seconds or settings.GITHUB_POLL_INTERVAL_SECONDS
        self._etag: Optional[str] = None
        self._last_seen_id: Optional[int] = None

        self._session = requests.Session()
        headers = {"Accept": "application/vnd.github+json"}
        if settings.GITHUB_TOKEN:
            headers["Authorization"] = f"Bearer {settings.GITHUB_TOKEN}"
        self._session.headers.update(headers)

    def _fetch_once(self) -> list[dict]:
        """Fetch the current page of events. Returns [] if nothing changed
        (304) or on a transient error."""

        headers = {}
        if self._etag:
            headers["If-None-Match"] = self._etag

        try:
            resp = self._session.get(self.url, headers=headers, timeout=15)
        except requests.RequestException as exc:
            logger.warning("GitHub request failed: %s", exc)
            return []

        remaining = resp.headers.get("X-RateLimit-Remaining")
        logger.debug("GitHub rate limit remaining: %s", remaining)

        if resp.status_code == 304:
            logger.debug("No new data (304 Not Modified)")
            return []

        if resp.status_code != 200:
            logger.warning("Unexpected status %s from GitHub: %s", resp.status_code, resp.text[:200])
            return []

        self._etag = resp.headers.get("ETag")
        return resp.json()

    def _to_domain_event(self, raw: dict) -> Optional[GitHubEvent]:
        try:
            return GitHubEvent(
                id=raw["id"],
                type=raw["type"],
                actor_login=raw.get("actor", {}).get("login", "unknown"),
                repo_name=raw.get("repo", {}).get("name", "unknown"),
                created_at=raw["created_at"],
                raw=raw,
            )
        except (KeyError, TypeError):
            logger.debug("Skipping malformed event: %s", raw)
            return None

    def poll_new_events(self) -> list[GitHubEvent]:
        """Single poll: returns only events not already returned by a
        previous call to this method."""

        raw_events = self._fetch_once()
        if not raw_events:
            return []

        new_events = []
        max_id_seen = self._last_seen_id
        for raw in raw_events:
            event_id = int(raw["id"])
            if self._last_seen_id is not None and event_id <= self._last_seen_id:
                continue
            event = self._to_domain_event(raw)
            if event:
                new_events.append(event)
            if max_id_seen is None or event_id > max_id_seen:
                max_id_seen = event_id

        self._last_seen_id = max_id_seen
        new_events.reverse()  # GitHub returns newest-first so reverse to publish oldest-first to replay order downstream
        return new_events

    def stream(self) -> Iterator[GitHubEvent]:
        """Continuously polls forever, sleeping between polls, yielding new
        events as they're found. This is what the producer's main loop uses."""

        logger.info(
            "Starting GitHub Events poll loop (interval=%ss, url=%s)",
            self.poll_interval, self.url,
        )
        while True:
            for event in self.poll_new_events():
                yield event
            time.sleep(self.poll_interval)
