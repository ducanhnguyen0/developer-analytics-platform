from collections import defaultdict
from datetime import datetime, timedelta, timezone

from backend.config import settings
from backend.service.categories import categorize
from backend.storage.mongo_client import (
    get_event_type_stats_collection,
    get_repo_stats_collection,
    get_actor_stats_collection,
    get_repo_first_seen_collection,
)


def _since(lookback_minutes: int = None) -> datetime:
    lookback_minutes = lookback_minutes or settings.DASHBOARD_LOOKBACK_MINUTES
    return datetime.now(timezone.utc) - timedelta(minutes=lookback_minutes)


# Event type volume
def get_event_counts_over_time(lookback_minutes: int = None) -> list[dict]:
    """Time series of event counts per minute, broken down by event type.
    Powers the main 'activity over time' chart."""

    collection = get_event_type_stats_collection()
    cursor = collection.find(
        {"window_start": {"$gte": _since(lookback_minutes)}},
        {"_id": 0, "window_start": 1, "event_type": 1, "event_count": 1},
    ).sort("window_start", 1)
    return list(cursor)


def get_top_event_types(lookback_minutes: int = None, limit: int = 10) -> list[dict]:
    """Total events per type over the lookback window, most active first."""

    collection = get_event_type_stats_collection()
    pipeline = [
        {"$match": {"window_start": {"$gte": _since(lookback_minutes)}}},
        {"$group": {"_id": "$event_type", "total_events": {"$sum": "$event_count"}}},
        {"$sort": {"total_events": -1}},
        {"$limit": limit},
        {"$project": {"_id": 0, "event_type": "$_id", "total_events": 1}},
    ]
    return list(collection.aggregate(pipeline))


def get_total_event_count(lookback_minutes: int = None) -> int:
    """Single number: total events processed in the lookback window —
    a simple 'pipeline is alive and healthy' indicator for the dashboard."""

    collection = get_event_type_stats_collection()
    pipeline = [
        {"$match": {"window_start": {"$gte": _since(lookback_minutes)}}},
        {"$group": {"_id": None, "total": {"$sum": "$event_count"}}},
    ]
    result = list(collection.aggregate(pipeline))
    return result[0]["total"] if result else 0


# Repos: top (all-time-in-window) + trending (recent vs prior)
def get_top_active_repos(lookback_minutes: int = None, limit: int = 10) -> list[dict]:
    """Most active repositories over the whole lookback window, by raw
    total count."""

    collection = get_repo_stats_collection()
    pipeline = [
        {"$match": {"window_start": {"$gte": _since(lookback_minutes)}}},
        {"$group": {"_id": "$repo_name", "total_events": {"$sum": "$event_count"}}},
        {"$sort": {"total_events": -1}},
        {"$limit": limit},
        {"$project": {"_id": 0, "repo_name": "$_id", "total_events": 1}},
    ]
    return list(collection.aggregate(pipeline))


def get_trending_repos(lookback_minutes: int = None, limit: int = 10) -> list[dict]:
    """Repos with the sharpest INCREASE in activity, comparing the more
    recent half of the lookback window to the earlier half."""

    lookback_minutes = lookback_minutes or settings.DASHBOARD_LOOKBACK_MINUTES
    now = datetime.now(timezone.utc)
    midpoint = now - timedelta(minutes=lookback_minutes / 2)
    window_start_floor = now - timedelta(minutes=lookback_minutes)

    collection = get_repo_stats_collection()
    docs = collection.find(
        {"window_start": {"$gte": window_start_floor}},
        {"_id": 0, "window_start": 1, "repo_name": 1, "event_count": 1},
    )

    recent_counts: dict[str, int] = defaultdict(int)
    prior_counts: dict[str, int] = defaultdict(int)
    for doc in docs:
        bucket = recent_counts if doc["window_start"] >= midpoint else prior_counts
        bucket[doc["repo_name"]] += doc["event_count"]

    trending = []
    for repo_name, recent in recent_counts.items():
        prior = prior_counts.get(repo_name, 0)
        if prior == 0:
            trending.append({"repo_name": repo_name, "recent_events": recent,
                              "prior_events": prior, "change": "new"})
        else:
            pct_change = round((recent - prior) / prior * 100, 1)
            trending.append({"repo_name": repo_name, "recent_events": recent,
                              "prior_events": prior, "change": f"{pct_change:+.1f}%"})

    # Sort: brand-new repos first (most novel signal), then by absolute growth in event count, descending.
    trending.sort(key=lambda r: (r["change"] != "new", -(r["recent_events"] - r["prior_events"])))
    return trending[:limit]


# New / emerging repos
def get_new_repos(lookback_minutes: int = None, limit: int = 10) -> list[dict]:
    """Repos first observed within the lookback window."""

    collection = get_repo_first_seen_collection()
    cursor = (
        collection.find(
            {"first_seen_at": {"$gte": _since(lookback_minutes)}},
            {"_id": 0, "repo_name": 1, "first_seen_at": 1},
        )
        .sort("first_seen_at", -1)
        .limit(limit)
    )
    return list(cursor)


# Contributors
def get_top_contributors(lookback_minutes: int = None, limit: int = 10) -> list[dict]:
    """Most active contributors (actors) over the lookback window."""

    collection = get_actor_stats_collection()
    pipeline = [
        {"$match": {"window_start": {"$gte": _since(lookback_minutes)}}},
        {"$group": {"_id": "$actor_login", "total_events": {"$sum": "$event_count"}}},
        {"$sort": {"total_events": -1}},
        {"$limit": limit},
        {"$project": {"_id": 0, "actor_login": "$_id", "total_events": 1}},
    ]
    return list(collection.aggregate(pipeline))


# Activity category breakdown
def get_activity_by_category(lookback_minutes: int = None) -> list[dict]:
    """Buckets event-type volume into Code Activity / Discussion /
    Social-Attention / Other."""
    
    per_type = get_top_event_types(lookback_minutes=lookback_minutes, limit=1000)

    totals: dict[str, int] = defaultdict(int)
    for row in per_type:
        category = categorize(row["event_type"])
        totals[category] += row["total_events"]

    return [
        {"category": category, "total_events": total}
        for category, total in sorted(totals.items(), key=lambda kv: -kv[1])
    ]
