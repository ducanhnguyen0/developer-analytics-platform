
CODE_ACTIVITY = "Code Activity"
DISCUSSION = "Discussion"
SOCIAL_ATTENTION = "Social / Attention"
OTHER = "Other"

CATEGORY_MAP = {
    "PushEvent": CODE_ACTIVITY,
    "PullRequestEvent": CODE_ACTIVITY,
    "PullRequestReviewEvent": CODE_ACTIVITY,
    "PullRequestReviewCommentEvent": CODE_ACTIVITY,
    "CreateEvent": CODE_ACTIVITY,  # new branch/tag/repo
    "DeleteEvent": CODE_ACTIVITY,  # deleted branch/tag

    "IssuesEvent": DISCUSSION,
    "IssueCommentEvent": DISCUSSION,
    "CommitCommentEvent": DISCUSSION,
    "DiscussionEvent": DISCUSSION,
    "DiscussionCommentEvent": DISCUSSION,

    "WatchEvent": SOCIAL_ATTENTION,  # starring a repo
    "ForkEvent": SOCIAL_ATTENTION,
    "MemberEvent": SOCIAL_ATTENTION,  # added as a collaborator
    "PublicEvent": SOCIAL_ATTENTION,  # repo made public
    "SponsorshipEvent": SOCIAL_ATTENTION
}


def categorize(event_type: str) -> str:
    return CATEGORY_MAP.get(event_type, OTHER)
