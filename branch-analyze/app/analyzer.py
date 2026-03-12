"""
analyzer.py — Dead branch detection engine.

Each rule is a standalone function that returns (is_triggered: bool, reason: DeadReason).
Rules are evaluated independently so multiple reasons can apply to one branch.
"""
import logging
from datetime import datetime, timedelta
from typing import List, Tuple, Optional

from app.config import settings
from app.database import BranchStatus, DeadReason, BranchRecord, PullRequestInfo

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Individual rule evaluators
# ---------------------------------------------------------------------------

def rule_no_recent_commits(latest_commit_date: Optional[datetime]) -> Tuple[bool, Optional[DeadReason]]:
    """
    RULE 1: No commits in the last N days (default 180 / 6 months).
    """
    if latest_commit_date is None:
        return True, DeadReason.NO_RECENT_COMMITS
    cutoff = datetime.utcnow() - timedelta(days=settings.dead_no_commit_days)
    return latest_commit_date < cutoff, DeadReason.NO_RECENT_COMMITS


def rule_stale_after_merge(
    last_merged_pr_date: Optional[datetime],
    open_pr_count: int,
) -> Tuple[bool, Optional[DeadReason]]:
    """
    RULE 2: Last action was a merge more than N days ago (default 30)
    AND there are no currently open PRs.
    """
    if open_pr_count > 0:
        return False, DeadReason.MERGED_STALE
    if last_merged_pr_date is None:
        return False, DeadReason.MERGED_STALE
    cutoff = datetime.utcnow() - timedelta(days=settings.dead_merged_pr_days)
    triggered = last_merged_pr_date < cutoff
    return triggered, DeadReason.MERGED_STALE


def rule_old_branch(branch_created_date: Optional[datetime]) -> Tuple[bool, Optional[DeadReason]]:
    """
    RULE 3: Branch is older than N days (default 365 / 1 year).
    """
    if branch_created_date is None:
        return False, DeadReason.OLD_BRANCH
    cutoff = datetime.utcnow() - timedelta(days=settings.dead_branch_age_days)
    return branch_created_date < cutoff, DeadReason.OLD_BRANCH


def rule_fully_merged(is_fully_merged: bool) -> Tuple[bool, Optional[DeadReason]]:
    """
    RULE 4: All commits already exist in the default branch — branch is redundant.
    """
    return is_fully_merged, DeadReason.FULLY_MERGED


def rule_no_open_prs_and_inactive(
    open_pr_count: int,
    latest_commit_date: Optional[datetime],
) -> Tuple[bool, Optional[DeadReason]]:
    """
    RULE 5: No open PRs AND the branch hasn't been updated in 60+ days.
    (Slightly looser than Rule 1 — catches abandoned feature branches
    with no review pathway.)
    """
    if open_pr_count > 0:
        return False, DeadReason.NO_OPEN_PRS
    if latest_commit_date is None:
        return True, DeadReason.NO_OPEN_PRS
    cutoff = datetime.utcnow() - timedelta(days=60)
    return latest_commit_date < cutoff, DeadReason.NO_OPEN_PRS


def rule_author_inactive(
    author_last_activity: Optional[datetime],
) -> Tuple[bool, Optional[DeadReason]]:
    """
    RULE 6: The branch author hasn't committed anywhere in the repo
    for N days (default 90). Strongly suggests abandonment.
    """
    if author_last_activity is None:
        return False, DeadReason.AUTHOR_INACTIVE
    cutoff = datetime.utcnow() - timedelta(days=settings.dead_author_inactive_days)
    return author_last_activity < cutoff, DeadReason.AUTHOR_INACTIVE


def rule_far_behind_default(commits_behind: int) -> Tuple[bool, Optional[DeadReason]]:
    """
    RULE 7: Branch is N+ commits behind the default branch (default 100).
    Indicates a deeply diverged branch unlikely to ever be merged cleanly.
    """
    return (
        commits_behind >= settings.dead_behind_default_commits,
        DeadReason.FAR_BEHIND_DEFAULT,
    )


def rule_single_commit(total_commits: int) -> Tuple[bool, Optional[DeadReason]]:
    """
    RULE 8: Branch has only 1 commit (was never developed beyond creation).
    Combined with age > 30 days this is a strong dead signal.
    """
    return total_commits == 1, DeadReason.SINGLE_COMMIT


# ---------------------------------------------------------------------------
# Rule weights for scoring (higher = stronger signal of being dead)
# ---------------------------------------------------------------------------

RULE_WEIGHTS: dict = {
    DeadReason.FULLY_MERGED: 10,         # Definitive — branch is already in default
    DeadReason.NO_RECENT_COMMITS: 8,     # Very strong signal
    DeadReason.MERGED_STALE: 7,          # Strong signal
    DeadReason.OLD_BRANCH: 5,            # Moderate — old doesn't always mean dead
    DeadReason.AUTHOR_INACTIVE: 5,       # Moderate
    DeadReason.FAR_BEHIND_DEFAULT: 4,    # Moderate — could still be rebased
    DeadReason.NO_OPEN_PRS: 3,           # Weak alone, strong in combination
    DeadReason.SINGLE_COMMIT: 3,         # Weak alone
}

DEAD_SCORE_THRESHOLD = 8  # Minimum weighted score to be classified as DEAD


# ---------------------------------------------------------------------------
# Main classification function
# ---------------------------------------------------------------------------

def classify_branch(record: BranchRecord) -> BranchRecord:
    """
    Run all rules against a populated BranchRecord and set status + dead_reasons.
    Protected branches always get PROTECTED status and skip rule evaluation.
    """
    if record.is_protected:
        record.status = BranchStatus.PROTECTED
        record.dead_reasons = []
        return record

    triggered_reasons: List[DeadReason] = []

    rules = [
        rule_no_recent_commits(record.latest_commit_date),
        rule_stale_after_merge(record.last_merged_pr_date, record.open_pr_count),
        rule_old_branch(record.branch_created_date),
        rule_fully_merged(record.is_fully_merged),
        rule_no_open_prs_and_inactive(record.open_pr_count, record.latest_commit_date),
        rule_author_inactive(record.author_last_activity_repo_wide),
        rule_far_behind_default(record.commits_behind_default),
        rule_single_commit(record.total_commits),
    ]

    for triggered, reason in rules:
        if triggered:
            triggered_reasons.append(reason)

    # Compute weighted dead score
    score = sum(RULE_WEIGHTS.get(r, 1) for r in triggered_reasons)

    if score >= DEAD_SCORE_THRESHOLD:
        record.status = BranchStatus.DEAD
    elif triggered_reasons:
        # Has some signals but below threshold — still flag as dead
        # if even one high-weight reason fired
        high_weight_fired = any(
            RULE_WEIGHTS.get(r, 0) >= 7 for r in triggered_reasons
        )
        record.status = BranchStatus.DEAD if high_weight_fired else BranchStatus.ACTIVE
    else:
        record.status = BranchStatus.ACTIVE

    record.dead_reasons = triggered_reasons
    return record


def describe_dead_reasons(reasons: List[DeadReason]) -> List[str]:
    """Return human-readable explanations for each dead reason."""
    descriptions = {
        DeadReason.NO_RECENT_COMMITS: (
            f"No commits in the last {settings.dead_no_commit_days} days"
        ),
        DeadReason.MERGED_STALE: (
            f"Last action was a merge more than {settings.dead_merged_pr_days} days ago "
            "with no open PRs"
        ),
        DeadReason.OLD_BRANCH: (
            f"Branch is over {settings.dead_branch_age_days} days old"
        ),
        DeadReason.FULLY_MERGED: (
            "All commits already exist in the default branch (fully merged)"
        ),
        DeadReason.NO_OPEN_PRS: (
            "No open pull requests and no commits in 60+ days"
        ),
        DeadReason.AUTHOR_INACTIVE: (
            f"Branch author has not committed anywhere in the repo "
            f"for {settings.dead_author_inactive_days}+ days"
        ),
        DeadReason.FAR_BEHIND_DEFAULT: (
            f"Branch is {settings.dead_behind_default_commits}+ commits behind "
            "the default branch"
        ),
        DeadReason.SINGLE_COMMIT: (
            "Branch has only a single commit — was never developed"
        ),
    }
    return [descriptions.get(r, r.value) for r in reasons]
