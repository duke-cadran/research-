"""
database.py — MongoDB models, indexes, and async Motor connection
"""
from datetime import datetime
from typing import Optional, List
from enum import Enum

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import IndexModel, ASCENDING, DESCENDING
from pydantic import BaseModel, Field

from app.config import settings


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class BranchStatus(str, Enum):
    ACTIVE = "active"
    DEAD = "dead"
    PROTECTED = "protected"
    UNKNOWN = "unknown"


class DeadReason(str, Enum):
    NO_RECENT_COMMITS = "no_commits_6_months"
    MERGED_STALE = "last_action_merge_30_days"
    OLD_BRANCH = "branch_over_1_year"
    FULLY_MERGED = "all_commits_in_default"
    NO_OPEN_PRS = "no_open_prs_and_inactive"
    AUTHOR_INACTIVE = "author_inactive_90_days"
    FAR_BEHIND_DEFAULT = "100_plus_commits_behind_default"
    SINGLE_COMMIT = "single_commit_never_developed"


# ---------------------------------------------------------------------------
# Pydantic models (also used as MongoDB document shapes)
# ---------------------------------------------------------------------------

class CommitInfo(BaseModel):
    hash: str
    message: str
    author_name: str
    author_email: str
    timestamp: datetime


class PullRequestInfo(BaseModel):
    pr_id: int
    title: str
    state: str  # OPEN | MERGED | DECLINED
    created_date: datetime
    updated_date: datetime
    author: str


class BranchRecord(BaseModel):
    # Identity
    repo_slug: str
    project_key: str
    branch_name: str
    display_id: str

    # Commit metadata
    latest_commit_hash: str
    latest_commit_message: str
    latest_commit_author: str
    latest_commit_author_email: str
    latest_commit_date: Optional[datetime] = None

    # Branch metadata
    branch_created_date: Optional[datetime] = None  # estimated from first commit
    total_commits: int = 0
    commits_behind_default: int = 0
    is_fully_merged: bool = False

    # PR metadata
    open_pr_count: int = 0
    last_pr: Optional[PullRequestInfo] = None
    last_merged_pr_date: Optional[datetime] = None

    # Author activity
    author_last_activity_repo_wide: Optional[datetime] = None

    # Analysis results
    status: BranchStatus = BranchStatus.UNKNOWN
    dead_reasons: List[DeadReason] = []
    is_protected: bool = False

    # Housekeeping
    last_analyzed: datetime = Field(default_factory=datetime.utcnow)
    scan_run_id: str = ""


class ScanRun(BaseModel):
    run_id: str
    project_key: str
    started_at: datetime
    completed_at: Optional[datetime] = None
    repos_scanned: int = 0
    branches_scanned: int = 0
    dead_branches_found: int = 0
    active_branches_found: int = 0
    protected_branches_found: int = 0
    errors: List[str] = []
    status: str = "running"  # running | completed | failed


# ---------------------------------------------------------------------------
# Database connection singleton
# ---------------------------------------------------------------------------

class Database:
    client: Optional[AsyncIOMotorClient] = None
    db = None

    @classmethod
    async def connect(cls):
        cls.client = AsyncIOMotorClient(settings.mongo_uri)
        cls.db = cls.client[settings.mongo_db_name]
        await cls._ensure_indexes()

    @classmethod
    async def disconnect(cls):
        if cls.client:
            cls.client.close()

    @classmethod
    async def _ensure_indexes(cls):
        branches = cls.db["branches"]
        await branches.create_indexes([
            IndexModel([("repo_slug", ASCENDING), ("branch_name", ASCENDING)], unique=True),
            IndexModel([("status", ASCENDING)]),
            IndexModel([("project_key", ASCENDING)]),
            IndexModel([("latest_commit_date", DESCENDING)]),
            IndexModel([("last_analyzed", DESCENDING)]),
            IndexModel([("scan_run_id", ASCENDING)]),
            IndexModel([("dead_reasons", ASCENDING)]),
            IndexModel([("repo_slug", ASCENDING), ("status", ASCENDING)]),
        ])

        scans = cls.db["scan_runs"]
        await scans.create_indexes([
            IndexModel([("run_id", ASCENDING)], unique=True),
            IndexModel([("started_at", DESCENDING)]),
            IndexModel([("project_key", ASCENDING)]),
        ])


db = Database()
