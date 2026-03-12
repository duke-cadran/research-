"""
api.py — FastAPI application exposing branch analysis data for the UI.
"""
import asyncio
import logging
from datetime import datetime
from typing import List, Optional, Any, Dict

from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from app.config import settings
from app.database import db, BranchStatus, DeadReason
from app.scanner import Scanner
from app.analyzer import describe_dead_reasons

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Bitbucket Dead Branch Analyzer",
    description="Analyzes Bitbucket branches and identifies stale/dead branches",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

@app.on_event("startup")
async def startup():
    await db.connect()
    logger.info("MongoDB connected")


@app.on_event("shutdown")
async def shutdown():
    await db.disconnect()


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------

class BranchSummary(BaseModel):
    repo_slug: str
    branch_name: str
    status: str
    latest_commit_author: str
    latest_commit_date: Optional[datetime]
    dead_reasons: List[str]
    dead_reason_descriptions: List[str]
    open_pr_count: int
    commits_behind_default: int
    is_fully_merged: bool
    total_commits: int
    last_analyzed: datetime


class StatsResponse(BaseModel):
    total_branches: int
    dead_branches: int
    active_branches: int
    protected_branches: int
    unknown_branches: int
    dead_percentage: float
    repos_scanned: int
    top_dead_reasons: List[Dict[str, Any]]
    last_scan: Optional[datetime]


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def serialize_doc(doc: dict) -> dict:
    """Convert MongoDB document to JSON-serializable dict."""
    doc.pop("_id", None)
    return doc


# ---------------------------------------------------------------------------
# Routes — Scan management
# ---------------------------------------------------------------------------

_scan_lock = asyncio.Lock()
_active_scan_id: Optional[str] = None


@app.post("/api/scans/trigger", summary="Trigger a new full scan")
async def trigger_scan(background_tasks: BackgroundTasks):
    global _active_scan_id
    if _scan_lock.locked():
        raise HTTPException(409, "A scan is already in progress")

    async def _run():
        global _active_scan_id
        async with _scan_lock:
            scanner = Scanner(concurrency=5)
            scan = await scanner.run_full_scan()
            _active_scan_id = scan.run_id

    background_tasks.add_task(_run)
    return {"message": "Scan started", "status": "running"}


@app.get("/api/scans", summary="List all scan runs")
async def list_scans(limit: int = Query(10, le=50)):
    cursor = db.db["scan_runs"].find().sort("started_at", -1).limit(limit)
    scans = [serialize_doc(s) async for s in cursor]
    return scans


@app.get("/api/scans/latest", summary="Get most recent scan run")
async def latest_scan():
    doc = await db.db["scan_runs"].find_one(sort=[("started_at", -1)])
    if not doc:
        raise HTTPException(404, "No scans found")
    return serialize_doc(doc)


@app.get("/api/scans/{run_id}", summary="Get a specific scan run")
async def get_scan(run_id: str):
    doc = await db.db["scan_runs"].find_one({"run_id": run_id})
    if not doc:
        raise HTTPException(404, "Scan not found")
    return serialize_doc(doc)


# ---------------------------------------------------------------------------
# Routes — Branch data
# ---------------------------------------------------------------------------

@app.get("/api/branches", summary="List branches with filtering")
async def list_branches(
    status: Optional[str] = Query(None, description="Filter by status: dead|active|protected"),
    repo: Optional[str] = Query(None, description="Filter by repo slug"),
    reason: Optional[str] = Query(None, description="Filter by dead reason"),
    author: Optional[str] = Query(None, description="Filter by author"),
    search: Optional[str] = Query(None, description="Search branch name"),
    sort_by: str = Query("latest_commit_date", description="Field to sort by"),
    sort_dir: int = Query(-1, description="Sort direction: -1 desc, 1 asc"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, le=200),
):
    query: Dict[str, Any] = {}

    if status:
        query["status"] = status
    if repo:
        query["repo_slug"] = repo
    if reason:
        query["dead_reasons"] = reason
    if author:
        query["latest_commit_author"] = {"$regex": author, "$options": "i"}
    if search:
        query["branch_name"] = {"$regex": search, "$options": "i"}

    skip = (page - 1) * page_size
    total = await db.db["branches"].count_documents(query)
    cursor = (
        db.db["branches"]
        .find(query)
        .sort(sort_by, sort_dir)
        .skip(skip)
        .limit(page_size)
    )

    branches = []
    async for doc in cursor:
        doc = serialize_doc(doc)
        doc["dead_reason_descriptions"] = describe_dead_reasons(
            [DeadReason(r) for r in doc.get("dead_reasons", [])]
        )
        branches.append(doc)

    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "pages": (total + page_size - 1) // page_size,
        "branches": branches,
    }


@app.get("/api/branches/{repo_slug}/{branch_name:path}", summary="Get single branch details")
async def get_branch(repo_slug: str, branch_name: str):
    doc = await db.db["branches"].find_one(
        {"repo_slug": repo_slug, "branch_name": branch_name}
    )
    if not doc:
        raise HTTPException(404, "Branch not found")
    doc = serialize_doc(doc)
    doc["dead_reason_descriptions"] = describe_dead_reasons(
        [DeadReason(r) for r in doc.get("dead_reasons", [])]
    )
    return doc


# ---------------------------------------------------------------------------
# Routes — Statistics / Dashboard
# ---------------------------------------------------------------------------

@app.get("/api/stats", response_model=StatsResponse, summary="Dashboard statistics")
async def get_stats():
    total = await db.db["branches"].count_documents({})
    dead = await db.db["branches"].count_documents({"status": "dead"})
    active = await db.db["branches"].count_documents({"status": "active"})
    protected = await db.db["branches"].count_documents({"status": "protected"})
    unknown = await db.db["branches"].count_documents({"status": "unknown"})

    repos = await db.db["branches"].distinct("repo_slug")

    # Top dead reasons
    pipeline = [
        {"$match": {"status": "dead"}},
        {"$unwind": "$dead_reasons"},
        {"$group": {"_id": "$dead_reasons", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 8},
    ]
    reason_docs = await db.db["branches"].aggregate(pipeline).to_list(length=8)
    top_reasons = [
        {
            "reason": d["_id"],
            "count": d["count"],
            "description": describe_dead_reasons([DeadReason(d["_id"])])[0]
            if d["_id"]
            else "",
        }
        for d in reason_docs
    ]

    # Last scan time
    last_scan_doc = await db.db["scan_runs"].find_one(
        {"status": "completed"}, sort=[("completed_at", -1)]
    )
    last_scan = last_scan_doc.get("completed_at") if last_scan_doc else None

    return StatsResponse(
        total_branches=total,
        dead_branches=dead,
        active_branches=active,
        protected_branches=protected,
        unknown_branches=unknown,
        dead_percentage=round((dead / total * 100) if total else 0, 1),
        repos_scanned=len(repos),
        top_dead_reasons=top_reasons,
        last_scan=last_scan,
    )


@app.get("/api/repos", summary="List all scanned repos with branch counts")
async def list_repos():
    pipeline = [
        {
            "$group": {
                "_id": "$repo_slug",
                "total": {"$sum": 1},
                "dead": {"$sum": {"$cond": [{"$eq": ["$status", "dead"]}, 1, 0]}},
                "active": {"$sum": {"$cond": [{"$eq": ["$status", "active"]}, 1, 0]}},
                "protected": {"$sum": {"$cond": [{"$eq": ["$status", "protected"]}, 1, 0]}},
            }
        },
        {"$sort": {"dead": -1}},
    ]
    repos = await db.db["branches"].aggregate(pipeline).to_list(length=None)
    return [
        {
            "repo_slug": r["_id"],
            "total": r["total"],
            "dead": r["dead"],
            "active": r["active"],
            "protected": r["protected"],
            "dead_pct": round(r["dead"] / r["total"] * 100, 1) if r["total"] else 0,
        }
        for r in repos
    ]


@app.get("/health")
async def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}
