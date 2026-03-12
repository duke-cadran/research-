"""
scanner.py — Orchestrates scanning all repos/branches in a Bitbucket project.
Fetches data from Bitbucket, populates BranchRecord objects, runs analysis,
and persists results to MongoDB.
"""
import asyncio
import logging
import uuid
from datetime import datetime
from typing import List, Optional

from app.bitbucket_client import BitbucketClient, BitbucketAPIError
from app.config import settings
from app.database import BranchRecord, ScanRun, db, BranchStatus, PullRequestInfo
from app.analyzer import classify_branch

logger = logging.getLogger(__name__)


class Scanner:
    def __init__(self, concurrency: int = 5):
        self.concurrency = concurrency
        self._semaphore = asyncio.Semaphore(concurrency)

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    async def run_full_scan(self, project_key: Optional[str] = None) -> ScanRun:
        project_key = project_key or settings.bitbucket_project_key
        run_id = str(uuid.uuid4())
        scan = ScanRun(
            run_id=run_id,
            project_key=project_key,
            started_at=datetime.utcnow(),
        )
        await db.db["scan_runs"].insert_one(scan.model_dump())
        logger.info("Starting scan run %s for project %s", run_id, project_key)

        try:
            async with BitbucketClient() as client:
                repos = await client.list_repos(project_key)
                scan.repos_scanned = len(repos)

                tasks = [
                    self._scan_repo(client, project_key, repo["slug"], run_id)
                    for repo in repos
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in results:
                if isinstance(result, Exception):
                    scan.errors.append(str(result))
                else:
                    branches: List[BranchRecord] = result
                    scan.branches_scanned += len(branches)
                    scan.dead_branches_found += sum(
                        1 for b in branches if b.status == BranchStatus.DEAD
                    )
                    scan.active_branches_found += sum(
                        1 for b in branches if b.status == BranchStatus.ACTIVE
                    )
                    scan.protected_branches_found += sum(
                        1 for b in branches if b.status == BranchStatus.PROTECTED
                    )

            scan.status = "completed"
        except Exception as e:
            scan.status = "failed"
            scan.errors.append(str(e))
            logger.exception("Scan run %s failed", run_id)

        scan.completed_at = datetime.utcnow()
        await db.db["scan_runs"].replace_one(
            {"run_id": run_id}, scan.model_dump(), upsert=True
        )
        logger.info(
            "Scan run %s complete — %d branches, %d dead",
            run_id,
            scan.branches_scanned,
            scan.dead_branches_found,
        )
        return scan

    # ------------------------------------------------------------------
    # Per-repo scanning
    # ------------------------------------------------------------------

    async def _scan_repo(
        self,
        client: BitbucketClient,
        project_key: str,
        repo_slug: str,
        run_id: str,
    ) -> List[BranchRecord]:
        async with self._semaphore:
            logger.info("Scanning repo %s", repo_slug)
            try:
                default_branch = await client.get_default_branch(project_key, repo_slug)
                branches_raw = await client.list_branches(project_key, repo_slug)
            except BitbucketAPIError as e:
                logger.error("Could not list branches for %s: %s", repo_slug, e)
                raise

            records: List[BranchRecord] = []
            branch_tasks = [
                self._analyze_branch(
                    client, project_key, repo_slug, branch, default_branch, run_id
                )
                for branch in branches_raw
            ]

            branch_results = await asyncio.gather(*branch_tasks, return_exceptions=True)
            for res in branch_results:
                if isinstance(res, Exception):
                    logger.warning("Branch analysis error in %s: %s", repo_slug, res)
                else:
                    records.append(res)

            # Bulk upsert all records for this repo
            if records:
                await self._upsert_branches(records)

            logger.info(
                "Repo %s: %d branches (%d dead)",
                repo_slug,
                len(records),
                sum(1 for r in records if r.status == BranchStatus.DEAD),
            )
            return records

    # ------------------------------------------------------------------
    # Per-branch analysis
    # ------------------------------------------------------------------

    async def _analyze_branch(
        self,
        client: BitbucketClient,
        project_key: str,
        repo_slug: str,
        branch_raw: dict,
        default_branch: str,
        run_id: str,
    ) -> BranchRecord:
        branch_name = branch_raw.get("displayId", "")
        latest_commit_raw = branch_raw.get("latestCommit") or {}

        # Latest commit hash and timestamp from the branch listing
        latest_hash = branch_raw.get("latestCommit", "")
        if isinstance(latest_hash, dict):
            # Some Bitbucket versions nest this differently
            latest_hash = latest_hash.get("id", "")

        # Build partial record
        record = BranchRecord(
            repo_slug=repo_slug,
            project_key=project_key,
            branch_name=branch_name,
            display_id=branch_name,
            latest_commit_hash=latest_hash if isinstance(latest_hash, str) else "",
            latest_commit_message="",
            latest_commit_author="",
            latest_commit_author_email="",
            scan_run_id=run_id,
            is_protected=settings.is_protected_branch(branch_name),
        )

        # Skip deep analysis for protected branches
        if record.is_protected:
            return classify_branch(record)

        # Fetch recent commits for this branch
        try:
            commits = await client.get_commits(project_key, repo_slug, branch_name, limit=50)
        except BitbucketAPIError:
            commits = []

        if commits:
            latest = commits[0]
            record.total_commits = len(commits)
            record.latest_commit_message = (latest.get("message") or "")[:200]

            author = latest.get("author") or {}
            record.latest_commit_author = author.get("name") or author.get("displayName") or ""
            record.latest_commit_author_email = author.get("emailAddress") or ""

            ts = latest.get("authorTimestamp") or latest.get("committerTimestamp")
            record.latest_commit_date = BitbucketClient.parse_timestamp(ts)

            # Estimate branch creation from oldest commit in our sample
            oldest = commits[-1]
            old_ts = oldest.get("authorTimestamp") or oldest.get("committerTimestamp")
            record.branch_created_date = BitbucketClient.parse_timestamp(old_ts)

        # Check how far behind default branch
        if branch_name != default_branch:
            record.commits_behind_default = await client.get_commits_behind(
                project_key, repo_slug, branch_name, default_branch
            )
            record.is_fully_merged = await client.is_branch_merged(
                project_key, repo_slug, branch_name, default_branch
            )

        # Pull request data
        try:
            prs_raw = await client.get_pull_requests(
                project_key, repo_slug, branch_name, state="ALL"
            )
        except BitbucketAPIError:
            prs_raw = []

        open_prs = [p for p in prs_raw if p.get("state") == "OPEN"]
        merged_prs = sorted(
            [p for p in prs_raw if p.get("state") == "MERGED"],
            key=lambda p: p.get("updatedDate", 0),
            reverse=True,
        )

        record.open_pr_count = len(open_prs)

        if merged_prs:
            last_pr_raw = merged_prs[0]
            record.last_merged_pr_date = BitbucketClient.parse_timestamp(
                last_pr_raw.get("updatedDate")
            )
            record.last_pr = PullRequestInfo(
                pr_id=last_pr_raw.get("id", 0),
                title=(last_pr_raw.get("title") or "")[:200],
                state=last_pr_raw.get("state", ""),
                created_date=BitbucketClient.parse_timestamp(
                    last_pr_raw.get("createdDate")
                ) or datetime.utcnow(),
                updated_date=BitbucketClient.parse_timestamp(
                    last_pr_raw.get("updatedDate")
                ) or datetime.utcnow(),
                author=(
                    (last_pr_raw.get("author") or {})
                    .get("user", {})
                    .get("displayName", "")
                ),
            )

        # Author activity across the whole repo
        if record.latest_commit_author_email:
            record.author_last_activity_repo_wide = (
                await client.get_author_last_commit_date(
                    project_key, repo_slug, record.latest_commit_author_email
                )
            )

        # Run classification rules
        return classify_branch(record)

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    async def _upsert_branches(self, records: List[BranchRecord]):
        for record in records:
            await db.db["branches"].update_one(
                {
                    "repo_slug": record.repo_slug,
                    "branch_name": record.branch_name,
                },
                {"$set": record.model_dump()},
                upsert=True,
            )
