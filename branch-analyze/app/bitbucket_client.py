"""
bitbucket_client.py — Async Bitbucket Server REST API 1.0 client
Handles pagination, retries, and rate-limit backoff automatically.
"""
import asyncio
import logging
from datetime import datetime
from typing import AsyncGenerator, Dict, List, Optional, Any

import httpx
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from app.config import settings

logger = logging.getLogger(__name__)

BB_API = f"{settings.bitbucket_base_url.rstrip('/')}/rest/api/1.0"
PAGE_LIMIT = 100  # max items per page for Bitbucket Server


class BitbucketAPIError(Exception):
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        super().__init__(f"Bitbucket API {status_code}: {message}")


class BitbucketClient:
    def __init__(self):
        self._client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self):
        self._client = httpx.AsyncClient(
            timeout=30.0,
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {settings.bitbucket_token}",
            },
        )
        return self

    async def __aexit__(self, *args):
        if self._client:
            await self._client.aclose()

    @retry(
        retry=retry_if_exception_type(httpx.TransportError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
    )
    async def _get(self, url: str, params: Dict = None) -> Dict:
        resp = await self._client.get(url, params=params or {})
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 5))
            logger.warning("Rate limited — sleeping %ds", retry_after)
            await asyncio.sleep(retry_after)
            resp = await self._client.get(url, params=params or {})
        if resp.status_code >= 400:
            raise BitbucketAPIError(resp.status_code, resp.text[:200])
        return resp.json()

    async def _paginate(self, url: str, params: Dict = None) -> AsyncGenerator[Any, None]:
        """Yield every item across all pages."""
        start = 0
        params = params or {}
        while True:
            data = await self._get(url, {**params, "limit": PAGE_LIMIT, "start": start})
            for item in data.get("values", []):
                yield item
            if data.get("isLastPage", True):
                break
            start = data["nextPageStart"]

    # ------------------------------------------------------------------
    # Repositories
    # ------------------------------------------------------------------

    async def list_repos(self, project_key: str) -> List[Dict]:
        """Return all repositories in a project."""
        url = f"{BB_API}/projects/{project_key}/repos"
        repos = []
        async for repo in self._paginate(url):
            repos.append(repo)
        logger.info("Found %d repos in project %s", len(repos), project_key)
        return repos

    # ------------------------------------------------------------------
    # Branches
    # ------------------------------------------------------------------

    async def list_branches(self, project_key: str, repo_slug: str) -> List[Dict]:
        """Return all branches in a repository."""
        url = f"{BB_API}/projects/{project_key}/repos/{repo_slug}/branches"
        branches = []
        async for branch in self._paginate(url, {"orderBy": "MODIFICATION"}):
            branches.append(branch)
        return branches

    async def get_default_branch(self, project_key: str, repo_slug: str) -> Optional[str]:
        """Return the default branch display ID."""
        url = f"{BB_API}/projects/{project_key}/repos/{repo_slug}/branches/default"
        try:
            data = await self._get(url)
            return data.get("displayId")
        except BitbucketAPIError:
            return "master"

    # ------------------------------------------------------------------
    # Commits
    # ------------------------------------------------------------------

    async def get_commits(
        self,
        project_key: str,
        repo_slug: str,
        branch: str,
        limit: int = 50,
    ) -> List[Dict]:
        """Return recent commits on a branch."""
        url = f"{BB_API}/projects/{project_key}/repos/{repo_slug}/commits"
        commits = []
        async for commit in self._paginate(url, {"until": branch}):
            commits.append(commit)
            if len(commits) >= limit:
                break
        return commits

    async def get_branch_commit_count(
        self, project_key: str, repo_slug: str, branch: str, base: str
    ) -> int:
        """
        Count commits on `branch` that are NOT reachable from `base`
        using the since/until diff endpoint.
        """
        url = f"{BB_API}/projects/{project_key}/repos/{repo_slug}/commits"
        count = 0
        try:
            async for _ in self._paginate(url, {"since": base, "until": branch}):
                count += 1
                if count > 500:  # cap to avoid huge scans
                    break
        except BitbucketAPIError:
            pass
        return count

    async def is_branch_merged(
        self, project_key: str, repo_slug: str, branch: str, base: str
    ) -> bool:
        """
        A branch is 'fully merged' if it has zero unique commits compared to base.
        Uses the compare/commits endpoint.
        """
        url = (
            f"{BB_API}/projects/{project_key}/repos/{repo_slug}"
            f"/compare/commits"
        )
        try:
            data = await self._get(
                url, {"from": base, "to": branch, "limit": 1}
            )
            return data.get("size", 1) == 0
        except BitbucketAPIError:
            return False

    async def get_commits_behind(
        self, project_key: str, repo_slug: str, branch: str, base: str
    ) -> int:
        """How many commits is `branch` behind `base`."""
        url = (
            f"{BB_API}/projects/{project_key}/repos/{repo_slug}"
            f"/compare/commits"
        )
        try:
            # from=branch, to=base gives commits in base NOT in branch
            count = 0
            async for _ in self._paginate(url, {"from": branch, "to": base}):
                count += 1
                if count > 200:
                    break
            return count
        except BitbucketAPIError:
            return 0

    # ------------------------------------------------------------------
    # Pull Requests
    # ------------------------------------------------------------------

    async def get_pull_requests(
        self,
        project_key: str,
        repo_slug: str,
        branch: str,
        state: str = "ALL",
    ) -> List[Dict]:
        """Return PRs for a specific source branch."""
        url = f"{BB_API}/projects/{project_key}/repos/{repo_slug}/pull-requests"
        prs = []
        async for pr in self._paginate(url, {"at": f"refs/heads/{branch}", "state": state}):
            prs.append(pr)
        return prs

    # ------------------------------------------------------------------
    # Author activity
    # ------------------------------------------------------------------

    async def get_author_last_commit_date(
        self,
        project_key: str,
        repo_slug: str,
        author_email: str,
    ) -> Optional[datetime]:
        """
        Find the most recent commit by this author across the whole repo
        (any branch) to detect globally inactive authors.
        Uses the /commits endpoint with author filter.
        """
        url = f"{BB_API}/projects/{project_key}/repos/{repo_slug}/commits"
        try:
            async for commit in self._paginate(url, {"author": author_email}):
                ts = commit.get("authorTimestamp") or commit.get("committerTimestamp")
                if ts:
                    return datetime.utcfromtimestamp(ts / 1000)
                break
        except BitbucketAPIError:
            pass
        return None

    @staticmethod
    def parse_timestamp(ts: Optional[int]) -> Optional[datetime]:
        if ts is None:
            return None
        return datetime.utcfromtimestamp(ts / 1000)
