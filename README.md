# 🔬 Branch Autopsy — Bitbucket Dead Branch Analyzer

Scans all repositories in a Bitbucket Server project, identifies dead/stale branches using 8 configurable rules, stores results in MongoDB, and serves a lightweight UI dashboard.

---

## Architecture

```
┌─────────────────────────────────────────────────┐
│  Bitbucket Server (REST API 1.0)                 │
│  /rest/api/1.0/projects/{key}/repos/...          │
└─────────────────────┬───────────────────────────┘
                      │ httpx async
                      ▼
┌─────────────────────────────────────────────────┐
│  Scanner (scanner.py)                           │
│  • Async concurrent repo/branch scanning        │
│  • Fetches commits, PRs, author activity        │
└─────────────────────┬───────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────┐
│  Analyzer (analyzer.py)                         │
│  • 8 independent dead-branch rules              │
│  • Weighted scoring + classification            │
└─────────────────────┬───────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────┐
│  MongoDB                                        │
│  Collections: branches, scan_runs              │
│  Indexed for fast filtering/aggregation         │
└─────────────────────┬───────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────┐
│  FastAPI (api.py)                               │
│  GET /api/stats  GET /api/branches              │
│  GET /api/repos  POST /api/scans/trigger        │
└─────────────────────┬───────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────┐
│  UI Dashboard (ui/index.html)                   │
│  Dashboard · All Branches · Dead · Repos        │
│  Single-file HTML/CSS/JS, served via nginx      │
└─────────────────────────────────────────────────┘
```

---

## Dead Branch Detection Rules

| # | Rule | Default Threshold | Weight |
|---|------|-------------------|--------|
| 1 | No commits in last N days | 180 days | 8 |
| 2 | Last action was a merge > N days ago (no open PRs) | 30 days | 7 |
| 3 | Branch is older than N days | 365 days | 5 |
| 4 | All commits already in default branch (fully merged) | — | 10 |
| 5 | No open PRs and no commits in 60+ days | 60 days | 3 |
| 6 | Author has no commits anywhere in repo for N days | 90 days | 5 |
| 7 | Branch is N+ commits behind the default branch | 100 commits | 4 |
| 8 | Branch has only a single commit (never developed) | — | 3 |

**A branch is classified as DEAD if:**
- Its total rule score ≥ 8, **OR**
- Any single high-weight rule (weight ≥ 7) fires

**Protected branches** (master, main, develop, release/*, rcfix/*, hotfix/*) are always skipped.

---

## Quick Start

### With Docker Compose (recommended)

```bash
# 1. Clone / copy this project
cd bitbucket-branch-analyzer

# 2. Configure
cp .env.example .env
# Edit .env with your Bitbucket URL, credentials, and project key

# 3. Start everything
docker-compose up -d

# 4. Open the UI
open http://localhost:3000

# 5. Trigger a scan via UI or CLI:
docker-compose exec api python main.py scan
```

### Without Docker

```bash
# Prerequisites: Python 3.11+, MongoDB running locally

pip install -r requirements.txt

cp .env.example .env
# Edit .env

# Start API server
python main.py serve

# In another terminal — run a scan
python main.py scan

# Open the UI (just open the HTML file directly, or serve it)
open ui/index.html
# NOTE: If CORS issues, serve with: python -m http.server 3000 --directory ui
```

---

## Configuration

All settings are in `.env`. Key options:

| Variable | Default | Description |
|----------|---------|-------------|
| `BITBUCKET_BASE_URL` | required | Your Bitbucket Server URL |
| `BITBUCKET_USERNAME` | required | Service account username |
| `BITBUCKET_PASSWORD` | required | Password or personal access token |
| `BITBUCKET_PROJECT_KEY` | required | Project key (e.g. `MYPROJ`) |
| `DEAD_NO_COMMIT_DAYS` | `180` | Rule 1 threshold |
| `DEAD_MERGED_PR_DAYS` | `30` | Rule 2 threshold |
| `DEAD_BRANCH_AGE_DAYS` | `365` | Rule 3 threshold |
| `DEAD_BEHIND_DEFAULT_COMMITS` | `100` | Rule 7 threshold |
| `DEAD_AUTHOR_INACTIVE_DAYS` | `90` | Rule 6 threshold |
| `PROTECTED_BRANCH_PATTERNS` | see .env | Comma-separated regex patterns |

---

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/scans/trigger` | Start a background scan |
| `GET` | `/api/scans` | List recent scan runs |
| `GET` | `/api/scans/latest` | Most recent scan |
| `GET` | `/api/stats` | Dashboard statistics |
| `GET` | `/api/branches` | List branches (filterable, paginated) |
| `GET` | `/api/branches/{repo}/{branch}` | Single branch detail |
| `GET` | `/api/repos` | Repos with branch counts |
| `GET` | `/health` | Health check |

### Branch filter query params

```
GET /api/branches?status=dead&repo=my-service&reason=no_commits_6_months&search=feat/&page=1&page_size=50
```

---

## Scheduling Regular Scans

### Cron (Linux/Mac)
```bash
# Run scan every night at 2 AM
0 2 * * * cd /path/to/project && python main.py scan >> /var/log/branch-scan.log 2>&1
```

### Docker Cron
Add a cron service to docker-compose.yml or use a scheduler like Ofelia.

---

## MongoDB Collections

### `branches`
One document per repo+branch combination. Updated on each scan.

Key fields: `repo_slug`, `branch_name`, `status`, `dead_reasons`, `latest_commit_date`, `latest_commit_author`, `commits_behind_default`, `is_fully_merged`, `open_pr_count`, `scan_run_id`

### `scan_runs`
One document per scan execution.

Key fields: `run_id`, `started_at`, `completed_at`, `branches_scanned`, `dead_branches_found`, `status`, `errors`

---

## Project Structure

```
bitbucket-branch-analyzer/
├── app/
│   ├── __init__.py
│   ├── api.py              # FastAPI routes
│   ├── analyzer.py         # Dead branch rules engine
│   ├── bitbucket_client.py # Async Bitbucket API client
│   ├── config.py           # Pydantic settings
│   ├── database.py         # MongoDB models + connection
│   └── scanner.py          # Orchestration + persistence
├── ui/
│   └── index.html          # Single-file dashboard UI
├── main.py                 # CLI entrypoint
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
└── .env.example
```
