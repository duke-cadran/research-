"""
config.py — Application configuration loaded from environment / .env file
"""
from pydantic_settings import BaseSettings
from pydantic import Field
from typing import List
import re


class Settings(BaseSettings):
    # Bitbucket
    bitbucket_base_url: str = Field(..., env="BITBUCKET_BASE_URL")
    bitbucket_token: str = Field(..., env="BITBUCKET_TOKEN")
    bitbucket_project_key: str = Field(..., env="BITBUCKET_PROJECT_KEY")

    # MongoDB
    mongo_uri: str = Field("mongodb://localhost:27017", env="MONGO_URI")
    mongo_db_name: str = Field("branch_analyzer", env="MONGO_DB_NAME")

    # Dead branch thresholds
    dead_no_commit_days: int = Field(180, env="DEAD_NO_COMMIT_DAYS")
    dead_merged_pr_days: int = Field(30, env="DEAD_MERGED_PR_DAYS")
    dead_branch_age_days: int = Field(365, env="DEAD_BRANCH_AGE_DAYS")
    dead_behind_default_commits: int = Field(100, env="DEAD_BEHIND_DEFAULT_COMMITS")
    dead_author_inactive_days: int = Field(90, env="DEAD_AUTHOR_INACTIVE_DAYS")

    # Protected branches — never flagged dead
    protected_branch_patterns: str = Field(
        r"^master$,^main$,^develop$,^release/,^rcfix/,^hotfix/,^HEAD$",
        env="PROTECTED_BRANCH_PATTERNS",
    )

    # API
    api_host: str = Field("0.0.0.0", env="API_HOST")
    api_port: int = Field(8000, env="API_PORT")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

    @property
    def protected_patterns(self) -> List[re.Pattern]:
        """Compile protected branch regex patterns."""
        return [
            re.compile(p.strip())
            for p in self.protected_branch_patterns.split(",")
            if p.strip()
        ]

    def is_protected_branch(self, branch_name: str) -> bool:
        return any(p.search(branch_name) for p in self.protected_patterns)


settings = Settings()
