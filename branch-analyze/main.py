"""
main.py — Entrypoint for both the API server and the CLI scanner.

Usage:
  # Run API server
  python main.py serve

  # Run a one-shot scan from CLI
  python main.py scan

  # Run scan for a specific project
  python main.py scan --project MY_PROJECT_KEY
"""
import asyncio
import logging
import sys
from typing import Optional

import typer
import uvicorn
from rich.console import Console
from rich.table import Table
from rich import box

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

console = Console()
cli = typer.Typer(help="Bitbucket Dead Branch Analyzer")


@cli.command()
def serve(
    host: str = typer.Option("0.0.0.0", help="Bind host"),
    port: int = typer.Option(8000, help="Bind port"),
    reload: bool = typer.Option(False, help="Auto-reload on code changes"),
):
    """Start the FastAPI web server."""
    uvicorn.run(
        "app.api:app",
        host=host,
        port=port,
        reload=reload,
        log_level="info",
    )


@cli.command()
def scan(
    project: Optional[str] = typer.Option(None, help="Bitbucket project key (overrides .env)"),
    concurrency: int = typer.Option(5, help="Concurrent repo scan threads"),
):
    """Run a branch scan and print a summary to the console."""
    from app.database import db as database
    from app.scanner import Scanner
    from app.database import BranchStatus

    async def _run():
        await database.connect()
        try:
            scanner = Scanner(concurrency=concurrency)
            with console.status("[bold green]Scanning Bitbucket project...[/bold green]"):
                run = await scanner.run_full_scan(project_key=project)

            # Print summary table
            table = Table(
                title=f"Scan Results — {run.project_key}",
                box=box.ROUNDED,
                show_lines=True,
            )
            table.add_column("Metric", style="cyan", no_wrap=True)
            table.add_column("Value", style="white")

            table.add_row("Scan ID", run.run_id)
            table.add_row("Status", f"[green]{run.status}[/green]" if run.status == "completed" else f"[red]{run.status}[/red]")
            table.add_row("Repos Scanned", str(run.repos_scanned))
            table.add_row("Branches Scanned", str(run.branches_scanned))
            table.add_row("Dead Branches", f"[red]{run.dead_branches_found}[/red]")
            table.add_row("Active Branches", f"[green]{run.active_branches_found}[/green]")
            table.add_row("Protected Branches", f"[blue]{run.protected_branches_found}[/blue]")
            if run.errors:
                table.add_row("Errors", f"[red]{len(run.errors)}[/red]")

            console.print(table)

            if run.errors:
                console.print("\n[bold red]Errors:[/bold red]")
                for err in run.errors[:10]:
                    console.print(f"  • {err}")

            # Print dead branch breakdown
            dead_cursor = database.db["branches"].find(
                {"status": "dead"}
            ).sort("latest_commit_date", 1).limit(30)

            dead_table = Table(
                title="Sample Dead Branches (up to 30)",
                box=box.SIMPLE,
                show_lines=False,
            )
            dead_table.add_column("Repo", style="cyan")
            dead_table.add_column("Branch", style="yellow")
            dead_table.add_column("Last Commit", style="dim")
            dead_table.add_column("Author", style="dim")
            dead_table.add_column("Reasons", style="red")

            async for doc in dead_cursor:
                reasons = ", ".join(doc.get("dead_reasons", []))
                last_commit = doc.get("latest_commit_date")
                last_commit_str = last_commit.strftime("%Y-%m-%d") if last_commit else "unknown"
                dead_table.add_row(
                    doc.get("repo_slug", ""),
                    doc.get("branch_name", ""),
                    last_commit_str,
                    doc.get("latest_commit_author", "")[:20],
                    reasons[:60],
                )

            console.print(dead_table)

        finally:
            await database.disconnect()

    asyncio.run(_run())


if __name__ == "__main__":
    cli()
