"""Lakekeeper command-line interface."""

from datetime import date

import polars as pl
import typer
from rich.console import Console
from rich.table import Table

from lakekeeper import __version__
from lakekeeper.config import get_settings

app = typer.Typer(
    name="lakekeeper",
    help="Multi-agent operated medallion lakehouse over synthetic banking data.",
    no_args_is_help=True,
)
console = Console()

DateOpt = typer.Option("2026-07-01", "--date", "-d", help="Business date (YYYY-MM-DD).")
SeedOpt = typer.Option(42, "--seed", help="Random seed (fixed seed = reproducible data).")
ChaosOpt = typer.Option(
    "none", "--chaos", help="Data-quality chaos profile to inject: none | low | high."
)


def _print_frame(title: str, df: pl.DataFrame) -> None:
    table = Table(title=title)
    for col in df.columns:
        table.add_column(col, justify="right" if df[col].dtype.is_numeric() else "left")
    for row in df.iter_rows():
        table.add_row(*[str(v) for v in row])
    console.print(table)


@app.command()
def version() -> None:
    """Print the Lakekeeper version."""
    console.print(f"lakekeeper [bold cyan]{__version__}[/bold cyan]")


@app.command()
def generate(
    run_date: str = DateOpt,
    seed: int = SeedOpt,
    chaos: str = ChaosOpt,
    customers: int = typer.Option(500, help="Number of customers to generate."),
    transactions: int = typer.Option(5000, help="Number of transactions to generate."),
) -> None:
    """Generate one business date's synthetic landing files."""
    from lakekeeper.datagen import generate_landing_files

    settings = get_settings()
    paths = generate_landing_files(
        date.fromisoformat(run_date),
        settings.landing_dir,
        seed=seed,
        chaos=chaos,
        n_customers=customers,
        n_transactions=transactions,
    )
    for p in paths:
        console.print(f"  [green]wrote[/green] {p}")


@app.command()
def run(
    run_date: str = DateOpt,
    no_agents: bool = typer.Option(
        False, "--no-agents", help="Run the deterministic pipeline without the agent layer."
    ),
    live: bool = typer.Option(
        False,
        "--live",
        help="Require live Claude agents (errors if no ANTHROPIC_API_KEY is configured).",
    ),
) -> None:
    """Run the bronze -> silver -> gold pipeline for one business date.

    By default the LangGraph agent layer operates the run; without an API key
    it uses deterministic mock policies, so this always works out of the box.
    """
    settings = get_settings()
    if not no_agents:
        from lakekeeper.agents.graph import run_with_agents

        if live and settings.mock_llm:
            console.print(
                "[red]--live requires ANTHROPIC_API_KEY (and LAKEKEEPER_MOCK_LLM != 1)[/red]"
            )
            raise typer.Exit(1)
        final = run_with_agents(settings, date.fromisoformat(run_date), console)
        status = final.get("status")
        color = "green" if status == "done" else "red"
        console.print(f"[bold]run {final['run_id']} finished: [{color}]{status}[/{color}][/bold]")
        return

    from lakekeeper.pipeline.runner import run_deterministic

    summary = run_deterministic(settings, date.fromisoformat(run_date))
    console.print(f"[bold]run_id[/bold] = {summary.run_id}")
    for r in summary.ingested:
        console.print(f"  bronze [cyan]{r.table:<13}[/cyan] +{r.rows} rows  ({r.file})")
    for t in summary.transformed:
        line = f"  silver [cyan]{t.table:<13}[/cyan] {t.rows_in} -> {t.rows_out} rows"
        if t.quarantined:
            line += f"  [red]({t.quarantined} quarantined)[/red]"
        console.print(line)
    for report in summary.dq_reports:
        for r in report.error_failures + report.warn_failures:
            color = "red" if r.severity == "error" else "yellow"
            console.print(
                f"    dq [{color}]{r.severity}[/{color}] {report.table}: "
                f"{r.rule_id} failed on {r.failed_rows}/{r.total_rows} rows"
            )
    for model, rows in summary.gold_models.items():
        console.print(f"  gold   [cyan]{model:<13}[/cyan] {rows} rows")
    if summary.reconciliation:
        for check in summary.reconciliation.checks:
            mark = "[green]ok[/green]" if check.ok else "[red]MISMATCH[/red]"
            console.print(f"  recon  {check.name:<28} {mark}  {check.detail}")


@app.command()
def report(run_date: str = DateOpt) -> None:
    """Show the gold KPI mart."""
    from lakekeeper.pipeline.store import TableStore

    store = TableStore(get_settings().lake_dir)
    _print_frame("gold.kpi_daily", store.read("gold", "kpi_daily"))


@app.command()
def demo(
    seed: int = SeedOpt,
    chaos: str = ChaosOpt,
    no_agents: bool = typer.Option(False, "--no-agents", help="Skip the agent layer."),
) -> None:
    """One-shot demo: generate data, run the agent-operated pipeline, show the KPIs."""
    run_date = "2026-07-01"
    console.rule("[bold]1. generate landing data")
    generate(run_date, seed, chaos, 500, 5000)
    console.rule("[bold]2. run pipeline (agent-operated)")
    run(run_date, no_agents=no_agents, live=False)
    console.rule("[bold]3. gold KPIs")
    report(run_date)
