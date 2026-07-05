"""LangGraph wiring: supervisor + five worker agents around the medallion pipeline.

Topology (every worker reports back to the supervisor; reporting ends the run):

    START -> supervisor -> {ingestion | transform | quality | validation | reporting}
                 ^                                   |
                 +-----------------------------------+
"""

import json
from datetime import date

from langgraph.graph import END, START, StateGraph
from rich.console import Console

from lakekeeper.agents import (
    ingestion_agent,
    quality_agent,
    reporting_agent,
    supervisor,
    transform_agent,
    validation_agent,
)
from lakekeeper.agents.context import AgentContext, failsafe
from lakekeeper.agents.llm import make_deciders
from lakekeeper.agents.state import PipelineRunState
from lakekeeper.config import Settings
from lakekeeper.pipeline.runner import SILVER_STEPS, new_run_id
from lakekeeper.pipeline.store import TableStore

HAPPY_PATH = [
    "ingest",
    *[
        step
        for table in SILVER_STEPS
        for step in (f"silver_{table}", f"quality_{table}")
        # fx_rates has no DQ rules; skip its quality step
        if step != "quality_fx_rates"
    ],
    "gold",
    "validate",
]


def build_graph(ctx: AgentContext):
    builder = StateGraph(PipelineRunState)
    builder.add_node("supervisor", supervisor.make_node(ctx))
    builder.add_node("ingestion", failsafe(ingestion_agent.make_node(ctx)))
    builder.add_node("transform", failsafe(transform_agent.make_node(ctx)))
    builder.add_node("quality", failsafe(quality_agent.make_node(ctx)))
    builder.add_node("validation", failsafe(validation_agent.make_node(ctx)))
    builder.add_node("reporting", reporting_agent.make_node(ctx))

    builder.add_edge(START, "supervisor")
    builder.add_conditional_edges(
        "supervisor",
        lambda state: state["next_node"],
        {
            "ingestion": "ingestion",
            "transform": "transform",
            "quality": "quality",
            "validation": "validation",
            "reporting": "reporting",
            "__end__": END,
        },
    )
    for worker in ("ingestion", "transform", "quality", "validation", "reporting"):
        builder.add_edge(worker, "supervisor")
    return builder.compile()


def make_context(settings: Settings, console: Console | None = None) -> AgentContext:
    mock, live, live_cheap = make_deciders(settings)
    return AgentContext(
        settings=settings,
        store=TableStore(settings.lake_dir),
        console=console or Console(),
        mock=mock,
        live=live,
        live_cheap=live_cheap,
    )


def run_with_agents(settings: Settings, run_date: date, console: Console | None = None) -> dict:
    """Run the full agent-operated pipeline for one business date."""
    ctx = make_context(settings, console)
    ctx.console.print(
        f"[bold]agent mode[/bold]: {ctx.mode}"
        + ("" if ctx.live else " (no API key or mock forced — deterministic mock policies)")
    )
    graph = build_graph(ctx)
    initial: PipelineRunState = {
        "run_id": new_run_id(run_date),
        "run_date": run_date.isoformat(),
        "plan": list(HAPPY_PATH),
        "status": "running",
        "retries_remaining": settings.max_step_retries,
        "pending_failure": None,
        "quarantined_total": 0,
    }
    final = graph.invoke(initial, config={"recursion_limit": 120})

    # Persist the run ledger next to the report — the audit trail of the run.
    settings.reports_dir.mkdir(parents=True, exist_ok=True)
    ledger_path = settings.reports_dir / f"run_log_{final['run_id']}.json"
    ledger_path.write_text(
        json.dumps(reporting_agent._ledger(final), default=str, indent=2), encoding="utf-8"
    )
    return final
