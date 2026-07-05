"""Transformation agent: runs silver builders and gold SQL models.

The happy path is pure deterministic code. Exceptions become pending_failures
via the failsafe wrapper (see graph.py) and are triaged by the supervisor.
"""

from dataclasses import asdict

from lakekeeper.agents import tools
from lakekeeper.agents.context import AgentContext
from lakekeeper.agents.state import PipelineRunState


def make_node(ctx: AgentContext):
    def transform(state: PipelineRunState) -> dict:
        step = state["plan"][0]
        if step == "gold":
            models = tools.run_gold_models(ctx.store)
            for model, rows in models.items():
                ctx.console.print(f"  gold   [cyan]{model:<17}[/cyan] {rows} rows")
            return {
                "plan": state["plan"][1:],
                "completed": [{"step": step, "models": models}],
            }
        table = step.removeprefix("silver_")
        result = tools.run_silver(ctx.store, table)
        ctx.console.print(
            f"  silver [cyan]{result.table:<17}[/cyan] {result.rows_in} -> {result.rows_out} rows"
        )
        return {
            "plan": state["plan"][1:],
            "completed": [{"step": step, **asdict(result)}],
        }

    return transform
