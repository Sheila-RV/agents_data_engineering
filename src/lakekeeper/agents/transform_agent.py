"""Transformation agent: runs silver builders and gold SQL models.

Happy path is pure deterministic code; the LLM is involved only when a step
raises, and that escalation happens in the supervisor (which sees the
pending_failure this node emits).
"""

import traceback
from dataclasses import asdict

from lakekeeper.agents import tools
from lakekeeper.agents.context import AgentContext
from lakekeeper.agents.state import PipelineRunState


def make_node(ctx: AgentContext):
    def transform(state: PipelineRunState) -> dict:
        step = state["plan"][0]
        try:
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
                f"  silver [cyan]{result.table:<17}[/cyan] "
                f"{result.rows_in} -> {result.rows_out} rows"
            )
            return {
                "plan": state["plan"][1:],
                "completed": [{"step": step, **asdict(result)}],
            }
        except Exception as exc:
            return {
                "pending_failure": {
                    "step": step,
                    "error": type(exc).__name__,
                    "message": str(exc)[:500],
                    "traceback_tail": traceback.format_exc(limit=3)[-1500:],
                }
            }

    return transform
