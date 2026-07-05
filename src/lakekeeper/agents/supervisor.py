"""Supervisor: deterministic router with LLM-only-on-escalation.

The happy path is a fixed plan advanced in code — no LLM involved. The LLM is
consulted exactly when something is off-plan: a worker left a pending_failure.
Even then the action space is closed (retry / skip_step / abort) and the retry
budget is enforced in code, so a confused model cannot loop the pipeline.
"""

from lakekeeper.agents.context import AgentContext
from lakekeeper.agents.decisions import FailureDecision
from lakekeeper.agents.state import PipelineRunState


def node_for(step: str) -> str:
    if step == "ingest":
        return "ingestion"
    if step.startswith("quality_"):
        return "quality"
    if step == "validate":
        return "validation"
    return "transform"  # silver_* and gold


def make_node(ctx: AgentContext):
    def supervise(state: PipelineRunState) -> dict:
        # A worker failed: escalate to the LLM (or mock policy) for a decision.
        failure = state.get("pending_failure")
        if failure:
            retries = state.get("retries_remaining", 0)
            decision, record = ctx.decide(
                FailureDecision,
                {**failure, "retries_remaining": retries, "remaining_plan": state["plan"]},
                agent="supervisor",
                step=failure["step"],
            )
            action = decision.action
            if action == "retry" and retries <= 0:
                action = "abort"  # the budget is code-enforced, not model-enforced
            update: dict = {
                "pending_failure": None,
                "decisions": [record],
                "failures": [{**failure, "resolution": action}],
            }
            if action == "retry":
                update["retries_remaining"] = retries - 1
                update["next_node"] = node_for(state["plan"][0])
            elif action == "skip_step":
                update["plan"] = state["plan"][1:]
                next_plan = state["plan"][1:]
                update["next_node"] = node_for(next_plan[0]) if next_plan else "reporting"
            else:
                update["status"] = "aborted"
                update["next_node"] = "reporting"
            return update

        # Aborted somewhere: go straight to the report (once).
        if state.get("status") == "aborted":
            return {"next_node": "__end__" if state.get("report_md") else "reporting"}

        # Plan finished: report, then end.
        if not state["plan"]:
            return {"next_node": "__end__" if state.get("report_md") else "reporting"}

        return {"next_node": node_for(state["plan"][0])}

    return supervise
