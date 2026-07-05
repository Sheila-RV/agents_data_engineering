"""Validation agent: runs deterministic reconciliation, LLM interprets the numbers."""

from lakekeeper.agents import tools
from lakekeeper.agents.context import AgentContext
from lakekeeper.agents.decisions import ValidationVerdict
from lakekeeper.agents.state import PipelineRunState


def make_node(ctx: AgentContext):
    def validate(state: PipelineRunState) -> dict:
        step = state["plan"][0]
        recon = tools.reconcile_all(ctx.store)
        for check in recon.checks:
            mark = "[green]ok[/green]" if check.ok else "[red]MISMATCH[/red]"
            ctx.console.print(f"  recon  {check.name:<30} {mark}  {check.detail}")

        verdict, record = (
            ctx.decide(
                ValidationVerdict,
                {
                    "ok": recon.ok,
                    "checks": [
                        {"name": c.name, "ok": c.ok, "detail": c.detail} for c in recon.checks
                    ],
                    "mismatches": [{"name": c.name, "detail": c.detail} for c in recon.mismatches],
                    "quarantined_total": state.get("quarantined_total", 0),
                },
                agent="validation",
                step=step,
            )
            if not recon.ok
            else (
                ValidationVerdict(verdict="pass", explanation="all reconciliation checks green"),
                None,
            )
        )

        update: dict = {
            "reconciliation": recon.to_dict(),
            "verdict": verdict.model_dump(),
            "plan": state["plan"][1:],
            "completed": [{"step": step, "verdict": verdict.verdict}],
        }
        if record:
            update["decisions"] = [record]
        if verdict.verdict == "fail":
            update["status"] = "aborted"
            update["failures"] = [
                {"step": step, "error": "ValidationFail", "message": verdict.explanation}
            ]
        return update

    return validate
