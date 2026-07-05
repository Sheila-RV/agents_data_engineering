"""Data-quality agent: evaluates the declarative rules, then decides per finding.

The rule engine computes the facts (which rules failed, how many rows, samples);
the LLM only chooses among quarantine / warn_and_keep / block per finding. With
a clean report there is no LLM call at all.
"""

from lakekeeper.agents import tools
from lakekeeper.agents.context import AgentContext
from lakekeeper.agents.decisions import QualityDecision
from lakekeeper.agents.state import PipelineRunState


def make_node(ctx: AgentContext):
    def quality(state: PipelineRunState) -> dict:
        step = state["plan"][0]
        table = step.removeprefix("quality_")
        report = tools.run_dq_rules(ctx.store, table)
        update: dict = {"dq_reports": [report.to_dict()]}

        for warn in report.warn_failures:
            ctx.console.print(
                f"  dq [yellow]warn[/yellow]  {report.table}: {warn.rule_id} "
                f"({warn.failed_rows}/{warn.total_rows} rows)"
            )

        if report.passed:
            tools.ensure_quarantine(ctx.store, table)
            ctx.console.print(f"  dq [green]pass[/green]  {report.table}")
            update.update(plan=state["plan"][1:], completed=[{"step": step, "quarantined": 0}])
            return update

        for err in report.error_failures:
            ctx.console.print(
                f"  dq [red]error[/red] {report.table}: {err.rule_id} "
                f"({err.failed_rows}/{err.total_rows} rows)"
            )
        decision, record = ctx.decide(
            QualityDecision,
            {
                "table": report.table,
                "total_rows": report.total_rows,
                "failed_rules": [
                    {
                        "rule_id": r.rule_id,
                        "rule_type": r.rule_type,
                        "column": r.column,
                        "failed_rows": r.failed_rows,
                        "total_rows": r.total_rows,
                        "sample_offending_rows": r.sample,
                    }
                    for r in report.error_failures
                ],
            },
            agent="quality",
            step=step,
        )
        update["decisions"] = [record]

        if any(a.action == "block" for a in decision.actions):
            update.update(
                status="aborted",
                failures=[
                    {
                        "step": step,
                        "error": "QualityBlock",
                        "message": f"quality agent blocked the run on {report.table}",
                    }
                ],
            )
            return update

        to_quarantine = [a.rule_id for a in decision.actions if a.action == "quarantine"]
        quarantined = 0
        if to_quarantine:
            quarantined = tools.quarantine_records(ctx.store, table, to_quarantine)
            ctx.console.print(f"  dq [red]quarantined[/red] {quarantined} rows from silver.{table}")
        else:
            tools.ensure_quarantine(ctx.store, table)
        update.update(
            plan=state["plan"][1:],
            completed=[{"step": step, "quarantined": quarantined}],
            quarantined_total=state.get("quarantined_total", 0) + quarantined,
        )
        return update

    return quality
