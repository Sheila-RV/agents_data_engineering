"""Ingestion agent: lands files into bronze, escalating schema drift to the LLM."""

from dataclasses import asdict

from lakekeeper.agents import tools
from lakekeeper.agents.context import AgentContext
from lakekeeper.agents.decisions import DriftDecision
from lakekeeper.agents.state import PipelineRunState


def make_node(ctx: AgentContext):
    def ingest(state: PipelineRunState) -> dict:
        step = state["plan"][0]
        stamp = state["run_date"].replace("-", "")
        files = tools.list_landing_files(ctx.settings, stamp)
        if not files:
            return {
                "pending_failure": {
                    "step": step,
                    "error": "FileNotFoundError",
                    "message": f"no landing files for {state['run_date']}",
                }
            }

        decisions, ingested, skipped = [], [], []
        for path in files:
            inspection = tools.inspect_landing_file(path)
            if not inspection.has_drift:
                result = tools.ingest_to_bronze(ctx.store, path, state["run_id"])
                ingested.append(asdict(result))
                continue
            decision, record = ctx.decide(
                DriftDecision,
                {
                    "file": inspection.file,
                    "table": inspection.table,
                    "rows": inspection.rows,
                    "missing_columns": inspection.missing_columns,
                    "unexpected_columns": inspection.unexpected_columns,
                },
                agent="ingestion",
                step=step,
            )
            decisions.append(record)
            if decision.action == "ingest_aligned":
                result = tools.ingest_to_bronze(ctx.store, path, state["run_id"], allow_drift=True)
                ingested.append(asdict(result))
            elif decision.action == "skip_file":
                skipped.append(inspection.file)
            else:  # abort
                return {
                    "decisions": decisions,
                    "status": "aborted",
                    "failures": [
                        {"step": step, "error": "SchemaDrift", "message": f"aborted on {path.name}"}
                    ],
                }

        for r in ingested:
            ctx.console.print(
                f"  bronze [cyan]{r['table']:<13}[/cyan] +{r['rows']} rows ({r['file']})"
            )
        for name in skipped:
            ctx.console.print(f"  bronze [yellow]skipped[/yellow] {name} (schema drift)")
        return {
            "plan": state["plan"][1:],
            "decisions": decisions,
            "completed": [{"step": step, "ingested": ingested, "skipped_files": skipped}],
        }

    return ingest
