"""LangGraph state for a pipeline run.

The state is the run's audit trail: every step result, DQ report, failure and
LLM decision (with rationale) accumulates here and is persisted as the run
ledger. All entries are plain JSON-serializable dicts.
"""

import operator
from typing import Annotated, Any, TypedDict


class PipelineRunState(TypedDict, total=False):
    run_id: str
    run_date: str

    # Remaining happy-path steps; workers pop their step on success.
    plan: list[str]
    # Which node the supervisor routes to next ("__end__" terminates).
    next_node: str
    # "running" | "aborted" | "done"
    status: str

    # Retry budget for failed steps — decremented in code, never by the LLM.
    retries_remaining: int
    # Set by a worker when its step blows up; consumed by the supervisor.
    pending_failure: dict[str, Any] | None

    completed: Annotated[list[dict[str, Any]], operator.add]
    dq_reports: Annotated[list[dict[str, Any]], operator.add]
    decisions: Annotated[list[dict[str, Any]], operator.add]
    failures: Annotated[list[dict[str, Any]], operator.add]

    quarantined_total: int
    reconciliation: dict[str, Any] | None
    verdict: dict[str, Any] | None

    report_md: str | None
    report_path: str | None
