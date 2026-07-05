"""Structured decision schemas — the ONLY things the LLM is allowed to output.

Every agent decision is a Pydantic model with a closed Literal action space, so
an agent can only ever pick between safe, pre-implemented actions; the code
executes them. This bounded-decision design (instead of free-form tool-calling
loops) is what keeps agent runs cheap, auditable and safe to demo.
"""

from typing import Literal

from pydantic import BaseModel, Field


class DriftDecision(BaseModel):
    """What to do with a landing file whose columns deviate from the contract."""

    action: Literal["skip_file", "ingest_aligned", "abort"] = Field(
        description=(
            "skip_file: leave the file out of bronze for manual review; "
            "ingest_aligned: ingest anyway, missing columns become nulls "
            "(downstream DQ rules will catch them); abort: stop the pipeline"
        )
    )
    rationale: str


class DQAction(BaseModel):
    """Action for one failed data-quality rule."""

    rule_id: str
    action: Literal["quarantine", "warn_and_keep", "block"] = Field(
        description=(
            "quarantine: move offending rows to the quarantine layer; "
            "warn_and_keep: keep rows, note the issue in the report; "
            "block: stop the pipeline (systemic corruption)"
        )
    )
    rationale: str


class QualityDecision(BaseModel):
    """One action per failed error-severity rule."""

    actions: list[DQAction]


class FailureDecision(BaseModel):
    """How to react to a failed pipeline step."""

    action: Literal["retry", "skip_step", "abort"] = Field(
        description=(
            "retry: re-run the step (a bounded retry budget is enforced in code); "
            "skip_step: move on without it (downstream steps may degrade); "
            "abort: stop and report"
        )
    )
    rationale: str


class ValidationVerdict(BaseModel):
    """Interpretation of the deterministic reconciliation results."""

    verdict: Literal["pass", "pass_with_warnings", "fail"]
    explanation: str = Field(
        description="Business-readable explanation of any mismatches or anomalies and their impact"
    )


SYSTEM_PROMPTS: dict[type[BaseModel], str] = {
    DriftDecision: (
        "You are the ingestion agent of a banking lakehouse. A landing file's schema "
        "deviates from its contract. Decide what to do. Losing data silently is worse "
        "than quarantining; aborting is a last resort for unrecognizable files."
    ),
    QualityDecision: (
        "You are the data-quality agent of a banking lakehouse. You receive failed "
        "validation rules with sample offending rows. Decide one action per failed rule. "
        "Quarantine is reversible (rows are kept, never deleted); block only for "
        "systemic corruption affecting most rows."
    ),
    FailureDecision: (
        "You are the operations supervisor of a banking data pipeline. A step failed "
        "with the given error. Decide whether to retry (transient errors), skip the step "
        "(optional steps only), or abort (data-corrupting or persistent failures)."
    ),
    ValidationVerdict: (
        "You are the validation agent of a banking lakehouse. You receive deterministic "
        "reconciliation results (row/amount conservation, KPI baselines). Interpret them "
        "for stakeholders: pass, pass_with_warnings (tolerable anomalies worth flagging, "
        "e.g. a fraud-rate spike that fraud ops should review), or fail (data integrity "
        "is broken and the gold layer should not be consumed)."
    ),
}
