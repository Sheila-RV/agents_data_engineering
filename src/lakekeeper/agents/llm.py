"""LLM access: live Claude decider, deterministic mock decider, and a hard
per-run call budget (the cost guard — enforced in code, never by the model).

Mock mode is the out-of-the-box default when no ANTHROPIC_API_KEY is set, so
the whole system runs (and is tested in CI) without any API access.
"""

import json

from pydantic import BaseModel

from lakekeeper.agents.decisions import (
    SYSTEM_PROMPTS,
    DQAction,
    DriftDecision,
    FailureDecision,
    QualityDecision,
    ValidationVerdict,
)
from lakekeeper.config import Settings


class BudgetExceededError(Exception):
    pass


class CallBudget:
    def __init__(self, max_calls: int) -> None:
        self.max_calls = max_calls
        self.used = 0

    def spend(self) -> None:
        if self.used >= self.max_calls:
            raise BudgetExceededError(f"LLM call budget of {self.max_calls} per run exhausted")
        self.used += 1


class MockDecider:
    """Deterministic, conservative policies standing in for the LLM.

    They mirror what a sensible operator would do, so mock runs exercise the
    exact same graph paths as live runs.
    """

    def decide(self, schema: type[BaseModel], context: dict) -> BaseModel:
        if schema is QualityDecision:
            return QualityDecision(
                actions=[
                    DQAction(
                        rule_id=rule["rule_id"],
                        action="quarantine",
                        rationale="mock policy: quarantine every error-severity failure",
                    )
                    for rule in context.get("failed_rules", [])
                ]
            )
        if schema is FailureDecision:
            retry_ok = context.get("retries_remaining", 0) > 0
            return FailureDecision(
                action="retry" if retry_ok else "abort",
                rationale=(
                    "mock policy: retry while budget remains"
                    if retry_ok
                    else "mock policy: retry budget exhausted, aborting"
                ),
            )
        if schema is DriftDecision:
            return DriftDecision(
                action="ingest_aligned",
                rationale=(
                    "mock policy: ingest aligned to contract — missing columns become "
                    "nulls that downstream DQ rules catch; bronze keeps raw lineage"
                ),
            )
        if schema is ValidationVerdict:
            mismatches = context.get("mismatches", [])
            if not mismatches:
                return ValidationVerdict(verdict="pass", explanation="all checks green")
            integrity = [
                m for m in mismatches if "conservation" in m["name"] or "orphan" in m["name"]
            ]
            if integrity:
                return ValidationVerdict(
                    verdict="fail",
                    explanation="mock policy: data-integrity checks failed: "
                    + "; ".join(m["detail"] for m in integrity),
                )
            return ValidationVerdict(
                verdict="pass_with_warnings",
                explanation="mock policy: baseline anomalies to review: "
                + "; ".join(m["detail"] for m in mismatches),
            )
        raise ValueError(f"no mock policy for {schema.__name__}")

    def generate_text(self, system: str, prompt: str) -> str:  # pragma: no cover - not used
        raise NotImplementedError("mock report generation lives in reporting_agent")


class LiveDecider:
    """Claude-backed decider using LangChain structured output."""

    def __init__(self, model: str, budget: CallBudget) -> None:
        self.model = model
        self.budget = budget
        self._llm = None

    def _client(self):
        if self._llm is None:
            from langchain_anthropic import ChatAnthropic

            # No temperature/top_p: Sonnet 5 rejects non-default sampling params.
            self._llm = ChatAnthropic(model=self.model, max_tokens=4096)
        return self._llm

    def decide(self, schema: type[BaseModel], context: dict) -> BaseModel:
        self.budget.spend()
        structured = self._client().with_structured_output(schema)
        return structured.invoke(
            [
                ("system", SYSTEM_PROMPTS[schema]),
                ("human", json.dumps(context, default=str, indent=2)),
            ]
        )

    def generate_text(self, system: str, prompt: str) -> str:
        self.budget.spend()
        response = self._client().invoke([("system", system), ("human", prompt)])
        return (
            response.text() if callable(getattr(response, "text", None)) else str(response.content)
        )


def make_deciders(settings: Settings) -> tuple[MockDecider, LiveDecider | None, LiveDecider | None]:
    """Returns (mock, live, live_cheap). Live deciders are None in mock mode."""
    mock = MockDecider()
    if settings.mock_llm:
        return mock, None, None
    budget = CallBudget(settings.max_llm_calls_per_run)
    return (
        mock,
        LiveDecider(settings.llm_model, budget),
        LiveDecider(settings.llm_model_cheap, budget),
    )
