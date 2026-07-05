"""Optional live smoke test - runs only with a real ANTHROPIC_API_KEY.

Never runs in CI (deselected via `-m "not live"` / no key present).
Usage: pytest -m live
"""

import os

import pytest

from lakekeeper.agents.decisions import FailureDecision
from lakekeeper.agents.llm import CallBudget, LiveDecider

pytestmark = pytest.mark.live


@pytest.mark.skipif(not os.getenv("ANTHROPIC_API_KEY"), reason="needs ANTHROPIC_API_KEY")
def test_live_structured_decision() -> None:
    decider = LiveDecider(os.getenv("LLM_MODEL", "claude-sonnet-5"), CallBudget(2))
    decision = decider.decide(
        FailureDecision,
        {
            "step": "silver_transactions",
            "error": "ConnectionResetError",
            "message": "connection reset by peer while reading bronze",
            "retries_remaining": 2,
        },
    )
    # A transient network error with retry budget left should be retried.
    assert decision.action in ("retry", "skip_step", "abort")
    assert decision.rationale
