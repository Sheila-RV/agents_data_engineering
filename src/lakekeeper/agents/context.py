"""Shared context threaded through all agent nodes."""

import traceback
from dataclasses import dataclass

from pydantic import BaseModel
from rich.console import Console

from lakekeeper.agents.llm import BudgetExceededError, LiveDecider, MockDecider
from lakekeeper.config import Settings
from lakekeeper.pipeline.store import TableStore


def failsafe(node):
    """Wrap a worker node so any exception becomes a pending_failure for the
    supervisor to triage (retry / skip_step / abort) instead of killing the graph."""

    def wrapper(state):
        try:
            return node(state)
        except Exception as exc:
            step = state["plan"][0] if state.get("plan") else "unknown"
            return {
                "pending_failure": {
                    "step": step,
                    "error": type(exc).__name__,
                    "message": str(exc)[:500],
                    "traceback_tail": traceback.format_exc(limit=3)[-1500:],
                }
            }

    return wrapper


@dataclass
class AgentContext:
    settings: Settings
    store: TableStore
    console: Console
    mock: MockDecider
    live: LiveDecider | None  # None => mock mode
    live_cheap: LiveDecider | None

    @property
    def mode(self) -> str:
        return "live" if self.live else "mock"

    def decide(
        self, schema: type[BaseModel], context: dict, *, agent: str, step: str
    ) -> tuple[BaseModel, dict]:
        """Ask for a structured decision; degrade gracefully to the mock policy on
        budget exhaustion or API failure. Returns (decision, ledger_record)."""
        mode = self.mode
        if self.live is None:
            decision = self.mock.decide(schema, context)
        else:
            try:
                decision = self.live.decide(schema, context)
            except BudgetExceededError:
                decision = self.mock.decide(schema, context)
                mode = "mock(budget-exhausted)"
            except Exception as exc:  # API/network errors must not kill the pipeline
                decision = self.mock.decide(schema, context)
                mode = f"mock(llm-error: {type(exc).__name__})"
        record = {
            "agent": agent,
            "step": step,
            "schema": schema.__name__,
            "mode": mode,
            "context": context,
            "decision": decision.model_dump(),
        }
        self._render(agent, decision, mode)
        return decision, record

    def _render(self, agent: str, decision: BaseModel, mode: str) -> None:
        summary = getattr(decision, "action", None) or getattr(decision, "verdict", None)
        rationale = getattr(decision, "rationale", None) or getattr(decision, "explanation", "")
        if summary is None and hasattr(decision, "actions"):  # QualityDecision
            summary = ", ".join(f"{a.rule_id}->{a.action}" for a in decision.actions)
            rationale = ""
        # Plain ASCII marker: emoji break Windows cp1252 consoles.
        self.console.print(
            f"  [magenta]agent[/magenta] [bold magenta]{agent}[/bold magenta] "
            f"({mode}) decided [bold]{summary}[/bold]" + (f" -- {rationale}" if rationale else "")
        )
