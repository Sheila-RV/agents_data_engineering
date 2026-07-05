# ADR 003 - Mock mode as the default

**Status**: accepted

## Context

A portfolio project gets cloned by people without an Anthropic API key, and CI must not
depend on secrets or network calls. Many agent projects "mock the LLM" only inside tests,
leaving the real entrypoint broken without a key.

## Decision

`MockDecider` implements a deterministic, conservative operator policy for every decision
schema and is selected automatically whenever no API key is configured (or
`LAKEKEEPER_MOCK_LLM=1`). The live path additionally **degrades** to mock policies on
budget exhaustion or API errors instead of failing the run.

## Rationale

- `python -m lakekeeper demo` has to work out of the box for anyone who clones the repo.
- Mock and live runs traverse the same graph paths, so CI meaningfully tests the
  orchestration, not a bypass.
- Graceful degradation matches how an operator system should behave when the LLM is
  unavailable: fall back to safe defaults and keep the audit trail.

## Consequences

- Every new decision schema requires a mock policy (enforced by `MockDecider` raising on
  unknown schemas).
- The run ledger records the decision mode (`live`, `mock`, `mock(budget-exhausted)`,
  `mock(llm-error: ...)`) so degraded runs are visible.
