# ADR 002 - Deterministic router, LLM only on escalation

**Status**: accepted

## Context

Agent frameworks make it tempting to let the LLM route every step ("what should I do
next?"). That is expensive, slow, non-reproducible, and adds failure modes to the happy
path - the part that runs 99% of the time.

## Decision

The supervisor advances a fixed happy-path plan in code. The LLM is consulted only when
something is **off-plan**: a step failure, an error-severity DQ finding, schema drift, or
a reconciliation mismatch. Decisions are structured output over closed `Literal` action
spaces; retry counts and the per-run LLM call budget are enforced in code.

## Rationale

- A clean run costs zero LLM calls and is fully reproducible.
- The model's judgment is genuinely valuable at escalation points (triage,
  interpretation, explanation), so that is where the calls go.
- A closed action space means the model can only choose among safe,
  pre-implemented actions, and every choice carries a logged rationale.

## Consequences

- The graph has exactly one escalation entry point per failure class, which makes mock
  policies (and therefore CI) straightforward.
- Adding a new agent capability means adding an action to a schema and implementing it,
  rather than prompt-tuning an open-ended loop.
