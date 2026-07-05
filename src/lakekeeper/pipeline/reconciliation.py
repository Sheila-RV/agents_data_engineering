"""Reconciliation: deterministic cross-layer consistency checks.

The numbers here are computed by code; in agent runs the validation agent only
interprets them (explains tolerated deltas, decides whether a mismatch blocks
the run).
"""

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class CheckResult:
    name: str
    ok: bool
    detail: str


@dataclass
class ReconciliationResult:
    checks: list[CheckResult] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return all(c.ok for c in self.checks)

    @property
    def mismatches(self) -> list[CheckResult]:
        return [c for c in self.checks if not c.ok]

    def to_dict(self) -> dict[str, Any]:
        return {"ok": self.ok, "checks": [asdict(c) for c in self.checks]}


KEYED_TABLES = [
    ("customers", "customer_id"),
    ("accounts", "account_id"),
    ("transactions", "txn_id"),
]


def reconcile(store, *, expected_fraud_rate_pct: float = 0.5, fraud_tolerance: float = 3.0):
    """Row-count, amount and KPI-baseline checks across bronze -> silver -> gold."""
    result = ReconciliationResult()

    # Conservation of rows: every distinct bronze key ends up in silver XOR quarantine.
    for table, key in KEYED_TABLES:
        bronze_keys = store.read("bronze", table).get_column(key).n_unique()
        silver_rows = store.read("silver", table).height
        quarantined = (
            store.read("quarantine", table).height if store.exists("quarantine", table) else 0
        )
        ok = bronze_keys == silver_rows + quarantined
        result.checks.append(
            CheckResult(
                name=f"row_conservation_{table}",
                ok=ok,
                detail=(
                    f"bronze distinct {key}={bronze_keys} vs "
                    f"silver={silver_rows} + quarantine={quarantined}"
                ),
            )
        )

    # Conservation of money: the fact table must sum to exactly the silver ledger.
    silver_sum = store.sql("SELECT ROUND(SUM(amount_bob), 2) AS s FROM silver_transactions")["s"][0]
    fact_sum = store.sql("SELECT ROUND(SUM(amount_bob), 2) AS s FROM gold_fact_transactions")["s"][
        0
    ]
    result.checks.append(
        CheckResult(
            name="amount_conservation_fact",
            ok=silver_sum == fact_sum,
            detail=f"silver amount_bob={silver_sum} vs fact amount_bob={fact_sum}",
        )
    )

    # No orphan facts: every fact row resolved its customer through dim_account.
    orphans = store.sql(
        "SELECT COUNT(*) AS n FROM gold_fact_transactions WHERE customer_id IS NULL"
    )["n"][0]
    result.checks.append(
        CheckResult(
            name="fact_no_orphans", ok=orphans == 0, detail=f"{orphans} facts without customer"
        )
    )

    # KPI baseline: the fraud-flag rate should stay near the historical baseline.
    threshold = expected_fraud_rate_pct * fraud_tolerance
    worst = store.sql("SELECT MAX(fraud_flag_rate_pct) AS r FROM gold_kpi_daily")["r"][0] or 0.0
    result.checks.append(
        CheckResult(
            name="fraud_rate_baseline",
            ok=worst <= threshold,
            detail=f"max daily fraud-flag rate {worst}% vs threshold {threshold}%",
        )
    )
    return result
