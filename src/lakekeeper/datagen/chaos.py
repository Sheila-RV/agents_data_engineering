"""Chaos injection: seeded, realistic data-quality problems.

Applied after clean generation so every issue is deliberate and reproducible.
Each ingredient exercises a specific agent path:

low profile
- duplicate transactions (~2%)      -> silver dedupe (visible in rows_in vs rows_out)
- null amounts (~1%)                -> amount_not_null error rule -> quarantine decision
- null customer doc_ids (~5%)       -> doc_id_not_null error rule -> quarantine decision
- late records (~3%, 2-5 days old)  -> is_late warn rule + FX fallback join

high profile (everything above, plus)
- accounts column renamed status -> estado  -> schema-drift escalation at ingest
- FX file missing the USD rate              -> fx_rate_missing warn rule
- fraud-flag rate spiked 5x                 -> fraud_rate_baseline recon mismatch
                                               -> validation agent verdict
"""

from datetime import date

import numpy as np
import polars as pl

PROFILES = ("none", "low", "high")


def _pick(rng: np.random.Generator, n: int, rate: float) -> np.ndarray:
    """Boolean mask selecting exactly max(1, rate*n) rows - deterministic counts."""
    k = max(1, int(rate * n))
    mask = np.zeros(n, dtype=bool)
    mask[rng.choice(n, size=min(k, n), replace=False)] = True
    return mask


def apply_chaos(
    customers: pl.DataFrame,
    accounts: pl.DataFrame,
    transactions: pl.DataFrame,
    *,
    profile: str,
    rng: np.random.Generator,
    run_date: date,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    if profile not in ("low", "high"):
        raise ValueError(f"unknown chaos profile {profile!r}; expected one of {PROFILES}")
    n = transactions.height

    # Null amounts -> quarantine material.
    null_amount = _pick(rng, n, 0.01)
    # Late records: shift the event timestamp 2-5 days into the past.
    late = _pick(rng, n, 0.03)
    shift_days = np.where(late, rng.integers(2, 6, n), 0)
    transactions = transactions.with_columns(
        pl.when(pl.lit(null_amount))
        .then(pl.lit(None, dtype=pl.Float64))
        .otherwise(pl.col("amount"))
        .alias("amount"),
        (pl.col("ts").str.to_datetime() - pl.duration(days=pl.lit(shift_days)))
        .dt.to_string("%Y-%m-%dT%H:%M:%S")
        .alias("ts"),
    )

    if profile == "high":
        # Fraud spike: 5x the normal 0.5% flag rate.
        transactions = transactions.with_columns(pl.lit(rng.random(n) < 0.025).alias("is_flagged"))

    # Duplicate transactions: re-emit ~2% with identical txn_ids.
    dup_idx = np.flatnonzero(_pick(rng, n, 0.02))
    transactions = pl.concat([transactions, transactions[dup_idx.tolist()]])

    # Null doc_ids on the customer master.
    null_doc = _pick(rng, customers.height, 0.05)
    customers = customers.with_columns(
        pl.when(pl.lit(null_doc))
        .then(pl.lit(None, dtype=pl.String))
        .otherwise(pl.col("doc_id"))
        .alias("doc_id")
    )

    if profile == "high":
        # Upstream "improved" their export: schema drift on the accounts feed.
        accounts = accounts.rename({"status": "estado"})

    return customers, accounts, transactions
