"""Seeded synthetic core-banking data generator (Bolivian flavor).

Produces the daily landing-zone drop for one business date:

- ``customers_YYYYMMDD.csv``     customer master snapshot
- ``accounts_YYYYMMDD.csv``      account master snapshot
- ``transactions_YYYYMMDD.jsonl``  one JSON object per transaction
- ``fx_rates_YYYYMMDD.json``     BOB rates for the date

Same seed + same date => byte-identical data, so demos and tests are reproducible.
"""

import json
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import polars as pl
from faker import Faker

from lakekeeper.datagen.fx_rates import write_fx_landing_file

CITIES = [
    "La Paz",
    "El Alto",
    "Santa Cruz de la Sierra",
    "Cochabamba",
    "Sucre",
    "Oruro",
    "Potosí",
    "Tarija",
    "Trinidad",
    "Cobija",
]
SEGMENTS = ["retail", "premium", "business"]
RISK_RATINGS = ["low", "medium", "high"]
ACCOUNT_TYPES = ["checking", "savings", "credit"]
CURRENCIES = ["BOB", "USD"]
CHANNELS = ["ATM", "POS", "web", "branch"]
TXN_TYPES = ["deposit", "withdrawal", "payment", "transfer_in", "transfer_out"]
MERCHANT_CATEGORIES = [
    "groceries",
    "restaurants",
    "fuel",
    "utilities",
    "telecom",
    "travel",
    "health",
    "education",
    "entertainment",
    "other",
]


def _make_customers(n: int, run_date: date, rng: np.random.Generator, fake: Faker) -> pl.DataFrame:
    ids = [f"CUST-{i:05d}" for i in range(1, n + 1)]
    return pl.DataFrame(
        {
            "customer_id": ids,
            "full_name": [fake.name() for _ in ids],
            "doc_id": [str(rng.integers(1_000_000, 9_999_999_999)) for _ in ids],
            "birth_date": [
                (run_date - timedelta(days=int(rng.integers(18 * 365, 90 * 365)))).isoformat()
                for _ in ids
            ],
            "city": rng.choice(CITIES, n).tolist(),
            "segment": rng.choice(SEGMENTS, n, p=[0.7, 0.2, 0.1]).tolist(),
            "risk_rating": rng.choice(RISK_RATINGS, n, p=[0.75, 0.2, 0.05]).tolist(),
            "created_at": [
                (run_date - timedelta(days=int(rng.integers(30, 3650)))).isoformat() for _ in ids
            ],
        }
    )


def _make_accounts(
    customers: pl.DataFrame, run_date: date, rng: np.random.Generator
) -> pl.DataFrame:
    rows = []
    seq = 1
    for cust_id, created in customers.select("customer_id", "created_at").iter_rows():
        for _ in range(int(rng.choice([1, 2, 3], p=[0.55, 0.35, 0.10]))):
            opened = date.fromisoformat(created) + timedelta(days=int(rng.integers(0, 300)))
            rows.append(
                {
                    "account_id": f"ACC-{seq:06d}",
                    "customer_id": cust_id,
                    "account_type": str(rng.choice(ACCOUNT_TYPES, p=[0.5, 0.35, 0.15])),
                    "currency": str(rng.choice(CURRENCIES, p=[0.8, 0.2])),
                    "opened_at": min(opened, run_date).isoformat(),
                    "status": str(rng.choice(["active", "dormant", "closed"], p=[0.85, 0.1, 0.05])),
                }
            )
            seq += 1
    return pl.DataFrame(rows)


def _make_transactions(
    accounts: pl.DataFrame, run_date: date, n: int, rng: np.random.Generator, fake: Faker
) -> pl.DataFrame:
    active = accounts.filter(pl.col("status") == "active")
    acc_ids = active.get_column("account_id").to_list()
    acc_ccy = dict(active.select("account_id", "currency").iter_rows())
    chosen = rng.choice(acc_ids, n)
    seconds = rng.integers(6 * 3600, 22 * 3600, n)  # business hours-ish
    # Log-normal amounts: lots of small purchases, a few big transfers.
    amounts = np.round(np.exp(rng.normal(4.6, 1.2, n)), 2)
    txn_types = rng.choice(TXN_TYPES, n, p=[0.2, 0.2, 0.35, 0.1, 0.15])
    channels = rng.choice(CHANNELS, n, p=[0.25, 0.3, 0.3, 0.15])
    flagged = rng.random(n) < 0.005
    base = datetime(run_date.year, run_date.month, run_date.day)
    return pl.DataFrame(
        {
            "txn_id": [f"TXN-{run_date:%Y%m%d}-{i:06d}" for i in range(1, n + 1)],
            "account_id": chosen.tolist(),
            "ts": [(base + timedelta(seconds=int(s))).isoformat() for s in seconds],
            "amount": amounts.tolist(),
            "currency": [acc_ccy[a] for a in chosen],
            "txn_type": txn_types.tolist(),
            "channel": channels.tolist(),
            "counterparty": [fake.company() for _ in range(n)],
            "merchant_category": rng.choice(MERCHANT_CATEGORIES, n).tolist(),
            "is_flagged": flagged.tolist(),
        }
    )


def generate_landing_files(
    run_date: date,
    landing_dir: Path,
    *,
    seed: int = 42,
    n_customers: int = 500,
    n_transactions: int = 5000,
    chaos: str = "none",
    live_fx: bool = False,
) -> list[Path]:
    """Generate one business date's landing files. Returns the written paths."""
    landing_dir.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(seed)
    Faker.seed(seed)
    fake = Faker("es_ES")

    customers = _make_customers(n_customers, run_date, rng, fake)
    accounts = _make_accounts(customers, run_date, rng)
    transactions = _make_transactions(accounts, run_date, n_transactions, rng, fake)

    if chaos != "none":
        from lakekeeper.datagen.chaos import apply_chaos

        customers, accounts, transactions = apply_chaos(
            customers, accounts, transactions, profile=chaos, rng=rng, run_date=run_date
        )

    stamp = f"{run_date:%Y%m%d}"
    paths = [
        landing_dir / f"customers_{stamp}.csv",
        landing_dir / f"accounts_{stamp}.csv",
        landing_dir / f"transactions_{stamp}.jsonl",
    ]
    customers.write_csv(paths[0])
    accounts.write_csv(paths[1])
    with paths[2].open("w", encoding="utf-8") as f:
        for row in transactions.iter_rows(named=True):
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    paths.append(
        write_fx_landing_file(
            run_date,
            landing_dir,
            live=live_fx,
            drop_currency="USD" if chaos == "high" else None,
        )
    )
    return paths
