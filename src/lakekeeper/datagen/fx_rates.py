"""FX rates for the lakehouse: offline snapshot by default, optional live fetch.

Rates are expressed as BOB (boliviano) per one unit of the foreign currency,
so `amount_bob = amount * rate_to_bob`.
"""

import json
from datetime import date
from pathlib import Path

import numpy as np

# Base snapshot so demos never need internet. BOB has been pegged ~6.96/USD for years.
BASE_RATES_TO_BOB = {"BOB": 1.0, "USD": 6.96, "EUR": 7.52}


def rates_for_date(rate_date: date, rng: np.random.Generator | None = None) -> dict[str, float]:
    """Snapshot rates with a small deterministic daily jitter on non-pegged pairs."""
    rng = rng or np.random.default_rng(rate_date.toordinal())
    rates = dict(BASE_RATES_TO_BOB)
    rates["EUR"] = round(rates["EUR"] * (1 + rng.normal(0, 0.004)), 4)
    return rates


def fetch_live_rates(rate_date: date) -> dict[str, float]:
    """Fetch real rates from the free open.er-api.com endpoint (no API key).

    Falls back to the offline snapshot on any network problem.
    """
    import httpx

    try:
        resp = httpx.get("https://open.er-api.com/v6/latest/BOB", timeout=10)
        resp.raise_for_status()
        per_bob = resp.json()["rates"]  # units of currency per 1 BOB
        return {ccy: round(1 / per_bob[ccy], 4) for ccy in BASE_RATES_TO_BOB if per_bob.get(ccy)}
    except Exception:
        return rates_for_date(rate_date)


def write_fx_landing_file(rate_date: date, landing_dir: Path, *, live: bool = False) -> Path:
    rates = fetch_live_rates(rate_date) if live else rates_for_date(rate_date)
    payload = {"date": rate_date.isoformat(), "base": "BOB", "rates_to_bob": rates}
    path = landing_dir / f"fx_rates_{rate_date:%Y%m%d}.json"
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path
