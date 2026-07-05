"""Bronze layer: raw, append-only ingestion from the landing zone.

Everything lands as strings, exactly as delivered, plus lineage metadata columns
(`_ingested_at`, `_source_file`, `_run_id`). Nothing is cleaned here - that is
silver's job. The one thing bronze *does* check is the schema contract: files
whose columns deviate raise `SchemaDriftError`, and it is the ingestion agent's
call (in agent runs) whether to skip the file or ingest it aligned to contract.
"""

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from lakekeeper.pipeline.store import TableStore

# Expected landing-file columns per source (the "contract").
CONTRACTS: dict[str, list[str]] = {
    "customers": [
        "customer_id",
        "full_name",
        "doc_id",
        "birth_date",
        "city",
        "segment",
        "risk_rating",
        "created_at",
    ],
    "accounts": ["account_id", "customer_id", "account_type", "currency", "opened_at", "status"],
    "transactions": [
        "txn_id",
        "account_id",
        "ts",
        "amount",
        "currency",
        "txn_type",
        "channel",
        "counterparty",
        "merchant_category",
        "is_flagged",
    ],
    "fx_rates": ["rate_date", "currency", "rate_to_bob"],
}


@dataclass
class IngestResult:
    file: str
    table: str
    rows: int
    missing_columns: list[str] = field(default_factory=list)
    unexpected_columns: list[str] = field(default_factory=list)

    @property
    def has_drift(self) -> bool:
        return bool(self.missing_columns or self.unexpected_columns)


class SchemaDriftError(Exception):
    def __init__(self, result: IngestResult) -> None:
        self.result = result
        super().__init__(
            f"schema drift in {result.file}: "
            f"missing={result.missing_columns} unexpected={result.unexpected_columns}"
        )


def source_of(path: Path) -> str:
    """Map a landing filename to its source table, e.g. customers_20260701.csv -> customers."""
    for source in CONTRACTS:
        if path.name.startswith(f"{source}_"):
            return source
    raise ValueError(f"unrecognized landing file: {path.name}")


def _read_raw(path: Path) -> pl.DataFrame:
    source = source_of(path)
    if path.suffix == ".csv":
        df = pl.read_csv(path, infer_schema=False)
    elif path.suffix == ".jsonl":
        df = pl.read_ndjson(path)
    elif source == "fx_rates":
        payload = json.loads(path.read_text(encoding="utf-8"))
        df = pl.DataFrame(
            {
                "rate_date": [payload["date"]] * len(payload["rates_to_bob"]),
                "currency": list(payload["rates_to_bob"].keys()),
                "rate_to_bob": [str(v) for v in payload["rates_to_bob"].values()],
            }
        )
    else:
        raise ValueError(f"unsupported landing format: {path.name}")
    # Bronze stores everything as strings: verbatim, schema-stable across appends.
    return df.with_columns(pl.all().cast(pl.String))


def inspect_file(path: Path) -> IngestResult:
    """Check a landing file against its contract without ingesting it."""
    source = source_of(path)
    df = _read_raw(path)
    expected, actual = set(CONTRACTS[source]), set(df.columns)
    return IngestResult(
        file=path.name,
        table=source,
        rows=df.height,
        missing_columns=sorted(expected - actual),
        unexpected_columns=sorted(actual - expected),
    )


def ingest_file(
    store: TableStore, path: Path, run_id: str, *, allow_drift: bool = False
) -> IngestResult:
    """Append one landing file to its bronze table.

    On schema drift: raises `SchemaDriftError` unless `allow_drift=True`, in which
    case the frame is aligned to the contract (missing columns become nulls,
    unexpected columns are dropped) and the drift is recorded in the result.
    """
    source = source_of(path)
    df = _read_raw(path)
    result = inspect_file(path)
    if result.has_drift and not allow_drift:
        raise SchemaDriftError(result)
    if result.has_drift:
        df = df.select(
            pl.col(c) if c in df.columns else pl.lit(None, dtype=pl.String).alias(c)
            for c in CONTRACTS[source]
        )
    df = df.with_columns(
        pl.lit(datetime.now(UTC).replace(tzinfo=None)).alias("_ingested_at"),
        pl.lit(path.name).alias("_source_file"),
        pl.lit(run_id).alias("_run_id"),
    )
    store.write("bronze", source, df, mode="append")
    return result


def list_landing_files(landing_dir: Path, run_date_stamp: str | None = None) -> list[Path]:
    """Landing files, optionally filtered to one date stamp (YYYYMMDD)."""
    files = sorted(p for p in landing_dir.glob("*") if p.is_file())
    if run_date_stamp:
        files = [p for p in files if run_date_stamp in p.name]
    return files
