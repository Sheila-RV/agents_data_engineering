"""Declarative data-quality engine.

Rules live in YAML files (one per table, see rules/) and are evaluated
deterministically with Polars — the LLM never invents checks, it only reasons
over the structured `DQReport` this engine produces and picks an action
(quarantine / warn / block) per finding.

Rule types:
- not_null:        column must not be null
- unique:          column must not contain duplicates
- allowed_values:  column must be one of `values` (nulls pass; use not_null too)
- range:           column must be within [min, max] (nulls pass)
- foreign_key:     column must exist in `ref` table's `ref_column` (nulls pass)
- fail_when:       SQL expression; rows where it is TRUE fail (e.g. "amount <= 0")

On Databricks the same specs map to Lakeflow pipeline expectations
(@dlt.expect_or_drop) — see docs/databricks_mapping.md.
"""

import functools
import operator
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from importlib import resources
from pathlib import Path
from typing import Any

import polars as pl
import yaml

from lakekeeper.pipeline.store import TableStore

RULES_DIR = Path(str(resources.files("lakekeeper.pipeline.quality"))) / "rules"


@dataclass
class RuleResult:
    rule_id: str
    rule_type: str
    column: str | None
    severity: str  # "error" | "warn"
    failed_rows: int
    total_rows: int
    sample: list[dict] = field(default_factory=list)

    @property
    def failed(self) -> bool:
        return self.failed_rows > 0


@dataclass
class DQReport:
    table: str  # "<layer>.<table>"
    total_rows: int
    results: list[RuleResult] = field(default_factory=list)

    @property
    def error_failures(self) -> list[RuleResult]:
        return [r for r in self.results if r.failed and r.severity == "error"]

    @property
    def warn_failures(self) -> list[RuleResult]:
        return [r for r in self.results if r.failed and r.severity == "warn"]

    @property
    def passed(self) -> bool:
        return not self.error_failures

    def to_dict(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "total_rows": self.total_rows,
            "passed": self.passed,
            "results": [asdict(r) for r in self.results],
        }


def load_spec(layer: str, table: str) -> dict:
    path = RULES_DIR / f"{layer}_{table}.yml"
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def load_rules(layer: str, table: str) -> list[dict]:
    return load_spec(layer, table).get("rules", [])


def _fail_expr(rule: dict, store: TableStore | None) -> pl.Expr:
    """Expression that is True for rows violating the rule."""
    kind, col = rule["type"], rule.get("column")
    if kind == "not_null":
        return pl.col(col).is_null()
    if kind == "unique":
        return pl.col(col).is_duplicated()
    if kind == "allowed_values":
        return pl.col(col).is_not_null() & ~pl.col(col).is_in(rule["values"])
    if kind == "range":
        checks = []
        if "min" in rule:
            checks.append(pl.col(col) < rule["min"])
        if "max" in rule:
            checks.append(pl.col(col) > rule["max"])
        return pl.col(col).is_not_null() & functools.reduce(operator.or_, checks)
    if kind == "foreign_key":
        if store is None:
            raise ValueError(f"rule {rule['id']!r} needs a TableStore for foreign_key lookup")
        ref_layer, ref_table = rule["ref"].split(".")
        ref_values = store.read(ref_layer, ref_table).get_column(rule["ref_column"])
        return pl.col(col).is_not_null() & ~pl.col(col).is_in(ref_values.implode())
    if kind == "fail_when":
        return pl.sql_expr(rule["expr"]).fill_null(False)
    raise ValueError(f"unknown rule type {rule['type']!r} in rule {rule.get('id')!r}")


def _masks(df: pl.DataFrame, rules: list[dict], store: TableStore | None) -> dict[str, pl.Series]:
    return {
        rule["id"]: df.select(_fail_expr(rule, store).fill_null(False).alias("m")).get_column("m")
        for rule in rules
    }


def evaluate(
    df: pl.DataFrame, table: str, rules: list[dict], store: TableStore | None = None
) -> DQReport:
    report = DQReport(table=table, total_rows=df.height)
    for rule in rules:
        mask = _masks(df, [rule], store)[rule["id"]]
        failed = int(mask.sum())
        report.results.append(
            RuleResult(
                rule_id=rule["id"],
                rule_type=rule["type"],
                column=rule.get("column"),
                severity=rule.get("severity", "error"),
                failed_rows=failed,
                total_rows=df.height,
                sample=df.filter(mask).head(5).to_dicts() if failed else [],
            )
        )
    return report


def evaluate_table(store: TableStore, layer: str, table: str) -> DQReport:
    return evaluate(store.read(layer, table), f"{layer}.{table}", load_rules(layer, table), store)


def _quarantine_schema(df: pl.DataFrame) -> pl.DataFrame:
    return df.clear().with_columns(
        pl.lit(None, dtype=pl.String).alias("_reject_reason"),
        pl.lit(None, dtype=pl.Datetime("us")).alias("_quarantined_at"),
    )


def ensure_quarantine_table(store: TableStore, table: str, like: pl.DataFrame) -> None:
    if not store.exists("quarantine", table):
        store.write("quarantine", table, _quarantine_schema(like), mode="append")


def quarantine_failures(
    store: TableStore,
    layer: str,
    table: str,
    *,
    rule_ids: list[str] | None = None,
) -> int:
    """Move rows that violate error-severity rules out of `<layer>.<table>` into
    `quarantine.<table>` (tagged with the violated rule ids). Returns rows moved.

    `rule_ids` limits the action to specific rules — that is the lever the
    data-quality agent pulls; the default policy quarantines all error failures.
    """
    df = store.read(layer, table)
    spec = load_spec(layer, table)
    rules = [
        r
        for r in spec.get("rules", [])
        if r.get("severity", "error") == "error" and (rule_ids is None or r["id"] in rule_ids)
    ]
    ensure_quarantine_table(store, table, df)
    if not rules:
        return 0
    masks = _masks(df, rules, store)
    any_fail = functools.reduce(operator.or_, masks.values())
    if int(any_fail.sum()) == 0:
        return 0

    # Tag every row with the rules it violates, then split good/bad.
    reason = pl.concat_str(
        [
            pl.when(pl.lit(masks[rid])).then(pl.lit(rid)).otherwise(pl.lit(None, dtype=pl.String))
            for rid in masks
        ],
        separator=";",
        ignore_nulls=True,
    ).alias("_reject_reason")
    bad = df.with_columns(
        reason,
        pl.lit(datetime.now(UTC).replace(tzinfo=None)).alias("_quarantined_at"),
    ).filter(any_fail)
    # Upsert on the business key so re-running a date never duplicates quarantine.
    key = spec.get("key")
    if key:
        store.merge("quarantine", table, bad, key=key)
    else:
        store.write("quarantine", table, bad, mode="append")
    store.write(layer, table, df.filter(~any_fail), mode="overwrite")
    return bad.height
