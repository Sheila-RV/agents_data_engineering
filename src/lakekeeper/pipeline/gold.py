"""Gold layer: star schema and KPI marts, defined as SQL files run by DuckDB.

Each model in pipeline/sql/<name>.sql selects from `<layer>_<table>` views
(e.g. silver_transactions) and its result is written to gold.<name>. The SQL
is deliberately engine-portable: on Databricks the same files become SQL
materialized views / Lakeflow declarative pipeline definitions.
"""

from importlib import resources
from pathlib import Path

from lakekeeper.pipeline.store import TableStore

SQL_DIR = Path(str(resources.files("lakekeeper.pipeline"))) / "sql"


def available_models() -> list[str]:
    return sorted(p.stem for p in SQL_DIR.glob("*.sql"))


def run_model(store: TableStore, name: str) -> int:
    """Execute one SQL model and materialize it as gold.<name>. Returns row count."""
    sql_path = SQL_DIR / f"{name}.sql"
    if not sql_path.exists():
        raise ValueError(f"unknown gold model {name!r}; available: {available_models()}")
    result = store.sql(sql_path.read_text(encoding="utf-8"))
    store.write("gold", name, result, mode="overwrite")
    return result.height
