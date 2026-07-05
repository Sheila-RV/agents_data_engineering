"""Full agent-graph runs with the mock decider - no API key needed anywhere."""

import json
from pathlib import Path

import pytest

from lakekeeper.agents.graph import HAPPY_PATH, run_with_agents
from lakekeeper.config import Settings
from lakekeeper.pipeline.store import TableStore
from tests.conftest import RUN_DATE


@pytest.fixture()
def settings(tmp_path: Path, landing_dir: Path) -> Settings:
    return Settings(lake_root=tmp_path, lakekeeper_mock_llm=True, _env_file=None)


def test_happy_path_completes(settings: Settings) -> None:
    final = run_with_agents(settings, RUN_DATE)
    assert final["status"] == "done"
    assert final["plan"] == []
    completed_steps = [c["step"] for c in final["completed"]]
    assert completed_steps == [*HAPPY_PATH, "report"]
    # Clean data: escalation-only design means zero decisions were needed.
    assert final.get("decisions", []) == []
    assert final["verdict"]["verdict"] == "pass"
    # Report and ledger written.
    assert Path(final["report_path"]).exists()
    ledgers = list(settings.reports_dir.glob("run_log_*.json"))
    assert len(ledgers) == 1
    ledger = json.loads(ledgers[0].read_text(encoding="utf-8"))
    assert ledger["status"] == "done"
    # Gold actually materialized.
    store = TableStore(settings.lake_dir)
    assert store.read("gold", "fact_transactions").height == 200


def test_bad_rows_trigger_quality_decision(settings: Settings, landing_dir: Path) -> None:
    txn_file = landing_dir / "transactions_20260701.jsonl"
    good_line = txn_file.read_text(encoding="utf-8").splitlines()[0]
    bad = json.loads(good_line)
    bad.update(txn_id="TXN-BAD-NEG", amount=-50.0)
    with txn_file.open("a", encoding="utf-8") as f:
        f.write(json.dumps(bad) + "\n")

    final = run_with_agents(settings, RUN_DATE)
    assert final["status"] == "done"
    quality_decisions = [d for d in final["decisions"] if d["agent"] == "quality"]
    assert len(quality_decisions) == 1
    actions = quality_decisions[0]["decision"]["actions"]
    assert {a["rule_id"] for a in actions} == {"amount_positive"}
    assert all(a["action"] == "quarantine" for a in actions)
    assert final["quarantined_total"] == 1
    assert final["verdict"]["verdict"] == "pass"
    # The quarantined row is mentioned in the report.
    assert "amount_positive" in final["report_md"]


def test_schema_drift_is_escalated_and_aligned(settings: Settings, landing_dir: Path) -> None:
    # Rename a column in the accounts file: mock ingestion policy ingests it
    # aligned to the contract (missing column becomes nulls).
    acc_file = landing_dir / "accounts_20260701.csv"
    content = acc_file.read_text(encoding="utf-8")
    acc_file.write_text(content.replace("account_type", "acct_type", 1), encoding="utf-8")

    final = run_with_agents(settings, RUN_DATE)
    drift_decisions = [d for d in final["decisions"] if d["agent"] == "ingestion"]
    assert len(drift_decisions) == 1
    assert drift_decisions[0]["decision"]["action"] == "ingest_aligned"
    assert drift_decisions[0]["context"]["missing_columns"] == ["account_type"]
    assert final["status"] == "done"
    store = TableStore(settings.lake_dir)
    accounts = store.read("silver", "accounts")
    assert accounts["account_type"].null_count() == accounts.height


def test_broken_step_is_retried_then_aborted(settings: Settings, landing_dir: Path) -> None:
    # An unparseable transactions file: ndjson read fails inside ingest ->
    # pending_failure -> supervisor retries (twice), then aborts, still reporting.
    txn_file = landing_dir / "transactions_20260701.jsonl"
    txn_file.write_text("this is not json\n", encoding="utf-8")

    final = run_with_agents(settings, RUN_DATE)
    assert final["status"] == "aborted"
    supervisor_decisions = [d for d in final["decisions"] if d["agent"] == "supervisor"]
    assert [d["decision"]["action"] for d in supervisor_decisions] == ["retry", "retry", "abort"]
    assert final["retries_remaining"] == 0
    assert final["failures"][-1]["resolution"] == "abort"
    # Even aborted runs produce a report.
    assert final["report_md"] is not None
    assert Path(final["report_path"]).exists()


def test_missing_landing_files_abort_gracefully(tmp_path: Path) -> None:
    settings = Settings(lake_root=tmp_path, lakekeeper_mock_llm=True, _env_file=None)
    (tmp_path / "landing").mkdir()
    final = run_with_agents(settings, RUN_DATE)
    assert final["status"] == "aborted"
    assert final["report_md"] is not None
