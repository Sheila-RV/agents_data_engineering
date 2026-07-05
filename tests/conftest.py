from datetime import date
from pathlib import Path

import pytest

from lakekeeper.datagen import generate_landing_files
from lakekeeper.pipeline.store import TableStore

RUN_DATE = date(2026, 7, 1)


@pytest.fixture()
def store(tmp_path: Path) -> TableStore:
    return TableStore(tmp_path / "lake")


@pytest.fixture()
def landing_dir(tmp_path: Path) -> Path:
    """A small, clean, seeded landing drop for RUN_DATE."""
    landing = tmp_path / "landing"
    generate_landing_files(RUN_DATE, landing, seed=7, n_customers=20, n_transactions=200)
    return landing
