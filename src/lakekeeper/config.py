"""Central configuration, loaded from environment / .env via pydantic-settings."""

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    anthropic_api_key: str = ""
    llm_model: str = "claude-sonnet-5"
    llm_model_cheap: str = "claude-haiku-4-5"
    lakekeeper_mock_llm: bool = False

    lake_root: Path = Path("data")

    # Hard budget for LLM calls in a single pipeline run (cost guard).
    max_llm_calls_per_run: int = 15
    # Retry budget for failed pipeline steps, decremented in code (never by the LLM).
    max_step_retries: int = 2

    @property
    def landing_dir(self) -> Path:
        return self.lake_root / "landing"

    @property
    def lake_dir(self) -> Path:
        return self.lake_root / "lake"

    @property
    def reports_dir(self) -> Path:
        return self.lake_root / "lake" / "reports"

    @property
    def mock_llm(self) -> bool:
        """Mock mode is explicit via env, or automatic when no API key is configured."""
        return self.lakekeeper_mock_llm or not self.anthropic_api_key


def get_settings() -> Settings:
    return Settings()
