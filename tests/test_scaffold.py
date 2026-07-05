from typer.testing import CliRunner

from lakekeeper import __version__
from lakekeeper.cli import app
from lakekeeper.config import Settings


def test_version_command() -> None:
    result = CliRunner().invoke(app, ["version"])
    assert result.exit_code == 0
    assert __version__ in result.output


def test_mock_llm_defaults_on_without_api_key() -> None:
    settings = Settings(anthropic_api_key="", lakekeeper_mock_llm=False, _env_file=None)
    assert settings.mock_llm is True


def test_live_llm_when_key_present() -> None:
    settings = Settings(anthropic_api_key="sk-test", lakekeeper_mock_llm=False, _env_file=None)
    assert settings.mock_llm is False
