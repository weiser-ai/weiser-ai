"""CLI-layer tests for the weiser/evals commands in weiser/main.py: argument parsing,
file I/O, and exit codes. The underlying business logic (generate_synthetic_goldens,
calibrate_judge, gate) is already covered in tests/unit/evals/ -- these patch just the
LLM-calling entrypoints so the CLI glue itself gets exercised for real, including real
schema resolution against a live SQLite/DuckDB-backed semantic layer where relevant.
"""

import json
from unittest.mock import AsyncMock, patch

import pytest
import yaml
from typer.testing import CliRunner

from weiser.evals.calibrate import CalibrationReport
from weiser.loader.models import EvalGolden
from weiser.main import app

runner = CliRunner()


@pytest.fixture
def sqlite_config(tmp_path):
    """A minimal config with a real (SQLite, via generic_sql) semantic layer -- avoids
    needing DuckDB/Postgres just to prove schema resolution works end-to-end."""
    import sqlite3

    db_path = tmp_path / "merchants.sqlite"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE merchants (id INTEGER, name TEXT, country TEXT)")
    conn.execute("INSERT INTO merchants VALUES (1, 'Acme', 'US'), (2, 'Globex', 'MX')")
    conn.commit()
    conn.close()

    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        f"""
version: 1
datasources:
  - name: local_db
    type: postgresql
    uri: sqlite:///{db_path}

connections:
  - name: metricstore
    type: metricstore
    db_type: duckdb
    db_name: {tmp_path}/metricstore.db

semantic_layers:
  - name: local_sl
    type: generic_sql
    datasource: local_db

agent_variants:
  - name: baseline
    framework: pydantic_ai
    entrypoint: tests.fixtures.stub_agents.build_agent

eval_suites:
  - name: smoke_suite
    arms:
      - {{name: baseline, agent_variant: baseline, semantic_layer: local_sl}}
    goldens:
      - {{id: q1, input: "How many merchants?"}}
    metrics:
      - type: llm_judge
        name: sql_soundness
        params:
          criteria: "Is it sound?"
"""
    )
    return str(config_path)


class TestEvalSynthCLI:
    def test_writes_golden_yaml_from_real_schema(self, sqlite_config, tmp_path):
        out_path = tmp_path / "synthetic.yaml"
        canned = [
            EvalGolden(id="synthetic-merchants-0", input="How many merchants?", source="synthetic"),
            EvalGolden(id="synthetic-merchants-1", input="Which merchants are in the US?", source="synthetic"),
        ]

        with patch("weiser.main.generate_synthetic_goldens", new=AsyncMock(return_value=canned)) as mock_gen:
            result = runner.invoke(
                app,
                [
                    "eval-synth",
                    sqlite_config,
                    "--semantic-layer", "local_sl",
                    "--out", str(out_path),
                    "--n-per-view", "2",
                ],
            )

        assert result.exit_code == 0, result.output
        assert mock_gen.await_count == 1
        # confirm the CLI actually resolved a real SchemaCatalog before calling the generator
        _, kwargs = mock_gen.call_args
        assert "merchants" in mock_gen.call_args.args[0].views

        written = yaml.safe_load(out_path.read_text())
        assert len(written["goldens"]) == 2
        assert written["goldens"][0]["source"] == "synthetic"

    def test_unknown_semantic_layer_exits_nonzero(self, sqlite_config, tmp_path):
        result = runner.invoke(
            app,
            ["eval-synth", sqlite_config, "--semantic-layer", "does_not_exist", "--out", str(tmp_path / "x.yaml")],
        )
        assert result.exit_code == 1


class TestEvalCalibrateCLI:
    def test_writes_report_and_exits_zero_when_calibrated(self, sqlite_config, tmp_path):
        canned_report = CalibrationReport(
            criterion="sql_soundness", judge_prompt_version="abc123",
            n=10, mae=0.05, spearman_correlation=0.9, below_threshold=False,
        )
        out_dir = tmp_path / "calibration"
        turns = tmp_path / "turns.jsonl"
        labels = tmp_path / "labels.jsonl"
        turns.write_text("")
        labels.write_text("")

        with patch("weiser.main.calibrate_judge", new=AsyncMock(return_value=canned_report)):
            result = runner.invoke(
                app,
                [
                    "eval-calibrate", sqlite_config,
                    "--suite", "smoke_suite",
                    "--metric-name", "sql_soundness",
                    "--turns", str(turns),
                    "--labels", str(labels),
                    "--out", str(out_dir),
                ],
            )

        assert result.exit_code == 0, result.output
        report_file = out_dir / "abc123.json"
        assert report_file.exists()
        assert json.loads(report_file.read_text())["spearman_correlation"] == 0.9

    def test_exits_nonzero_when_below_threshold(self, sqlite_config, tmp_path):
        canned_report = CalibrationReport(
            criterion="sql_soundness", judge_prompt_version="abc123",
            n=10, mae=0.4, spearman_correlation=0.1, below_threshold=True,
        )
        turns = tmp_path / "turns.jsonl"
        labels = tmp_path / "labels.jsonl"
        turns.write_text("")
        labels.write_text("")

        with patch("weiser.main.calibrate_judge", new=AsyncMock(return_value=canned_report)):
            result = runner.invoke(
                app,
                [
                    "eval-calibrate", sqlite_config,
                    "--suite", "smoke_suite",
                    "--metric-name", "sql_soundness",
                    "--turns", str(turns),
                    "--labels", str(labels),
                    "--out", str(tmp_path / "calibration"),
                ],
            )
        assert result.exit_code == 1

    def test_unknown_metric_name_exits_nonzero(self, sqlite_config, tmp_path):
        turns = tmp_path / "turns.jsonl"
        labels = tmp_path / "labels.jsonl"
        turns.write_text("")
        labels.write_text("")
        result = runner.invoke(
            app,
            [
                "eval-calibrate", sqlite_config,
                "--suite", "smoke_suite",
                "--metric-name", "does_not_exist",
                "--turns", str(turns),
                "--labels", str(labels),
            ],
        )
        assert result.exit_code == 1
