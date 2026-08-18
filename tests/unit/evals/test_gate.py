import json

from weiser.evals.gate import gate


def _write_jsonl(path, rows):
    with open(path, "w") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")


def _row(arm, attribution, hit_limit=False, golden_id="q1"):
    return {
        "result": {"arm": arm, "level": "easy", "failure_attribution": attribution, "golden_id": golden_id},
        "trace": {"cost_usd": 0.01, "tool_calls": [{}], "hit_limit": hit_limit},
    }


class TestGate:
    def test_passes_when_no_regression(self, tmp_path):
        baseline = tmp_path / "baseline.jsonl"
        candidate = tmp_path / "candidate.jsonl"
        _write_jsonl(baseline, [_row("baseline", "clean"), _row("baseline", "agent")])
        _write_jsonl(candidate, [_row("baseline", "clean"), _row("baseline", "clean")])

        result = gate(str(candidate), str(baseline), arm="baseline", max_regression_pp=3.0)
        assert result.passed is True
        assert result.delta_pp > 0

    def test_fails_on_regression_beyond_threshold(self, tmp_path):
        baseline = tmp_path / "baseline.jsonl"
        candidate = tmp_path / "candidate.jsonl"
        _write_jsonl(baseline, [_row("baseline", "clean")] * 10)
        _write_jsonl(candidate, [_row("baseline", "agent")] * 10)

        result = gate(str(candidate), str(baseline), arm="baseline", max_regression_pp=3.0)
        assert result.passed is False
        assert any("regressed" in r for r in result.reasons)

    def test_excludes_data_quality_confounded_rows_by_default(self, tmp_path):
        baseline = tmp_path / "baseline.jsonl"
        candidate = tmp_path / "candidate.jsonl"
        _write_jsonl(baseline, [_row("baseline", "clean")] * 10)
        # All 10 candidate rows "fail," but every one is DQ-confounded -- should not gate.
        _write_jsonl(candidate, [_row("baseline", "data_quality")] * 10)

        result = gate(str(candidate), str(baseline), arm="baseline", max_regression_pp=3.0)
        assert result.excluded_confounded_count == 10
        # With every candidate row excluded, n=0 -> gate fails safe (can't evaluate), not silently passes.
        assert result.passed is False
        assert "zero rows" in result.reasons[0]

    def test_hit_limit_regression_fails_even_without_accuracy_drop(self, tmp_path):
        baseline = tmp_path / "baseline.jsonl"
        candidate = tmp_path / "candidate.jsonl"
        _write_jsonl(baseline, [_row("baseline", "clean", hit_limit=False)] * 5)
        _write_jsonl(candidate, [_row("baseline", "clean", hit_limit=True)] * 5)

        result = gate(str(candidate), str(baseline), arm="baseline", max_regression_pp=3.0)
        assert result.passed is False
        assert any("hit-limit" in r for r in result.reasons)

    def test_can_include_confounded_rows(self, tmp_path):
        baseline = tmp_path / "baseline.jsonl"
        candidate = tmp_path / "candidate.jsonl"
        _write_jsonl(baseline, [_row("baseline", "clean")] * 10)
        _write_jsonl(candidate, [_row("baseline", "data_quality")] * 10)

        result = gate(
            str(candidate), str(baseline), arm="baseline", max_regression_pp=3.0,
            exclude_confounded=False,
        )
        assert result.excluded_confounded_count == 0
        assert result.passed is False  # data_quality rows aren't "clean", so accuracy still drops
