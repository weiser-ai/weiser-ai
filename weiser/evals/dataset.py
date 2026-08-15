import json
from pathlib import Path
from typing import List, Optional

from weiser.loader.config import load_config
from weiser.loader.models import EvalGolden


def _load_goldens_from_path(path: str) -> List[EvalGolden]:
    """Loads an external golden file. YAML files are `{goldens: [...]}`-shaped, loaded
    through weiser.loader.config.load_config so Jinja2 templating and `includes:` work
    the same as every other weiser config section -- this is what lets a held-out set be
    its own versioned, git-committed artifact (playbook Step 2). JSONL files hold one
    golden dict per line for bulk/synthetic/production-mined sets."""
    suffix = Path(path).suffix.lower()
    if suffix in (".yaml", ".yml"):
        data = load_config(path, verbose=False)
        rows = (data or {}).get("goldens", [])
    elif suffix == ".jsonl":
        rows = []
        with open(path, "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
    else:
        raise ValueError(
            f"Unsupported golden_set file extension: '{path}' "
            "(expected .yaml, .yml, or .jsonl)"
        )
    return [EvalGolden(**row) for row in rows]


class EvalDataset:
    def __init__(self, goldens: List[EvalGolden]) -> None:
        self.goldens = goldens

    @classmethod
    def load(
        cls,
        golden_set: Optional[str] = None,
        goldens: Optional[List[EvalGolden]] = None,
    ) -> "EvalDataset":
        """Inline goldens (EvalSuite.goldens) and an external golden_set file are
        merged if both are present."""
        all_goldens = list(goldens) if goldens else []
        if golden_set:
            all_goldens = all_goldens + _load_goldens_from_path(golden_set)
        return cls(all_goldens)

    def filter(
        self, split: Optional[str] = None, level: Optional[str] = None
    ) -> List[EvalGolden]:
        result = self.goldens
        if split:
            result = [g for g in result if g.split == split]
        if level:
            result = [g for g in result if g.level == level]
        return result
