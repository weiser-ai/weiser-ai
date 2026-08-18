from typing import List, Set, Tuple

import sqlglot
from sqlglot import exp


def extract_touched_tables(sql: str) -> Set[str]:
    tables: Set[str] = set()
    try:
        statements = sqlglot.parse(sql)
    except Exception:
        return tables
    for stmt in statements:
        if stmt is None:
            continue
        for table in stmt.find_all(exp.Table):
            if table.name:
                tables.add(table.name.lower())
    return tables


def extract_touched_datasets(predicted_sqls: List[str]) -> Set[str]:
    touched: Set[str] = set()
    for sql in predicted_sqls:
        touched |= extract_touched_tables(sql)
    return touched


def extract_columns(sql: str) -> List[Tuple[str, str]]:
    """Best-effort (table_or_alias, column) pairs. Table aliases are NOT resolved to
    real table names -- sqlglot surfaces the alias exactly as written in the query, the
    same limitation flagged in eval_harness_improvement_spec.md's schema-membership gap
    analysis. Treat column-level results as best-effort, not a hard signal.
    """
    pairs: List[Tuple[str, str]] = []
    try:
        statements = sqlglot.parse(sql)
    except Exception:
        return pairs
    for stmt in statements:
        if stmt is None:
            continue
        for col in stmt.find_all(exp.Column):
            if col.table and col.name:
                pairs.append((col.table.lower(), col.name.lower()))
    return pairs
