"""Creates the tiny local DuckDB file examples/eval-example.yaml points at. Run once:

    python examples/eval_seed_db.py
"""

import os

import duckdb

DB_PATH = os.path.join(os.path.dirname(__file__), "eval_example.duckdb")

if __name__ == "__main__":
    con = duckdb.connect(DB_PATH)
    con.execute("CREATE OR REPLACE TABLE merchants (id INTEGER, name VARCHAR, country VARCHAR)")
    con.execute(
        "INSERT INTO merchants VALUES (1, 'Acme', 'US'), (2, 'Globex', 'MX'), (3, 'Initech', 'CA')"
    )
    con.close()
    print(f"Seeded {DB_PATH}")
