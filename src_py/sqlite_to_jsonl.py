"""Export rows from a sqlite database table to JSONL."""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path


def _resolve_output_path(db_file: Path) -> Path:
    assert db_file.suffix, "Input file must have an extension"
    return db_file.with_suffix(".jsonl")


def _query_table_name(connection: sqlite3.Connection) -> str:
    cursor = connection.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
        ORDER BY name ASC
        """
    )
    table_names = [row[0] for row in cursor.fetchall()]
    assert len(table_names) == 1, f"Expected exactly 1 user table, found {len(table_names)}"
    return table_names[0]


def _build_select_query(table_name: str, limit: int | None, offset: int) -> tuple[str, list[int]]:
    query = f"SELECT payload_json FROM {table_name}"
    params: list[int] = []
    if limit is not None:
        query += " LIMIT ?"
        params.append(limit)
    elif offset > 0:
        query += " LIMIT -1"
    if offset > 0:
        query += " OFFSET ?"
        params.append(offset)
    return query, params


def main() -> None:
    parser = argparse.ArgumentParser(description="View sqlite rows as JSONL")
    parser.add_argument("--file", type=Path, required=True, help="Path to sqlite database file")
    parser.add_argument("--limit", type=int, help="Maximum number of rows to export")
    parser.add_argument("--offset", type=int, default=0, help="Number of rows to skip before export")
    args = parser.parse_args()

    db_file = args.file
    limit = args.limit
    offset = args.offset
    assert db_file.exists(), f"Database file does not exist: {db_file}"
    assert db_file.is_file(), f"Expected --file to point to a file: {db_file}"
    if limit is not None:
        assert limit >= 0, "--limit must be >= 0"
    assert offset >= 0, "--offset must be >= 0"

    output_file = _resolve_output_path(db_file)
    with sqlite3.connect(db_file) as connection:
        table_name = _query_table_name(connection)
        query, params = _build_select_query(table_name=table_name, limit=limit, offset=offset)
        cursor = connection.execute(query, params)
        with output_file.open("w", encoding="utf-8") as handle:
            for row in cursor:
                assert len(row) == 1, "Expected exactly one selected column: payload_json"
                payload = row[0]
                assert isinstance(payload, str), "payload_json must be stored as TEXT"
                handle.write(json.dumps(json.loads(payload), ensure_ascii=False))
                handle.write("\n")

    print(f"Wrote JSONL output to {output_file.resolve()}")


if __name__ == "__main__":
    main()
