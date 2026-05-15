"""Convert sqlite payload stores under results/ into JSONL files."""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path


def _query_single_table_name(connection: sqlite3.Connection) -> str:
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


def _output_path_for(db_path: Path) -> Path:
    assert db_path.suffix, f"Input sqlite file must have an extension: {db_path}"
    return db_path.with_suffix(".jsonl")


def _convert_one_sqlite(db_path: Path, limit: int, offset: int) -> Path:
    output_path = _output_path_for(db_path)
    with sqlite3.connect(db_path) as connection:
        table_name = _query_single_table_name(connection)
        cursor = connection.execute(
            f"SELECT payload_json FROM {table_name} LIMIT ? OFFSET ?",
            [limit, offset],
        )
        with output_path.open("w", encoding="utf-8") as handle:
            for row in cursor:
                assert len(row) == 1, "Expected exactly one selected column: payload_json"
                payload = row[0]
                assert isinstance(payload, str), "payload_json must be stored as TEXT"
                handle.write(json.dumps(json.loads(payload), ensure_ascii=False))
                handle.write("\n")
    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Recursively convert sqlite files under results/ into JSONL files"
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        required=True,
        help="Path to repository root that contains results/",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Maximum number of rows to export from each sqlite file",
    )
    parser.add_argument(
        "--offset",
        type=int,
        default=0,
        help="Number of rows to skip before export for each sqlite file",
    )
    args = parser.parse_args()

    repo_root = args.repo_root
    limit = args.limit
    offset = args.offset
    assert repo_root.exists(), f"Repository root does not exist: {repo_root}"
    assert repo_root.is_dir(), f"Expected --repo-root to be a directory: {repo_root}"
    assert limit >= 0, "--limit must be >= 0"
    assert offset >= 0, "--offset must be >= 0"

    results_dir = repo_root / "results"
    assert results_dir.exists(), f"Missing results directory: {results_dir}"
    assert results_dir.is_dir(), f"Expected results to be a directory: {results_dir}"

    sqlite_paths = sorted(results_dir.rglob("*.sqlite"))
    assert sqlite_paths, f"No sqlite files found under {results_dir}"

    converted = 0
    skipped = 0
    for sqlite_path in sqlite_paths:
        try:
            output_path = _convert_one_sqlite(sqlite_path, limit=limit, offset=offset)
            converted += 1
            print(f"Converted {sqlite_path} -> {output_path}")
        except Exception as error:
            skipped += 1
            print(f"Skipped {sqlite_path}: {error}")

    print(f"Done. converted={converted}, skipped={skipped}, scanned={len(sqlite_paths)}")


if __name__ == "__main__":
    main()
