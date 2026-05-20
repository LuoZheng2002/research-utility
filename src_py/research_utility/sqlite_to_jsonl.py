"""Export rows from a sqlite database table to JSONL."""

from __future__ import annotations

import argparse
from pathlib import Path

from .sqlite_payload_jsonl import (
    export_sqlite_payload_table_to_jsonl,
    resolve_output_path,
)


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

    output_file = resolve_output_path(db_file)
    export_sqlite_payload_table_to_jsonl(db_file, output_file, limit=limit, offset=offset)

    print(f"Wrote JSONL output to {output_file.resolve()}")


if __name__ == "__main__":
    main()
