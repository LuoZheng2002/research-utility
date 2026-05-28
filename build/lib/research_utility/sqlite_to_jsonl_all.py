"""Convert sqlite payload stores under a repository root into JSONL files."""

from __future__ import annotations

import argparse
from pathlib import Path

from .sqlite_payload_jsonl import export_sqlite_payload_table_to_jsonl, resolve_output_path


def _convert_one_sqlite(db_path: Path, limit: int, offset: int) -> Path:
    output_path = resolve_output_path(db_path)
    export_sqlite_payload_table_to_jsonl(db_path, output_path, limit=limit, offset=offset)
    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Recursively convert sqlite files into JSONL files"
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        required=True,
        help="Path to repository root",
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

    sqlite_paths = sorted(repo_root.rglob("*.sqlite"))
    assert sqlite_paths, f"No sqlite files found under {repo_root}"

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
