"""Shared helpers for exporting sqlite payload tables to JSONL."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import msgpack


def resolve_output_path(db_path: Path) -> Path:
    assert db_path.suffix, f"Input sqlite file must have an extension: {db_path}"
    return db_path.with_suffix(".jsonl")


def query_single_table_name(connection: sqlite3.Connection) -> str:
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


def query_num_rows(connection: sqlite3.Connection, table_name: str) -> int:
    cursor = connection.execute(f"SELECT COUNT(*) FROM {table_name}")
    row = cursor.fetchone()
    assert row is not None, "COUNT(*) must return a row"
    assert len(row) == 1, "Expected COUNT(*) to return one column"
    num_rows = row[0]
    assert isinstance(num_rows, int), "COUNT(*) value must be an integer"
    return num_rows


def query_payload_column_name(connection: sqlite3.Connection, table_name: str) -> str:
    cursor = connection.execute(f"PRAGMA table_info({table_name})")
    column_names = {row[1] for row in cursor.fetchall()}
    if "payload_msgpack" in column_names:
        return "payload_msgpack"
    if "payload_json" in column_names:
        return "payload_json"
    raise AssertionError(
        f"Table {table_name} must contain payload_msgpack (BLOB) or payload_json (TEXT)"
    )


def build_payload_select_query(
    table_name: str,
    payload_column_name: str,
    limit: int | None,
    offset: int,
) -> tuple[str, list[int]]:
    query = f"SELECT {payload_column_name} FROM {table_name}"
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


def export_sqlite_payload_table_to_jsonl(
    db_path: Path,
    output_path: Path,
    limit: int | None,
    offset: int,
) -> int:
    with sqlite3.connect(db_path) as connection:
        table_name = query_single_table_name(connection)
        payload_column_name = query_payload_column_name(connection, table_name)
        num_rows = query_num_rows(connection, table_name)
        query, params = build_payload_select_query(table_name, payload_column_name, limit, offset)
        cursor = connection.execute(query, params)
        with output_path.open("w", encoding="utf-8") as handle:
            handle.write(json.dumps({"num_rows": num_rows}, ensure_ascii=False))
            handle.write("\n")
            for row in cursor:
                assert len(row) == 1, "Expected exactly one selected payload column"
                payload = row[0]
                if payload_column_name == "payload_msgpack":
                    if isinstance(payload, memoryview):
                        payload = payload.tobytes()
                    assert isinstance(payload, bytes), "payload_msgpack must be stored as BLOB"
                    payload_obj = msgpack.unpackb(payload, raw=False)
                else:
                    assert isinstance(payload, str), "payload_json must be stored as TEXT"
                    payload_obj = json.loads(payload)
                handle.write(json.dumps(payload_obj, ensure_ascii=False))
                handle.write("\n")
    return num_rows
