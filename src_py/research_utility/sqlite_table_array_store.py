from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Callable, Generic, TypeVar

import msgpack

K = TypeVar("K")
V = TypeVar("V")


class SqliteTableArrayStore(Generic[K, V]):
    def __init__(
        self,
        db_path: str | Path,
        *,
        encode_payload: Callable[[V], bytes] | None = None,
        decode_payload: Callable[[bytes], V] | None = None,
    ) -> None:
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(self._db_path)
        self._encode_payload = encode_payload or _default_encode
        self._decode_payload = decode_payload or _default_decode

    def close(self) -> None:
        self._connection.close()

    def append(self, table_key: K, value: V) -> None:
        table_name = _table_name(_to_table_key_text(table_key))
        self._initialize_table(table_name)
        try:
            payload_msgpack = self._encode_payload(value)
        except Exception as error:
            raise RuntimeError(
                f"Failed to serialize sqlite payload for table {table_name} in {self._db_path}: {error}"
            ) from error
        self._connection.execute(
            f"""
            INSERT INTO {table_name} (payload_msgpack)
            VALUES (?)
            """,
            (payload_msgpack,),
        )
        self._connection.commit()

    def load_table(self, table_key: K) -> list[V]:
        table_name = _table_name(_to_table_key_text(table_key))
        if not self.table_exists(table_key):
            return []
        cursor = self._connection.execute(
            f"""
            SELECT payload_msgpack
            FROM {table_name}
            ORDER BY id ASC
            """
        )
        values: list[V] = []
        for row in cursor:
            assert len(row) == 1, "Expected exactly one selected column: payload_msgpack"
            payload_msgpack = row[0]
            if isinstance(payload_msgpack, memoryview):
                payload_msgpack = payload_msgpack.tobytes()
            assert isinstance(payload_msgpack, bytes), "payload_msgpack must be stored as BLOB"
            try:
                values.append(self._decode_payload(payload_msgpack))
            except Exception as error:
                raise RuntimeError(
                    f"Failed to deserialize sqlite payload for table {table_name} in {self._db_path}: {error}"
                ) from error
        return values

    def clear_table(self, table_key: K) -> None:
        table_name = _table_name(_to_table_key_text(table_key))
        if not self._table_exists_with_name(table_name):
            return
        self._connection.execute(f"DELETE FROM {table_name}")
        self._connection.commit()

    def drop_table(self, table_key: K) -> None:
        table_name = _table_name(_to_table_key_text(table_key))
        self._connection.execute(f"DROP TABLE IF EXISTS {table_name}")
        self._connection.commit()

    def table_exists(self, table_key: K) -> bool:
        table_name = _table_name(_to_table_key_text(table_key))
        return self._table_exists_with_name(table_name)

    def _initialize_table(self, table_name: str) -> None:
        self._connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INTEGER PRIMARY KEY,
                payload_msgpack BLOB NOT NULL
            )
            """
        )
        self._connection.commit()

    def _table_exists_with_name(self, table_name: str) -> bool:
        cursor = self._connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table_name,),
        )
        return cursor.fetchone() is not None


def _to_table_key_text(table_key: object) -> str:
    return str(table_key)


def _table_name(table_key_text: str) -> str:
    return f"table_{table_key_text.encode('utf-8').hex()}"


def _default_encode(value: object) -> bytes:
    return msgpack.packb(value, use_bin_type=True)


def _default_decode(payload_msgpack: bytes):
    return msgpack.unpackb(payload_msgpack, raw=False)
