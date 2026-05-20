from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Callable, Generic, Iterable, TypeVar

import msgpack

K = TypeVar("K")
V = TypeVar("V")


class SqliteStoreStatement(Generic[V]):
    def __init__(
        self,
        connection: sqlite3.Connection,
        db_path: Path,
        decode_payload: Callable[[bytes], V],
    ) -> None:
        self._connection = connection
        self._db_path = db_path
        self._decode_payload = decode_payload

    def try_iter(self) -> Iterable[V]:
        try:
            cursor = self._connection.execute(
                """
                SELECT payload_msgpack
                FROM store_entries
                ORDER BY id ASC
                """
            )
        except Exception as error:
            raise RuntimeError(
                f"Failed to execute scan query for table store_entries in {self._db_path}: {error}"
            ) from error
        for row in cursor:
            assert len(row) == 1, "Expected exactly one selected column: payload_msgpack"
            payload_msgpack = row[0]
            if isinstance(payload_msgpack, memoryview):
                payload_msgpack = payload_msgpack.tobytes()
            assert isinstance(payload_msgpack, bytes), "payload_msgpack must be stored as BLOB"
            try:
                yield self._decode_payload(payload_msgpack)
            except Exception as error:
                raise RuntimeError(
                    f"Failed to deserialize sqlite payload from table store_entries in {self._db_path}: {error}"
                ) from error


class SqliteStore(Generic[K, V]):
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
        self.initialize_schema()

    def close(self) -> None:
        self._connection.close()

    def initialize_schema(self) -> None:
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS store_entries (
                id TEXT PRIMARY KEY,
                payload_msgpack BLOB NOT NULL
            )
            """
        )
        self._connection.commit()

    def clear(self) -> None:
        self.initialize_schema()
        self._connection.execute("DELETE FROM store_entries")
        self._connection.commit()

    def upsert(self, key: K, value: V) -> None:
        self.initialize_schema()
        key_text = _to_key_text(key)
        try:
            payload_msgpack = self._encode_payload(value)
        except Exception as error:
            raise RuntimeError(
                f"Failed to serialize sqlite payload for table store_entries in {self._db_path}: {error}"
            ) from error
        self._connection.execute(
            """
            INSERT INTO store_entries (id, payload_msgpack)
            VALUES (?, ?)
            ON CONFLICT(id) DO UPDATE SET payload_msgpack = excluded.payload_msgpack
            """,
            (key_text, payload_msgpack),
        )
        self._connection.commit()

    def get(self, key: K) -> V | None:
        self.initialize_schema()
        key_text = _to_key_text(key)
        cursor = self._connection.execute(
            "SELECT payload_msgpack FROM store_entries WHERE id = ?",
            (key_text,),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        assert len(row) == 1, "Expected exactly one selected column: payload_msgpack"
        payload_msgpack = row[0]
        if isinstance(payload_msgpack, memoryview):
            payload_msgpack = payload_msgpack.tobytes()
        assert isinstance(payload_msgpack, bytes), "payload_msgpack must be stored as BLOB"
        try:
            return self._decode_payload(payload_msgpack)
        except Exception as error:
            raise RuntimeError(
                f"Failed to deserialize sqlite payload for key {key_text} from table store_entries at {self._db_path}: {error}"
            ) from error

    def statement(self) -> SqliteStoreStatement[V]:
        self.initialize_schema()
        return SqliteStoreStatement(
            connection=self._connection,
            db_path=self._db_path,
            decode_payload=self._decode_payload,
        )

    def load_all(self) -> list[V]:
        statement = self.statement()
        return list(statement.try_iter())


def _to_key_text(key: object) -> str:
    return str(key)


def _default_encode(value: object) -> bytes:
    return msgpack.packb(value, use_bin_type=True)


def _default_decode(payload_msgpack: bytes):
    return msgpack.unpackb(payload_msgpack, raw=False)
