use rusqlite::{Connection, OptionalExtension, Row, params};
use serde::{Serialize, de::DeserializeOwned};
use std::{marker::PhantomData, path::PathBuf};

pub trait SqliteTableArrayKey {
    fn to_table_key_text(&self) -> String;
}

impl SqliteTableArrayKey for usize {
    fn to_table_key_text(&self) -> String {
        self.to_string()
    }
}

impl SqliteTableArrayKey for i64 {
    fn to_table_key_text(&self) -> String {
        self.to_string()
    }
}

impl SqliteTableArrayKey for String {
    fn to_table_key_text(&self) -> String {
        self.clone()
    }
}

impl SqliteTableArrayKey for &str {
    fn to_table_key_text(&self) -> String {
        self.to_string()
    }
}

#[derive(Debug)]
/// A persistent hashmap-of-ordered-arrays abstraction on top of SQLite.
///
/// Semantics:
/// - `table_key` is the hashmap key.
/// - Each key maps to one physical SQLite table.
/// - Each table stores an ordered array of MessagePack payload rows.
///
/// Operational properties:
/// - `append(table_key, value)` appends to that key's array in O(1) amortized time.
/// - `load_table(table_key)` loads all entries for one key ordered by insertion id.
/// - `drop_table(table_key)` removes all rows for one key efficiently by dropping the table.
///
/// Table naming:
/// - User-provided key text is hex-encoded into `table_<hex>`.
/// - This keeps table names deterministic and SQL-identifier-safe.
pub struct SqliteTableArrayStore<K, V> {
    db_path: PathBuf,
    connection: Connection,
    key_marker: PhantomData<K>,
    value_marker: PhantomData<V>,
}

impl<K, V> SqliteTableArrayStore<K, V>
where
    K: SqliteTableArrayKey,
    V: Serialize + DeserializeOwned,
{
    pub fn new(db_path: impl Into<PathBuf>) -> Result<Self, String> {
        let db_path = db_path.into();
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                format!(
                    "Failed to create parent directory for sqlite database {}: {}",
                    db_path.display(),
                    e
                )
            })?;
        }
        let connection = Connection::open(&db_path)
            .map_err(|e| format!("Failed to open sqlite database {}: {}", db_path.display(), e))?;
        Ok(Self {
            db_path,
            connection,
            key_marker: PhantomData,
            value_marker: PhantomData,
        })
    }

    pub fn append(&self, table_key: K, value: &V) -> Result<(), String> {
        let table_name = Self::table_name(table_key.to_table_key_text());
        self.initialize_table(&table_name)?;
        let payload_msgpack = rmp_serde::to_vec_named(value).map_err(|e| {
            format!(
                "Failed to serialize sqlite payload for table {} in {}: {}",
                table_name,
                self.db_path.display(),
                e
            )
        })?;
        self.connection
            .execute(
                &format!(
                    "
                    INSERT INTO {} (payload_msgpack)
                    VALUES (?1)
                    ",
                    table_name
                ),
                params![payload_msgpack],
            )
            .map_err(|e| {
                format!(
                    "Failed to append sqlite payload into table {} at {}: {}",
                    table_name,
                    self.db_path.display(),
                    e
                )
            })?;
        Ok(())
    }

    pub fn load_table(&self, table_key: K) -> Result<Vec<V>, String> {
        let table_name = Self::table_name(table_key.to_table_key_text());
        if !self.table_exists_with_name(&table_name)? {
            return Ok(Vec::new());
        }
        let mut statement = self
            .connection
            .prepare(&format!(
                "
                SELECT payload_msgpack
                FROM {}
                ORDER BY id ASC
                ",
                table_name
            ))
            .map_err(|e| {
                format!(
                    "Failed to prepare ordered scan statement for table {} in {}: {}",
                    table_name,
                    self.db_path.display(),
                    e
                )
            })?;
        let rows = statement.query_map([], decode_payload_row::<V>).map_err(|e| {
            format!(
                "Failed to execute ordered scan query for table {} in {}: {}",
                table_name,
                self.db_path.display(),
                e
            )
        })?;
        let mut values = Vec::new();
        for row in rows {
            values.push(row.map_err(|e| {
                format!(
                    "Failed to read row from table {} in {}: {}",
                    table_name,
                    self.db_path.display(),
                    e
                )
            })?);
        }
        Ok(values)
    }

    pub fn clear_table(&self, table_key: K) -> Result<(), String> {
        let table_name = Self::table_name(table_key.to_table_key_text());
        if !self.table_exists_with_name(&table_name)? {
            return Ok(());
        }
        self.connection
            .execute(&format!("DELETE FROM {}", table_name), [])
            .map_err(|e| {
                format!(
                    "Failed to clear table {} in {}: {}",
                    table_name,
                    self.db_path.display(),
                    e
                )
            })?;
        Ok(())
    }

    pub fn drop_table(&self, table_key: K) -> Result<(), String> {
        let table_name = Self::table_name(table_key.to_table_key_text());
        self.connection
            .execute(&format!("DROP TABLE IF EXISTS {}", table_name), [])
            .map_err(|e| {
                format!(
                    "Failed to drop table {} in {}: {}",
                    table_name,
                    self.db_path.display(),
                    e
                )
            })?;
        Ok(())
    }

    pub fn table_exists(&self, table_key: K) -> Result<bool, String> {
        let table_name = Self::table_name(table_key.to_table_key_text());
        self.table_exists_with_name(&table_name)
    }

    fn initialize_table(&self, table_name: &str) -> Result<(), String> {
        self.connection
            .execute_batch(&format!(
                "
                CREATE TABLE IF NOT EXISTS {} (
                    id INTEGER PRIMARY KEY,
                    payload_msgpack BLOB NOT NULL
                );
                ",
                table_name
            ))
            .map_err(|e| {
                format!(
                    "Failed to initialize table {} in {}: {}",
                    table_name,
                    self.db_path.display(),
                    e
                )
            })?;
        Ok(())
    }

    fn table_exists_with_name(&self, table_name: &str) -> Result<bool, String> {
        let existing_table_name: Option<String> = self
            .connection
            .query_row(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?1",
                params![table_name],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| {
                format!(
                    "Failed to query sqlite_master for table {} in {}: {}",
                    table_name,
                    self.db_path.display(),
                    e
                )
            })?;
        Ok(existing_table_name.is_some())
    }

    fn table_name(table_key_text: String) -> String {
        format!("table_{}", hex_encode(table_key_text.as_bytes()))
    }
}

fn decode_payload_row<V>(row: &Row<'_>) -> rusqlite::Result<V>
where
    V: DeserializeOwned,
{
    let payload_msgpack: Vec<u8> = row.get(0)?;
    rmp_serde::from_slice(&payload_msgpack).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Blob, Box::new(e))
    })
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}
