use rusqlite::{Connection, OptionalExtension, Row, Statement, params};
use serde::{Serialize, de::DeserializeOwned};
use std::{marker::PhantomData, path::PathBuf};

const SQLITE_STORE_TABLE_NAME: &str = "store_entries";

pub trait SqliteStoreKey {
    fn to_key_text(&self) -> String;
}

impl SqliteStoreKey for usize {
    fn to_key_text(&self) -> String {
        self.to_string()
    }
}

impl SqliteStoreKey for i64 {
    fn to_key_text(&self) -> String {
        self.to_string()
    }
}

impl SqliteStoreKey for String {
    fn to_key_text(&self) -> String {
        self.clone()
    }
}

impl SqliteStoreKey for &str {
    fn to_key_text(&self) -> String {
        self.to_string()
    }
}

#[derive(Debug)]
pub struct SqliteStore<K, V> {
    db_path: PathBuf,
    connection: Connection,
    key_marker: PhantomData<K>,
    value_marker: PhantomData<V>,
}

impl<K, V> SqliteStore<K, V>
where
    K: SqliteStoreKey,
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
        let connection = Connection::open(&db_path).map_err(|e| {
            format!(
                "Failed to open sqlite database {} for table {}: {}",
                db_path.display(),
                SQLITE_STORE_TABLE_NAME,
                e
            )
        })?;
        let store = Self {
            db_path,
            connection,
            key_marker: PhantomData,
            value_marker: PhantomData,
        };
        store.initialize_schema()?;
        Ok(store)
    }

    pub fn initialize_schema(&self) -> Result<(), String> {
        self.connection
            .execute_batch(&format!(
                "
                CREATE TABLE IF NOT EXISTS {} (
                    id TEXT PRIMARY KEY,
                    payload_msgpack BLOB NOT NULL
                );
                ",
                SQLITE_STORE_TABLE_NAME
            ))
            .map_err(|e| {
                format!(
                    "Failed to initialize sqlite table {} in {}: {}",
                    SQLITE_STORE_TABLE_NAME,
                    self.db_path.display(),
                    e
                )
            })?;
        Ok(())
    }

    pub fn clear(&self) -> Result<(), String> {
        self.initialize_schema()?;
        self.connection
            .execute(&format!("DELETE FROM {}", SQLITE_STORE_TABLE_NAME), [])
            .map_err(|e| {
                format!(
                    "Failed to clear sqlite table {} in {}: {}",
                    SQLITE_STORE_TABLE_NAME,
                    self.db_path.display(),
                    e
                )
            })?;
        Ok(())
    }

    pub fn upsert(&self, key: K, value: &V) -> Result<(), String> {
        self.initialize_schema()?;
        let key_text = key.to_key_text();
        let payload_msgpack = rmp_serde::to_vec_named(value).map_err(|e| {
            format!(
                "Failed to serialize sqlite payload for table {} in {}: {}",
                SQLITE_STORE_TABLE_NAME,
                self.db_path.display(),
                e
            )
        })?;
        self.connection
            .execute(
                &format!(
                    "
                    INSERT INTO {} (id, payload_msgpack)
                    VALUES (?1, ?2)
                    ON CONFLICT(id) DO UPDATE SET payload_msgpack = excluded.payload_msgpack
                    ",
                    SQLITE_STORE_TABLE_NAME
                ),
                params![key_text, payload_msgpack],
            )
            .map_err(|e| {
                format!(
                    "Failed to upsert sqlite payload for key {} in table {} at {}: {}",
                    key.to_key_text(),
                    SQLITE_STORE_TABLE_NAME,
                    self.db_path.display(),
                    e
                )
            })?;
        Ok(())
    }

    pub fn get(&self, key: K) -> Result<Option<V>, String> {
        self.initialize_schema()?;
        let key_text = key.to_key_text();
        let payload: Option<Vec<u8>> = self
            .connection
            .query_row(
                &format!(
                    "SELECT payload_msgpack FROM {} WHERE id = ?1",
                    SQLITE_STORE_TABLE_NAME
                ),
                params![key_text],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| {
                format!(
                    "Failed to query key {} from table {} at {}: {}",
                    key.to_key_text(),
                    SQLITE_STORE_TABLE_NAME,
                    self.db_path.display(),
                    e
                )
            })?;
        payload
            .map(|msgpack| {
                rmp_serde::from_slice::<V>(&msgpack).map_err(|e| {
                    format!(
                        "Failed to deserialize sqlite payload for key {} from table {} at {}: {}",
                        key.to_key_text(),
                        SQLITE_STORE_TABLE_NAME,
                        self.db_path.display(),
                        e
                    )
                })
            })
            .transpose()
    }

    pub fn statement(&self) -> Result<SqliteStoreStatement<'_, V>, String> {
        self.initialize_schema()?;
        let statement = self
            .connection
            .prepare(&format!(
                "
                SELECT payload_msgpack
                FROM {}
                ORDER BY id ASC
                ",
                SQLITE_STORE_TABLE_NAME
            ))
            .map_err(|e| {
                format!(
                    "Failed to prepare scan statement for table {} in {}: {}",
                    SQLITE_STORE_TABLE_NAME,
                    self.db_path.display(),
                    e
                )
            })?;
        Ok(SqliteStoreStatement {
            statement,
            db_path: self.db_path.clone(),
            value_marker: PhantomData,
        })
    }

    pub fn load_all(&self) -> Result<Vec<V>, String> {
        let mut statement = self.statement()?;
        let rows = statement.try_iter()?;
        let mut values = Vec::new();
        for row in rows {
            let value = row.map_err(|e| {
                format!(
                    "Failed to read row from table {} in {}: {}",
                    SQLITE_STORE_TABLE_NAME,
                    self.db_path.display(),
                    e
                )
            })?;
            values.push(value);
        }
        Ok(values)
    }
}

pub struct SqliteStoreStatement<'conn, V> {
    statement: Statement<'conn>,
    db_path: PathBuf,
    value_marker: PhantomData<V>,
}

impl<V> SqliteStoreStatement<'_, V>
where
    V: DeserializeOwned,
{
    pub fn try_iter(
        &mut self,
    ) -> Result<rusqlite::MappedRows<'_, fn(&Row<'_>) -> rusqlite::Result<V>>, String> {
        self.statement
            .query_map(
                [],
                decode_payload_row::<V> as fn(&Row<'_>) -> rusqlite::Result<V>,
            )
            .map_err(|e| {
                format!(
                    "Failed to execute scan query for table {} in {}: {}",
                    SQLITE_STORE_TABLE_NAME,
                    self.db_path.display(),
                    e
                )
            })
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
