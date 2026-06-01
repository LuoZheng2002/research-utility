use serde::{Serialize, de::DeserializeOwned};
use sqlx::{Row, SqlitePool, sqlite::SqliteConnectOptions, sqlite::SqlitePoolOptions};
use std::{collections::HashSet, marker::PhantomData, path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::Mutex;

const SQLITE_BUSY_MAX_RETRIES: usize = 12;
const SQLITE_BUSY_BASE_DELAY_MS: u64 = 25;
const SQLITE_BUSY_TIMEOUT_SECS: u64 = 30;
const SQLITE_POOL_ACQUIRE_TIMEOUT_SECS: u64 = 300;

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

#[derive(Debug, Clone)]
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
    pool: SqlitePool,
    initialized_tables: Arc<Mutex<HashSet<String>>>,
    key_marker: PhantomData<K>,
    value_marker: PhantomData<V>,
}

impl<K, V> SqliteTableArrayStore<K, V>
where
    K: SqliteTableArrayKey,
    V: Serialize + DeserializeOwned,
{
    fn sqlite_connect_options(db_path: &PathBuf, create_if_missing: bool) -> SqliteConnectOptions {
        SqliteConnectOptions::new()
            .filename(db_path)
            .create_if_missing(create_if_missing)
            .busy_timeout(Duration::from_secs(SQLITE_BUSY_TIMEOUT_SECS))
            .pragma("journal_mode", "WAL")
            .pragma("synchronous", "NORMAL")
    }

    fn is_sqlite_busy_or_locked(error: &sqlx::Error) -> bool {
        let message = error.to_string().to_ascii_lowercase();
        if message.contains("database is locked") || message.contains("database table is locked") {
            return true;
        }
        let sqlx::Error::Database(database_error) = error else {
            return false;
        };
        if let Some(code) = database_error.code() {
            return code == "5" || code == "6";
        }
        false
    }

    fn is_pool_timeout(error: &sqlx::Error) -> bool {
        matches!(error, sqlx::Error::PoolTimedOut)
    }

    fn is_retryable_write_error(error: &sqlx::Error) -> bool {
        Self::is_sqlite_busy_or_locked(error) || Self::is_pool_timeout(error)
    }

    fn busy_retry_delay(attempt: usize, base_delay_ms: u64) -> Duration {
        let shift = attempt.min(8);
        Duration::from_millis(base_delay_ms * (1_u64 << shift))
    }

    pub async fn new(db_path: impl Into<PathBuf>) -> Result<Self, String> {
        Self::new_with_max_connections(db_path, 1).await
    }

    pub async fn new_with_max_connections(
        db_path: impl Into<PathBuf>,
        max_connections: u32,
    ) -> Result<Self, String> {
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

        let connect_options = Self::sqlite_connect_options(&db_path, true);
        let pool = SqlitePoolOptions::new()
            .max_connections(max_connections)
            .acquire_timeout(Duration::from_secs(SQLITE_POOL_ACQUIRE_TIMEOUT_SECS))
            .connect_with(connect_options)
            .await
            .map_err(|e| {
                format!(
                    "Failed to open sqlite database {}: {}",
                    db_path.display(),
                    e
                )
            })?;

        Ok(Self {
            db_path,
            pool,
            initialized_tables: Arc::new(Mutex::new(HashSet::new())),
            key_marker: PhantomData,
            value_marker: PhantomData,
        })
    }

    pub async fn append(&self, table_key: K, value: &V) -> Result<(), String> {
        let table_name = Self::table_name(table_key.to_table_key_text());
        self.initialize_table(&table_name).await?;
        let next_row_index = self.next_row_index(&table_name).await?;
        self.append_payload_at_index(&table_name, next_row_index, value)
            .await
    }

    pub async fn append_at(&self, table_key: K, row_index: usize, value: &V) -> Result<(), String> {
        let table_name = Self::table_name(table_key.to_table_key_text());
        self.initialize_table(&table_name).await?;
        self.append_payload_at_index(&table_name, row_index as i64, value)
            .await
    }

    async fn append_payload_at_index(
        &self,
        table_name: &str,
        row_index: i64,
        value: &V,
    ) -> Result<(), String> {
        let payload_msgpack = rmp_serde::to_vec_named(value).map_err(|e| {
            format!(
                "Failed to serialize sqlite payload for table {} in {}: {}",
                table_name,
                self.db_path.display(),
                e
            )
        })?;
        for attempt in 0..=SQLITE_BUSY_MAX_RETRIES {
            let result = sqlx::query(&format!(
                "
                INSERT INTO {} (row_index, payload_msgpack)
                VALUES (?1, ?2)
                ON CONFLICT(row_index) DO NOTHING
                ",
                table_name
            ))
            .bind(row_index)
            .bind(&payload_msgpack)
            .execute(&self.pool)
            .await;
            match result {
                Ok(query_result) => {
                    if query_result.rows_affected() > 0 {
                        return Ok(());
                    }
                    let existing_payload = self
                        .load_payload_at_index(table_name, row_index)
                        .await?
                        .ok_or_else(|| {
                            format!(
                                "Conflict insert at row_index {} for table {} in {} returned no row",
                                row_index,
                                table_name,
                                self.db_path.display()
                            )
                        })?;
                    if existing_payload == payload_msgpack {
                        return Ok(());
                    }
                    return Err(format!(
                        "Conflicting payload at row_index {} for table {} in {}",
                        row_index,
                        table_name,
                        self.db_path.display()
                    ));
                }
                Err(error)
                    if Self::is_retryable_write_error(&error) && attempt < SQLITE_BUSY_MAX_RETRIES =>
                {
                    tokio::time::sleep(Self::busy_retry_delay(attempt, SQLITE_BUSY_BASE_DELAY_MS))
                        .await;
                }
                Err(error) => {
                    return Err(format!(
                        "Failed to append sqlite payload into table {} at {} after {} retries: {}",
                        table_name,
                        self.db_path.display(),
                        attempt,
                        error
                    ));
                }
            }
        }
        Err(format!(
            "Failed to append sqlite payload into table {} at {} due to persistent sqlite lock",
            table_name,
            self.db_path.display()
        ))
    }

    async fn load_payload_at_index(
        &self,
        table_name: &str,
        row_index: i64,
    ) -> Result<Option<Vec<u8>>, String> {
        sqlx::query(&format!(
            "
            SELECT payload_msgpack
            FROM {}
            WHERE row_index = ?1
            ",
            table_name
        ))
        .bind(row_index)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            format!(
                "Failed to fetch row_index {} from table {} in {}: {}",
                row_index,
                table_name,
                self.db_path.display(),
                e
            )
        })?
        .map(|row| {
            row.try_get::<Vec<u8>, _>(0).map_err(|e| {
                format!(
                    "Failed to decode row payload for row_index {} in table {} in {}: {}",
                    row_index,
                    table_name,
                    self.db_path.display(),
                    e
                )
            })
        })
        .transpose()
    }

    async fn next_row_index(&self, table_name: &str) -> Result<i64, String> {
        let max_row_index: Option<i64> = sqlx::query_scalar(&format!(
            "SELECT MAX(row_index) FROM {}",
            table_name
        ))
        .fetch_one(&self.pool)
        .await
        .map_err(|e| {
            format!(
                "Failed to query max row_index for table {} in {}: {}",
                table_name,
                self.db_path.display(),
                e
            )
        })?;
        Ok(max_row_index.map_or(0, |value| value + 1))
    }

    pub async fn load_table(&self, table_key: K) -> Result<Vec<V>, String> {
        self.load_table_with_indices(table_key)
            .await
            .map(|rows| rows.into_iter().map(|(_, value)| value).collect())
    }

    pub async fn load_table_with_indices(&self, table_key: K) -> Result<Vec<(usize, V)>, String> {
        let table_name = Self::table_name(table_key.to_table_key_text());
        if !self.table_exists_with_name(&table_name).await? {
            return Ok(Vec::new());
        }

        let rows = sqlx::query(&format!(
            "
            SELECT row_index, payload_msgpack
            FROM {}
            ORDER BY row_index ASC
            ",
            table_name
        ))
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            format!(
                "Failed to execute ordered scan query for table {} in {}: {}",
                table_name,
                self.db_path.display(),
                e
            )
        })?;

        rows.into_iter()
            .map(|row| {
                let row_index: i64 = row.try_get(0).map_err(|e| {
                    format!(
                        "Failed to decode row index for table {} in {}: {}",
                        table_name,
                        self.db_path.display(),
                        e
                    )
                })?;
                if row_index < 0 {
                    return Err(format!(
                        "Negative row index {} found in table {} in {}",
                        row_index,
                        table_name,
                        self.db_path.display()
                    ));
                }
                let payload_msgpack: Vec<u8> = row.try_get(1).map_err(|e| {
                    format!(
                        "Failed to decode row payload for table {} in {}: {}",
                        table_name,
                        self.db_path.display(),
                        e
                    )
                })?;
                let value = rmp_serde::from_slice(&payload_msgpack).map_err(|e| {
                    format!(
                        "Failed to deserialize row payload for table {} in {}: {}",
                        table_name,
                        self.db_path.display(),
                        e
                    )
                })?;
                Ok((row_index as usize, value))
            })
            .collect()
    }

    pub async fn clear_table(&self, table_key: K) -> Result<(), String> {
        let table_name = Self::table_name(table_key.to_table_key_text());
        if !self.table_exists_with_name(&table_name).await? {
            return Ok(());
        }
        sqlx::query(&format!("DELETE FROM {}", table_name))
            .execute(&self.pool)
            .await
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

    pub async fn drop_table(&self, table_key: K) -> Result<(), String> {
        let table_name = Self::table_name(table_key.to_table_key_text());
        sqlx::query(&format!("DROP TABLE IF EXISTS {}", table_name))
            .execute(&self.pool)
            .await
            .map_err(|e| {
                format!(
                    "Failed to drop table {} in {}: {}",
                    table_name,
                    self.db_path.display(),
                    e
                )
            })?;
        self.initialized_tables.lock().await.remove(&table_name);
        Ok(())
    }

    pub async fn table_exists(&self, table_key: K) -> Result<bool, String> {
        let table_name = Self::table_name(table_key.to_table_key_text());
        self.table_exists_with_name(&table_name).await
    }

    async fn initialize_table(&self, table_name: &str) -> Result<(), String> {
        {
            if self.initialized_tables.lock().await.contains(table_name) {
                return Ok(());
            }
        }
        sqlx::query(&format!(
            "
            CREATE TABLE IF NOT EXISTS {} (
                id INTEGER PRIMARY KEY,
                row_index INTEGER NOT NULL,
                UNIQUE(row_index),
                payload_msgpack BLOB NOT NULL
            )
            ",
            table_name
        ))
        .execute(&self.pool)
        .await
        .map_err(|e| {
            format!(
                "Failed to initialize table {} in {}: {}",
                table_name,
                self.db_path.display(),
                e
            )
        })?;
        self.initialized_tables
            .lock()
            .await
            .insert(table_name.to_string());
        Ok(())
    }

    async fn table_exists_with_name(&self, table_name: &str) -> Result<bool, String> {
        let existing_table_name: Option<String> =
            sqlx::query_scalar("SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?1")
                .bind(table_name)
                .fetch_optional(&self.pool)
                .await
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

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}
