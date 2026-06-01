use rusqlite::{Connection, Error as RusqliteError, ErrorCode, OptionalExtension, params};
use serde::{Serialize, de::DeserializeOwned};
use std::{marker::PhantomData, path::PathBuf, sync::Arc, time::Duration};

const SQLITE_STORE_TABLE_NAME: &str = "store_entries";
const SQLITE_BUSY_MAX_RETRIES: usize = 12;
const SQLITE_BUSY_BASE_DELAY_MS: u64 = 25;
const SQLITE_BUSY_TIMEOUT_SECS: u64 = 30;

#[derive(Debug, Clone, Copy)]
pub struct SqliteBusyRetryConfig {
    pub max_retries: usize,
    pub base_delay_ms: u64,
}

impl SqliteBusyRetryConfig {
    pub fn none() -> Option<Self> {
        None
    }

    pub fn aggressive() -> Option<Self> {
        Some(Self::default())
    }
}

impl Default for SqliteBusyRetryConfig {
    fn default() -> Self {
        Self {
            max_retries: SQLITE_BUSY_MAX_RETRIES,
            base_delay_ms: SQLITE_BUSY_BASE_DELAY_MS,
        }
    }
}

pub trait SqliteStoreKey {
    fn to_key_text(&self) -> String;
    fn from_key_text(key_text: &str) -> Result<Self, String>
    where
        Self: Sized;
}

impl SqliteStoreKey for usize {
    fn to_key_text(&self) -> String {
        self.to_string()
    }

    fn from_key_text(key_text: &str) -> Result<Self, String> {
        key_text
            .parse::<usize>()
            .map_err(|e| format!("Failed to parse key text '{}' as usize: {}", key_text, e))
    }
}

impl SqliteStoreKey for i64 {
    fn to_key_text(&self) -> String {
        self.to_string()
    }

    fn from_key_text(key_text: &str) -> Result<Self, String> {
        key_text
            .parse::<i64>()
            .map_err(|e| format!("Failed to parse key text '{}' as i64: {}", key_text, e))
    }
}

impl SqliteStoreKey for String {
    fn to_key_text(&self) -> String {
        self.clone()
    }

    fn from_key_text(key_text: &str) -> Result<Self, String> {
        Ok(key_text.to_string())
    }
}

#[derive(Debug)]
pub struct SqliteStore<K, V> {
    db_path: PathBuf,
    connection: Arc<std::sync::Mutex<Connection>>,
    key_marker: PhantomData<K>,
    value_marker: PhantomData<V>,
}

impl<K, V> Clone for SqliteStore<K, V> {
    fn clone(&self) -> Self {
        Self {
            db_path: self.db_path.clone(),
            connection: Arc::clone(&self.connection),
            key_marker: PhantomData,
            value_marker: PhantomData,
        }
    }
}

impl<K, V> SqliteStore<K, V>
where
    K: SqliteStoreKey,
    V: Serialize + DeserializeOwned,
{
    fn open_connection(db_path: &PathBuf, create_if_missing: bool) -> Result<Connection, String> {
        let flags = if create_if_missing {
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE
                | rusqlite::OpenFlags::SQLITE_OPEN_URI
        } else {
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_URI
        };

        let connection = Connection::open_with_flags(db_path, flags).map_err(|e| {
            format!(
                "Failed to open sqlite database {} for table {}: {}",
                db_path.display(),
                SQLITE_STORE_TABLE_NAME,
                e
            )
        })?;

        connection
            .busy_timeout(Duration::from_secs(SQLITE_BUSY_TIMEOUT_SECS))
            .map_err(|e| {
                format!(
                    "Failed to set sqlite busy timeout on {}: {}",
                    db_path.display(),
                    e
                )
            })?;
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .map_err(|e| {
                format!(
                    "Failed to set sqlite journal_mode on {}: {}",
                    db_path.display(),
                    e
                )
            })?;
        connection
            .pragma_update(None, "synchronous", "NORMAL")
            .map_err(|e| {
                format!(
                    "Failed to set sqlite synchronous pragma on {}: {}",
                    db_path.display(),
                    e
                )
            })?;

        Ok(connection)
    }

    fn is_sqlite_busy_or_locked(error: &RusqliteError) -> bool {
        let message = error.to_string().to_ascii_lowercase();
        if message.contains("database is locked") || message.contains("database table is locked") {
            return true;
        }

        let RusqliteError::SqliteFailure(sqlite_error, _) = error else {
            return false;
        };
        matches!(
            sqlite_error.code,
            ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked
        )
    }

    fn busy_retry_delay(attempt: usize, base_delay_ms: u64) -> Duration {
        let shift = attempt.min(8);
        Duration::from_millis(base_delay_ms * (1_u64 << shift))
    }

    pub async fn initialize(db_path: impl Into<PathBuf>) -> Self {
        let db_path = db_path.into();
        assert!(
            !db_path.exists(),
            "Expected sqlite database {} to not exist when initializing table {}",
            db_path.display(),
            SQLITE_STORE_TABLE_NAME
        );
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent).unwrap_or_else(|e| {
                panic!(
                    "Failed to create parent directory for sqlite database {}: {}",
                    db_path.display(),
                    e
                )
            });
        }

        let connection = Self::open_connection(&db_path, true).unwrap_or_else(|e| panic!("{}", e));

        let store = Self {
            db_path,
            connection: Arc::new(std::sync::Mutex::new(connection)),
            key_marker: PhantomData,
            value_marker: PhantomData,
        };

        {
            let connection = store.connection.lock().expect("sqlite mutex poisoned");
            connection
                .execute(
                    &format!(
                        "
                        CREATE TABLE IF NOT EXISTS {} (
                            id TEXT PRIMARY KEY,
                            payload_msgpack BLOB NOT NULL
                        )
                        ",
                        SQLITE_STORE_TABLE_NAME
                    ),
                    [],
                )
                .unwrap_or_else(|e| {
                    panic!(
                        "Failed to initialize sqlite table {} in {}: {}",
                        SQLITE_STORE_TABLE_NAME,
                        store.db_path.display(),
                        e
                    )
                });
        }

        store
    }

    pub async fn assume_initialized(db_path: impl Into<PathBuf>) -> Self {
        let db_path = db_path.into();
        assert!(
            db_path.exists(),
            "Expected sqlite database {} to exist before assuming table {} is initialized",
            db_path.display(),
            SQLITE_STORE_TABLE_NAME
        );

        let connection = Self::open_connection(&db_path, false).unwrap_or_else(|e| panic!("{}", e));

        let store = Self {
            db_path,
            connection: Arc::new(std::sync::Mutex::new(connection)),
            key_marker: PhantomData,
            value_marker: PhantomData,
        };

        assert!(
            store.table_exists().await,
            "Expected sqlite table {} to exist in {} when assuming initialized",
            SQLITE_STORE_TABLE_NAME,
            store.db_path.display()
        );
        store
    }

    pub async fn initialize_if_missing(db_path: impl Into<PathBuf>) -> Self {
        let db_path = db_path.into();
        if db_path.exists() {
            let store = Self::assume_initialized(db_path).await;
            assert!(
                store.table_exists().await,
                "Expected sqlite table {} to exist in {} when initializing-if-missing",
                SQLITE_STORE_TABLE_NAME,
                store.db_path.display()
            );
            store
        } else {
            Self::initialize(db_path).await
        }
    }

    pub async fn clear(&self) -> Result<(), String> {
        for attempt in 0..=SQLITE_BUSY_MAX_RETRIES {
            let result = {
                let connection = self.connection.lock().expect("sqlite mutex poisoned");
                connection.execute(&format!("DELETE FROM {}", SQLITE_STORE_TABLE_NAME), [])
            };

            match result {
                Ok(_) => return Ok(()),
                Err(error)
                    if Self::is_sqlite_busy_or_locked(&error)
                        && attempt < SQLITE_BUSY_MAX_RETRIES =>
                {
                    tokio::time::sleep(Self::busy_retry_delay(attempt, SQLITE_BUSY_BASE_DELAY_MS))
                        .await;
                }
                Err(error) => {
                    return Err(format!(
                        "Failed to clear sqlite table {} in {} after {} retries: {}",
                        SQLITE_STORE_TABLE_NAME,
                        self.db_path.display(),
                        attempt,
                        error
                    ));
                }
            }
        }

        Err(format!(
            "Failed to clear sqlite table {} in {} due to persistent sqlite lock",
            SQLITE_STORE_TABLE_NAME,
            self.db_path.display(),
        ))
    }

    pub async fn upsert(
        &self,
        key: K,
        value: &V,
        retry_config: Option<SqliteBusyRetryConfig>,
    ) -> Result<(), String> {
        let key_text = key.to_key_text();
        let payload_msgpack = rmp_serde::to_vec_named(value).map_err(|e| {
            format!(
                "Failed to serialize sqlite payload for table {} in {}: {}",
                SQLITE_STORE_TABLE_NAME,
                self.db_path.display(),
                e
            )
        })?;

        let (max_retries, base_delay_ms) = if let Some(retry_config) = retry_config {
            (retry_config.max_retries, retry_config.base_delay_ms)
        } else {
            (0, SQLITE_BUSY_BASE_DELAY_MS)
        };

        for attempt in 0..=max_retries {
            let result = {
                let connection = self.connection.lock().expect("sqlite mutex poisoned");
                connection.execute(
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
            };

            match result {
                Ok(_) => return Ok(()),
                Err(error) if Self::is_sqlite_busy_or_locked(&error) && attempt < max_retries => {
                    tokio::time::sleep(Self::busy_retry_delay(attempt, base_delay_ms)).await;
                }
                Err(error) => {
                    return Err(format!(
                        "Failed to upsert sqlite payload for key {} in table {} at {} after {} retries: {}",
                        key.to_key_text(),
                        SQLITE_STORE_TABLE_NAME,
                        self.db_path.display(),
                        attempt,
                        error
                    ));
                }
            }
        }

        Err(format!(
            "Failed to upsert sqlite payload for key {} in table {} at {} due to persistent sqlite lock",
            key.to_key_text(),
            SQLITE_STORE_TABLE_NAME,
            self.db_path.display(),
        ))
    }

    pub async fn get(&self, key: K) -> Result<Option<V>, String> {
        let key_text = key.to_key_text();
        let payload_msgpack: Option<Vec<u8>> = {
            let connection = self.connection.lock().expect("sqlite mutex poisoned");
            connection
                .query_row(
                    &format!(
                        "SELECT payload_msgpack FROM {} WHERE id = ?1",
                        SQLITE_STORE_TABLE_NAME
                    ),
                    params![key_text],
                    |row| row.get(0),
                )
                .optional()
        }
        .map_err(|e| {
            format!(
                "Failed to query key {} from table {} at {}: {}",
                key.to_key_text(),
                SQLITE_STORE_TABLE_NAME,
                self.db_path.display(),
                e
            )
        })?;

        payload_msgpack
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

    pub async fn get_keys(&self) -> Result<Vec<K>, String> {
        let key_texts = {
            let connection = self.connection.lock().expect("sqlite mutex poisoned");
            let mut statement = connection
                .prepare(&format!(
                    "SELECT id FROM {} ORDER BY id ASC",
                    SQLITE_STORE_TABLE_NAME
                ))
                .map_err(|e| {
                    format!(
                        "Failed to query keys from table {} in {}: {}",
                        SQLITE_STORE_TABLE_NAME,
                        self.db_path.display(),
                        e
                    )
                })?;

            statement
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(|e| {
                    format!(
                        "Failed to decode key text from table {} in {}: {}",
                        SQLITE_STORE_TABLE_NAME,
                        self.db_path.display(),
                        e
                    )
                })?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| {
                    format!(
                        "Failed to decode key text from table {} in {}: {}",
                        SQLITE_STORE_TABLE_NAME,
                        self.db_path.display(),
                        e
                    )
                })?
        };

        key_texts
            .into_iter()
            .map(|key_text| K::from_key_text(&key_text))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                format!(
                    "Failed to deserialize key from table {} in {}: {}",
                    SQLITE_STORE_TABLE_NAME,
                    self.db_path.display(),
                    e
                )
            })
    }

    pub async fn load_all(&self) -> Result<Vec<V>, String> {
        let payload_rows: Vec<Vec<u8>> = {
            let connection = self.connection.lock().expect("sqlite mutex poisoned");
            let mut statement = connection
                .prepare(&format!(
                    "SELECT payload_msgpack FROM {} ORDER BY id ASC",
                    SQLITE_STORE_TABLE_NAME
                ))
                .map_err(|e| {
                    format!(
                        "Failed to execute scan query for table {} in {}: {}",
                        SQLITE_STORE_TABLE_NAME,
                        self.db_path.display(),
                        e
                    )
                })?;

            statement
                .query_map([], |row| row.get::<_, Vec<u8>>(0))
                .map_err(|e| {
                    format!(
                        "Failed to decode row payload for table {} in {}: {}",
                        SQLITE_STORE_TABLE_NAME,
                        self.db_path.display(),
                        e
                    )
                })?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| {
                    format!(
                        "Failed to decode row payload for table {} in {}: {}",
                        SQLITE_STORE_TABLE_NAME,
                        self.db_path.display(),
                        e
                    )
                })?
        };

        payload_rows
            .into_iter()
            .map(|payload_msgpack| {
                rmp_serde::from_slice::<V>(&payload_msgpack).map_err(|e| {
                    format!(
                        "Failed to deserialize row payload for table {} in {}: {}",
                        SQLITE_STORE_TABLE_NAME,
                        self.db_path.display(),
                        e
                    )
                })
            })
            .collect()
    }

    async fn table_exists(&self) -> bool {
        let existing_table_name: Option<String> = {
            let connection = self.connection.lock().expect("sqlite mutex poisoned");
            connection
                .query_row(
                    "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?1",
                    params![SQLITE_STORE_TABLE_NAME],
                    |row| row.get(0),
                )
                .optional()
        }
        .unwrap_or_else(|e| {
            panic!(
                "Failed to query sqlite_master for table {} in {}: {}",
                SQLITE_STORE_TABLE_NAME,
                self.db_path.display(),
                e
            )
        });

        existing_table_name.is_some()
    }
}
