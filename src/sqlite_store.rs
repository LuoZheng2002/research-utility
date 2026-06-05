use rusqlite::{Connection, Error as RusqliteError, ErrorCode, OptionalExtension, params};
use serde::{Serialize, de::DeserializeOwned};
use std::{marker::PhantomData, path::PathBuf, thread::sleep, time::Duration};

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
    connection: Connection,
    key_marker: PhantomData<K>,
    value_marker: PhantomData<V>,
}

impl<K, V> SqliteStore<K, V>
where
    K: SqliteStoreKey,
    V: Serialize + DeserializeOwned,
{
    fn is_locking_protocol_error(error: &RusqliteError) -> bool {
        error
            .to_string()
            .to_ascii_lowercase()
            .contains("locking protocol")
    }

    fn open_connection(
        db_path: &PathBuf,
        create_if_missing: bool,
        readonly: bool,
    ) -> Result<Connection, String> {
        if create_if_missing && readonly {
            return Err(format!(
                "Cannot create sqlite database {} in readonly mode for table {}",
                db_path.display(),
                SQLITE_STORE_TABLE_NAME
            ));
        }

        let flags = if readonly {
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_URI
        } else if create_if_missing {
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE
                | rusqlite::OpenFlags::SQLITE_OPEN_URI
        } else {
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_URI
        };

        let connection = if readonly {
            let sqlite_uri = format!("file:{}?mode=ro&immutable=1", db_path.display());
            Connection::open_with_flags(&sqlite_uri, flags).map_err(|e| {
                format!(
                    "Failed to open sqlite database {} for table {} in readonly immutable mode: {}",
                    db_path.display(),
                    SQLITE_STORE_TABLE_NAME,
                    e
                )
            })?
        } else {
            Connection::open_with_flags(db_path, flags).map_err(|e| {
                format!(
                    "Failed to open sqlite database {} for table {}: {}",
                    db_path.display(),
                    SQLITE_STORE_TABLE_NAME,
                    e
                )
            })?
        };

        connection
            .busy_timeout(Duration::from_secs(SQLITE_BUSY_TIMEOUT_SECS))
            .map_err(|e| {
                format!(
                    "Failed to set sqlite busy timeout on {}: {}",
                    db_path.display(),
                    e
                )
            })?;
        if !readonly {
            match connection.pragma_update(None, "journal_mode", "WAL") {
                Ok(()) => {}
                Err(error) if Self::is_locking_protocol_error(&error) => {
                    connection
                        .pragma_update(None, "journal_mode", "DELETE")
                        .map_err(|fallback_error| {
                            format!(
                                "Failed to set sqlite journal_mode on {} (WAL error: {}; DELETE fallback error: {})",
                                db_path.display(),
                                error,
                                fallback_error
                            )
                        })?;
                }
                Err(error) => {
                    return Err(format!(
                        "Failed to set sqlite journal_mode on {}: {}",
                        db_path.display(),
                        error
                    ));
                }
            }
            connection
                .pragma_update(None, "synchronous", "NORMAL")
                .map_err(|e| {
                    format!(
                        "Failed to set sqlite synchronous pragma on {}: {}",
                        db_path.display(),
                        e
                    )
                })?;
        }

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

    fn table_exists_sync(connection: &Connection, db_path: &PathBuf) -> Result<bool, String> {
        let existing_table_name: Result<Option<String>, String> = connection
            .query_row(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?1",
                params![SQLITE_STORE_TABLE_NAME],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| {
                format!(
                    "Failed to query sqlite_master for table {} in {}: {}",
                    SQLITE_STORE_TABLE_NAME,
                    db_path.display(),
                    e
                )
            });
        Ok(existing_table_name?.is_some())
    }

    fn create_table_if_missing(connection: &Connection, db_path: &PathBuf) -> Result<(), String> {
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
            .map_err(|e| {
                format!(
                    "Failed to initialize sqlite table {} in {}: {}",
                    SQLITE_STORE_TABLE_NAME,
                    db_path.display(),
                    e
                )
            })
            .map(|_| ())
    }

    pub fn initialize(db_path: impl Into<PathBuf>) -> Result<Self, String> {
        let db_path = db_path.into();
        if db_path.exists() {
            return Err(format!(
                "Expected sqlite database {} to not exist when initializing table {}",
                db_path.display(),
                SQLITE_STORE_TABLE_NAME
            ));
        }
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                format!(
                    "Failed to create parent directory for sqlite database {}: {}",
                    db_path.display(),
                    e
                )
            })?;
        }

        let connection = Self::open_connection(&db_path, true, false)?;
        Self::create_table_if_missing(&connection, &db_path)?;
        Ok(Self {
            db_path,
            connection,
            key_marker: PhantomData,
            value_marker: PhantomData,
        })
    }

    pub fn assume_initialized(db_path: impl Into<PathBuf>, readonly: bool) -> Result<Self, String> {
        let db_path = db_path.into();
        if !db_path.exists() {
            return Err(format!(
                "Expected sqlite database {} to exist before assuming table {} is initialized",
                db_path.display(),
                SQLITE_STORE_TABLE_NAME
            ));
        }
        let connection = Self::open_connection(&db_path, false, readonly)?;
        if !Self::table_exists_sync(&connection, &db_path)? {
            return Err(format!(
                "Expected sqlite table {} to exist in {} when assuming initialized",
                SQLITE_STORE_TABLE_NAME,
                db_path.display()
            ));
        }
        Ok(Self {
            db_path,
            connection,
            key_marker: PhantomData,
            value_marker: PhantomData,
        })
    }

    pub fn initialize_if_missing(
        db_path: impl Into<PathBuf>,
        readonly: bool,
    ) -> Result<Self, String> {
        let db_path = db_path.into();
        if db_path.exists() {
            Self::assume_initialized(db_path, readonly)
        } else if readonly {
            Err(format!(
                "Expected sqlite database {} to exist before opening readonly table {}",
                db_path.display(),
                SQLITE_STORE_TABLE_NAME
            ))
        } else {
            Self::initialize(db_path)
        }
    }

    pub fn clear(&self) -> Result<(), String> {
        self.connection
            .execute(&format!("DELETE FROM {}", SQLITE_STORE_TABLE_NAME), [])
            .map(|_| ())
            .map_err(|e| {
                format!(
                    "Failed to clear sqlite table {} in {}: {}",
                    SQLITE_STORE_TABLE_NAME,
                    self.db_path.display(),
                    e
                )
            })
    }

    pub fn upsert(
        &self,
        key: K,
        value: &V,
        retry_config: Option<SqliteBusyRetryConfig>,
    ) -> Result<(), String> {
        let payload_msgpack = rmp_serde::to_vec_named(value).map_err(|e| {
            format!(
                "Failed to serialize sqlite payload for table {} in {}: {}",
                SQLITE_STORE_TABLE_NAME,
                self.db_path.display(),
                e
            )
        })?;
        let key_text = key.to_key_text();
        let (max_retries, base_delay_ms) = if let Some(retry_config) = retry_config {
            (retry_config.max_retries, retry_config.base_delay_ms)
        } else {
            (0, SQLITE_BUSY_BASE_DELAY_MS)
        };
        for attempt in 0..=max_retries {
            let upsert_result = self.connection.execute(
                &format!(
                    "
                    INSERT INTO {} (id, payload_msgpack)
                    VALUES (?1, ?2)
                    ON CONFLICT(id) DO UPDATE SET payload_msgpack = excluded.payload_msgpack
                    ",
                    SQLITE_STORE_TABLE_NAME
                ),
                params![key_text, payload_msgpack],
            );
            match upsert_result {
                Ok(_) => return Ok(()),
                Err(error)
                    if Self::is_sqlite_busy_or_locked(&error) && attempt < max_retries =>
                {
                    sleep(Self::busy_retry_delay(attempt, base_delay_ms));
                }
                Err(error) => {
                    return Err(format!(
                        "Failed to upsert sqlite payload for key {} in table {} at {} after {} retries: {}",
                        key_text,
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
            key_text,
            SQLITE_STORE_TABLE_NAME,
            self.db_path.display()
        ))
    }

    pub fn get(&self, key: K) -> Result<Option<V>, String> {
        let key_text = key.to_key_text();
        let payload_msgpack = self
            .connection
            .query_row(
                &format!(
                    "SELECT payload_msgpack FROM {} WHERE id = ?1",
                    SQLITE_STORE_TABLE_NAME
                ),
                params![key_text],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()
            .map_err(|e| {
                format!(
                    "Failed to query key {} from table {} at {}: {}",
                    key_text,
                    SQLITE_STORE_TABLE_NAME,
                    self.db_path.display(),
                    e
                )
            })?;
        payload_msgpack
            .map(|msgpack| {
                rmp_serde::from_slice::<V>(&msgpack).map_err(|e| {
                    format!(
                        "Failed to deserialize sqlite payload for table {} in {}: {}",
                        SQLITE_STORE_TABLE_NAME,
                        self.db_path.display(),
                        e
                    )
                })
            })
            .transpose()
    }

    pub fn get_or_init<F>(
        &self,
        key: K,
        initialize_value: F,
        retry_config: Option<SqliteBusyRetryConfig>,
    ) -> Result<V, String>
    where
        F: FnOnce() -> V,
    {
        let key_text = key.to_key_text();
        let payload_msgpack = self
            .connection
            .query_row(
                &format!(
                    "SELECT payload_msgpack FROM {} WHERE id = ?1",
                    SQLITE_STORE_TABLE_NAME
                ),
                params![&key_text],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()
            .map_err(|e| {
                format!(
                    "Failed to query key {} from table {} at {}: {}",
                    key_text,
                    SQLITE_STORE_TABLE_NAME,
                    self.db_path.display(),
                    e
                )
            })?;

        if let Some(payload_msgpack) = payload_msgpack {
            return rmp_serde::from_slice::<V>(&payload_msgpack).map_err(|e| {
                format!(
                    "Failed to deserialize sqlite payload for table {} in {}: {}",
                    SQLITE_STORE_TABLE_NAME,
                    self.db_path.display(),
                    e
                )
            });
        }

        let initial_value = initialize_value();
        let initial_payload_msgpack = rmp_serde::to_vec_named(&initial_value).map_err(|e| {
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
            let insert_result = self.connection.execute(
                &format!(
                    "
                    INSERT INTO {} (id, payload_msgpack)
                    VALUES (?1, ?2)
                    ON CONFLICT(id) DO NOTHING
                    ",
                    SQLITE_STORE_TABLE_NAME
                ),
                params![&key_text, &initial_payload_msgpack],
            );
            match insert_result {
                Ok(rows_affected) => {
                    if rows_affected > 0 {
                        return Ok(initial_value);
                    }

                    let existing_payload_msgpack = self
                        .connection
                        .query_row(
                            &format!(
                                "SELECT payload_msgpack FROM {} WHERE id = ?1",
                                SQLITE_STORE_TABLE_NAME
                            ),
                            params![&key_text],
                            |row| row.get::<_, Vec<u8>>(0),
                        )
                        .optional()
                        .map_err(|e| {
                            format!(
                                "Failed to query key {} from table {} at {}: {}",
                                key_text,
                                SQLITE_STORE_TABLE_NAME,
                                self.db_path.display(),
                                e
                            )
                        })?
                        .ok_or_else(|| {
                            format!(
                                "Insert conflict for key {} in table {} at {} returned no row",
                                key_text,
                                SQLITE_STORE_TABLE_NAME,
                                self.db_path.display()
                            )
                        })?;

                    return rmp_serde::from_slice::<V>(&existing_payload_msgpack).map_err(|e| {
                        format!(
                            "Failed to deserialize sqlite payload for table {} in {}: {}",
                            SQLITE_STORE_TABLE_NAME,
                            self.db_path.display(),
                            e
                        )
                    });
                }
                Err(error) if Self::is_sqlite_busy_or_locked(&error) && attempt < max_retries => {
                    sleep(Self::busy_retry_delay(attempt, base_delay_ms));
                }
                Err(error) => {
                    return Err(format!(
                        "Failed to initialize sqlite payload for key {} in table {} at {} after {} retries: {}",
                        key_text,
                        SQLITE_STORE_TABLE_NAME,
                        self.db_path.display(),
                        attempt,
                        error
                    ));
                }
            }
        }
        Err(format!(
            "Failed to initialize sqlite payload for key {} in table {} at {} due to persistent sqlite lock",
            key_text,
            SQLITE_STORE_TABLE_NAME,
            self.db_path.display()
        ))
    }

    pub fn get_keys(&self) -> Result<Vec<K>, String> {
        let mut statement = self
            .connection
            .prepare(&format!("SELECT id FROM {} ORDER BY id ASC", SQLITE_STORE_TABLE_NAME))
            .map_err(|e| {
                format!(
                    "Failed to query keys from table {} in {}: {}",
                    SQLITE_STORE_TABLE_NAME,
                    self.db_path.display(),
                    e
                )
            })?;
        let key_texts = statement
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
            })?;
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

    pub fn load_all(&self) -> Result<Vec<V>, String> {
        let mut statement = self
            .connection
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
        let payload_rows = statement
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
            })?;
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

}
