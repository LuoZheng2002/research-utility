use serde::{Serialize, de::DeserializeOwned};
use sqlx::{Row, SqlitePool, sqlite::SqliteConnectOptions, sqlite::SqlitePoolOptions};
use std::{marker::PhantomData, path::PathBuf};

const SQLITE_STORE_TABLE_NAME: &str = "store_entries";

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
    pool: SqlitePool,
    key_marker: PhantomData<K>,
    value_marker: PhantomData<V>,
}

impl<K, V> Clone for SqliteStore<K, V> {
    fn clone(&self) -> Self {
        Self {
            db_path: self.db_path.clone(),
            pool: self.pool.clone(),
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

        let connect_options = SqliteConnectOptions::new()
            .filename(&db_path)
            .create_if_missing(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(connect_options)
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "Failed to open sqlite database {} for table {}: {}",
                    db_path.display(),
                    SQLITE_STORE_TABLE_NAME,
                    e
                )
            });

        let store = Self {
            db_path,
            pool,
            key_marker: PhantomData,
            value_marker: PhantomData,
        };

        sqlx::query(&format!(
            "
            CREATE TABLE IF NOT EXISTS {} (
                id TEXT PRIMARY KEY,
                payload_msgpack BLOB NOT NULL
            )
            ",
            SQLITE_STORE_TABLE_NAME
        ))
        .execute(&store.pool)
        .await
        .unwrap_or_else(|e| {
            panic!(
                "Failed to initialize sqlite table {} in {}: {}",
                SQLITE_STORE_TABLE_NAME,
                store.db_path.display(),
                e
            )
        });

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

        let connect_options = SqliteConnectOptions::new()
            .filename(&db_path)
            .create_if_missing(false);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(connect_options)
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "Failed to open sqlite database {} for table {}: {}",
                    db_path.display(),
                    SQLITE_STORE_TABLE_NAME,
                    e
                )
            });

        let store = Self {
            db_path,
            pool,
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
        sqlx::query(&format!("DELETE FROM {}", SQLITE_STORE_TABLE_NAME))
            .execute(&self.pool)
            .await
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

    pub async fn upsert(&self, key: K, value: &V) -> Result<(), String> {
        let key_text = key.to_key_text();
        let payload_msgpack = rmp_serde::to_vec_named(value).map_err(|e| {
            format!(
                "Failed to serialize sqlite payload for table {} in {}: {}",
                SQLITE_STORE_TABLE_NAME,
                self.db_path.display(),
                e
            )
        })?;

        sqlx::query(&format!(
            "
            INSERT INTO {} (id, payload_msgpack)
            VALUES (?1, ?2)
            ON CONFLICT(id) DO UPDATE SET payload_msgpack = excluded.payload_msgpack
            ",
            SQLITE_STORE_TABLE_NAME
        ))
        .bind(&key_text)
        .bind(payload_msgpack)
        .execute(&self.pool)
        .await
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

    pub async fn get(&self, key: K) -> Result<Option<V>, String> {
        let key_text = key.to_key_text();
        let row = sqlx::query(&format!(
            "SELECT payload_msgpack FROM {} WHERE id = ?1",
            SQLITE_STORE_TABLE_NAME
        ))
        .bind(&key_text)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            format!(
                "Failed to query key {} from table {} at {}: {}",
                key.to_key_text(),
                SQLITE_STORE_TABLE_NAME,
                self.db_path.display(),
                e
            )
        })?;

        row.map(|row| {
            let msgpack: Vec<u8> = row.try_get(0).map_err(|e| {
                format!(
                    "Failed to decode sqlite payload blob for key {} from table {} at {}: {}",
                    key.to_key_text(),
                    SQLITE_STORE_TABLE_NAME,
                    self.db_path.display(),
                    e
                )
            })?;
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
        let rows: Vec<sqlx::sqlite::SqliteRow> = sqlx::query(&format!(
            "SELECT id FROM {} ORDER BY id ASC",
            SQLITE_STORE_TABLE_NAME
        ))
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            format!(
                "Failed to query keys from table {} in {}: {}",
                SQLITE_STORE_TABLE_NAME,
                self.db_path.display(),
                e
            )
        })?;

        rows.into_iter()
            .map(|row| {
                let key_text: String = row.try_get(0).map_err(|e| {
                    format!(
                        "Failed to decode key text from table {} in {}: {}",
                        SQLITE_STORE_TABLE_NAME,
                        self.db_path.display(),
                        e
                    )
                })?;
                K::from_key_text(&key_text)
            })
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
        let rows: Vec<sqlx::sqlite::SqliteRow> = sqlx::query(&format!(
            "SELECT payload_msgpack FROM {} ORDER BY id ASC",
            SQLITE_STORE_TABLE_NAME
        ))
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            format!(
                "Failed to execute scan query for table {} in {}: {}",
                SQLITE_STORE_TABLE_NAME,
                self.db_path.display(),
                e
            )
        })?;

        rows.into_iter()
            .map(|row| {
                let payload_msgpack: Vec<u8> = row.try_get(0).map_err(|e| {
                    format!(
                        "Failed to decode row payload for table {} in {}: {}",
                        SQLITE_STORE_TABLE_NAME,
                        self.db_path.display(),
                        e
                    )
                })?;
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
        let existing_table_name: Option<String> = sqlx::query_scalar(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?1",
        )
        .bind(SQLITE_STORE_TABLE_NAME)
        .fetch_optional(&self.pool)
        .await
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
