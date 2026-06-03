use rusqlite::{Connection, Error as RusqliteError, ErrorCode, OptionalExtension, params};
use serde::{Serialize, de::DeserializeOwned};
use std::{marker::PhantomData, path::PathBuf, thread::sleep, time::Duration};

const SQLITE_BUSY_MAX_RETRIES: usize = 12;
const SQLITE_BUSY_BASE_DELAY_MS: u64 = 25;
const SQLITE_BUSY_TIMEOUT_SECS: u64 = 30;

pub trait SqliteTableArrayKey {
    fn to_table_key_text(&self) -> String;
    fn from_table_key_text(table_key_text: &str) -> Result<Self, String>
    where
        Self: Sized;
}

impl SqliteTableArrayKey for usize {
    fn to_table_key_text(&self) -> String {
        self.to_string()
    }

    fn from_table_key_text(table_key_text: &str) -> Result<Self, String> {
        table_key_text
            .parse::<usize>()
            .map_err(|e| format!("Failed to parse usize key '{}': {}", table_key_text, e))
    }
}

impl SqliteTableArrayKey for i64 {
    fn to_table_key_text(&self) -> String {
        self.to_string()
    }

    fn from_table_key_text(table_key_text: &str) -> Result<Self, String> {
        table_key_text
            .parse::<i64>()
            .map_err(|e| format!("Failed to parse i64 key '{}': {}", table_key_text, e))
    }
}

impl SqliteTableArrayKey for String {
    fn to_table_key_text(&self) -> String {
        self.clone()
    }

    fn from_table_key_text(table_key_text: &str) -> Result<Self, String> {
        Ok(table_key_text.to_string())
    }
}

impl SqliteTableArrayKey for &str {
    fn to_table_key_text(&self) -> String {
        self.to_string()
    }

    fn from_table_key_text(_table_key_text: &str) -> Result<Self, String> {
        Err("Cannot decode table key text into &str; use String instead".to_string())
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
                "Failed to open sqlite database {}: {}",
                db_path.display(),
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

    fn table_name(table_key_text: &str) -> String {
        let table_name = Self::table_name_from_key_text_unchecked(table_key_text);
        let decoded_table_key_text = Self::table_key_text_from_table_name(&table_name)
            .expect("failed to decode internally encoded sqlite table key text");
        assert_eq!(
            decoded_table_key_text, table_key_text,
            "sqlite table key encoding/decoding must be inverse"
        );
        table_name
    }

    fn table_name_from_key_text_unchecked(table_key_text: &str) -> String {
        format!("table_{}", hex_encode(table_key_text.as_bytes()))
    }

    fn table_key_text_from_table_name(table_name: &str) -> Result<String, String> {
        let Some(hex_encoded_key) = table_name.strip_prefix("table_") else {
            return Err(format!("Unexpected table name format '{}'", table_name));
        };
        let key_bytes = hex_decode(hex_encoded_key)
            .map_err(|e| format!("Failed to decode key from table '{}': {}", table_name, e))?;
        let key_text = String::from_utf8(key_bytes).map_err(|e| {
            format!(
                "Failed to decode UTF-8 key from table '{}': {}",
                table_name, e
            )
        })?;

        let reencoded_table_name = Self::table_name_from_key_text_unchecked(&key_text);
        assert_eq!(
            reencoded_table_name, table_name,
            "sqlite table key decoding/encoding must be inverse"
        );
        Ok(key_text)
    }

    fn initialize_table(
        connection: &Connection,
        table_name: &str,
        db_path: &PathBuf,
    ) -> Result<(), String> {
        connection
            .execute(
                &format!(
                    "
                    CREATE TABLE IF NOT EXISTS {} (
                        id INTEGER PRIMARY KEY,
                        row_index INTEGER NOT NULL UNIQUE,
                        payload_msgpack BLOB NOT NULL
                    )
                    ",
                    table_name
                ),
                [],
            )
            .map_err(|e| {
                format!(
                    "Failed to initialize table {} in {}: {}",
                    table_name,
                    db_path.display(),
                    e
                )
            })
            .map(|_| ())
    }

    fn table_exists_with_name(
        connection: &Connection,
        table_name: &str,
        db_path: &PathBuf,
    ) -> Result<bool, String> {
        let existing_table_name: Option<String> = connection
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
                    db_path.display(),
                    e
                )
            })?;
        Ok(existing_table_name.is_some())
    }

    fn next_row_index(
        connection: &Connection,
        table_name: &str,
        db_path: &PathBuf,
    ) -> Result<i64, String> {
        let max_row_index: Option<i64> = connection
            .query_row(
                &format!("SELECT MAX(row_index) FROM {}", table_name),
                [],
                |row| row.get(0),
            )
            .map_err(|e| {
                format!(
                    "Failed to query max row_index for table {} in {}: {}",
                    table_name,
                    db_path.display(),
                    e
                )
            })?;
        Ok(max_row_index.map_or(0, |value| value + 1))
    }

    fn load_payload_at_index(
        connection: &Connection,
        table_name: &str,
        row_index: i64,
        db_path: &PathBuf,
    ) -> Result<Option<Vec<u8>>, String> {
        connection
            .query_row(
                &format!(
                    "
                    SELECT payload_msgpack
                    FROM {}
                    WHERE row_index = ?1
                    ",
                    table_name
                ),
                params![row_index],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| {
                format!(
                    "Failed to fetch row_index {} from table {} in {}: {}",
                    row_index,
                    table_name,
                    db_path.display(),
                    e
                )
            })
    }

    fn append_payload(
        &self,
        table_name: &str,
        row_index: i64,
        payload_msgpack: Vec<u8>,
    ) -> Result<(), String> {
        for attempt in 0..=SQLITE_BUSY_MAX_RETRIES {
            let insert_result = self.connection.execute(
                &format!(
                    "
                    INSERT INTO {} (row_index, payload_msgpack)
                    VALUES (?1, ?2)
                    ON CONFLICT(row_index) DO NOTHING
                    ",
                    table_name
                ),
                params![row_index, payload_msgpack],
            );
            match insert_result {
                Ok(rows_affected) => {
                    if rows_affected > 0 {
                        return Ok(());
                    }
                    let existing_payload = Self::load_payload_at_index(
                        &self.connection,
                        table_name,
                        row_index,
                        &self.db_path,
                    )?
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
                    if Self::is_sqlite_busy_or_locked(&error)
                        && attempt < SQLITE_BUSY_MAX_RETRIES =>
                {
                    sleep(Self::busy_retry_delay(attempt, SQLITE_BUSY_BASE_DELAY_MS));
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

    pub fn initialize(db_path: impl Into<PathBuf>) -> Self {
        let db_path = db_path.into();
        assert!(
            !db_path.exists(),
            "Expected sqlite database {} to not exist when initializing sqlite table array store",
            db_path.display()
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
        Self {
            db_path,
            connection,
            key_marker: PhantomData,
            value_marker: PhantomData,
        }
    }

    pub fn assume_initialized(db_path: impl Into<PathBuf>) -> Self {
        let db_path = db_path.into();
        assert!(
            db_path.exists(),
            "Expected sqlite database {} to exist before assuming sqlite table array store is initialized",
            db_path.display()
        );

        let connection = Self::open_connection(&db_path, false).unwrap_or_else(|e| panic!("{}", e));
        Self {
            db_path,
            connection,
            key_marker: PhantomData,
            value_marker: PhantomData,
        }
    }

    pub fn initialize_if_missing(db_path: impl Into<PathBuf>) -> Self {
        let db_path = db_path.into();
        if db_path.exists() {
            Self::assume_initialized(db_path)
        } else {
            Self::initialize(db_path)
        }
    }

    pub fn new(db_path: impl Into<PathBuf>) -> Result<Self, String> {
        Ok(Self::initialize_if_missing(db_path))
    }

    pub fn append(&self, table_key: K, value: &V) -> Result<(), String> {
        let payload_msgpack = rmp_serde::to_vec_named(value).map_err(|e| {
            format!(
                "Failed to serialize sqlite payload in {}: {}",
                self.db_path.display(),
                e
            )
        })?;
        let table_key_text = table_key.to_table_key_text();
        let table_name = Self::table_name(&table_key_text);
        Self::initialize_table(&self.connection, &table_name, &self.db_path)?;
        let row_index = Self::next_row_index(&self.connection, &table_name, &self.db_path)?;
        self.append_payload(&table_name, row_index, payload_msgpack)
    }

    pub fn append_at(&self, table_key: K, row_index: usize, value: &V) -> Result<(), String> {
        let payload_msgpack = rmp_serde::to_vec_named(value).map_err(|e| {
            format!(
                "Failed to serialize sqlite payload in {}: {}",
                self.db_path.display(),
                e
            )
        })?;
        let table_key_text = table_key.to_table_key_text();
        let table_name = Self::table_name(&table_key_text);
        Self::initialize_table(&self.connection, &table_name, &self.db_path)?;
        self.append_payload(&table_name, row_index as i64, payload_msgpack)
    }

    pub fn load_table_sorted(&self, table_key: K) -> Result<Vec<V>, String> {
        self.load_table_with_indices(table_key).map(|mut rows| {
            rows.sort_by_key(|(row_index, _)| *row_index);
            rows.into_iter().map(|(_, value)| value).collect()
        })
    }

    pub fn load_or_init_table_sorted<F>(
        &self,
        table_key: K,
        initialize_rows: F,
    ) -> Result<Vec<V>, String>
    where
        F: FnOnce() -> Vec<(usize, V)>,
    {
        let table_key_text = table_key.to_table_key_text();
        let table_name = Self::table_name(&table_key_text);

        if !Self::table_exists_with_name(&self.connection, &table_name, &self.db_path)? {
            Self::initialize_table(&self.connection, &table_name, &self.db_path)?;
            for (row_index, value) in initialize_rows() {
                let row_index = i64::try_from(row_index).map_err(|_| {
                    format!(
                        "Row index is too large for table {} in {}",
                        table_name,
                        self.db_path.display()
                    )
                })?;
                let payload_msgpack = rmp_serde::to_vec_named(&value).map_err(|e| {
                    format!(
                        "Failed to serialize sqlite payload in {}: {}",
                        self.db_path.display(),
                        e
                    )
                })?;
                self.append_payload(&table_name, row_index, payload_msgpack)?;
            }
        }

        self.load_table_with_indices(table_key)
            .map(|mut rows| {
                rows.sort_by_key(|(row_index, _)| *row_index);
                rows.into_iter().map(|(_, value)| value).collect()
            })
    }

    pub fn load_table_with_indices(&self, table_key: K) -> Result<Vec<(usize, V)>, String> {
        let table_key_text = table_key.to_table_key_text();
        let table_name = Self::table_name(&table_key_text);
        if !Self::table_exists_with_name(&self.connection, &table_name, &self.db_path)? {
            return Ok(Vec::new());
        }
        let mut statement = self
            .connection
            .prepare(&format!(
                "
                SELECT row_index, payload_msgpack
                FROM {}
                ORDER BY row_index ASC
                ",
                table_name
            ))
            .map_err(|e| {
                format!(
                    "Failed to execute ordered scan query for table {} in {}: {}",
                    table_name,
                    self.db_path.display(),
                    e
                )
            })?;
        let rows: Vec<(i64, Vec<u8>)> = statement
            .query_map([], |row| {
                let row_index: i64 = row.get(0)?;
                let payload_msgpack: Vec<u8> = row.get(1)?;
                Ok((row_index, payload_msgpack))
            })
            .map_err(|e| {
                format!(
                    "Failed to decode row data for table {} in {}: {}",
                    table_name,
                    self.db_path.display(),
                    e
                )
            })?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                format!(
                    "Failed to decode row data for table {} in {}: {}",
                    table_name,
                    self.db_path.display(),
                    e
                )
            })?;
        let mut decoded_rows = rows
            .into_iter()
            .map(|(row_index, payload_msgpack)| {
                if row_index < 0 {
                    return Err(format!(
                        "Negative row index {} found in table {} in {}",
                        row_index,
                        table_name,
                        self.db_path.display()
                    ));
                }
                let value = rmp_serde::from_slice(&payload_msgpack).map_err(|e| {
                    format!(
                        "Failed to deserialize row payload in {}: {}",
                        self.db_path.display(),
                        e
                    )
                })?;
                Ok((row_index as usize, value))
            })
            .collect::<Result<Vec<_>, _>>()?;
        decoded_rows.sort_by_key(|(row_index, _)| *row_index);
        Ok(decoded_rows)
    }

    pub fn clear_table(&self, table_key: K) -> Result<(), String> {
        let table_key_text = table_key.to_table_key_text();
        let table_name = Self::table_name(&table_key_text);
        if !Self::table_exists_with_name(&self.connection, &table_name, &self.db_path)? {
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
        let table_key_text = table_key.to_table_key_text();
        let table_name = Self::table_name(&table_key_text);
        self.connection
            .execute(&format!("DROP TABLE IF EXISTS {}", table_name), [])
            .map(|_| ())
            .map_err(|e| {
                format!(
                    "Failed to drop table {} in {}: {}",
                    table_name,
                    self.db_path.display(),
                    e
                )
            })
    }

    pub fn table_exists(&self, table_key: K) -> Result<bool, String> {
        let table_key_text = table_key.to_table_key_text();
        let table_name = Self::table_name(&table_key_text);
        Self::table_exists_with_name(&self.connection, &table_name, &self.db_path)
    }

    pub fn get_keys(&self) -> Result<Vec<K>, String> {
        let mut statement = self
            .connection
            .prepare(
                "
                SELECT name
                FROM sqlite_master
                WHERE type = 'table' AND name LIKE 'table_%'
                ORDER BY name ASC
                ",
            )
            .map_err(|e| {
                format!(
                    "Failed to prepare key scan query in {}: {}",
                    self.db_path.display(),
                    e
                )
            })?;
        let table_names: Vec<String> = statement
            .query_map([], |row| row.get(0))
            .map_err(|e| {
                format!(
                    "Failed to scan sqlite table names in {}: {}",
                    self.db_path.display(),
                    e
                )
            })?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                format!(
                    "Failed to decode sqlite table names in {}: {}",
                    self.db_path.display(),
                    e
                )
            })?;

        table_names
            .into_iter()
            .map(|table_name| {
                let key_text = Self::table_key_text_from_table_name(&table_name).map_err(|e| {
                    format!(
                        "Failed to decode key from table '{}' in {}: {}",
                        table_name,
                        self.db_path.display(),
                        e
                    )
                })?;
                K::from_table_key_text(&key_text).map_err(|e| {
                    format!(
                        "Failed to parse key '{}' from table '{}' in {}: {}",
                        key_text,
                        table_name,
                        self.db_path.display(),
                        e
                    )
                })
            })
            .collect()
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

fn hex_decode(hex_str: &str) -> Result<Vec<u8>, String> {
    if !hex_str.len().is_multiple_of(2) {
        return Err(format!("hex string has odd length: {}", hex_str.len()));
    }
    let mut output = Vec::with_capacity(hex_str.len() / 2);
    let bytes = hex_str.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        let high = hex_nibble(bytes[index]).ok_or_else(|| {
            format!(
                "invalid hex character '{}' at index {}",
                bytes[index] as char,
                index
            )
        })?;
        let low = hex_nibble(bytes[index + 1]).ok_or_else(|| {
            format!(
                "invalid hex character '{}' at index {}",
                bytes[index + 1] as char,
                index + 1
            )
        })?;
        output.push((high << 4) | low);
        index += 2;
    }
    Ok(output)
}

fn hex_nibble(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}
