use lru::LruCache;
use serde::{Serialize, de::DeserializeOwned};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::path::PathBuf;

const LENGTH_PREFIX_BYTES: u64 = 8;
const DEFAULT_CACHE_CAPACITY: usize = 1024;

#[derive(Debug)]
pub struct BincodeLogFile<T> {
    path: PathBuf,
    file: File,
    len: usize,
    next_offset: u64,
    offset_cache: LruCache<usize, u64>,
    marker: PhantomData<T>,
}

impl<T> BincodeLogFile<T>
where
    T: Serialize + DeserializeOwned,
{
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, String> {
        Self::open_with_cache_capacity(path, DEFAULT_CACHE_CAPACITY)
    }

    pub fn open_with_cache_capacity(
        path: impl Into<PathBuf>,
        cache_capacity: usize,
    ) -> Result<Self, String> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    format!(
                        "Failed to create parent directories for {}: {}",
                        path.display(),
                        e
                    )
                })?;
            }
        }

        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|e| format!("Failed to open bincode log file {}: {}", path.display(), e))?;

        let (len, next_offset) = Self::scan_len_and_tail(&mut file, &path)?;

        let capacity =
            NonZeroUsize::new(cache_capacity.max(1)).expect("cache capacity must be non-zero");
        let mut offset_cache = LruCache::new(capacity);
        if len > 0 {
            offset_cache.put(0, 0);
        }

        Ok(Self {
            path,
            file,
            len,
            next_offset,
            offset_cache,
            marker: PhantomData,
        })
    }

    pub fn append(&mut self, item: &T) -> Result<(), String> {
        let payload = bincode::serialize(item).map_err(|e| {
            format!(
                "Failed to bincode-serialize value for {}: {}",
                self.path.display(),
                e
            )
        })?;

        let payload_len = u64::try_from(payload.len())
            .map_err(|_| format!("Serialized payload too large for {}", self.path.display()))?;

        let end_offset = self
            .file
            .seek(SeekFrom::End(0))
            .map_err(|e| format!("Failed to seek end of {}: {}", self.path.display(), e))?;
        if end_offset != self.next_offset {
            return Err(format!(
                "File {} changed unexpectedly (expected end {}, got {})",
                self.path.display(),
                self.next_offset,
                end_offset
            ));
        }

        let index = self.len;
        self.file
            .write_all(&payload_len.to_le_bytes())
            .map_err(|e| {
                format!(
                    "Failed to write length prefix to {}: {}",
                    self.path.display(),
                    e
                )
            })?;
        self.file
            .write_all(&payload)
            .map_err(|e| format!("Failed to write payload to {}: {}", self.path.display(), e))?;

        self.offset_cache.put(index, self.next_offset);
        self.len = self.len.checked_add(1).ok_or_else(|| {
            format!(
                "Item count overflow while appending to {}",
                self.path.display()
            )
        })?;
        self.next_offset = self
            .next_offset
            .checked_add(LENGTH_PREFIX_BYTES + payload_len)
            .ok_or_else(|| {
                format!(
                    "Byte offset overflow while appending to {}",
                    self.path.display()
                )
            })?;

        Ok(())
    }

    pub fn append_and_flush(&mut self, item: &T) -> Result<(), String> {
        self.append(item)?;
        self.file
            .flush()
            .map_err(|e| format!("Failed to flush {}: {}", self.path.display(), e))
    }

    pub fn get(&mut self, index: usize) -> Result<Option<T>, String> {
        if index >= self.len {
            return Ok(None);
        }

        let offset = self.offset_for_index(index)?;
        let value = self.read_value_at_offset(offset)?;
        Ok(Some(value))
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn reload(&mut self) -> Result<usize, String> {
        let (len, next_offset) = Self::scan_len_and_tail(&mut self.file, &self.path)?;
        self.len = len;
        self.next_offset = next_offset;

        while let Some((cached_index, _)) = self.offset_cache.peek_lru() {
            if *cached_index < self.len {
                break;
            }
            self.offset_cache.pop_lru();
        }

        if self.len > 0 && self.offset_cache.peek(&0).is_none() {
            self.offset_cache.put(0, 0);
        }

        Ok(self.len)
    }

    pub fn iter(&self) -> Result<BincodeLogFileIterator<T>, String> {
        let mut file = self.file.try_clone().map_err(|e| {
            format!(
                "Failed to clone file handle for {} iterator: {}",
                self.path.display(),
                e
            )
        })?;
        file.seek(SeekFrom::Start(0)).map_err(|e| {
            format!(
                "Failed to seek iterator start for {}: {}",
                self.path.display(),
                e
            )
        })?;

        Ok(BincodeLogFileIterator {
            path: self.path.clone(),
            file,
            remaining: self.len,
            marker: PhantomData,
        })
    }

    fn offset_for_index(&mut self, target_index: usize) -> Result<u64, String> {
        if let Some(offset) = self.offset_cache.get(&target_index).copied() {
            return Ok(offset);
        }

        let mut start_index = 0usize;
        let mut start_offset = 0u64;
        for (cached_index, cached_offset) in self.offset_cache.iter() {
            if *cached_index <= target_index && *cached_index >= start_index {
                start_index = *cached_index;
                start_offset = *cached_offset;
            }
        }

        if start_index == 0 {
            self.offset_cache.put(0, 0);
        }

        let mut current_index = start_index;
        let mut current_offset = start_offset;
        while current_index < target_index {
            let next_offset = self.advance_offset(current_offset)?;
            current_index += 1;
            current_offset = next_offset;
            self.offset_cache.put(current_index, current_offset);
        }

        Ok(current_offset)
    }

    fn advance_offset(&mut self, offset: u64) -> Result<u64, String> {
        self.file.seek(SeekFrom::Start(offset)).map_err(|e| {
            format!(
                "Failed to seek {} to offset {}: {}",
                self.path.display(),
                offset,
                e
            )
        })?;

        let mut len_bytes = [0_u8; LENGTH_PREFIX_BYTES as usize];
        self.file.read_exact(&mut len_bytes).map_err(|e| {
            format!(
                "Failed to read length prefix at offset {} in {}: {}",
                offset,
                self.path.display(),
                e
            )
        })?;

        let payload_len = u64::from_le_bytes(len_bytes);
        offset
            .checked_add(LENGTH_PREFIX_BYTES + payload_len)
            .ok_or_else(|| format!("Byte offset overflow in {}", self.path.display()))
    }

    fn read_value_at_offset(&mut self, offset: u64) -> Result<T, String> {
        self.file.seek(SeekFrom::Start(offset)).map_err(|e| {
            format!(
                "Failed to seek {} to offset {}: {}",
                self.path.display(),
                offset,
                e
            )
        })?;

        let mut len_bytes = [0_u8; LENGTH_PREFIX_BYTES as usize];
        self.file.read_exact(&mut len_bytes).map_err(|e| {
            format!(
                "Failed to read length prefix at offset {} in {}: {}",
                offset,
                self.path.display(),
                e
            )
        })?;

        let payload_len = u64::from_le_bytes(len_bytes);
        let payload_len_usize = usize::try_from(payload_len).map_err(|_| {
            format!(
                "Payload length {} at offset {} exceeds usize in {}",
                payload_len,
                offset,
                self.path.display()
            )
        })?;

        let mut payload = vec![0_u8; payload_len_usize];
        self.file.read_exact(&mut payload).map_err(|e| {
            format!(
                "Failed to read payload of {} bytes at offset {} in {}: {}",
                payload_len,
                offset,
                self.path.display(),
                e
            )
        })?;

        bincode::deserialize::<T>(&payload).map_err(|e| {
            format!(
                "Failed to bincode-deserialize item at offset {} in {}: {}",
                offset,
                self.path.display(),
                e
            )
        })
    }

    fn scan_len_and_tail(file: &mut File, path: &PathBuf) -> Result<(usize, u64), String> {
        let file_size = file
            .metadata()
            .map_err(|e| format!("Failed to read metadata for {}: {}", path.display(), e))?
            .len();

        file.seek(SeekFrom::Start(0))
            .map_err(|e| format!("Failed to seek start of {}: {}", path.display(), e))?;

        let mut offset = 0_u64;
        let mut count = 0_usize;
        while offset < file_size {
            if file_size - offset < LENGTH_PREFIX_BYTES {
                return Err(format!(
                    "Corrupted bincode log {}: trailing {} bytes smaller than length prefix",
                    path.display(),
                    file_size - offset
                ));
            }

            let mut len_bytes = [0_u8; LENGTH_PREFIX_BYTES as usize];
            file.read_exact(&mut len_bytes).map_err(|e| {
                format!(
                    "Failed to read length prefix at offset {} in {}: {}",
                    offset,
                    path.display(),
                    e
                )
            })?;

            let payload_len = u64::from_le_bytes(len_bytes);
            offset = offset
                .checked_add(LENGTH_PREFIX_BYTES)
                .ok_or_else(|| format!("Byte offset overflow while scanning {}", path.display()))?;

            if payload_len > file_size - offset {
                return Err(format!(
                    "Corrupted bincode log {}: payload length {} at offset {} exceeds remaining {} bytes",
                    path.display(),
                    payload_len,
                    offset - LENGTH_PREFIX_BYTES,
                    file_size - offset
                ));
            }

            let payload_len_i64 = i64::try_from(payload_len)
                .map_err(|_| format!("Payload length too large in {}", path.display()))?;
            file.seek(SeekFrom::Current(payload_len_i64)).map_err(|e| {
                format!(
                    "Failed to skip {} payload bytes in {}: {}",
                    payload_len,
                    path.display(),
                    e
                )
            })?;

            offset = offset
                .checked_add(payload_len)
                .ok_or_else(|| format!("Byte offset overflow while scanning {}", path.display()))?;
            count = count
                .checked_add(1)
                .ok_or_else(|| format!("Item count overflow while scanning {}", path.display()))?;
        }

        Ok((count, offset))
    }
}

#[derive(Debug)]
pub struct BincodeLogFileIterator<T> {
    path: PathBuf,
    file: File,
    remaining: usize,
    marker: PhantomData<T>,
}

impl<T> Iterator for BincodeLogFileIterator<T>
where
    T: DeserializeOwned,
{
    type Item = Result<T, String>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }

        let mut len_bytes = [0_u8; LENGTH_PREFIX_BYTES as usize];
        if let Err(e) = self.file.read_exact(&mut len_bytes) {
            self.remaining = 0;
            return Some(Err(format!(
                "Failed to read iterator length prefix in {}: {}",
                self.path.display(),
                e
            )));
        }

        let payload_len = u64::from_le_bytes(len_bytes);
        let payload_len_usize = match usize::try_from(payload_len) {
            Ok(v) => v,
            Err(_) => {
                self.remaining = 0;
                return Some(Err(format!(
                    "Iterator payload length {} exceeds usize in {}",
                    payload_len,
                    self.path.display()
                )));
            }
        };

        let mut payload = vec![0_u8; payload_len_usize];
        if let Err(e) = self.file.read_exact(&mut payload) {
            self.remaining = 0;
            return Some(Err(format!(
                "Failed to read iterator payload ({} bytes) in {}: {}",
                payload_len,
                self.path.display(),
                e
            )));
        }

        self.remaining -= 1;
        Some(bincode::deserialize::<T>(&payload).map_err(|e| {
            format!(
                "Failed to deserialize iterator item in {}: {}",
                self.path.display(),
                e
            )
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::BincodeLogFile;
    use serde::{Deserialize, Serialize};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct TestItem {
        id: u32,
        text: String,
    }

    fn temp_file_path(label: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before UNIX_EPOCH")
            .as_nanos();
        path.push(format!(
            "research_utility_bincode_log_{}_{}_{}.bin",
            label,
            std::process::id(),
            now
        ));
        path
    }

    #[test]
    fn append_get_len_and_iter_work() {
        let path = temp_file_path("append_get_len_iter");

        let mut log = BincodeLogFile::<TestItem>::open_with_cache_capacity(&path, 2)
            .expect("failed to open log file");

        let items = vec![
            TestItem {
                id: 1,
                text: "first".to_string(),
            },
            TestItem {
                id: 2,
                text: "second".to_string(),
            },
            TestItem {
                id: 3,
                text: "third".to_string(),
            },
        ];

        for item in &items {
            log.append(item).expect("append failed");
        }

        assert_eq!(log.len(), items.len());
        assert_eq!(
            log.get(0).expect("get index 0 failed"),
            Some(items[0].clone())
        );
        assert_eq!(
            log.get(2).expect("get index 2 failed"),
            Some(items[2].clone())
        );

        let iter_items: Vec<TestItem> = log
            .iter()
            .expect("iterator creation failed")
            .map(|result| result.expect("iterator item failed"))
            .collect();
        assert_eq!(iter_items, items);

        drop(log);
        std::fs::remove_file(&path).expect("failed to remove temporary file");
    }

    #[test]
    fn get_out_of_bounds_returns_none() {
        let path = temp_file_path("out_of_bounds");

        let mut log =
            BincodeLogFile::<TestItem>::open(&path).expect("failed to open bincode log file");
        assert_eq!(log.get(0).expect("get failed"), None);

        log.append(&TestItem {
            id: 9,
            text: "item".to_string(),
        })
        .expect("append failed");

        assert_eq!(log.get(1).expect("get failed"), None);

        drop(log);
        std::fs::remove_file(&path).expect("failed to remove temporary file");
    }
}
