use research_utility::bincode_log_file::BincodeLogFile;
use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;
use std::io::Write;
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

    let mut log = BincodeLogFile::<TestItem>::open(&path).expect("failed to open bincode log file");
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

#[test]
fn open_ignores_incomplete_trailing_payload() {
    let path = temp_file_path("incomplete_payload_tail");

    let item = TestItem {
        id: 1,
        text: "ok".to_string(),
    };

    {
        let mut log = BincodeLogFile::<TestItem>::open(&path).expect("failed to open log file");
        log.append_and_flush(&item).expect("append failed");
    }

    {
        let mut file = OpenOptions::new()
            .append(true)
            .open(&path)
            .expect("failed to reopen test file");
        let partial_payload_len = 64_u64;
        file.write_all(&partial_payload_len.to_le_bytes())
            .expect("failed to write partial length prefix");
        file.write_all(&[1_u8, 2, 3, 4])
            .expect("failed to write partial payload");
        file.flush().expect("failed to flush partial write");
    }

    let mut reopened = BincodeLogFile::<TestItem>::open(&path).expect("reopen failed");
    assert_eq!(reopened.len(), 1);
    assert_eq!(reopened.get(0).expect("get failed"), Some(item));

    drop(reopened);
    std::fs::remove_file(&path).expect("failed to remove temporary file");
}

#[test]
fn open_ignores_incomplete_trailing_length_prefix() {
    let path = temp_file_path("incomplete_prefix_tail");
    std::fs::write(&path, [7_u8, 8, 9]).expect("failed to seed file");

    let log = BincodeLogFile::<TestItem>::open(&path).expect("open should tolerate partial tail");
    assert_eq!(log.len(), 0);

    drop(log);
    std::fs::remove_file(&path).expect("failed to remove temporary file");
}

#[test]
fn open_ignores_truncated_tail_record_with_matching_length_prefix() {
    let path = temp_file_path("truncated_tail_record");
    let item = TestItem {
        id: 7,
        text: "abcdef".to_string(),
    };
    let payload = bincode::serialize(&item).expect("failed to serialize item");
    let truncated_len = payload.len() / 2;

    {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .expect("failed to create test file");
        file.write_all(
            &(u64::try_from(truncated_len).expect("len conversion failed")).to_le_bytes(),
        )
        .expect("failed to write length");
        file.write_all(&payload[..truncated_len])
            .expect("failed to write truncated payload");
        file.flush().expect("failed to flush file");
    }

    let log =
        BincodeLogFile::<TestItem>::open(&path).expect("open should tolerate truncated tail item");
    assert_eq!(log.len(), 0);

    drop(log);
    std::fs::remove_file(&path).expect("failed to remove temporary file");
}

#[test]
fn get_treats_unexpected_eof_deserialize_as_missing_item() {
    let path = temp_file_path("unexpected_eof_deserialize_get");
    let bad_item = TestItem {
        id: 11,
        text: "this payload is intentionally truncated".to_string(),
    };
    let good_item = TestItem {
        id: 12,
        text: "good".to_string(),
    };
    let bad_payload = bincode::serialize(&bad_item).expect("failed to serialize bad item");
    let bad_len = bad_payload.len() / 2;
    let good_payload = bincode::serialize(&good_item).expect("failed to serialize good item");

    {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .expect("failed to create test file");
        file.write_all(&(u64::try_from(bad_len).expect("bad len conversion failed")).to_le_bytes())
            .expect("failed to write bad length");
        file.write_all(&bad_payload[..bad_len])
            .expect("failed to write bad payload");
        file.write_all(
            &(u64::try_from(good_payload.len()).expect("good len conversion failed")).to_le_bytes(),
        )
        .expect("failed to write good length");
        file.write_all(&good_payload)
            .expect("failed to write good payload");
        file.flush().expect("failed to flush file");
    }

    let mut log = BincodeLogFile::<TestItem>::open(&path).expect("failed to open log");
    assert_eq!(log.len(), 2);
    assert_eq!(log.get(0).expect("get bad index failed"), None);
    assert_eq!(log.get(1).expect("get good index failed"), Some(good_item));

    drop(log);
    std::fs::remove_file(&path).expect("failed to remove temporary file");
}
