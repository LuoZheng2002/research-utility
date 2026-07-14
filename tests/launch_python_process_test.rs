use std::path::Path;
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

use research_utility::{
    bincode_log_file::BincodeLogFile,
    launch_python_process::{bind_wrapper_tui_listener, run_wrapper_tui_listener},
    message::{Severity, TuiMessage},
    progress_tui_logger::{ProgressLogFrame, ProgressTuiLogger},
};
use tokio::io::AsyncWriteExt;
use tokio::net::UnixStream;
use tokio::sync::watch;

#[test]
fn state_tui_message_json_parses() {
    let json = r#"{"State":{"state":"Inference wrapper started"}}"#;
    let parsed = serde_json::from_str::<TuiMessage>(json).expect("state message should parse");
    match parsed {
        TuiMessage::State { state } => assert_eq!(state, "Inference wrapper started"),
        _ => panic!("expected state message"),
    }
}

#[test]
fn line_tui_message_json_parses() {
    let json =
        r#"{"Line":{"message":"Training failed: torchrun exited with code 1","severity":"Error"}}"#;
    let parsed = serde_json::from_str::<TuiMessage>(json).expect("line message should parse");
    match parsed {
        TuiMessage::Line { message, severity } => {
            assert_eq!(message, "Training failed: torchrun exited with code 1");
            assert_eq!(severity, Severity::Error);
        }
        _ => panic!("expected line message"),
    }
}

#[test]
fn key_value_tui_message_json_parses() {
    let json = r#"{"KeyValuePair":{"key":"checkpoint_dir","value":"/tmp/checkpoints"}}"#;
    let parsed = serde_json::from_str::<TuiMessage>(json).expect("key value message should parse");
    match parsed {
        TuiMessage::KeyValuePair { key, value } => {
            assert_eq!(key, "checkpoint_dir");
            assert_eq!(value, "/tmp/checkpoints");
        }
        _ => panic!("expected key value message"),
    }
}

#[tokio::test]
async fn unix_socket_tui_listener_forwards_messages_into_progress_log() {
    let _guard = progress_logger_test_lock().lock().await;
    let log_path = temp_progress_log_path("wrapper_socket_forward");
    ProgressTuiLogger::initialize(&log_path)
        .await
        .expect("progress logger should initialize");

    let (socket_path, listener) = bind_wrapper_tui_listener("test").expect("listener should bind");
    let socket_path_for_listener = socket_path.clone();
    let (_stop_signal_tx, stop_signal_rx) = watch::channel(false);
    let listener_task = tokio::spawn(async move {
        run_wrapper_tui_listener(
            listener,
            &socket_path_for_listener,
            "test wrapper",
            stop_signal_rx,
        )
        .await
        .expect("listener should complete successfully");
    });

    let mut stream = UnixStream::connect(&socket_path)
        .await
        .expect("client should connect");
    stream
        .write_all(b"{\"State\":{\"state\":\"Training wrapper started\"}}\n")
        .await
        .expect("state message should write");
    stream
        .write_all(
            b"{\"KeyValuePair\":{\"key\":\"checkpoint_dir\",\"value\":\"/tmp/checkpoints\"}}\n",
        )
        .await
        .expect("key value message should write");
    stream
        .write_all(
            b"{\"Line\":{\"message\":\"Training failed: torchrun exited with code 1\",\"severity\":\"Error\"}}\n",
        )
        .await
        .expect("line message should write");
    drop(stream);

    listener_task
        .await
        .expect("listener task should join successfully");
    ProgressTuiLogger::shutdown()
        .await
        .expect("progress logger should shutdown cleanly");

    let frames = read_all_progress_frames(&log_path);
    assert!(
        !frames.is_empty(),
        "expected at least one progress log frame after socket forwarding"
    );
    assert!(
        frames
            .iter()
            .any(|frame| frame.state.as_deref() == Some("Training wrapper started")),
        "expected forwarded state message in progress frames"
    );
    assert!(
        frames.iter().any(|frame| {
            frame
                .key_values
                .iter()
                .any(|(key, value)| key == "checkpoint_dir" && value == "/tmp/checkpoints")
        }),
        "expected forwarded key/value message in progress frames"
    );
    assert!(
        frames.iter().any(|frame| {
            frame.log_lines.iter().any(|line| {
                line.message == "Training failed: torchrun exited with code 1"
                    && line.severity == Severity::Error
            })
        }),
        "expected forwarded log line in progress frames"
    );

    let _ = std::fs::remove_file(&log_path);
}

fn progress_logger_test_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

fn read_all_progress_frames(path: &Path) -> Vec<ProgressLogFrame> {
    let log_file = BincodeLogFile::<ProgressLogFrame>::open(path)
        .expect("progress log file should open for reading");
    log_file
        .iter()
        .expect("progress log iterator should open")
        .map(|frame| frame.expect("progress log frame should deserialize"))
        .collect()
}

fn temp_progress_log_path(label: &str) -> std::path::PathBuf {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before UNIX_EPOCH")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "credit_assignment_progress_tui_{}_{}_{}.bin",
        label,
        std::process::id(),
        now
    ))
}
