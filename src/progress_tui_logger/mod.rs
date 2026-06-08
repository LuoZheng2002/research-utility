use std::collections::BTreeMap;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwapOption;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use crate::bincode_log_file::BincodeLogFile;
use crate::message::{Severity, TuiMessage};

const FRAME_INTERVAL: Duration = Duration::from_millis(500);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProgressGaugeState {
    pub progress: f32,
    pub label: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgressLogLine {
    pub message: String,
    pub severity: Severity,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProgressLogFrame {
    pub elapsed_seconds: f64,
    pub state: Option<String>,
    pub window_name: Option<String>,
    pub exit_hint: Option<String>,
    pub key_values: Vec<(String, String)>,
    pub worker_progress: Vec<(String, ProgressGaugeState)>,
    pub deleted_workers: Vec<String>,
    pub master_progress: Option<ProgressGaugeState>,
    pub log_lines: Vec<ProgressLogLine>,
}

impl ProgressLogFrame {
    fn is_empty(&self) -> bool {
        self.state.is_none()
            && self.window_name.is_none()
            && self.exit_hint.is_none()
            && self.key_values.is_empty()
            && self.worker_progress.is_empty()
            && self.deleted_workers.is_empty()
            && self.master_progress.is_none()
            && self.log_lines.is_empty()
    }
}

#[derive(Debug, Clone)]
struct ProgressSnapshot {
    state: String,
    window_name: String,
    exit_hint: String,
    key_values: BTreeMap<String, String>,
    worker_progress: BTreeMap<String, ProgressGaugeState>,
    master_progress: ProgressGaugeState,
}

impl Default for ProgressSnapshot {
    fn default() -> Self {
        Self {
            state: String::new(),
            window_name: "Progress Screen".to_string(),
            exit_hint: "Reader disconnected. Ctrl+C to exit.".to_string(),
            key_values: BTreeMap::new(),
            worker_progress: BTreeMap::new(),
            master_progress: ProgressGaugeState {
                progress: 0.0,
                label: "0%".to_string(),
            },
        }
    }
}

impl ProgressSnapshot {
    fn apply_message(&mut self, message: &TuiMessage) {
        match message {
            TuiMessage::Line { .. } => {}
            TuiMessage::State { state } => {
                self.state = state.clone();
            }
            TuiMessage::WindowName { window_name } => {
                self.window_name = window_name.clone();
            }
            TuiMessage::KeyValuePair { key, value } => {
                self.key_values.insert(key.clone(), value.clone());
            }
            TuiMessage::WorkerProgress {
                worker_name,
                progress,
                label,
            } => {
                assert!(
                    (0.0..=1.0).contains(progress),
                    "WorkerProgress.progress must be in [0, 1], got {}",
                    progress
                );
                self.worker_progress.insert(
                    worker_name.clone(),
                    ProgressGaugeState {
                        progress: *progress,
                        label: label.clone(),
                    },
                );
            }
            TuiMessage::MasterProgress { progress, label } => {
                assert!(
                    (0.0..=1.0).contains(progress),
                    "MasterProgress.progress must be in [0, 1], got {}",
                    progress
                );
                self.master_progress = ProgressGaugeState {
                    progress: *progress,
                    label: label.clone(),
                };
            }
            TuiMessage::DeleteWorkerBar { worker_name } => {
                self.worker_progress.remove(worker_name);
            }
            TuiMessage::ExitHint(hint) => {
                self.exit_hint = hint.clone();
            }
        }
    }

    fn delta_from(&self, previous: &Self, log_lines: Vec<ProgressLogLine>) -> ProgressLogFrame {
        let state = if self.state != previous.state {
            Some(self.state.clone())
        } else {
            None
        };
        let window_name = if self.window_name != previous.window_name {
            Some(self.window_name.clone())
        } else {
            None
        };
        let exit_hint = if self.exit_hint != previous.exit_hint {
            Some(self.exit_hint.clone())
        } else {
            None
        };

        let mut key_values = Vec::new();
        for (key, value) in &self.key_values {
            if previous.key_values.get(key) != Some(value) {
                key_values.push((key.clone(), value.clone()));
            }
        }

        let mut worker_progress = Vec::new();
        for (worker_name, gauge) in &self.worker_progress {
            if previous.worker_progress.get(worker_name) != Some(gauge) {
                worker_progress.push((worker_name.clone(), gauge.clone()));
            }
        }

        let mut deleted_workers = Vec::new();
        for worker_name in previous.worker_progress.keys() {
            if !self.worker_progress.contains_key(worker_name) {
                deleted_workers.push(worker_name.clone());
            }
        }

        let master_progress = if self.master_progress != previous.master_progress {
            Some(self.master_progress.clone())
        } else {
            None
        };

        ProgressLogFrame {
            elapsed_seconds: 0.0,
            state,
            window_name,
            exit_hint,
            key_values,
            worker_progress,
            deleted_workers,
            master_progress,
            log_lines,
        }
    }
}

struct ProgressTuiLoggerState {
    start_instant: Instant,
    snapshot: parking_lot::Mutex<ProgressSnapshot>,
    pending_log_lines: parking_lot::Mutex<Vec<ProgressLogLine>>,
    log_file: parking_lot::Mutex<BincodeLogFile<ProgressLogFrame>>,
    last_flushed_snapshot: parking_lot::Mutex<ProgressSnapshot>,
    shutdown_tx: parking_lot::Mutex<Option<oneshot::Sender<()>>>,
    join_handle: parking_lot::Mutex<Option<tokio::task::JoinHandle<io::Result<()>>>>,
}

static PROGRESS_TUI_LOGGER_STATE: ArcSwapOption<ProgressTuiLoggerState> =
    ArcSwapOption::const_empty();

pub struct ProgressTuiLogger;

impl ProgressTuiLogger {
    pub async fn initialize(log_file_path: impl Into<PathBuf>) -> io::Result<()> {
        if PROGRESS_TUI_LOGGER_STATE.load_full().is_some() {
            return Ok(());
        }

        let log_file_path = log_file_path.into();
        if let Some(parent) = log_file_path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        if log_file_path.exists() {
            std::fs::remove_file(&log_file_path)?;
        }

        let log_file = BincodeLogFile::open(log_file_path)
            .map_err(|e| io::Error::other(format!("failed to open progress log file: {e}")))?;

        let state = Arc::new(ProgressTuiLoggerState {
            start_instant: Instant::now(),
            snapshot: parking_lot::Mutex::new(ProgressSnapshot::default()),
            pending_log_lines: parking_lot::Mutex::new(Vec::new()),
            log_file: parking_lot::Mutex::new(log_file),
            last_flushed_snapshot: parking_lot::Mutex::new(ProgressSnapshot::default()),
            shutdown_tx: parking_lot::Mutex::new(None),
            join_handle: parking_lot::Mutex::new(None),
        });

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let state_for_task = Arc::clone(&state);
        let join_handle =
            tokio::spawn(async move { run_flush_loop(state_for_task, shutdown_rx).await });

        *state.shutdown_tx.lock() = Some(shutdown_tx);
        *state.join_handle.lock() = Some(join_handle);
        PROGRESS_TUI_LOGGER_STATE.store(Some(state));
        Ok(())
    }

    pub async fn shutdown() -> io::Result<()> {
        let Some(state) = PROGRESS_TUI_LOGGER_STATE.load_full() else {
            return Ok(());
        };
        PROGRESS_TUI_LOGGER_STATE.store(None);

        if let Some(shutdown_tx) = state.shutdown_tx.lock().take() {
            let _ = shutdown_tx.send(());
        }

        match state.join_handle.lock().take() {
            Some(join_handle) => match join_handle.await {
                Ok(result) => result,
                Err(err) => Err(io::Error::other(format!(
                    "progress tui logger task join error: {err}"
                ))),
            },
            None => Ok(()),
        }
    }
}

pub fn log_message(message: TuiMessage) {
    let Some(state) = PROGRESS_TUI_LOGGER_STATE.load_full() else {
        println!("{}", message.to_string());
        return;
    };

    if let TuiMessage::Line { message, severity } = &message {
        state.pending_log_lines.lock().push(ProgressLogLine {
            message: message.clone(),
            severity: *severity,
        });
    }
    state.snapshot.lock().apply_message(&message);
}

pub fn log_info(message: impl Into<String>) {
    log_message(TuiMessage::Line {
        message: message.into(),
        severity: Severity::Info,
    });
}

pub fn log_warning(message: impl Into<String>) {
    log_message(TuiMessage::Line {
        message: message.into(),
        severity: Severity::Warning,
    });
}

pub fn log_error(message: impl Into<String>) {
    log_message(TuiMessage::Line {
        message: message.into(),
        severity: Severity::Error,
    });
}

pub fn log_state(state: impl Into<String>) {
    log_message(TuiMessage::State {
        state: state.into(),
    });
}

pub fn log_window_name(window_name: impl Into<String>) {
    log_message(TuiMessage::WindowName {
        window_name: window_name.into(),
    });
}

pub fn log_key_value_pair(key: impl Into<String>, value: impl Into<String>) {
    log_message(TuiMessage::KeyValuePair {
        key: key.into(),
        value: value.into(),
    });
}

pub fn log_master_progress(progress: f32, label: impl Into<String>) {
    log_message(TuiMessage::MasterProgress {
        progress,
        label: label.into(),
    });
}

pub fn log_worker_progress(
    worker_name: impl Into<String>,
    progress: f32,
    label: impl Into<String>,
) {
    log_message(TuiMessage::WorkerProgress {
        worker_name: worker_name.into(),
        progress,
        label: label.into(),
    });
}

pub fn delete_worker_progress_bar(worker_name: impl Into<String>) {
    log_message(TuiMessage::DeleteWorkerBar {
        worker_name: worker_name.into(),
    });
}

pub fn log_exit_hint(hint: impl Into<String>) {
    log_message(TuiMessage::ExitHint(hint.into()));
}

async fn run_flush_loop(
    state: Arc<ProgressTuiLoggerState>,
    mut shutdown_rx: oneshot::Receiver<()>,
) -> io::Result<()> {
    let mut interval = tokio::time::interval(FRAME_INTERVAL);
    loop {
        tokio::select! {
            _ = interval.tick() => {
                flush_frame_if_needed(&state)?;
            }
            _ = &mut shutdown_rx => {
                flush_frame_if_needed(&state)?;
                break;
            }
        }
    }

    Ok(())
}

fn flush_frame_if_needed(state: &Arc<ProgressTuiLoggerState>) -> io::Result<()> {
    let snapshot = state.snapshot.lock().clone();
    let log_lines = {
        let mut guard = state.pending_log_lines.lock();
        std::mem::take(&mut *guard)
    };
    let previous = state.last_flushed_snapshot.lock().clone();
    let mut frame = snapshot.delta_from(&previous, log_lines);
    frame.elapsed_seconds = state.start_instant.elapsed().as_secs_f64();

    if frame.is_empty() {
        return Ok(());
    }

    {
        let mut log_file = state.log_file.lock();
        log_file
            .append(&frame)
            .map_err(|e| io::Error::other(format!("failed to append log frame: {e}")))?;
    }
    *state.last_flushed_snapshot.lock() = snapshot;
    Ok(())
}
