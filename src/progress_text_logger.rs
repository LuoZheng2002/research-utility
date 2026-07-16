use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwapOption;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use crate::message::{Severity, TuiMessage};

const FLUSH_INTERVAL: Duration = Duration::from_secs(5);

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

#[derive(Debug, Clone)]
struct TextSnapshot {
    state: String,
    window_name: String,
    exit_hint: String,
    key_values: BTreeMap<String, String>,
    worker_progress: BTreeMap<String, ProgressGaugeState>,
    master_progress: ProgressGaugeState,
}

impl Default for TextSnapshot {
    fn default() -> Self {
        Self {
            state: String::new(),
            window_name: String::new(),
            exit_hint: String::new(),
            key_values: BTreeMap::new(),
            worker_progress: BTreeMap::new(),
            master_progress: ProgressGaugeState {
                progress: 0.0,
                label: "0%".to_string(),
            },
        }
    }
}

impl TextSnapshot {
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

    fn changed_since(&self, previous: &Self) -> bool {
        self.state != previous.state
            || self.window_name != previous.window_name
            || self.exit_hint != previous.exit_hint
            || self.key_values != previous.key_values
            || self.worker_progress != previous.worker_progress
            || self.master_progress != previous.master_progress
    }
}

struct ProgressTextLoggerState {
    start_instant: Instant,
    snapshot: parking_lot::Mutex<TextSnapshot>,
    pending_log_lines: parking_lot::Mutex<Vec<ProgressLogLine>>,
    summary_file: parking_lot::Mutex<BufWriter<File>>,
    verbose_file: parking_lot::Mutex<BufWriter<File>>,
    last_flushed_snapshot: parking_lot::Mutex<TextSnapshot>,
    shutdown_tx: parking_lot::Mutex<Option<oneshot::Sender<()>>>,
    join_handle: parking_lot::Mutex<Option<tokio::task::JoinHandle<io::Result<()>>>>,
}

static PROGRESS_TEXT_LOGGER_STATE: ArcSwapOption<ProgressTextLoggerState> =
    ArcSwapOption::const_empty();

pub struct ProgressTextLogger;

impl ProgressTextLogger {
    pub async fn initialize(
        summary_path: impl Into<PathBuf>,
        verbose_path: impl Into<PathBuf>,
    ) -> io::Result<()> {
        if PROGRESS_TEXT_LOGGER_STATE.load_full().is_some() {
            return Ok(());
        }

        let summary_path = summary_path.into();
        let verbose_path = verbose_path.into();

        for path in [&summary_path, &verbose_path] {
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() {
                    std::fs::create_dir_all(parent)?;
                }
            }
        }

        let summary_file = BufWriter::new(File::create(&summary_path).map_err(|e| {
            io::Error::other(format!(
                "failed to create summary log file {}: {e}",
                summary_path.display()
            ))
        })?);
        let verbose_file = BufWriter::new(File::create(&verbose_path).map_err(|e| {
            io::Error::other(format!(
                "failed to create verbose log file {}: {e}",
                verbose_path.display()
            ))
        })?);

        let start_instant = Instant::now();
        let state = Arc::new(ProgressTextLoggerState {
            start_instant,
            snapshot: parking_lot::Mutex::new(TextSnapshot::default()),
            pending_log_lines: parking_lot::Mutex::new(Vec::new()),
            summary_file: parking_lot::Mutex::new(summary_file),
            verbose_file: parking_lot::Mutex::new(verbose_file),
            last_flushed_snapshot: parking_lot::Mutex::new(TextSnapshot::default()),
            shutdown_tx: parking_lot::Mutex::new(None),
            join_handle: parking_lot::Mutex::new(None),
        });

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let state_for_task = Arc::clone(&state);
        let join_handle =
            tokio::spawn(async move { run_flush_loop(state_for_task, shutdown_rx).await });

        *state.shutdown_tx.lock() = Some(shutdown_tx);
        *state.join_handle.lock() = Some(join_handle);
        PROGRESS_TEXT_LOGGER_STATE.store(Some(state));
        Ok(())
    }

    pub async fn shutdown() -> io::Result<()> {
        let Some(state) = PROGRESS_TEXT_LOGGER_STATE.load_full() else {
            return Ok(());
        };
        PROGRESS_TEXT_LOGGER_STATE.store(None);

        if let Some(shutdown_tx) = state.shutdown_tx.lock().take() {
            let _ = shutdown_tx.send(());
        }

        match state.join_handle.lock().take() {
            Some(join_handle) => match join_handle.await {
                Ok(result) => result,
                Err(err) => Err(io::Error::other(format!(
                    "progress text logger task join error: {err}"
                ))),
            },
            None => Ok(()),
        }
    }
}

pub fn log_message(message: TuiMessage) {
    let Some(state) = PROGRESS_TEXT_LOGGER_STATE.load_full() else {
        println!("{}", message.to_string());
        return;
    };

    match &message {
        TuiMessage::Line { message, severity } => {
            state.pending_log_lines.lock().push(ProgressLogLine {
                message: message.clone(),
                severity: *severity,
            });

            // Info and Error messages go to both the summary and verbose files.
            if *severity == Severity::Info || *severity == Severity::Error {
                let mut summary_file = state.summary_file.lock();
                if let Err(err) = writeln!(summary_file, "{message}") {
                    eprintln!("failed to write to summary log: {err}");
                }
                if let Err(err) = summary_file.flush() {
                    eprintln!("failed to flush summary log: {err}");
                }
                println!("{message}");
            }
        }
        _ => {}
    }

    state.snapshot.lock().apply_message(&message);
}

pub fn log_info(message: impl Into<String>) {
    log_message(TuiMessage::Line {
        message: message.into(),
        severity: Severity::Info,
    });
}

pub fn log_verbose(message: impl Into<String>) {
    log_message(TuiMessage::Line {
        message: message.into(),
        severity: Severity::Verbose,
    });
}

pub fn log_warning(message: impl Into<String>) {
    log_message(TuiMessage::Line {
        message: format!("[WARNING] {}", message.into()),
        severity: Severity::Warning,
    });
}

pub fn log_error(message: impl Into<String>) {
    log_message(TuiMessage::Line {
        message: format!("[ERROR] {}", message.into()),
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
    state: Arc<ProgressTextLoggerState>,
    mut shutdown_rx: oneshot::Receiver<()>,
) -> io::Result<()> {
    let mut interval = tokio::time::interval(FLUSH_INTERVAL);
    loop {
        tokio::select! {
            _ = interval.tick() => {
                flush_verbose_section(&state)?;
            }
            _ = &mut shutdown_rx => {
                flush_verbose_section(&state)?;
                break;
            }
        }
    }

    Ok(())
}

fn flush_verbose_section(state: &ProgressTextLoggerState) -> io::Result<()> {
    let snapshot = state.snapshot.lock().clone();
    let log_lines: Vec<ProgressLogLine> = {
        let mut guard = state.pending_log_lines.lock();
        std::mem::take(&mut *guard)
    };

    let has_log_lines = !log_lines.is_empty();
    let previous = state.last_flushed_snapshot.lock().clone();
    let has_snapshot_changes = snapshot.changed_since(&previous);

    if !has_log_lines && !has_snapshot_changes {
        return Ok(());
    }

    let elapsed_seconds = state.start_instant.elapsed().as_secs_f64();
    let mut file = state.verbose_file.lock();

    writeln!(file, "{}", "=".repeat(70))?;
    writeln!(file, "Elapsed: {:.1}s", elapsed_seconds)?;
    writeln!(file, "{}", "=".repeat(70))?;

    if !snapshot.state.is_empty() {
        writeln!(file, "State: {}", snapshot.state)?;
    }
    if !snapshot.window_name.is_empty() {
        writeln!(file, "Window: {}", snapshot.window_name)?;
    }
    if !snapshot.exit_hint.is_empty() {
        writeln!(file, "Exit Hint: {}", snapshot.exit_hint)?;
    }

    if !snapshot.key_values.is_empty() {
        writeln!(file)?;
        writeln!(file, "Key-Value Pairs:")?;
        for (key, value) in &snapshot.key_values {
            writeln!(file, "  {}: {}", key, value)?;
        }
    }

    if !snapshot.worker_progress.is_empty() {
        writeln!(file)?;
        writeln!(file, "Workers:")?;
        for (name, gauge) in &snapshot.worker_progress {
            let pct = gauge.progress * 100.0;
            writeln!(file, "  {}: {:.1}% - {}", name, pct, gauge.label)?;
        }
    }

    if snapshot.master_progress.progress > 0.0 {
        writeln!(file)?;
        let pct = snapshot.master_progress.progress * 100.0;
        writeln!(
            file,
            "Master: {:.1}% - {}",
            pct, snapshot.master_progress.label
        )?;
    }

    if !log_lines.is_empty() {
        writeln!(file)?;
        writeln!(file, "Log Lines:")?;
        for line in &log_lines {
            let severity_str = match line.severity {
                Severity::Info => "INFO",
                Severity::Warning => "WARN",
                Severity::Error => "ERROR",
                Severity::Verbose => "VERBOSE",
            };
            writeln!(file, "  [{}] {}", severity_str, line.message)?;
        }
    }

    writeln!(file)?;
    file.flush()?;

    // Update the last-flushed snapshot only if there were snapshot changes.
    // Log lines are always drained, so we don't need to track them separately.
    if has_snapshot_changes {
        *state.last_flushed_snapshot.lock() = snapshot;
    }

    Ok(())
}
