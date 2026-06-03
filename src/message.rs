// for communicating about rollout progress, training stats, etc.

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    Info,
    Warning,
    Error,
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum TuiMessage {
    Line {
        message: String,
        severity: Severity,
    },
    State {
        state: String,
    },
    WindowName {
        window_name: String,
    },
    KeyValuePair {
        key: String,
        value: String,
    },
    WorkerProgress {
        worker_name: String,
        progress: f32,
        label: String,
    },
    MasterProgress {
        progress: f32,
        label: String,
    },
    DeleteWorkerBar {
        worker_name: String,
    },
}

impl TuiMessage {
    pub fn to_string(&self) -> String {
        match self {
            TuiMessage::Line { message, severity } => match severity {
                Severity::Info => format!("INFO: {message}"),
                Severity::Warning => format!("WARNING: {message}"),
                Severity::Error => format!("ERROR: {message}"),
            },
            TuiMessage::State { state } => format!("STATE: {state}"),
            TuiMessage::WindowName { window_name } => format!("WINDOW NAME: {window_name}"),
            TuiMessage::KeyValuePair { key, value } => format!("{key}: {value}"),
            TuiMessage::WorkerProgress {
                worker_name,
                progress,
                label,
            } => format!("Worker {worker_name} Progress - {label}: {progress:.2}%"),
            TuiMessage::MasterProgress { progress, label } => {
                format!("Master Progress - {label}: {progress:.2}%")
            }
            TuiMessage::DeleteWorkerBar { worker_name } => {
                format!("Delete progress bar for worker {worker_name}")
            }
        }
    }
}
