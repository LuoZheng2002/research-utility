// for communicating about rollout progress, training stats, etc.

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    Info,
    Warning,
    Error,
}
#[derive(Serialize, Deserialize, Debug)]
pub enum MyLogMessage {
    Line {
        message: String,
        severity: Severity,
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

impl MyLogMessage {
    pub fn to_string(&self) -> String {
        match self {
            MyLogMessage::Line { message, severity } => match severity {
                Severity::Info => format!("INFO: {message}"),
                Severity::Warning => format!("WARNING: {message}"),
                Severity::Error => format!("ERROR: {message}"),
            },
            MyLogMessage::KeyValuePair { key, value } => format!("{key}: {value}"),
            MyLogMessage::WorkerProgress {
                worker_name,
                progress,
                label,
            } => format!("Worker {worker_name} Progress - {label}: {progress:.2}%"),
            MyLogMessage::MasterProgress { progress, label } => {
                format!("Master Progress - {label}: {progress:.2}%")
            }
            MyLogMessage::DeleteWorkerBar { worker_name } => {
                format!("Delete progress bar for worker {worker_name}")
            }
        }
    }
}
