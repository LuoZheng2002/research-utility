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
        // worker_id: usize,
        worker_name: String,
        progress: f32,
        label: String,
    },
    MasterProgress {
        progress: f32,
        label: String,
    },
}

impl MyLogMessage {
    pub fn key_value_pair(key: String, value: String) -> Self {
        MyLogMessage::KeyValuePair { key, value }
    }
    pub fn master_progress(progress: f32, label: String) -> Self {
        MyLogMessage::MasterProgress { progress, label }
    }
    pub fn worker_progress(worker_name: String, progress: f32, label: String) -> Self {
        MyLogMessage::WorkerProgress {
            worker_name,
            progress,
            label,
        }
    }
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
        }
    }
}
