use crate::{message::MyLogMessage, progress_screen::PROGRESS_SCREEN_MESSAGE_TX};

pub fn log_message(message: MyLogMessage) {
    if let Some(my_log_message_tx) = PROGRESS_SCREEN_MESSAGE_TX.load_full() {
        let _ = my_log_message_tx.send(message);
    } else {
        println!("{}", message.to_string());
    }
}
pub fn log_info(message: impl Into<String>) {
    let message = message.into();
    log_message(MyLogMessage::Line {
        message,
        severity: crate::message::Severity::Info,
    });
}
pub fn log_warning(message: impl Into<String>) {
    let message = message.into();
    log_message(MyLogMessage::Line {
        message,
        severity: crate::message::Severity::Warning,
    });
}
pub fn log_error(message: impl Into<String>) {
    let message = message.into();
    log_message(MyLogMessage::Line {
        message,
        severity: crate::message::Severity::Error,
    });
}

pub fn log_key_value_pair(key: impl Into<String>, value: impl Into<String>) {
    let key = key.into();
    let value = value.into();
    log_message(MyLogMessage::KeyValuePair { key, value });
}

pub fn log_master_progress(progress: f32, label: impl Into<String>) {
    let label = label.into();
    log_message(MyLogMessage::MasterProgress { progress, label });
}

pub fn log_worker_progress(
    worker_name: impl Into<String>,
    progress: f32,
    label: impl Into<String>,
) {
    let worker_name = worker_name.into();
    let label = label.into();
    log_message(MyLogMessage::WorkerProgress {
        worker_name,
        progress,
        label,
    });
}
