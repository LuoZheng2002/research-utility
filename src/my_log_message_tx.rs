use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering::Relaxed},
};

use arc_swap::ArcSwapOption;
use tokio::sync::mpsc;

use crate::message::MyLogMessage;

pub static MY_LOG_MESSAGE_TX: ArcSwapOption<mpsc::UnboundedSender<MyLogMessage>> =
    ArcSwapOption::const_empty();

pub static FALL_BACK_TO_STDOUT: AtomicBool = AtomicBool::new(false);

pub fn set_my_log_message_tx(my_log_message_tx: mpsc::UnboundedSender<MyLogMessage>) {
    MY_LOG_MESSAGE_TX.store(Some(Arc::new(my_log_message_tx)));
}

pub fn clear_my_log_message_tx() {
    MY_LOG_MESSAGE_TX.store(None);
}

pub fn log_message(message: MyLogMessage) {
    if let Some(my_log_message_tx) = MY_LOG_MESSAGE_TX.load_full() {
        my_log_message_tx
            .send(message)
            .expect("failed to send worker message");
    } else if FALL_BACK_TO_STDOUT.load(Relaxed) {
        println!("{}", message.to_string());
    }
}
pub fn log_info(message: String) {
    log_message(MyLogMessage::Line {
        message,
        severity: crate::message::Severity::Info,
    });
}
pub fn log_warning(message: String) {
    log_message(MyLogMessage::Line {
        message,
        severity: crate::message::Severity::Warning,
    });
}
pub fn log_error(message: String) {
    log_message(MyLogMessage::Line {
        message,
        severity: crate::message::Severity::Error,
    });
}

pub fn log_key_value_pair(key: String, value: String) {
    log_message(MyLogMessage::KeyValuePair { key, value });
}

pub fn log_master_progress(progress: f32, label: String) {
    log_message(MyLogMessage::MasterProgress { progress, label });
}

pub fn log_worker_progress(worker_name: String, progress: f32, label: String) {
    log_message(MyLogMessage::WorkerProgress {
        worker_name,
        progress,
        label,
    });
}