use std::sync::Arc;

use arc_swap::ArcSwapOption;
use tokio::sync::mpsc;

use crate::message::WorkerMessage;

pub static WORKER_MESSAGE_TX: ArcSwapOption<mpsc::UnboundedSender<WorkerMessage>> =
    ArcSwapOption::const_empty();

pub fn set_worker_message_tx(worker_message_tx: mpsc::UnboundedSender<WorkerMessage>) {
    WORKER_MESSAGE_TX.store(Some(Arc::new(worker_message_tx)));
}

pub fn clear_worker_message_tx() {
    WORKER_MESSAGE_TX.store(None);
}

pub fn log_worker_message(message: WorkerMessage) {
    if let Some(worker_message_tx) = WORKER_MESSAGE_TX.load_full() {
        worker_message_tx
            .send(message)
            .expect("failed to send worker message");
    }
}

pub fn log_key_value_pair(key: String, value: String) {
    if let Some(worker_message_tx) = WORKER_MESSAGE_TX.load_full() {
        worker_message_tx
            .send(WorkerMessage::KeyValuePair { key, value })
            .expect("failed to send KeyValuePair worker message");
    }
}

pub fn log_master_progress(progress: f32, label: String) {
    if let Some(worker_message_tx) = WORKER_MESSAGE_TX.load_full() {
        worker_message_tx
            .send(WorkerMessage::MasterProgress { progress, label })
            .expect("failed to send MasterProgress worker message");
    }
}

pub fn log_worker_progress(worker_id: usize, progress: f32, label: String) {
    if let Some(worker_message_tx) = WORKER_MESSAGE_TX.load_full() {
        worker_message_tx
            .send(WorkerMessage::WorkerProgress {
                worker_id,
                progress,
                label,
            })
            .expect("failed to send WorkerProgress worker message");
    }
}
