// for communicating about rollout progress, training stats, etc.
#[derive(Debug)]
pub enum WorkerMessage {
    KeyValuePair {
        key: String,
        value: String,
    },
    WorkerProgress {
        worker_id: usize,
        progress: f32,
        label: String,
    },
    MasterProgress {
        progress: f32,
        label: String,
    },
}

impl WorkerMessage {
    pub fn key_value_pair(key: String, value: String) -> Self {
        WorkerMessage::KeyValuePair { key, value }
    }
    pub fn master_progress(progress: f32, label: String) -> Self {
        WorkerMessage::MasterProgress { progress, label }
    }
    pub fn worker_progress(worker_id: usize, progress: f32, label: String) -> Self {
        WorkerMessage::WorkerProgress {
            worker_id,
            progress,
            label,
        }
    }
}
