use std::collections::{BTreeMap, HashMap};
use std::env;
use std::io;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::message::TuiMessage;

pub const DEFAULT_PROGRESS_SCREEN_TCP_ADDR: &str = "127.0.0.1:7878";
pub const PROGRESS_SCREEN_TCP_ADDR_ENV: &str = "RESEARCH_UTILITY_PROGRESS_TUI_ADDR";

const MAX_WIRE_MESSAGE_BYTES: u32 = 16 * 1024 * 1024;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ProgressGaugeState {
    pub progress: f32,
    pub label: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ProgressStats {
    pub state: String,
    pub window_name: String,
    pub key_values: HashMap<String, String>,
    pub worker_progress: BTreeMap<String, ProgressGaugeState>,
    pub master_progress: ProgressGaugeState,
}

impl Default for ProgressStats {
    fn default() -> Self {
        Self {
            state: String::new(),
            window_name: "Progress Screen".to_string(),
            key_values: HashMap::new(),
            worker_progress: BTreeMap::new(),
            master_progress: ProgressGaugeState {
                progress: 0.0,
                label: "0%".to_string(),
            },
        }
    }
}

impl ProgressStats {
    pub fn apply_message(&mut self, message: &TuiMessage) {
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
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ProgressClientMessage {
    SnapshotRequest,
    SubmitCommand { command: String },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ProgressServerMessage {
    Snapshot(ProgressStats),
    Update(TuiMessage),
}

pub fn progress_screen_server_addr() -> String {
    env::var(PROGRESS_SCREEN_TCP_ADDR_ENV)
        .unwrap_or_else(|_| DEFAULT_PROGRESS_SCREEN_TCP_ADDR.to_string())
}

pub async fn send_framed_message<W, T>(writer: &mut W, message: &T) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let bytes = rmp_serde::to_vec_named(message).map_err(|err| {
        io::Error::new(io::ErrorKind::InvalidData, format!("encode error: {err}"))
    })?;
    let length = u32::try_from(bytes.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "wire message too large"))?;
    writer.write_u32(length).await?;
    writer.write_all(&bytes).await?;
    Ok(())
}

pub async fn read_framed_message<R, T>(reader: &mut R) -> io::Result<Option<T>>
where
    R: AsyncRead + Unpin,
    T: for<'de> Deserialize<'de>,
{
    let length = match reader.read_u32().await {
        Ok(length) => length,
        Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(err),
    };

    if length > MAX_WIRE_MESSAGE_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("wire message too large: {length} bytes"),
        ));
    }

    let mut bytes = vec![0u8; length as usize];
    if let Err(err) = reader.read_exact(&mut bytes).await {
        if err.kind() == io::ErrorKind::UnexpectedEof {
            return Ok(None);
        }
        return Err(err);
    }

    let message = rmp_serde::from_slice::<T>(&bytes).map_err(|err| {
        io::Error::new(io::ErrorKind::InvalidData, format!("decode error: {err}"))
    })?;
    Ok(Some(message))
}
