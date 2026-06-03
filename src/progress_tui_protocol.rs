use std::collections::{BTreeMap, HashMap};
use std::env;
use std::io;

use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

use crate::message::TuiMessage;

pub const DEFAULT_PROGRESS_SCREEN_TCP_ADDR: &str = "127.0.0.1:7878";
pub const PROGRESS_SCREEN_TCP_ADDR_ENV: &str = "RESEARCH_UTILITY_PROGRESS_TUI_ADDR";

const MAX_WIRE_MESSAGE_BYTES: usize = 16 * 1024 * 1024;

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

pub fn framed_reader<R>(reader: R) -> FramedRead<R, LengthDelimitedCodec>
where
    R: AsyncRead + Unpin,
{
    FramedRead::new(reader, length_delimited_codec())
}

pub fn framed_writer<W>(writer: W) -> FramedWrite<W, LengthDelimitedCodec>
where
    W: AsyncWrite + Unpin,
{
    FramedWrite::new(writer, length_delimited_codec())
}

pub fn progress_screen_server_addr() -> String {
    env::var(PROGRESS_SCREEN_TCP_ADDR_ENV)
        .unwrap_or_else(|_| DEFAULT_PROGRESS_SCREEN_TCP_ADDR.to_string())
}

pub async fn send_framed_message<W, T>(
    writer: &mut FramedWrite<W, LengthDelimitedCodec>,
    message: &T,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let bytes = rmp_serde::to_vec_named(message).map_err(|err| {
        io::Error::new(io::ErrorKind::InvalidData, format!("encode error: {err}"))
    })?;

    if bytes.len() > MAX_WIRE_MESSAGE_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("wire message too large: {} bytes", bytes.len()),
        ));
    }

    writer
        .send(bytes.into())
        .await
        .map_err(|err| io::Error::new(io::ErrorKind::BrokenPipe, format!("write error: {err}")))?;
    Ok(())
}

pub async fn read_framed_message<R, T>(
    reader: &mut FramedRead<R, LengthDelimitedCodec>,
) -> io::Result<Option<T>>
where
    R: AsyncRead + Unpin,
    T: for<'de> Deserialize<'de>,
{
    let Some(frame_result) = reader.next().await else {
        return Ok(None);
    };
    let bytes = frame_result
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, format!("read error: {err}")))?;

    let message = rmp_serde::from_slice::<T>(&bytes).map_err(|err| {
        io::Error::new(io::ErrorKind::InvalidData, format!("decode error: {err}"))
    })?;
    Ok(Some(message))
}

fn length_delimited_codec() -> LengthDelimitedCodec {
    LengthDelimitedCodec::builder()
        .length_field_type::<u32>()
        .max_frame_length(MAX_WIRE_MESSAGE_BYTES)
        .new_codec()
}
