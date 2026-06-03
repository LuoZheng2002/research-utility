use std::fs::OpenOptions;
use std::io;
use std::io::Write;
use std::sync::{Arc, OnceLock};

use arc_swap::ArcSwapOption;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, oneshot};

use crate::message::{Severity, TuiMessage};
use crate::progress_tui_protocol::{
    ProgressClientMessage, ProgressServerMessage, ProgressSnapshot, progress_screen_server_addr,
    read_framed_message, send_framed_message,
};

const SERVER_BROADCAST_CHANNEL_CAPACITY: usize = 1024;

type ClientCommandHandler = Arc<dyn Fn(String) + Send + Sync + 'static>;

struct RunConfig {
    client_command_handler: ClientCommandHandler,
}

struct ProgressTuiServerRuntime {
    join_handle: Option<tokio::task::JoinHandle<io::Result<()>>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

struct ProgressTuiServerShared {
    state: parking_lot::Mutex<ProgressSnapshot>,
    update_tx: broadcast::Sender<TuiMessage>,
    log_file_writer: parking_lot::Mutex<Option<std::fs::File>>,
}

static PROGRESS_TUI_SERVER_RUNTIME: OnceLock<parking_lot::Mutex<ProgressTuiServerRuntime>> =
    OnceLock::new();
static PROGRESS_TUI_SERVER_SHARED: ArcSwapOption<ProgressTuiServerShared> =
    ArcSwapOption::const_empty();

fn runtime_state() -> &'static parking_lot::Mutex<ProgressTuiServerRuntime> {
    PROGRESS_TUI_SERVER_RUNTIME.get_or_init(|| {
        parking_lot::Mutex::new(ProgressTuiServerRuntime {
            join_handle: None,
            shutdown_tx: None,
        })
    })
}

pub struct ProgressTuiServer;

impl ProgressTuiServer {
    pub async fn initialize<F>(
        log_file: Option<String>,
        client_command_handler: F,
    ) -> io::Result<()>
    where
        F: Fn(String) + Send + Sync + 'static,
    {
        let mut runtime = runtime_state().lock();
        if runtime.join_handle.is_some() {
            return Ok(());
        }

        let log_file_writer = log_file
            .map(|path| {
                OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(path)
            })
            .transpose()?;

        let (update_tx, _unused_rx) =
            broadcast::channel::<TuiMessage>(SERVER_BROADCAST_CHANNEL_CAPACITY);
        let shared = Arc::new(ProgressTuiServerShared {
            state: parking_lot::Mutex::new(ProgressSnapshot::default()),
            update_tx,
            log_file_writer: parking_lot::Mutex::new(log_file_writer),
        });
        PROGRESS_TUI_SERVER_SHARED.store(Some(Arc::clone(&shared)));

        let config = RunConfig {
            client_command_handler: Arc::new(client_command_handler),
        };
        let listener = TcpListener::bind(progress_screen_server_addr()).await?;
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let join_handle =
            tokio::spawn(async move { run(config, listener, shared, shutdown_rx).await });
        runtime.shutdown_tx = Some(shutdown_tx);
        runtime.join_handle = Some(join_handle);
        Ok(())
    }

    pub async fn shutdown() -> io::Result<()> {
        let (shutdown_tx, join_handle) = {
            let mut runtime = runtime_state().lock();
            (runtime.shutdown_tx.take(), runtime.join_handle.take())
        };
        PROGRESS_TUI_SERVER_SHARED.store(None);

        if let Some(shutdown_tx) = shutdown_tx {
            let _ = shutdown_tx.send(());
        }

        match join_handle {
            Some(join_handle) => match join_handle.await {
                Ok(result) => result,
                Err(err) => Err(io::Error::other(format!(
                    "progress tui server task join error: {err}"
                ))),
            },
            None => Ok(()),
        }
    }
}

pub fn log_message(message: TuiMessage) {
    let maybe_shared = PROGRESS_TUI_SERVER_SHARED.load_full();
    let Some(shared) = maybe_shared else {
        println!("{}", message.to_string());
        return;
    };

    write_line_to_log_file_if_enabled(&message, &shared.log_file_writer);
    shared.state.lock().apply_message(&message);
    let _ = shared.update_tx.send(message);
}

pub fn log_info(message: impl Into<String>) {
    log_message(TuiMessage::Line {
        message: message.into(),
        severity: Severity::Info,
    });
}

pub fn log_warning(message: impl Into<String>) {
    log_message(TuiMessage::Line {
        message: message.into(),
        severity: Severity::Warning,
    });
}

pub fn log_error(message: impl Into<String>) {
    log_message(TuiMessage::Line {
        message: message.into(),
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

async fn run(
    config: RunConfig,
    listener: TcpListener,
    shared: Arc<ProgressTuiServerShared>,
    mut shutdown_rx: oneshot::Receiver<()>,
) -> io::Result<()> {
    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                let (socket, _) = accept_result?;
                let shared = Arc::clone(&shared);
                let client_command_handler = Arc::clone(&config.client_command_handler);
                tokio::spawn(async move {
                    let _ = handle_client(socket, shared, client_command_handler).await;
                });
            }
            _ = &mut shutdown_rx => {
                break;
            }
        }
    }

    Ok(())
}

async fn handle_client(
    socket: TcpStream,
    shared: Arc<ProgressTuiServerShared>,
    client_command_handler: ClientCommandHandler,
) -> io::Result<()> {
    let (mut reader, mut writer) = socket.into_split();
    let mut update_rx = shared.update_tx.subscribe();

    loop {
        tokio::select! {
            wire_message = read_framed_message::<_, ProgressClientMessage>(&mut reader) => {
                match wire_message? {
                    Some(ProgressClientMessage::SnapshotRequest) => {
                        let snapshot = shared.state.lock().clone();
                        send_framed_message(&mut writer, &ProgressServerMessage::Snapshot(snapshot)).await?;
                    }
                    Some(ProgressClientMessage::SubmitCommand { command }) => {
                        (client_command_handler)(command);
                    }
                    None => break,
                }
            }
            recv_result = update_rx.recv() => {
                match recv_result {
                    Ok(message) => {
                        send_framed_message(&mut writer, &ProgressServerMessage::Update(message)).await?;
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }

    Ok(())
}

fn write_line_to_log_file_if_enabled(
    message: &TuiMessage,
    log_file_writer: &parking_lot::Mutex<Option<std::fs::File>>,
) {
    let mut guard = log_file_writer.lock();
    let Some(file) = guard.as_mut() else {
        return;
    };

    if let TuiMessage::Line { message, .. } = message {
        if file.write_all(message.as_bytes()).is_err() {
            return;
        }
        let _ = file.write_all(b"\n");
    }
}
