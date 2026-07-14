use std::{
    path::{Path, PathBuf},
    process::Stdio,
    sync::atomic::{AtomicU64, Ordering},
};

use serde::Serialize;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::process::{Child, Command};
use tokio::sync::watch;

use crate::{
    message::TuiMessage,
    progress_tui_logger::{log_message, log_warning},
};

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Handle to a running Python wrapper process, bundling the child process,
/// the TUI listener task, and the stop-signal channel that coordinates them.
pub struct PythonProcessHandle {
    pub child: Child,
    pub stop_signal_tx: watch::Sender<bool>,
    pub listener_handle: tokio::task::JoinHandle<()>,
}

impl PythonProcessHandle {
    /// Wait for the child process to exit, then signal the TUI listener to
    /// stop and join it.  Returns the child's exit status.
    pub async fn wait_and_shutdown(self) -> Result<std::process::ExitStatus, String> {
        let Self {
            mut child,
            stop_signal_tx,
            listener_handle,
        } = self;

        let status = child
            .wait()
            .await
            .map_err(|err| format!("failed while waiting for child process: {}", err))?;

        let _ = stop_signal_tx.send(true);
        let _ = listener_handle.await;

        Ok(status)
    }
}

/// Builder that launches a Python wrapper subprocess with:
/// - CLI arguments
/// - optional JSON payload on stdin
/// - a Unix-domain socket for TUI messages back to the orchestrator
pub struct PythonProcessLauncher {
    kind: &'static str,
    module: &'static str,
    args: Vec<(&'static str, String)>,
    stdin_json: Option<Vec<u8>>,
    #[cfg(unix)]
    process_group: bool,
}

impl PythonProcessLauncher {
    pub fn new(kind: &'static str, module: &'static str) -> Self {
        Self {
            kind,
            module,
            args: Vec::new(),
            stdin_json: None,
            #[cfg(unix)]
            process_group: false,
        }
    }

    pub fn with_arg(mut self, key: &'static str, value: impl Into<String>) -> Self {
        self.args.push((key, value.into()));
        self
    }

    /// Serialize `payload` as JSON and send it to the child's stdin after
    /// the process starts.
    pub fn with_stdin_json<T: Serialize>(mut self, payload: &T) -> Result<Self, String> {
        let bytes = serde_json::to_vec(payload).map_err(|err| {
            format!(
                "failed to serialize {} stdin payload as JSON: {}",
                self.kind, err
            )
        })?;
        self.stdin_json = Some(bytes);
        Ok(self)
    }

    /// On Unix, put the child in its own process group so that signals can
    /// be delivered to the whole tree.  No-op on other platforms.
    #[cfg(unix)]
    pub fn with_process_group(mut self, enabled: bool) -> Self {
        self.process_group = enabled;
        self
    }

    /// Bind the TUI socket, build the `uv run python -m <module>` command,
    /// spawn the child, write the optional stdin payload, and spawn the TUI
    /// listener.  Returns a [`PythonProcessHandle`] that bundles everything.
    pub async fn launch(self) -> Result<PythonProcessHandle, String> {
        let (socket_path, listener) = bind_wrapper_tui_listener(self.kind)?;
        let socket_path_arg = socket_path_to_arg(&socket_path)?;

        let mut command = Command::new("uv");
        command
            .arg("run")
            .arg("python")
            .arg("-m")
            .arg(self.module)
            .arg("--orchestrator-socket-path")
            .arg(&socket_path_arg);

        for (key, value) in &self.args {
            command.arg(key);
            command.arg(value);
        }

        command.stdout(Stdio::null());
        command.stderr(Stdio::null());

        if self.stdin_json.is_some() {
            command.stdin(Stdio::piped());
        }

        #[cfg(unix)]
        {
            if self.process_group {
                command.process_group(0);
            }
        }

        let mut child = command.spawn().map_err(|err| {
            cleanup_socket_path(&socket_path);
            format!("failed to launch {} wrapper process: {}", self.kind, err)
        })?;

        if let Some(payload) = self.stdin_json {
            write_stdin_payload(&mut child, &payload, self.kind).await?;
        }

        let (stop_signal_tx, stop_signal_rx) = watch::channel(false);
        let listener_handle = spawn_tui_listener(listener, socket_path, self.kind, stop_signal_rx);

        Ok(PythonProcessHandle {
            child,
            stop_signal_tx,
            listener_handle,
        })
    }
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

static WRAPPER_TUI_SOCKET_COUNTER: AtomicU64 = AtomicU64::new(0);

pub fn bind_wrapper_tui_listener(wrapper_kind: &str) -> Result<(PathBuf, UnixListener), String> {
    let socket_path = wrapper_tui_socket_path(wrapper_kind);
    cleanup_socket_path(&socket_path);
    let listener = UnixListener::bind(&socket_path).map_err(|err| {
        format!(
            "failed to bind Unix socket for {} wrapper TUI listener at {}: {}",
            wrapper_kind,
            socket_path.display(),
            err
        )
    })?;
    Ok((socket_path, listener))
}

fn socket_path_to_arg(socket_path: &Path) -> Result<String, String> {
    socket_path
        .to_str()
        .map(str::to_owned)
        .ok_or_else(|| format!("socket path is not valid UTF-8: {}", socket_path.display()))
}

async fn write_stdin_payload(
    child: &mut Child,
    payload: &[u8],
    process_name: &str,
) -> Result<(), String> {
    let mut stdin = child.stdin.take().ok_or_else(|| {
        format!(
            "{} stdin is unavailable; expected a piped stdin handle",
            process_name
        )
    })?;
    stdin.write_all(payload).await.map_err(|err| {
        format!(
            "failed to write JSON stdin payload to {}: {}",
            process_name, err
        )
    })?;
    stdin.shutdown().await.map_err(|err| {
        format!(
            "failed to close {} stdin after writing JSON payload: {}",
            process_name, err
        )
    })?;
    drop(stdin);
    Ok(())
}

fn spawn_tui_listener(
    listener: UnixListener,
    socket_path: PathBuf,
    wrapper_name: &'static str,
    stop_signal: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(err) =
            run_wrapper_tui_listener(listener, &socket_path, wrapper_name, stop_signal).await
        {
            log_warning(format!(
                "{} TUI socket listener ended with error: {}",
                wrapper_name, err
            ));
        }
        cleanup_socket_path(&socket_path);
    })
}

pub async fn run_wrapper_tui_listener(
    listener: UnixListener,
    socket_path: &Path,
    wrapper_name: &str,
    mut stop_signal: watch::Receiver<bool>,
) -> Result<(), String> {
    if *stop_signal.borrow() {
        return Ok(());
    }
    let (stream, _) = tokio::select! {
        accept_result = listener.accept() => {
            accept_result.map_err(|err| {
                format!(
                    "failed while accepting {} Unix socket connection at {}: {}",
                    wrapper_name,
                    socket_path.display(),
                    err
                )
            })?
        }
        stop_result = stop_signal.changed() => {
            let _ = stop_result;
            return Ok(());
        }
    };

    let mut reader_task = tokio::spawn(read_tui_stream(stream, wrapper_name.to_string()));

    tokio::select! {
        reader_result = &mut reader_task => {
            match reader_result {
                Ok(Ok(())) => Ok(()),
                Ok(Err(err)) => Err(err),
                Err(err) => Err(format!(
                    "{} TUI socket reader task join failure: {}",
                    wrapper_name, err
                )),
            }
        }
        stop_result = stop_signal.changed() => {
            let _ = stop_result;
            reader_task.abort();
            let _ = reader_task.await;
            Ok(())
        }
    }
}

async fn read_tui_stream(stream: UnixStream, wrapper_name: String) -> Result<(), String> {
    let mut lines = BufReader::new(stream).lines();
    loop {
        match lines.next_line().await {
            Ok(Some(content)) => {
                if content.trim().is_empty() {
                    continue;
                }
                match serde_json::from_str::<TuiMessage>(&content) {
                    Ok(message) => log_message(message),
                    Err(err) => log_warning(format!(
                        "failed to parse {} TUI socket message as TuiMessage: {} (payload={})",
                        wrapper_name, err, content
                    )),
                }
            }
            Ok(None) => return Ok(()),
            Err(err) => {
                return Err(format!(
                    "failed while reading {} TUI socket stream: {}",
                    wrapper_name, err
                ));
            }
        }
    }
}

fn wrapper_tui_socket_path(wrapper_kind: &str) -> PathBuf {
    let id = WRAPPER_TUI_SOCKET_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "credit_assignment_{}_wrapper_{}_{}.sock",
        wrapper_kind,
        std::process::id(),
        id
    ))
}

fn cleanup_socket_path(socket_path: &Path) {
    if let Err(err) = std::fs::remove_file(socket_path) {
        if err.kind() != std::io::ErrorKind::NotFound {
            log_warning(format!(
                "failed to remove Unix socket path {}: {}",
                socket_path.display(),
                err
            ));
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------
