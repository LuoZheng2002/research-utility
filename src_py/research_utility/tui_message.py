from __future__ import annotations

import json
import socket
import threading
from typing import Any

SeverityName = str


def line_message(message: str, severity: SeverityName) -> dict[str, Any]:
    return {"Line": {"message": str(message), "severity": str(severity)}}


def info_line_message(message: str) -> dict[str, Any]:
    return line_message(message, "Info")


def warning_line_message(message: str) -> dict[str, Any]:
    return line_message(message, "Warning")


def error_line_message(message: str) -> dict[str, Any]:
    return line_message(message, "Error")


def state_message(state: str) -> dict[str, Any]:
    return {"State": {"state": str(state)}}


def window_name_message(window_name: str) -> dict[str, Any]:
    return {"WindowName": {"window_name": str(window_name)}}


def key_value_pair_message(key: str, value: str) -> dict[str, Any]:
    return {"KeyValuePair": {"key": str(key), "value": str(value)}}


def worker_progress_message(
    worker_name: str, progress: float, label: str
) -> dict[str, Any]:
    return {
        "WorkerProgress": {
            "worker_name": str(worker_name),
            "progress": float(progress),
            "label": str(label),
        }
    }


def master_progress_message(progress: float, label: str) -> dict[str, Any]:
    return {"MasterProgress": {"progress": float(progress), "label": str(label)}}


def delete_worker_bar_message(worker_name: str) -> dict[str, Any]:
    return {"DeleteWorkerBar": {"worker_name": str(worker_name)}}


def exit_hint_message(hint: str) -> dict[str, Any]:
    return {"ExitHint": str(hint)}


def serialize_tui_message(message: dict[str, Any]) -> str:
    return json.dumps(message, ensure_ascii=True, separators=(",", ":"))


class UnixTuiForwarder:
    def __init__(self, socket_path: str | None) -> None:
        self._socket_path = (socket_path or "").strip()
        self._socket: socket.socket | None = None
        self._lock = threading.Lock()
        if not self._socket_path:
            return
        client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            client.settimeout(5.0)
            client.connect(self._socket_path)
            client.settimeout(None)
            self._socket = client
        except Exception as error:  # noqa: BLE001
            try:
                client.close()
            except Exception:
                pass
            print(
                f"[TUI_SOCKET] failed to connect to orchestrator socket {self._socket_path}: {error}",
                flush=True,
            )

    def close(self) -> None:
        with self._lock:
            sock = self._socket
            self._socket = None
        if sock is None:
            return
        try:
            sock.close()
        except Exception:
            pass

    def send_message(self, message: dict[str, Any]) -> None:
        payload = serialize_tui_message(message).encode("utf-8") + b"\n"
        with self._lock:
            if self._socket is None:
                return
            try:
                self._socket.sendall(payload)
            except Exception as error:  # noqa: BLE001
                print(
                    f"[TUI_SOCKET] failed to send TuiMessage to {self._socket_path}: {error}",
                    flush=True,
                )
                try:
                    self._socket.close()
                except Exception:
                    pass
                self._socket = None

    def send_line(self, message: str, severity: SeverityName) -> None:
        self.send_message(line_message(message, severity))

    def send_info(self, message: str) -> None:
        self.send_message(info_line_message(message))

    def send_warning(self, message: str) -> None:
        self.send_message(warning_line_message(message))

    def send_error(self, message: str) -> None:
        self.send_message(error_line_message(message))

    def send_state(self, state: str) -> None:
        self.send_message(state_message(state))

    def send_key_value_pair(self, key: str, value: str) -> None:
        self.send_message(key_value_pair_message(key, value))

    def send_worker_progress(
        self, worker_name: str, progress: float, label: str
    ) -> None:
        self.send_message(worker_progress_message(worker_name, progress, label))

    def send_master_progress(self, progress: float, label: str) -> None:
        self.send_message(master_progress_message(progress, label))

    def send_delete_worker_bar(self, worker_name: str) -> None:
        self.send_message(delete_worker_bar_message(worker_name))

    def send_exit_hint(self, hint: str) -> None:
        self.send_message(exit_hint_message(hint))


__all__ = [
    "UnixTuiForwarder",
    "delete_worker_bar_message",
    "error_line_message",
    "exit_hint_message",
    "info_line_message",
    "key_value_pair_message",
    "line_message",
    "master_progress_message",
    "serialize_tui_message",
    "state_message",
    "warning_line_message",
    "window_name_message",
    "worker_progress_message",
]
