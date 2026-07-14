"""Common interface for Python wrappers to communicate with the Rust parent process.

The Rust orchestrator (via PythonProcessLauncher) passes:
1. CLI argument ``--orchestrator-socket-path`` for TUI message forwarding
2. An optional JSON payload on stdin (only for training wrappers)

This module provides a :class:`RustParentConnection` that bundles:
- Connecting the Unix-domain socket for TUI messages
- Reading (and validating) the optional stdin JSON payload
- Convenience methods for sending status / error / state messages back to Rust
"""

from __future__ import annotations

import sys
from typing import Any, Generic, TypeVar

from pydantic import BaseModel

from research_utility.text_message import UnixTextForwarder

T = TypeVar("T", bound=BaseModel)


def read_orchestrator_socket_path() -> str:
    """Extract the ``--orchestrator-socket-path`` value from ``sys.argv``.

    The Rust ``PythonProcessLauncher`` always appends this argument when
    spawning a Python wrapper.  Returns an empty string when the argument
    is absent (e.g. running the wrapper standalone for testing).
    """
    for i, arg in enumerate(sys.argv):
        if arg == "--orchestrator-socket-path" and i + 1 < len(sys.argv):
            return sys.argv[i + 1]
    return ""


def read_stdin_json(model_type: type[T]) -> T:
    """Read and parse a JSON payload from stdin into *model_type*.

    This mirrors the Rust-side ``write_json_payload_to_child_stdin`` /
    ``PythonProcessLauncher.with_stdin_json`` and uses Pydantic for validation.
    """
    raw = sys.stdin.buffer.read()
    if not raw or not raw.strip():
        raise ValueError(f"expected JSON payload on stdin for {model_type.__name__}")
    return model_type.model_validate_json(raw)


class RustParentConnection(Generic[T]):
    """Connection from a Python wrapper subprocess back to its Rust parent.

    Parameters
    ----------
    orchestrator_socket_path:
        The Unix-domain socket path passed via ``--orchestrator-socket-path``.
        If empty or ``None``, TUI forwarding is disabled (the wrapper runs
        stand-alone).
    stdin_model:
        When provided, stdin is read immediately and validated as this
        Pydantic model.  The result is available via :attr:`stdin_data`.
    """

    def __init__(
        self,
        orchestrator_socket_path: str,
        *,
        stdin_model: type[T] | None = None,
    ) -> None:
        self._forwarder: UnixTextForwarder | None = None
        socket_path = (orchestrator_socket_path or "").strip()
        if socket_path:
            self._forwarder = UnixTextForwarder(socket_path)

        self._stdin_data: T | None = None
        if stdin_model is not None:
            self._stdin_data = read_stdin_json(stdin_model)

    # -- stdin ----------------------------------------------------------------

    @property
    def stdin_data(self) -> T:
        """The parsed stdin JSON payload (only available when *stdin_model*
        was passed to the constructor)."""
        if self._stdin_data is None:
            raise RuntimeError(
                "stdin_data is not available; pass stdin_model to "
                "RustParentConnection(...)"
            )
        return self._stdin_data

    def has_stdin_data(self) -> bool:
        """Return ``True`` when a stdin payload was read and parsed."""
        return self._stdin_data is not None

    # -- text message helpers --------------------------------------------------

    def send_info(self, message: str) -> None:
        """Send an informational log line to the Rust orchestrator."""
        if self._forwarder is not None:
            self._forwarder.send_info(message)

    def send_verbose(self, message: str) -> None:
        """Send a verbose log line to the Rust orchestrator."""
        if self._forwarder is not None:
            self._forwarder.send_verbose(message)

    def send_warning(self, message: str) -> None:
        """Send a warning log line to the Rust orchestrator."""
        if self._forwarder is not None:
            self._forwarder.send_warning(message)

    def send_error(self, message: str) -> None:
        """Send an error log line to the Rust orchestrator."""
        if self._forwarder is not None:
            self._forwarder.send_error(message)

    def send_state(self, state: str) -> None:
        """Update the state label shown in the Rust orchestrator."""
        if self._forwarder is not None:
            self._forwarder.send_state(state)

    def send_key_value(self, key: str, value: str) -> None:
        """Emit a key-value pair to the Rust orchestrator."""
        if self._forwarder is not None:
            self._forwarder.send_key_value_pair(key, value)

    def send_raw_message(self, payload: dict[str, Any]) -> None:
        """Send an arbitrary text message dictionary (e.g. from a subprocess
        stdout line)."""
        if self._forwarder is not None:
            self._forwarder.send_message(payload)

    # -- lifecycle ------------------------------------------------------------

    def close(self) -> None:
        """Close the socket connection (best-effort)."""
        if self._forwarder is not None:
            self._forwarder.close()
            self._forwarder = None

    @property
    def is_connected(self) -> bool:
        """Return ``True`` when the socket is open."""
        return self._forwarder is not None
