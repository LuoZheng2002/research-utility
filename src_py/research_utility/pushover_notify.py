"""Pushover notification library.

Sends a Pushover notification with a single function call.  Credentials are
read from a ``.env`` file in the **current working directory** of the calling
process, or from environment variables (which take precedence).

.env keys
---------
``PUSHOVER_TOKEN`` — Application API token (create at https://pushover.net/apps/build)
``PUSHOVER_USER``  — Your user key (find at https://pushover.net)
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from urllib import error, parse, request

API_URL = "https://api.pushover.net/1/messages.json"


def _load_dotenv() -> None:
    """Parse ``Path.cwd() / ".env"`` and load into ``os.environ`` (does not overwrite)."""
    env_path = Path.cwd() / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = val


def push_notification(
    message: str,
    *,
    title: str = "",
    priority: int = 0,
    device: str = "",
    sound: str = "",
    url: str = "",
    url_title: str = "",
) -> dict:
    """Send a Pushover notification. Returns the parsed JSON response dict.

    Reads ``PUSHOVER_TOKEN`` and ``PUSHOVER_USER`` from the environment (or the
    ``.env`` file in the current working directory, loaded automatically on the
    first call).

    Raises ``RuntimeError`` when credentials are missing or the API call fails.
    """
    _load_dotenv()

    token = os.getenv("PUSHOVER_TOKEN", "")
    user = os.getenv("PUSHOVER_USER", "")

    if not token:
        env_path = Path.cwd() / ".env"
        raise RuntimeError(
            "PUSHOVER_TOKEN not set. Add it to "
            f"{env_path} or set the environment variable."
        )
    if not user:
        env_path = Path.cwd() / ".env"
        raise RuntimeError(
            "PUSHOVER_USER not set. Add it to "
            f"{env_path} or set the environment variable."
        )

    params: dict[str, str | int] = {
        "token": token,
        "user": user,
        "message": message,
    }
    if title:
        params["title"] = title
    if priority:
        params["priority"] = str(priority)
    if device:
        params["device"] = device
    if sound:
        params["sound"] = sound
    if url:
        params["url"] = url
    if url_title:
        params["url_title"] = url_title

    data = parse.urlencode(params).encode("utf-8")
    req = request.Request(API_URL, data=data, method="POST")
    try:
        with request.urlopen(req, timeout=15) as resp:
            raw = resp.read().decode("utf-8")
            result = json.loads(raw)
    except error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Pushover API error (HTTP {exc.code}): {raw}") from exc

    if result.get("status") != 1:
        errors = result.get("errors", [])
        raise RuntimeError(f"Pushover API error: {errors}")

    return result
