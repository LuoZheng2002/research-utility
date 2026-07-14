#!/usr/bin/env python3
"""Push a notification to Pushover.

Usage:
    python scripts/pushover_notify.py "Your message here"
    echo "Task done" | python scripts/pushover_notify.py
    python scripts/pushover_notify.py "Build failed" --title "CI Alert" --priority 1

Credentials — read from the project-root .env file:

    PUSHOVER_TOKEN          Application API token (create at https://pushover.net/apps/build)
    PUSHOVER_USER           Your user key (find at https://pushover.net)

These can also be set as environment variables, which take precedence over .env.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from urllib import error, parse, request

# ---------------------------------------------------------------------------
# .env loading
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
ENV_PATH = PROJECT_ROOT / ".env"


def _load_dotenv() -> None:
    """Parse PROJECT_ROOT/.env and load into os.environ (does not overwrite)."""
    if not ENV_PATH.exists():
        return
    for line in ENV_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = val


# ---------------------------------------------------------------------------
# Pushover API
# ---------------------------------------------------------------------------

API_URL = "https://api.pushover.net/1/messages.json"


def push_notification(
    message: str,
    *,
    token: str = "",
    user: str = "",
    title: str = "",
    priority: int = 0,
    device: str = "",
    sound: str = "",
    url: str = "",
    url_title: str = "",
) -> dict:
    """Send a Pushover notification. Returns the parsed JSON response."""
    if not token:
        token = os.getenv("PUSHOVER_TOKEN", "")
    if not user:
        user = os.getenv("PUSHOVER_USER", "")

    if not token:
        raise RuntimeError(
            "PUSHOVER_TOKEN not set. Add it to the project .env file "
            f"({ENV_PATH}) or set the environment variable."
        )
    if not user:
        raise RuntimeError(
            "PUSHOVER_USER not set. Add it to the project .env file "
            f"({ENV_PATH}) or set the environment variable."
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
            result = __import__("json").loads(raw)
    except error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Pushover API error (HTTP {exc.code}): {raw}") from exc

    if result.get("status") != 1:
        errors = result.get("errors", [])
        raise RuntimeError(f"Pushover API error: {errors}")

    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    _load_dotenv()

    parser = argparse.ArgumentParser(
        description="Push a notification to Pushover.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "message",
        nargs="*",
        help="The notification message (multiple words are joined). "
        "If piped, reads from stdin.",
    )
    parser.add_argument(
        "--token",
        help="Pushover application API token. Overrides PUSHOVER_TOKEN env/.env.",
    )
    parser.add_argument(
        "--user",
        help="Pushover user key. Overrides PUSHOVER_USER env/.env.",
    )
    parser.add_argument(
        "--title", "-t",
        help="Notification title (optional).",
    )
    parser.add_argument(
        "--priority", "-p",
        type=int,
        choices=[-2, -1, 0, 1, 2],
        default=0,
        help="Notification priority: -2 (quietest) to 2 (emergency). Default: 0.",
    )
    parser.add_argument(
        "--device", "-d",
        help="Target a specific device by name (optional).",
    )
    parser.add_argument(
        "--sound", "-s",
        help="Notification sound name (optional, e.g. 'pushover', 'bike', 'cosmic').",
    )
    parser.add_argument(
        "--url",
        help="Supplementary URL to include in the notification.",
    )
    parser.add_argument(
        "--url-title",
        help="Title for the supplementary URL.",
    )

    args = parser.parse_args()

    # Determine the message: CLI args take priority, fall back to stdin
    message = " ".join(args.message).strip()
    if not message and not sys.stdin.isatty():
        message = sys.stdin.read().strip()

    if not message:
        parser.error("No message provided. Pass a message as an argument or pipe one via stdin.")

    try:
        result = push_notification(
            message,
            token=args.token or "",
            user=args.user or "",
            title=args.title or "",
            priority=args.priority,
            device=args.device or "",
            sound=args.sound or "",
            url=args.url or "",
            url_title=args.url_title or "",
        )
        print(f"✅ Pushover notification sent. (request: {result.get('request')})")
    except RuntimeError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
