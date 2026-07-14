"""Shared SLURM job submission logic for HPC launcher scripts.

Provides a ``SlurmJobSpec`` dataclass and a ``submit()`` function used by
the per-binary launcher scripts in ``scripts/hpc/``.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path

SLURM_ACCOUNT = "bfdz-delta-gpu"


def _hours_to_slurm_time(total_hours: float) -> str:
    """Convert hours to SLURM time format HH:MM:SS with a 10% buffer."""
    buffered = total_hours * 1.1
    total_seconds = int(buffered * 3600)
    h = total_seconds // 3600
    m = (total_seconds % 3600) // 60
    s = total_seconds % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def _parse_toml(config_path: Path) -> dict[str, object]:
    """Read and parse a TOML config file."""
    try:
        with open(config_path, "rb") as f:
            return tomllib.load(f)
    except tomllib.TOMLDecodeError as e:
        print(f"Error: failed to parse TOML config '{config_path}': {e}", file=sys.stderr)
        sys.exit(1)


def _require_str(config: dict[str, object], key: str) -> str:
    value = config.get(key)
    if not isinstance(value, str):
        print(f"Error: '{key}' missing or not a string in config", file=sys.stderr)
        sys.exit(1)
    return value


def _require_positive_int(config: dict[str, object], key: str) -> int:
    value = config.get(key)
    if not isinstance(value, int) or value < 1:
        print(f"Error: '{key}' missing or not a positive integer in config", file=sys.stderr)
        sys.exit(1)
    return value


def _require_positive_number(config: dict[str, object], key: str) -> float:
    value = config.get(key)
    if not isinstance(value, (int, float)) or value <= 0:
        print(f"Error: '{key}' missing or not a positive number in config", file=sys.stderr)
        sys.exit(1)
    return float(value)


@dataclass(frozen=True)
class SlurmJobSpec:
    """Parameters that vary between different SLURM job launchers."""

    nickname_key: str
    """TOML key for the config nickname (e.g. ``"config_nickname"``)."""

    job_prefix: str
    """SLURM job name prefix (e.g. ``"orch_"``)."""

    slurm_script_name: str
    """.slurm filename relative to ``slurm/`` (e.g. ``"orchestrator.slurm"``)."""

    description: str
    """Short description for argparse."""

    repo_root: Path
    """Absolute path to the credit_assignment repository root."""


def submit(spec: SlurmJobSpec) -> int:
    """Parse CLI args, validate the TOML config, and submit a SLURM job.

    Returns the exit code (0 on success, non-zero on failure or if ``sbatch`` fails).
    """
    parser = argparse.ArgumentParser(description=spec.description)
    parser.add_argument(
        "-c", "--config-path",
        required=True,
        help="Path to the TOML config file",
    )
    parser.add_argument(
        "-j", "--job-name",
        default=None,
        help=f"SLURM job name (default: {spec.job_prefix}<model>_<nickname>)",
    )
    args = parser.parse_args()

    root = spec.repo_root

    # Resolve config path
    config_path = Path(args.config_path)
    if not config_path.is_absolute():
        config_path = root / config_path
    if not config_path.is_file():
        print(f"Error: config file not found: {config_path}", file=sys.stderr)
        return 1

    # Read and validate config
    config = _parse_toml(config_path)
    model_cli_name = _require_str(config, "model_cli_name")
    config_nickname = _require_str(config, spec.nickname_key)
    num_gpus = _require_positive_int(config, "num_gpus")
    total_time_limit_hours = _require_positive_number(config, "total_time_limit_hours")
    slurm_time = _hours_to_slurm_time(total_time_limit_hours)

    # Build job name
    job_name = args.job_name or f"{spec.job_prefix}{model_cli_name}_{config_nickname}"

    # Derive log prefix from job_prefix (strip trailing underscore)
    log_prefix = spec.job_prefix.rstrip("_")

    # Ensure SLURM log directory exists
    log_dir = root / "slurm" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    slurm_script = root / "slurm" / spec.slurm_script_name

    print("Submitting SLURM job:")
    print(f"  Config:       {config_path}")
    print(f"  Job name:     {job_name}")
    print(f"  GPUs:         {num_gpus}")
    print(f"  Time limit:   {slurm_time} (raw: {total_time_limit_hours}h + 10% buffer)")
    print(f"  Slurm script: {slurm_script}")

    notify_msg = f"{spec.job_prefix}{model_cli_name}_{config_nickname} finished running."

    cmd = [
        "sbatch",
        "--job-name", job_name,
        "--account", SLURM_ACCOUNT,
        "--output", f"slurm/logs/{log_prefix}_%j.out",
        "--error", f"slurm/logs/{log_prefix}_%j.err",
        "--gres", f"gpu:nvidia_a100:{num_gpus}",
        "--time", slurm_time,
        str(slurm_script),
        str(config_path),
        notify_msg,
    ]

    result = subprocess.run(cmd, cwd=str(root), check=False)
    return result.returncode
