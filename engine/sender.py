"""Sender construction helpers for workflow runners."""

import os
from pathlib import Path
from typing import Optional, Union

from engine.command_layer import CommandSender, TaskCatalog
from engine.simulated_robot_client import SimulatedRobotClient
from engine.ugo_robot_client import UgoRobotClient
from Library.credentials import credentials
from Library.login import login

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_TASK_CATALOG = PROJECT_ROOT / "Available_Tasks.json"


def _as_bool(raw: Optional[str], default: bool = False) -> bool:
    if raw is None:
        return default
    txt = str(raw).strip().lower()
    if txt in {"1", "true", "yes", "on"}:
        return True
    if txt in {"0", "false", "no", "off"}:
        return False
    return default


def build_sender(
    task_catalog_path: Union[str, Path] = DEFAULT_TASK_CATALOG,
    max_attempts: int = 1,
    simulate: Optional[bool] = None,
) -> CommandSender:
    use_simulation = (
        bool(simulate)
        if simulate is not None
        else _as_bool(os.getenv("UGO_SIMULATE_DEVICES", None), default=False)
    )

    catalog = TaskCatalog.from_file(str(task_catalog_path))

    if use_simulation:
        sender = CommandSender(robot=SimulatedRobotClient(), catalog=catalog)
        sender.max_attempts = int(max_attempts)
        sender.post_error_on_fail = False
        sender.clear_error_immediately = False
        print("Sender mode: simulated devices enabled (UGO_SIMULATE_DEVICES=1)")
        return sender

    token = login(credentials["url"], credentials["user"], credentials["password"])
    if not token:
        raise RuntimeError("Login failed: received empty token")

    robot = UgoRobotClient(base_url=credentials["url"], token=token)
    sender = CommandSender(robot=robot, catalog=catalog)
    sender.max_attempts = int(max_attempts)
    return sender


__all__ = ["build_sender", "DEFAULT_TASK_CATALOG"]
