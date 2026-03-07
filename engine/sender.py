"""Sender construction helpers for workflow runners."""

from pathlib import Path
from typing import Union

from engine.command_layer import CommandSender, TaskCatalog
from engine.ugo_robot_client import UgoRobotClient
from Library.credentials import credentials
from Library.login import login

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_TASK_CATALOG = PROJECT_ROOT / "Available_Tasks.json"


def build_sender(
    task_catalog_path: Union[str, Path] = DEFAULT_TASK_CATALOG,
    max_attempts: int = 1,
) -> CommandSender:
    token = login(credentials["url"], credentials["user"], credentials["password"])
    if not token:
        raise RuntimeError("Login failed: received empty token")

    robot = UgoRobotClient(base_url=credentials["url"], token=token)
    catalog = TaskCatalog.from_file(str(task_catalog_path))
    sender = CommandSender(robot=robot, catalog=catalog)
    sender.max_attempts = int(max_attempts)
    return sender


__all__ = ["build_sender", "DEFAULT_TASK_CATALOG"]
