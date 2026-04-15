"""Sender construction helpers for workflow runners."""

import os
import sys
from pathlib import Path
from typing import Optional, Union

from engine.command_layer import CommandSender, TaskCatalog
from engine.simulated_robot_client import SimulatedRobotClient
from engine.ugo_robot_client import UgoRobotClient
from Library.credentials import credentials
from Library.login import login

if os.environ.get("UGO_PROJECT_ROOT"):
    PROJECT_ROOT = Path(os.environ["UGO_PROJECT_ROOT"]).resolve()
elif getattr(sys, "frozen", False):
    PROJECT_ROOT = Path(sys.executable).resolve().parent.parent
else:
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


def _build_ur5e_direct_robot(http_fallback=None):
    """Create a Ur5eDirectClient backed by a TCP server.

    The TCP server starts listening and waits for the UR5e to connect.
    An optional http_fallback (UgoRobotClient) is used for prompts/errors.
    """
    from engine.ur5e_tcp_server import Ur5eTcpServer
    from engine.ur5e_direct_client import Ur5eDirectClient

    host = os.getenv("UGO_UR5E_TCP_HOST", "0.0.0.0")
    port = int(os.getenv("UGO_UR5E_TCP_PORT", "30001"))
    hb_interval = float(os.getenv("UGO_UR5E_HEARTBEAT_S", "3.0"))

    server = Ur5eTcpServer(host=host, port=port, heartbeat_interval_s=hb_interval)
    server.start()
    print(f"Sender mode: direct UR5e TCP on {host}:{port} (UGO_USE_DIRECT_UR5E=1)")
    return Ur5eDirectClient(server=server, http_fallback=http_fallback)


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
    use_direct_ur5e = _as_bool(os.getenv("UGO_USE_DIRECT_UR5E", None), default=False)

    catalog = TaskCatalog.from_file(str(task_catalog_path))

    if use_simulation:
        sender = CommandSender(robot=SimulatedRobotClient(), catalog=catalog)
        sender.max_attempts = int(max_attempts)
        sender.post_error_on_fail = False
        sender.clear_error_immediately = False
        print("Sender mode: simulated devices enabled (UGO_SIMULATE_DEVICES=1)")
        return sender

    # Always login for HTTP — needed for prompts/errors even in direct mode
    token = login(credentials["url"], credentials["user"], credentials["password"])
    if not token:
        raise RuntimeError("Login failed: received empty token")

    http_robot = UgoRobotClient(base_url=credentials["url"], token=token)

    if use_direct_ur5e:
        robot = _build_ur5e_direct_robot(http_fallback=http_robot)
    else:
        robot = http_robot
        print("Sender mode: uGO backend HTTP (default)")

    sender = CommandSender(robot=robot, catalog=catalog)
    sender.max_attempts = int(max_attempts)
    return sender


__all__ = ["build_sender", "DEFAULT_TASK_CATALOG"]
