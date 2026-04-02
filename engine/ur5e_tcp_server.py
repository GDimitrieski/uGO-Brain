"""TCP server that accepts the UR5e socket connection and exchanges DKVM messages.

The UR5e is the *client* — it connects to this server.  The planner
runs this server in a background thread and pushes CMD messages;
the UR5e sends back ACK / STS messages.
"""

from __future__ import annotations

import socket
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from engine.ur5e_protocol import (
    PROTOCOL_VERSION,
    DKVMMessage,
    MessageType,
    build_cmd,
    build_hb,
    generate_msg_id,
)


def _now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="milliseconds")


@dataclass
class PendingTask:
    msg_id: str
    payload: Dict[str, Any]
    sent_at: str = ""
    state: str = "QUEUED"           # QUEUED -> SENT -> RECEIVED -> EXECUTING -> terminal
    state_history: List[Dict[str, str]] = field(default_factory=list)
    result: Dict[str, str] = field(default_factory=dict)
    completed: threading.Event = field(default_factory=threading.Event)

    def update_state(self, new_state: str, **extra: str) -> None:
        self.state = new_state
        self.state_history.append({"timestamp": _now_iso(), "state": new_state})
        self.result.update(extra)
        if new_state in {"COMPLETE", "ERROR", "ABORTED", "STOPPED"}:
            self.completed.set()


class Ur5eTcpServer:
    """Single-client TCP server for the UR5e connection."""

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 30001,
        heartbeat_interval_s: float = 3.0,
        on_connect: Optional[Callable[[], None]] = None,
        on_disconnect: Optional[Callable[[], None]] = None,
    ) -> None:
        self.host = host
        self.port = port
        self.heartbeat_interval_s = heartbeat_interval_s
        self.on_connect = on_connect
        self.on_disconnect = on_disconnect

        self._server_socket: Optional[socket.socket] = None
        self._client_socket: Optional[socket.socket] = None
        self._client_addr: Optional[tuple] = None
        self._connected = threading.Event()
        self._shutdown = threading.Event()
        self._lock = threading.Lock()
        self._recv_buffer = ""

        # Pending tasks keyed by msg_id
        self._tasks: Dict[str, PendingTask] = {}
        self._send_queue: List[str] = []  # raw encoded messages to send

        self._threads: List[threading.Thread] = []

    @property
    def connected(self) -> bool:
        return self._connected.is_set()

    def start(self) -> None:
        """Start the server in background threads."""
        self._shutdown.clear()
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_socket.settimeout(2.0)
        self._server_socket.bind((self.host, self.port))
        self._server_socket.listen(1)
        print(f"[UR5e TCP] Server listening on {self.host}:{self.port}")

        t_accept = threading.Thread(target=self._accept_loop, daemon=True, name="ur5e-accept")
        t_recv = threading.Thread(target=self._recv_loop, daemon=True, name="ur5e-recv")
        t_send = threading.Thread(target=self._send_loop, daemon=True, name="ur5e-send")
        t_hb = threading.Thread(target=self._heartbeat_loop, daemon=True, name="ur5e-hb")
        self._threads = [t_accept, t_recv, t_send, t_hb]
        for t in self._threads:
            t.start()

    def stop(self) -> None:
        """Shutdown the server and release resources."""
        self._shutdown.set()
        self._close_client()
        if self._server_socket:
            try:
                self._server_socket.close()
            except Exception:
                pass
        for t in self._threads:
            t.join(timeout=5.0)

    def wait_for_connection(self, timeout: float = 30.0) -> bool:
        return self._connected.wait(timeout=timeout)

    def send_task(self, payload: Dict[str, Any]) -> str:
        """Queue a CMD message. Returns msg_id for tracking."""
        msg_id = generate_msg_id()
        task = PendingTask(msg_id=msg_id, payload=dict(payload))
        self._tasks[msg_id] = task
        cmd = build_cmd(payload, msg_id=msg_id)
        with self._lock:
            self._send_queue.append(cmd.encode())
        task.update_state("QUEUED")
        return msg_id

    def wait_task(self, msg_id: str, timeout_s: float = 300.0) -> Dict[str, Any]:
        """Block until the task reaches a terminal state. Returns result dict."""
        task = self._tasks.get(msg_id)
        if task is None:
            return {
                "status": "failed",
                "message": f"Unknown task id '{msg_id}'",
                "raw": {},
                "state_history": [],
            }
        completed = task.completed.wait(timeout=timeout_s)
        if not completed:
            return {
                "status": "failed",
                "message": f"Timeout after {timeout_s}s waiting for task {msg_id}",
                "raw": task.result,
                "state_history": task.state_history,
            }
        terminal = task.state.upper()
        status = "succeeded" if terminal == "COMPLETE" else "failed"
        return {
            "status": status,
            "message": task.result.get("error", "") if status == "failed" else "",
            "raw": {
                "status": "OK" if status == "succeeded" else "ERROR",
                "data": {
                    "id": msg_id,
                    "status": terminal,
                    "outputs": dict(task.result),
                },
            },
            "state_history": task.state_history,
        }

    def get_task(self, msg_id: str) -> Optional[PendingTask]:
        return self._tasks.get(msg_id)

    # ---- internal loops ----

    def _accept_loop(self) -> None:
        while not self._shutdown.is_set():
            if self._connected.is_set():
                time.sleep(0.5)
                continue
            try:
                client, addr = self._server_socket.accept()
                with self._lock:
                    self._client_socket = client
                    self._client_addr = addr
                    self._recv_buffer = ""
                self._connected.set()
                print(f"[UR5e TCP] Client connected from {addr}")
                if self.on_connect:
                    self.on_connect()
            except socket.timeout:
                continue
            except OSError:
                if self._shutdown.is_set():
                    break
                continue

    def _close_client(self) -> None:
        with self._lock:
            if self._client_socket:
                try:
                    self._client_socket.close()
                except Exception:
                    pass
                self._client_socket = None
            self._connected.clear()
            self._recv_buffer = ""
        addr = self._client_addr
        self._client_addr = None
        if addr:
            print(f"[UR5e TCP] Client disconnected: {addr}")
            if self.on_disconnect:
                self.on_disconnect()

    def _recv_loop(self) -> None:
        while not self._shutdown.is_set():
            if not self._connected.is_set():
                time.sleep(0.1)
                continue
            try:
                with self._lock:
                    sock = self._client_socket
                if sock is None:
                    time.sleep(0.1)
                    continue
                sock.settimeout(1.0)
                data = sock.recv(2048)
                if not data:
                    self._close_client()
                    continue
                self._recv_buffer += data.decode("utf-8", errors="replace")
                while "\n" in self._recv_buffer:
                    line, self._recv_buffer = self._recv_buffer.split("\n", 1)
                    line = line.strip()
                    if not line:
                        continue
                    self._handle_incoming(line + "\n")
            except socket.timeout:
                continue
            except (ConnectionResetError, BrokenPipeError, OSError):
                self._close_client()
                continue

    def _send_loop(self) -> None:
        while not self._shutdown.is_set():
            if not self._connected.is_set() or not self._send_queue:
                time.sleep(0.05)
                continue
            with self._lock:
                if not self._send_queue:
                    continue
                raw = self._send_queue.pop(0)
                sock = self._client_socket
            if sock is None:
                continue
            try:
                sock.sendall(raw.encode("utf-8"))
                # Mark task as SENT
                try:
                    msg = DKVMMessage.decode(raw)
                    task = self._tasks.get(msg.msg_id)
                    if task and task.state == "QUEUED":
                        task.update_state("SENT")
                        task.sent_at = _now_iso()
                except Exception:
                    pass
            except (ConnectionResetError, BrokenPipeError, OSError):
                self._close_client()

    def _heartbeat_loop(self) -> None:
        while not self._shutdown.is_set():
            time.sleep(self.heartbeat_interval_s)
            if not self._connected.is_set():
                continue
            hb = build_hb(server_ts=_now_iso())
            with self._lock:
                self._send_queue.append(hb.encode())

    def _handle_incoming(self, raw: str) -> None:
        try:
            msg = DKVMMessage.decode(raw)
        except Exception as exc:
            print(f"[UR5e TCP] Failed to decode message: {exc} raw={raw!r}")
            return

        if msg.msg_type == MessageType.ACK:
            task = self._tasks.get(msg.msg_id)
            if task:
                state = msg.payload.get("state", "RECEIVED")
                task.update_state(state)
            return

        if msg.msg_type == MessageType.STS:
            task = self._tasks.get(msg.msg_id)
            if task:
                state = msg.payload.get("state", "UNKNOWN")
                extra = {k: v for k, v in msg.payload.items() if k != "state"}
                task.update_state(state, **extra)
            return

        if msg.msg_type == MessageType.HB:
            # Heartbeat from UR5e — could log ulm_state etc.
            ulm_state = msg.payload.get("ulm_state", "")
            if ulm_state:
                pass  # available for monitoring
            return

        print(f"[UR5e TCP] Unknown message type: {msg.msg_type}")
