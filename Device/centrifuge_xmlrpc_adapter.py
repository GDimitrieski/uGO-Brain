from __future__ import annotations

import json
import os
import time
import xmlrpc.client
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Sequence, Tuple


DEFAULT_CENTRIFUGE_RPC_URL = "http://192.168.137.104:50002"
DEFAULT_CENTRIFUGE_RPC_TIMEOUT_S = 50.0


class _TimeoutTransport(xmlrpc.client.Transport):
    def __init__(self, timeout_s: float) -> None:
        super().__init__()
        self._timeout_s = max(0.1, float(timeout_s))

    def make_connection(self, host: str) -> Any:  # pragma: no cover - exercised in integration
        conn = super().make_connection(host)
        try:
            conn.timeout = self._timeout_s
        except Exception:
            pass
        return conn


@dataclass(frozen=True)
class CentrifugeRpcStatus:
    state: int
    hatch_state: int
    lid_state: int
    key_state: int
    error_code: str


class CentrifugeXmlRpcAdapter:
    # Script-equivalent constants from "SCRIPT Centrifuge Actions V1.1.script"
    STATE_UNSTARTABLE = 1
    STATE_STOPPED = 2
    STATE_STOPPED_UNSTARTABLE = 3
    STATE_RUN_UP = 4

    KEY_LOCKED = 2
    LID_CLOSED = 0
    HATCH_UNDEFINED = 0
    HATCH_CLOSED = 1
    HATCH_OPENED = 2

    NO_ERROR = "00000000"
    POWER_ERROR = "88888888"

    def __init__(
        self,
        *,
        rpc_url: str = DEFAULT_CENTRIFUGE_RPC_URL,
        rpc_timeout_s: float = DEFAULT_CENTRIFUGE_RPC_TIMEOUT_S,
        state_wait_timeout_s: float = 60.0,
        start_wait_timeout_s: float = 60.0,
        inspect_attempts: int = 5,
        inspect_poll_s: float = 1.0,
        state_poll_s: float = 0.5,
        rotor_settle_s: float = 1.5,
    ) -> None:
        self.rpc_url = str(rpc_url).strip() or DEFAULT_CENTRIFUGE_RPC_URL
        self.rpc_timeout_s = max(0.1, float(rpc_timeout_s))
        self.state_wait_timeout_s = max(0.1, float(state_wait_timeout_s))
        self.start_wait_timeout_s = max(0.1, float(start_wait_timeout_s))
        self.inspect_attempts = max(1, int(inspect_attempts))
        self.inspect_poll_s = max(0.1, float(inspect_poll_s))
        self.state_poll_s = max(0.1, float(state_poll_s))
        self.rotor_settle_s = max(0.0, float(rotor_settle_s))
        self.log_enabled = str(os.getenv("UGO_CENTRIFUGE_XMLRPC_LOG", "1")).strip().lower() in {
            "1",
            "true",
            "yes",
        }
        log_default = "tracing/centrifuge_xmlrpc_commands.log"
        self.log_path = Path(str(os.getenv("UGO_CENTRIFUGE_XMLRPC_LOG_FILE", log_default))).resolve()

    @staticmethod
    def _now_iso() -> str:
        return datetime.now().astimezone().isoformat(timespec="milliseconds")

    def _append_log(self, payload: Dict[str, Any]) -> None:
        if not self.log_enabled:
            return
        try:
            self.log_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(payload, ensure_ascii=True) + "\n")
        except Exception:
            pass

    def _proxy(self) -> xmlrpc.client.ServerProxy:
        transport = _TimeoutTransport(timeout_s=self.rpc_timeout_s)
        return xmlrpc.client.ServerProxy(
            self.rpc_url,
            allow_none=True,
            use_builtin_types=True,
            transport=transport,
        )

    def _call(self, method: str, *args: Any) -> Any:
        self._append_log(
            {
                "timestamp": self._now_iso(),
                "phase": "request",
                "rpc_url": self.rpc_url,
                "method": str(method),
                "args": [repr(x) for x in args],
            }
        )
        proxy = self._proxy()
        try:
            func = getattr(proxy, method)
            result = func(*args)
            self._append_log(
                {
                    "timestamp": self._now_iso(),
                    "phase": "response",
                    "rpc_url": self.rpc_url,
                    "method": str(method),
                    "result": repr(result),
                }
            )
            return result
        except Exception as exc:
            self._append_log(
                {
                    "timestamp": self._now_iso(),
                    "phase": "error",
                    "rpc_url": self.rpc_url,
                    "method": str(method),
                    "error": str(exc),
                }
            )
            raise RuntimeError(f"Centrifuge XML-RPC '{method}' failed: {exc}") from exc

    @staticmethod
    def rotor_pose_for_slot(slot_index: int) -> int:
        idx = int(slot_index)
        if idx <= 0:
            raise ValueError(f"Invalid centrifuge slot index '{slot_index}'. Expected >= 1")
        # Script formula: 6*(slot-1)+1
        return int(6 * (idx - 1) + 1)

    def get_status(self) -> CentrifugeRpcStatus:
        return CentrifugeRpcStatus(
            state=int(self._call("state")),
            hatch_state=int(self._call("hatchstate")),
            lid_state=int(self._call("lidstate")),
            key_state=int(self._call("keystate")),
            error_code=str(self._call("geterror")),
        )

    def diagnose(self) -> Dict[str, Any]:
        status = self.get_status()
        return {
            "rpc_url": self.rpc_url,
            "state": int(status.state),
            "hatch_state": int(status.hatch_state),
            "lid_state": int(status.lid_state),
            "key_state": int(status.key_state),
            "error_code": str(status.error_code),
            "hatch_open": bool(int(status.hatch_state) == self.HATCH_OPENED),
            "running": bool(int(status.state) == self.STATE_RUN_UP),
        }

    def _assert_device_ok(self) -> None:
        status = self.get_status()
        if str(status.error_code) == self.POWER_ERROR:
            raise RuntimeError("Centrifuge reports power error (88888888)")
        if str(status.error_code) != self.NO_ERROR:
            raise RuntimeError(f"Centrifuge reports error code '{status.error_code}'")
        if int(status.key_state) != self.KEY_LOCKED:
            raise RuntimeError(f"Centrifuge key state invalid: {status.key_state} (expected LOCKED=2)")
        if int(status.lid_state) != self.LID_CLOSED:
            raise RuntimeError(f"Centrifuge lid is open (lid_state={status.lid_state})")

    def _wait_for_state(self, target_states: Sequence[int], timeout_s: Optional[float] = None) -> int:
        expected = {int(x) for x in target_states}
        wait_s = self.state_wait_timeout_s if timeout_s is None else max(0.1, float(timeout_s))
        started = time.time()
        last_state = -1
        while (time.time() - started) < wait_s:
            last_state = int(self._call("state"))
            if last_state in expected:
                return last_state
            time.sleep(self.state_poll_s)
        raise RuntimeError(
            f"Centrifuge state wait timeout after {wait_s:.1f}s; "
            f"expected={sorted(expected)}, last_state={last_state}"
        )

    def inspect_position(self, slot_index: int) -> bool:
        pose = self.rotor_pose_for_slot(slot_index)
        reached = bool(self._call("posreached", pose))
        if reached:
            return True
        for _ in range(self.inspect_attempts):
            time.sleep(self.inspect_poll_s)
            reached = bool(self._call("posreached", pose))
            if reached:
                return True
        return False

    def open_hatch(self) -> None:
        self._assert_device_ok()
        hatch_state = int(self._call("hatchstate"))
        if hatch_state == self.HATCH_OPENED:
            return
        state = int(self._call("state"))
        if state not in {self.STATE_STOPPED, self.STATE_STOPPED_UNSTARTABLE}:
            self._wait_for_state((self.STATE_STOPPED,), timeout_s=self.state_wait_timeout_s)
        result = int(self._call("move_hatch", 2))
        if result == -1:
            raise RuntimeError("Centrifuge rejected hatch open command")

    def close_hatch(self) -> None:
        self._assert_device_ok()
        result = int(self._call("move_hatch", 1))
        if result == -1:
            raise RuntimeError("Centrifuge rejected hatch close command")

    def move_rotor(self, slot_index: int) -> None:
        self._assert_device_ok()
        pose = self.rotor_pose_for_slot(slot_index)
        _ = self._call("move_rotor", pose)
        if self.rotor_settle_s > 0:
            time.sleep(self.rotor_settle_s)
        if not self.inspect_position(slot_index):
            raise RuntimeError(f"Centrifuge rotor did not reach requested slot {slot_index}")

    def start(self) -> None:
        self._assert_device_ok()
        _ = self._call("start")
        self._wait_for_state((self.STATE_RUN_UP,), timeout_s=self.start_wait_timeout_s)
