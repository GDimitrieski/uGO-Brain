from __future__ import annotations

import json
import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional, Tuple


def _now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="milliseconds")


def _parse_int_tuple(raw: Any, default: Tuple[int, ...]) -> Tuple[int, ...]:
    if raw is None:
        return default
    if isinstance(raw, (list, tuple)):
        out = []
        for item in raw:
            try:
                out.append(int(item))
            except Exception:
                continue
        return tuple(out) if out else default

    txt = str(raw).strip()
    if not txt:
        return default
    out = []
    for token in txt.split(","):
        token = token.strip()
        if not token:
            continue
        try:
            out.append(int(token))
        except Exception:
            continue
    return tuple(out) if out else default


@dataclass
class SimulatedRobotClient:
    """
    In-process task simulator used to run workflows without backend/uGO calls.
    """

    default_camera_positions: Tuple[int, ...] = (1, 2, 3, 4)
    default_sample_types: Tuple[int, ...] = (1, 2, 3, 4)
    default_barcode_prefix: str = "SIMBC"

    _tasks: Dict[str, Dict[str, Any]] = field(default_factory=dict, init=False)
    _barcode_counter: int = field(default=0, init=False)
    _sample_type_cursor: int = field(default=0, init=False)
    _error_ids: Dict[str, Dict[str, Any]] = field(default_factory=dict, init=False)

    def _camera_positions(self) -> Tuple[int, ...]:
        return _parse_int_tuple(os.getenv("UGO_SIM_CAMERA_POSITIONS"), self.default_camera_positions)

    def _sample_types(self) -> Tuple[int, ...]:
        return _parse_int_tuple(os.getenv("UGO_SIM_3FG_SAMPLE_TYPES"), self.default_sample_types)

    def _barcode_prefix(self) -> str:
        txt = str(os.getenv("UGO_SIM_BARCODE_PREFIX", self.default_barcode_prefix)).strip()
        return txt or self.default_barcode_prefix

    def _next_sample_type(self) -> int:
        sample_types = self._sample_types()
        idx = self._sample_type_cursor % len(sample_types)
        self._sample_type_cursor += 1
        value = int(sample_types[idx])
        if value < 1 or value > 4:
            return 1
        return value

    def _next_barcode(self) -> str:
        self._barcode_counter += 1
        return f"{self._barcode_prefix()}_{self._barcode_counter:04d}"

    def send_task(self, payload: Dict[str, Any]) -> str:
        task_id = str(uuid.uuid4())
        self._tasks[task_id] = dict(payload or {})
        return task_id

    def post_error(self, code: str, message: str, action: str) -> str:
        error_id = str(uuid.uuid4())
        self._error_ids[error_id] = {
            "code": str(code),
            "message": str(message),
            "action": str(action),
            "timestamp": _now_iso(),
        }
        return error_id

    def clear_error(self, error_id: str) -> None:
        self._error_ids.pop(str(error_id), None)

    def get_planner_state(self) -> Optional[int]:
        # Keep planner in a stable "running/ready" style value for local simulation.
        return 4

    def _simulated_outputs(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        task_name = str(payload.get("taskName", "")).strip()

        if task_name == "CameraInspect":
            positions = [int(x) for x in self._camera_positions()]
            return {
                "Positions": positions,
                "Results": json.dumps(positions),
            }

        if task_name == "ProcessAt3FingerStation":
            try:
                action = int(payload.get("ACTION", 3))
            except Exception:
                action = 3
            if action == 3:
                sample_type = self._next_sample_type()
                barcode = self._next_barcode()
                return {
                    "SampleType": str(sample_type),
                    "Barcode": barcode,
                    "Results": barcode,
                }
            return {"Results": "Success"}

        if task_name == "SingleDeviceAction":
            try:
                act = int(payload.get("ACT", 0))
            except Exception:
                act = 0
            if act == 30:
                return {"Position": "0", "Results": "p[0, 0, 0, 0, 0, 0]"}
            return {"Results": "Success"}

        if task_name == "SingleTask":
            obj_nbr = payload.get("OBJ_Nbr")
            return {
                "Position": str(obj_nbr if obj_nbr is not None else "0"),
                "Results": "Success",
            }

        if task_name in {"Navigate", "Charge"}:
            return {}

        return {"Results": "Success"}

    def wait_task(
        self,
        task_id: str,
        timeout_s: float = 120.0,
        poll_s: float = 1.0,
        allowed_planner_states: Optional[set[int]] = None,
        abort_planner_states: Optional[set[int]] = None,
        max_consecutive_none: int = 5,
    ) -> Dict[str, Any]:
        _ = timeout_s, poll_s, allowed_planner_states, abort_planner_states, max_consecutive_none
        payload = self._tasks.get(str(task_id))
        if payload is None:
            return {
                "status": "failed",
                "message": f"Unknown simulated task id '{task_id}'",
                "raw": {},
                "state_history": [],
            }

        outputs = self._simulated_outputs(payload)
        state_history = [
            {"timestamp": _now_iso(), "state": "PENDING"},
            {"timestamp": _now_iso(), "state": "EXECUTE"},
            {"timestamp": _now_iso(), "state": "COMPLETE"},
        ]
        raw = {
            "status": "OK",
            "data": {
                "id": str(task_id),
                "status": "COMPLETE",
                "error": "null",
                "outputs": outputs,
            },
        }
        return {
            "status": "succeeded",
            "message": "simulated",
            "raw": raw,
            "state_history": state_history,
        }
