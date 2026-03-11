from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from engine.ugo_robot_client import UgoRobotClient


@dataclass(frozen=True)
class TaskCatalog:
    """
    Expects Available_Tasks.json like:

    {
      "Available_Tasks": {
        "Navigate": {
          "payload_template": {"Navigate": {"taskName":"Navigate","AMR_PosTarget":"{AMR_PosTarget}",...}},
          "parameters": {"AMR_PosTarget":{"type":"string","default":"1"}, ...},
          "required": ["AMR_PosTarget"]
        },
        "Pick": {...},
        "Place": {...}
      }
    }
    """
    raw: Dict[str, Any]

    @classmethod
    def from_file(cls, path: str) -> "TaskCatalog":
        with open(path, "r", encoding="utf-8") as f:
            return cls(json.load(f))

    def _tasks(self) -> Dict[str, Any]:
        tasks = self.raw.get("Available_Tasks")
        if not isinstance(tasks, dict):
            raise ValueError("Available_Tasks.json must contain top-level key 'Available_Tasks' as a dict")
        return tasks

    def get_receiver(self, task_key: str) -> str:
        task_def = self._tasks().get(task_key)
        if not isinstance(task_def, dict):
            raise ValueError(f"Unknown task '{task_key}'")

        receiver = str(task_def.get("receiver", "")).strip().upper()
        if receiver in {"AMR", "ARM", "WRIST_CAMERA", "3FG", "ULM"}:
            return receiver

        # Backward-compatible inference if "receiver" is missing in catalog.
        inferred = {
            "Navigate": "AMR",
            "Dock": "AMR",
            "Charge": "AMR",
            "Pick": "ARM",
            "Place": "ARM",
            "MoveTo": "ARM",
            "SingleTask": "ARM",
            "CameraInspect": "WRIST_CAMERA",
            "InspectRackAtStation": "WRIST_CAMERA",
            "LandmarkScan": "WRIST_CAMERA",
            "ColorDetection": "WRIST_CAMERA",
        }.get(task_key, "UNKNOWN")
        return inferred

    def dispatch_path(self, task_key: str) -> List[str]:
        receiver = self.get_receiver(task_key)
        return ["uGO_CONTROLLER", "UR5E_POLYSCOPE", receiver]

    def build_payload(
        self,
        task_key: str,
        overrides: Optional[Dict[str, Any]] = None,
        include_meta: bool = False
    ) -> Dict[str, Any]:
        tasks = self._tasks()

        if task_key not in tasks:
            raise ValueError(f"Unknown task '{task_key}'. Allowed: {list(tasks.keys())}")

        task_def = tasks[task_key]
        template = task_def.get("payload_template")
        if not isinstance(template, dict):
            raise ValueError(f"Task '{task_key}' is missing payload_template")

        params_def = task_def.get("parameters", {})
        required = task_def.get("required", [])

        # 1) build final parameter values: defaults + overrides
        values: Dict[str, Any] = {}
        for p, meta in params_def.items():
            if isinstance(meta, dict) and "default" in meta:
                values[p] = meta["default"]

        if overrides:
            values.update(overrides)

        # 2) required check
        for r in required:
            if r not in values or values[r] is None:
                raise ValueError(f"Task '{task_key}' missing required param '{r}'")

        # 3) deep-copy template and substitute placeholders like "{AMR_PosTarget}"
        rendered = json.loads(json.dumps(template))

        def substitute(obj: Any) -> Any:
            if isinstance(obj, dict):
                return {k: substitute(v) for k, v in obj.items()}
            if isinstance(obj, list):
                return [substitute(x) for x in obj]
            if isinstance(obj, str):
                s = obj
                for k, v in values.items():
                    s = s.replace("{" + k + "}", str(v))
                return s
            return obj

        payload = substitute(rendered)

        # 4) Validate final payload structure required by API: FLAT dict with taskName
        if not isinstance(payload, dict) or "taskName" not in payload:
            raise ValueError(
                f"Rendered payload for '{task_key}' must be a dict containing 'taskName', got: {payload}"
            )

        # 5) Cast types according to parameters.type (important: keep ints as ints)
        for p, meta in params_def.items():
            if not isinstance(meta, dict):
                continue
            if p not in payload:
                continue

            t = meta.get("type")
            if t == "integer":
                try:
                    payload[p] = int(payload[p])
                except Exception:
                    raise TypeError(f"Param '{p}' must be integer, got value: {payload[p]}")
            elif t == "string":
                payload[p] = str(payload[p])

            enum_values = meta.get("enum")
            if isinstance(enum_values, list) and enum_values:
                allowed: List[Any]
                if t == "integer":
                    try:
                        allowed = [int(v) for v in enum_values]
                    except Exception:
                        allowed = list(enum_values)
                elif t == "string":
                    allowed = [str(v) for v in enum_values]
                else:
                    allowed = list(enum_values)

                if payload[p] not in allowed:
                    raise ValueError(
                        f"Param '{p}' for task '{task_key}' must be one of {allowed}, got: {payload[p]}"
                    )

        # Optional meta (ONLY if controller accepts unknown fields)
        if include_meta:
            payload["_meta"] = {
                "task_key": task_key,
                "command_id": str(uuid.uuid4()),
                "created_ts": time.time(),
                "params": values,
            }

        return payload


@dataclass
class CommandSender:
    robot: UgoRobotClient
    catalog: TaskCatalog

    default_timeout_s: float = 120.0
    poll_s: float = 1.0
    max_attempts: int = 3

    post_error_on_fail: bool = True
    clear_error_immediately: bool = True

    def run(
        self,
        task_key: str,
        overrides: Optional[Dict[str, Any]] = None,
        timeout_s: Optional[float] = None,
        task_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        task_name = task_name or task_key
        last: Dict[str, Any] = {}
        try:
            receiver = self.catalog.get_receiver(task_key)
            dispatch_path = self.catalog.dispatch_path(task_key)
        except Exception as exc:
            return {
                "task_id": None,
                "task_key": task_key,
                "receiver": "",
                "dispatch_path": [],
                "status": "failed",
                "message": f"{task_name} validation failed before dispatch: {exc}",
                "state_history": [],
                "raw": {},
            }

        for attempt in range(1, self.max_attempts + 1):
            try:
                payload = self.catalog.build_payload(task_key, overrides=overrides)
            except Exception as exc:
                return {
                    "task_id": None,
                    "task_key": task_key,
                    "receiver": receiver,
                    "dispatch_path": dispatch_path,
                    "status": "failed",
                    "message": f"{task_name} validation failed before dispatch: {exc}",
                    "state_history": [],
                    "raw": {},
                }

            task_id = self.robot.send_task(payload)
            result = self.robot.wait_task(
                task_id,
                timeout_s=timeout_s or self.default_timeout_s,
                poll_s=self.poll_s,
            )

            last = {
                "task_id": task_id,
                "task_key": task_key,
                "receiver": receiver,
                "dispatch_path": dispatch_path,
                **result,
            }

            if result.get("status") == "succeeded":
                return last

            # Timeout often means controller is still processing; do not auto-resubmit.
            msg = str(result.get("message", ""))
            if "timeout" in msg.lower():
                if self.post_error_on_fail:
                    err_id = self.robot.post_error(
                        code="STEP_TIMEOUT",
                        message=f"{task_name} timed out on attempt {attempt}/{self.max_attempts}",
                        action="ABORT",
                    )
                    if self.clear_error_immediately:
                        self.robot.clear_error(err_id)
                return last

            if self.post_error_on_fail:
                err_id = self.robot.post_error(
                    code="STEP_FAILED",
                    message=f"{task_name} failed attempt {attempt}/{self.max_attempts}",
                    action="RETRY" if attempt < self.max_attempts else "ABORT",
                )
                if self.clear_error_immediately:
                    self.robot.clear_error(err_id)

        return last
