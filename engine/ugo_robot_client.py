from __future__ import annotations

import time
import requests
from datetime import datetime
from dataclasses import dataclass
from typing import Any, Dict, Optional

from Library.workflow_post_task_send import task_post_send
from Library.workflow_get_request_status import get_request_status
from Library.error_post_planner import post_planner_error
from Library.error_post_planner_clear import clear_planner_error
from Library.post_planner_message import (
    post_planner_prompt,
    dismiss_planner_prompt,
    get_prompt_response,
    wait_for_user_response,
)


@dataclass
class UgoRobotClient:
    base_url: str
    token: str

    def _headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"}

    # ---- Core transport ----
    def send_task(self, payload: Dict[str, Any]) -> str:
        return task_post_send(self.base_url, self.token, payload)

    def post_error(self, code: str, message: str, action: str) -> str:
        return post_planner_error(self.base_url, self.token, code, message, action)

    def clear_error(self, error_id: str) -> None:
        clear_planner_error(self.base_url, self.token, error_id)

    # ---- Control-system prompts ----
    def post_prompt(self, title: str, body: str, actions=None) -> Optional[str]:
        return post_planner_prompt(self.base_url, self.token, title, body, actions)

    def dismiss_prompt(self, prompt_id: str = None) -> None:
        dismiss_planner_prompt(self.base_url, self.token, prompt_id)

    def get_prompt(self, prompt_id: str):
        return get_prompt_response(self.base_url, self.token, prompt_id)

    def wait_for_prompt_response(self, prompt_id: str, poll_interval: float = 2, timeout: float = 300) -> Optional[str]:
        return wait_for_user_response(self.base_url, self.token, prompt_id, poll_interval, timeout)

    def prompt_and_wait(self, title: str, body: str, actions=None, timeout: float = 300) -> Optional[str]:
        """Post a prompt and block until the user responds. Returns chosen actionId or None."""
        prompt_id = self.post_prompt(title, body, actions)
        if not prompt_id:
            return None
        return self.wait_for_prompt_response(prompt_id, timeout=timeout)

    # ---- New: planner state ----
    def get_planner_state(self) -> Optional[int]:
        """
        Returns integer state from /api/planner/state, e.g. 4.
        """
        try:
            r = requests.get(f"{self.base_url}/api/planner/state", headers=self._headers(), timeout=10)
            r.raise_for_status()
            js = r.json()
            print(js)
            print(js.get("data", {}).get("state"))
            return int(js.get("data", {}).get("state"))
        except Exception:
            return None
        
        

    def wait_task(
        self,
        task_id: str,
        timeout_s: float = 120.0,
        poll_s: float = 1.0,
        allowed_planner_states: Optional[set[int]] = None,
        abort_planner_states: Optional[set[int]] = None,
        max_consecutive_none: int = 5,
    ) -> Dict[str, Any]:
        """
        Wait for task result using task state only (PackML task state).
        Safe against get_request_status returning None.

        Returns:
        {"status":"succeeded"|"failed", "raw":..., "message":...}
        """
        start = time.time()
        last_status: Optional[Dict[str, Any]] = None

        none_count = 0
        unknown_count = 0
        state_history: list[dict[str, str]] = []
        last_seen_state: Optional[str] = None

        while (time.time() - start) < timeout_s:
            # Poll task status only. Planner state is managed separately.
            status = get_request_status(self.base_url, self.token, task_id)

            # If API call failed and returned None, tolerate a few times
            if status is None:
                none_count += 1
                if none_count >= max_consecutive_none:
                    return {
                        "status": "failed",
                        "message": f"get_request_status returned None {none_count} times in a row",
                        "raw": last_status,
                    }
                time.sleep(poll_s)
                continue

            none_count = 0

            # Ensure dict
            if not isinstance(status, dict):
                # treat unexpected payload as transient
                last_status = {"unexpected_status_type": str(type(status)), "value": str(status)}
                time.sleep(poll_s)
                continue

            last_status = status

            # ---- Extract status string robustly (adapt if needed) ----
            data = status.get("data", {}) if isinstance(status.get("data", {}), dict) else {}
            # Prefer task-level fields from data over top-level response metadata.
            st_val = (
                data.get("taskState")
                or data.get("task_state")
                or data.get("executionState")
                or data.get("packmlState")
                or data.get("state")
                or data.get("status")
                or status.get("taskState")
                or status.get("task_state")
                or status.get("executionState")
                or status.get("packmlState")
                or status.get("state")
                or status.get("status")
                or ""
            )
            st = str(st_val).upper()
            alias_map = {
                "EXECUTING": "EXECUTE",
                "COMPLETED": "COMPLETE",
                "SUSPEND": "SUSPENDED",
                "UNSUSPEND": "UNSUSPENDING",
            }
            st = alias_map.get(st, st)
            if st:
                # Record state transitions with timestamp for trace export.
                if st != last_seen_state:
                    state_history.append(
                        {
                            "timestamp": datetime.now().astimezone().isoformat(timespec="milliseconds"),
                            "state": st,
                        }
                    )
                    last_seen_state = st
            # ---------------------------------------------------------
            success_states = {"COMPLETE"}
            failure_states = {"ABORTED", "STOPPED"}
            running_states = {
                "PENDING",
                "STARTING",
                "IDLE",
                "SUSPENDED",
                "STOPPING",
                "EXECUTE",
                "EXECUTING",
                "HELD",
                "HOLDING",
                "UNHOLDING",
                "SUSPENDING",
                "UNSUSPENDING",
                "CLEARING",
                "RESETTING",
                "COMPLETING",
                "ABORTING",
            }

            if st in success_states:
                return {"status": "succeeded", "raw": status, "state_history": state_history}

            if st in failure_states:
                return {
                    "status": "failed",
                    "message": str(status),
                    "raw": status,
                    "state_history": state_history,
                }

            if st not in running_states:
                unknown_count += 1
                if unknown_count >= 3:
                    return {
                        "status": "failed",
                        "message": f"Unknown task state '{st}'",
                        "raw": status,
                        "state_history": state_history,
                    }
            else:
                unknown_count = 0

            time.sleep(poll_s)

        return {
            "status": "failed",
            "message": f"timeout after {timeout_s}s",
            "raw": last_status,
            "state_history": state_history,
        }
