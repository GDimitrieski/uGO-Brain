"""Drop-in replacement for UgoRobotClient that sends tasks directly to UR5e via TCP.

Same interface as UgoRobotClient: send_task(), wait_task(), post_error(), clear_error(),
post_prompt(), etc.  Error/prompt methods still go through the uGO HTTP backend so the
UI keeps working.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from engine.ur5e_tcp_server import Ur5eTcpServer


@dataclass
class Ur5eDirectClient:
    """Sends task commands via TCP to the UR5e and polls status over the same socket.

    Non-task operations (errors, prompts, planner state) are delegated to an
    optional ``http_fallback`` client (UgoRobotClient) so the web UI stays functional.
    """

    server: Ur5eTcpServer
    http_fallback: Any = None  # UgoRobotClient or None

    default_timeout_s: float = float(os.getenv("UGO_STEP_TIMEOUT_S", "300.0"))

    def send_task(self, payload: Dict[str, Any]) -> str:
        """Send task to UR5e over TCP. Returns msg_id."""
        if not self.server.connected:
            if not self.server.wait_for_connection(timeout=30.0):
                raise ConnectionError("UR5e not connected — cannot send task")
        return self.server.send_task(payload)

    def wait_task(
        self,
        task_id: str,
        timeout_s: float = 300.0,
        poll_s: float = 1.0,
        allowed_planner_states: Optional[set] = None,
        abort_planner_states: Optional[set] = None,
        max_consecutive_none: int = 5,
    ) -> Dict[str, Any]:
        """Wait for UR5e task completion via TCP status messages."""
        _ = poll_s, allowed_planner_states, abort_planner_states, max_consecutive_none
        return self.server.wait_task(task_id, timeout_s=timeout_s)

    # ---- HTTP fallback for non-task operations ----

    def post_error(self, code: str, message: str, action: str) -> str:
        if self.http_fallback:
            return self.http_fallback.post_error(code, message, action)
        print(f"[UR5e Direct] Error (no HTTP fallback): {code} {message} {action}")
        return ""

    def clear_error(self, error_id: str) -> None:
        if self.http_fallback:
            self.http_fallback.clear_error(error_id)

    def get_planner_state(self) -> Optional[int]:
        if self.http_fallback:
            return self.http_fallback.get_planner_state()
        return None

    def post_prompt(self, title: str, body: str, actions=None) -> Optional[str]:
        if self.http_fallback:
            return self.http_fallback.post_prompt(title, body, actions)
        return None

    def dismiss_prompt(self, prompt_id: str = None) -> None:
        if self.http_fallback:
            self.http_fallback.dismiss_prompt(prompt_id)

    def get_prompt(self, prompt_id: str):
        if self.http_fallback:
            return self.http_fallback.get_prompt(prompt_id)
        return None

    def wait_for_prompt_response(self, prompt_id: str, poll_interval: float = 2, timeout: float = 300) -> Optional[str]:
        if self.http_fallback:
            return self.http_fallback.wait_for_prompt_response(prompt_id, poll_interval, timeout)
        return None

    def prompt_and_wait(self, title: str, body: str, actions=None, timeout: float = 300) -> Optional[str]:
        if self.http_fallback:
            return self.http_fallback.prompt_and_wait(title, body, actions, timeout)
        return None
