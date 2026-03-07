from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum
from typing import Any, Dict, Optional

import requests


class PlannerEvent(IntEnum):
    RESET = 0
    START = 1
    STOP = 2
    SC = 3


class SystemState(IntEnum):
    STOPPED = 0
    EXECUTE = 1
    STOPPING = 2
    RESETTING = 3


@dataclass
class PlannerController:
    base_url: str
    token: str
    timeout_s: float = 10.0

    def _headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"}

    def get_state(self) -> Optional[SystemState]:
        try:
            response = requests.get(
                f"{self.base_url}/api/planner/state",
                headers=self._headers(),
                timeout=self.timeout_s,
            )
            response.raise_for_status()
            state_raw = response.json().get("data", {}).get("state")
            if state_raw is None:
                return None
            return SystemState(int(state_raw))
        except (requests.RequestException, ValueError):
            return None

    def post_state(self, state: SystemState) -> Optional[Dict[str, Any]]:
        payload = {"state": int(state)}
        try:
            response = requests.post(
                f"{self.base_url}/api/planner/state",
                headers=self._headers(),
                json=payload,
                timeout=self.timeout_s,
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException:
            return None

    def post_event(self, event: PlannerEvent) -> Optional[Dict[str, Any]]:
        payload = {"event": int(event)}
        try:
            response = requests.post(
                f"{self.base_url}/api/planner/event",
                headers=self._headers(),
                json=payload,
                timeout=self.timeout_s,
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException:
            return None
