from __future__ import annotations

from typing import Any, Dict, Optional

from .analyzer_device import (
    AnalyzerDeviceCapabilities,
    AnalyzerDeviceIdentity,
    AnalyzerDeviceRuntime,
)
from .packml import PackMLCommand, PackMLState


DEVICE_ACTION_OPEN_HATCH = 1
DEVICE_ACTION_START_CENTRIFUGE = 2
DEVICE_ACTION_CLOSE_HATCH = 3
DEVICE_ACTION_MOVE_ROTOR = 4
DEVICE_ACTION_SCAN_LANDMARK = 30


class CentrifugeAnalyzerDevice(AnalyzerDeviceRuntime):
    """Simple centrifuge runtime interface callable from workflow logic."""

    def __init__(
        self,
        *,
        identity: AnalyzerDeviceIdentity,
        capabilities: AnalyzerDeviceCapabilities,
        usage_profile: Optional[Any] = None,
    ) -> None:
        super().__init__(identity=identity, capabilities=capabilities)
        self.usage_profile = usage_profile
        self.hatch_open: bool = False
        self.rotor_spinning: bool = False
        self.rotor_step_index: int = 0
        self.last_landmark_scan_ts: str = ""
        self.last_action_act: int = 0

    def scan_landmark(self) -> bool:
        if self._fault_code:
            return False
        self.last_landmark_scan_ts = self.get_status().timestamp
        return True

    def open_hatch(self) -> bool:
        if self._fault_code or self.rotor_spinning:
            return False
        self.hatch_open = True
        return True

    def close_hatch(self) -> bool:
        if self._fault_code or self.rotor_spinning:
            return False
        self.hatch_open = False
        return True

    def move_rotor(self) -> bool:
        if self._fault_code or self.rotor_spinning:
            return False
        self.rotor_step_index += 1
        return True

    def start_centrifuge(self) -> bool:
        if self._fault_code:
            return False
        if self.hatch_open:
            return False
        if not self._loaded_racks:
            return False
        if not self.transition(PackMLCommand.START):
            return False
        self._state = PackMLState.EXECUTE
        self.rotor_spinning = True
        return True

    def stop_centrifuge(self) -> bool:
        if self._fault_code:
            return False
        if self.rotor_spinning:
            self.rotor_spinning = False
        return self.transition(PackMLCommand.STOP)

    def complete_cycle(self) -> bool:
        if self._fault_code:
            return False
        if self._state != PackMLState.EXECUTE:
            return False
        self.rotor_spinning = False
        return self.transition(PackMLCommand.COMPLETE)

    def is_ready(self) -> bool:
        if self._fault_code:
            return False
        if self.rotor_spinning:
            return False
        return self._state != PackMLState.FAULTED

    def apply_single_device_action(self, act: int) -> bool:
        self.last_action_act = int(act)
        if int(act) == DEVICE_ACTION_OPEN_HATCH:
            return self.open_hatch()
        if int(act) == DEVICE_ACTION_START_CENTRIFUGE:
            return self.start_centrifuge()
        if int(act) == DEVICE_ACTION_CLOSE_HATCH:
            return self.close_hatch()
        if int(act) == DEVICE_ACTION_MOVE_ROTOR:
            return self.move_rotor()
        if int(act) == DEVICE_ACTION_SCAN_LANDMARK:
            return self.scan_landmark()
        raise ValueError(f"Unsupported centrifuge SingleDeviceAction ACT '{act}'")

    def diagnose(self) -> Dict[str, Any]:
        payload = super().diagnose()
        payload.update(
            {
                "hatch_open": bool(self.hatch_open),
                "rotor_spinning": bool(self.rotor_spinning),
                "rotor_step_index": int(self.rotor_step_index),
                "last_landmark_scan_ts": self.last_landmark_scan_ts,
                "last_action_act": int(self.last_action_act),
            }
        )
        return payload
