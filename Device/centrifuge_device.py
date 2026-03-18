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


class CentrifugeAnalyzerDevice(AnalyzerDeviceRuntime):
    """Simple centrifuge runtime interface callable from workflow logic."""

    def __init__(
        self,
        *,
        identity: AnalyzerDeviceIdentity,
        capabilities: AnalyzerDeviceCapabilities,
        usage_profile: Optional[Any] = None,
        controller: Optional[Any] = None,
    ) -> None:
        super().__init__(identity=identity, capabilities=capabilities)
        self.usage_profile = usage_profile
        self.controller = controller
        self.hatch_open: bool = False
        self.rotor_spinning: bool = False
        self.rotor_step_index: int = 0
        self.last_action_act: int = 0

    def set_controller(self, controller: Optional[Any]) -> None:
        self.controller = controller

    def _sync_from_controller_status(self) -> None:
        if self.controller is None:
            return
        try:
            diag = self.controller.diagnose()
        except Exception:
            return

        remote_error = str(diag.get("error_code", "")).strip()
        remote_state = int(diag.get("state", 0) or 0)
        remote_hatch = int(diag.get("hatch_state", 0) or 0)
        remote_running = bool(diag.get("running", False))

        self.hatch_open = remote_hatch == 2
        self.rotor_spinning = bool(remote_running)

        if remote_error and remote_error != "00000000":
            self._fault_code = remote_error
            self._fault_message = f"Centrifuge XML-RPC fault code {remote_error}"
            self._state = PackMLState.FAULTED
        else:
            if self._fault_message.startswith("Centrifuge XML-RPC fault code"):
                self._fault_code = ""
                self._fault_message = ""
            if remote_state == 4:
                self._state = PackMLState.EXECUTE
            elif remote_state in {2, 3}:
                if self._state == PackMLState.EXECUTE:
                    self._state = PackMLState.COMPLETE
                elif self._state not in {PackMLState.IDLE, PackMLState.COMPLETE, PackMLState.STOPPED}:
                    self._state = PackMLState.STOPPED

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

    def move_rotor(self, rotor_slot_index: int = 0) -> bool:
        if self._fault_code or self.rotor_spinning:
            return False
        slot_index = int(rotor_slot_index)
        if slot_index <= 0:
            return False
        self.rotor_step_index = slot_index
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

    def apply_single_device_action(self, act: int, rotor_slot_index: int = 0) -> bool:
        self.last_action_act = int(act)

        # Hard switch for centrifuge actions: if XML-RPC controller exists, execute there.
        if self.controller is not None and int(act) in {
            DEVICE_ACTION_OPEN_HATCH,
            DEVICE_ACTION_CLOSE_HATCH,
            DEVICE_ACTION_START_CENTRIFUGE,
            DEVICE_ACTION_MOVE_ROTOR,
        }:
            try:
                if int(act) == DEVICE_ACTION_OPEN_HATCH:
                    self.controller.open_hatch()
                    self.hatch_open = True
                elif int(act) == DEVICE_ACTION_CLOSE_HATCH:
                    self.controller.close_hatch()
                    self.hatch_open = False
                elif int(act) == DEVICE_ACTION_START_CENTRIFUGE:
                    self.controller.start()
                    self.hatch_open = False
                    self.rotor_spinning = True
                    self._state = PackMLState.EXECUTE
                elif int(act) == DEVICE_ACTION_MOVE_ROTOR:
                    if int(rotor_slot_index) <= 0:
                        raise ValueError(
                            "Centrifuge MoveRotor requires rotor_slot_index >= 1 for XML-RPC control"
                        )
                    self.controller.move_rotor(int(rotor_slot_index))
                    self.rotor_step_index = int(rotor_slot_index)
                self._sync_from_controller_status()
                return True
            except Exception as exc:
                self._fault_code = "CENTRIFUGE_XMLRPC"
                self._fault_message = str(exc)
                self._state = PackMLState.FAULTED
                return False

        if int(act) == DEVICE_ACTION_OPEN_HATCH:
            return self.open_hatch()
        if int(act) == DEVICE_ACTION_START_CENTRIFUGE:
            return self.start_centrifuge()
        if int(act) == DEVICE_ACTION_CLOSE_HATCH:
            return self.close_hatch()
        if int(act) == DEVICE_ACTION_MOVE_ROTOR:
            return self.move_rotor(rotor_slot_index=int(rotor_slot_index))
        raise ValueError(f"Unsupported centrifuge SingleDeviceAction ACT '{act}'")

    def diagnose(self) -> Dict[str, Any]:
        self._sync_from_controller_status()
        payload = super().diagnose()
        payload.update(
            {
                "hatch_open": bool(self.hatch_open),
                "rotor_spinning": bool(self.rotor_spinning),
                "rotor_step_index": int(self.rotor_step_index),
                "last_action_act": int(self.last_action_act),
                "controller": type(self.controller).__name__ if self.controller is not None else "",
            }
        )
        return payload
