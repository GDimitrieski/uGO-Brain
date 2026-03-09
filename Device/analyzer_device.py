from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Tuple

from .packml import PackMLCommand, PackMLMode, PackMLState, next_state, parse_command, parse_mode


def _now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="milliseconds")


@dataclass(frozen=True)
class AnalyzerDeviceIdentity:
    device_id: str
    name: str
    station_id: str
    model: str = ""


@dataclass(frozen=True)
class AnalyzerDeviceCapabilities:
    supported_processes: Tuple[str, ...]
    supported_rack_types: Tuple[str, ...]
    max_racks: int
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class AnalyzerDeviceStatus:
    timestamp: str
    mode: PackMLMode
    state: PackMLState
    fault_code: str
    fault_message: str
    loaded_rack_ids: Tuple[str, ...]


class AnalyzerDeviceRuntime:
    """Minimal runtime contract for analyzer-like devices."""

    def __init__(
        self,
        *,
        identity: AnalyzerDeviceIdentity,
        capabilities: AnalyzerDeviceCapabilities,
    ) -> None:
        max_racks = int(capabilities.max_racks)
        if max_racks <= 0:
            raise ValueError("AnalyzerDeviceCapabilities.max_racks must be > 0")

        self.identity = identity
        self.capabilities = AnalyzerDeviceCapabilities(
            supported_processes=tuple(str(x).strip().upper() for x in capabilities.supported_processes if str(x).strip()),
            supported_rack_types=tuple(str(x).strip().upper() for x in capabilities.supported_rack_types if str(x).strip()),
            max_racks=max_racks,
            metadata=dict(capabilities.metadata),
        )
        self._mode: PackMLMode = PackMLMode.AUTOMATIC
        self._state: PackMLState = PackMLState.IDLE
        self._fault_code: str = ""
        self._fault_message: str = ""
        self._loaded_racks: Dict[str, str] = {}

    def set_mode(self, mode: PackMLMode | str) -> None:
        self._mode = parse_mode(mode)

    def supports_process(self, process_name: str) -> bool:
        return str(process_name).strip().upper() in self.capabilities.supported_processes

    def supports_rack_type(self, rack_type: str) -> bool:
        return str(rack_type).strip().upper() in self.capabilities.supported_rack_types

    def can_accept_rack(self, rack_id: str, rack_type: str) -> bool:
        if self._fault_code:
            return False
        rack_key = str(rack_id).strip()
        rack_type_key = str(rack_type).strip().upper()
        if not rack_key:
            return False
        if rack_key in self._loaded_racks:
            return True
        if not self.supports_rack_type(rack_type_key):
            return False
        return len(self._loaded_racks) < self.capabilities.max_racks

    def load_rack(self, rack_id: str, rack_type: str) -> bool:
        if not self.can_accept_rack(rack_id, rack_type):
            return False
        rack_key = str(rack_id).strip()
        rack_type_key = str(rack_type).strip().upper()
        self._loaded_racks[rack_key] = rack_type_key
        return True

    def unload_rack(self, rack_id: str) -> bool:
        rack_key = str(rack_id).strip()
        if rack_key not in self._loaded_racks:
            return False
        self._loaded_racks.pop(rack_key, None)
        if not self._loaded_racks and self._state in {PackMLState.COMPLETE, PackMLState.STOPPED}:
            self._state = PackMLState.IDLE
        return True

    def transition(self, command: PackMLCommand | str) -> bool:
        cmd = parse_command(command)
        nxt = next_state(self._state, cmd)
        if nxt is None:
            return False
        self._state = nxt
        if self._state == PackMLState.IDLE:
            self._fault_code = ""
            self._fault_message = ""
        return True

    def set_fault(self, code: str, message: str) -> None:
        self._fault_code = str(code).strip() or "UNKNOWN_FAULT"
        self._fault_message = str(message or "")
        self._state = PackMLState.FAULTED

    def clear_fault(self) -> bool:
        if self._state == PackMLState.FAULTED:
            if not self.transition(PackMLCommand.RESET):
                return False
        self._fault_code = ""
        self._fault_message = ""
        return True

    def get_status(self) -> AnalyzerDeviceStatus:
        return AnalyzerDeviceStatus(
            timestamp=_now_iso(),
            mode=self._mode,
            state=self._state,
            fault_code=self._fault_code,
            fault_message=self._fault_message,
            loaded_rack_ids=tuple(sorted(self._loaded_racks.keys())),
        )

    def diagnose(self) -> Dict[str, Any]:
        status = self.get_status()
        return {
            "device_id": self.identity.device_id,
            "station_id": self.identity.station_id,
            "model": self.identity.model,
            "mode": status.mode.value,
            "packml_state": status.state.value,
            "fault_code": status.fault_code,
            "fault_message": status.fault_message,
            "loaded_rack_ids": list(status.loaded_rack_ids),
            "supported_processes": list(self.capabilities.supported_processes),
            "supported_rack_types": list(self.capabilities.supported_rack_types),
            "max_racks": int(self.capabilities.max_racks),
        }
