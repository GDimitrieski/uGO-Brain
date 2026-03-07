from __future__ import annotations

import time
import uuid
from datetime import datetime
from typing import Any, Dict, Optional

from .enums import Mode, ProcessState
from .models import (
    Carrier,
    DeviceCapabilities,
    DeviceIdentity,
    DeviceSession,
    DeviceStatusSnapshot,
    LoadInterfaceConfig,
)
from .strategies import (
    ConfigurableStartStrategy,
    ConfigurableStatusStrategy,
    StartStrategy,
    StatusStrategy,
)


def _now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="milliseconds")


class SampleProcessingDevice:
    """Rack-level processing contract for analyzers/devices."""

    def __init__(
        self,
        *,
        identity: DeviceIdentity,
        capabilities: DeviceCapabilities,
        load_interface: LoadInterfaceConfig,
        start_strategy: Optional[StartStrategy] = None,
        status_strategy: Optional[StatusStrategy] = None,
    ) -> None:
        self.identity = identity
        self.capabilities = capabilities
        self.load_interface = load_interface
        self.start_strategy = start_strategy or ConfigurableStartStrategy()
        self.status_strategy = status_strategy or ConfigurableStatusStrategy()

        self.mode: Mode = Mode.AUTOMATIC
        self.process_state: ProcessState = ProcessState.IDLE
        self._fault_code: Optional[str] = None
        self._fault_message: str = ""
        self._owned_carriers: Dict[str, Carrier] = {}
        self._active_session: Optional[DeviceSession] = None

    def _max_carriers(self) -> Optional[int]:
        if self.load_interface.max_carriers is not None:
            return int(self.load_interface.max_carriers)
        if self.capabilities.max_carriers is not None:
            return int(self.capabilities.max_carriers)
        return None

    def _build_status_snapshot(self) -> DeviceStatusSnapshot:
        return DeviceStatusSnapshot(
            timestamp=_now_iso(),
            mode=self.mode,
            process_state=self.process_state,
            is_faulted=self._fault_code is not None,
            fault_code=self._fault_code,
            message=self._fault_message,
            owned_carrier_ids=tuple(sorted(self._owned_carriers.keys())),
            active_session_id=self._active_session.session_id if self._active_session else None,
            source="device_internal",
            raw={},
        )

    def GetStatus(self) -> DeviceStatusSnapshot:
        snapshot = self._build_status_snapshot()
        return self.status_strategy.read_status(snapshot=snapshot)

    def CanAccept(self, carrier: Carrier) -> bool:
        if self._fault_code is not None:
            return False
        if carrier.carrier_id in self._owned_carriers:
            return False
        if carrier.carrier_type != self.load_interface.carrier_type:
            return False

        max_carriers = self._max_carriers()
        if max_carriers is not None and len(self._owned_carriers) >= max_carriers:
            return False

        if not self.capabilities.continuous_loading and self.process_state in {
            ProcessState.STARTING,
            ProcessState.EXECUTING,
        }:
            return False
        return True

    def PrepareForLoad(self) -> bool:
        if self._fault_code is not None:
            return False
        if self.process_state == ProcessState.FAULTED:
            return False
        self.process_state = ProcessState.PREPARING_FOR_LOAD
        return True

    def Load(self, carrier: Carrier) -> bool:
        if not self.CanAccept(carrier):
            return False
        if self.process_state == ProcessState.IDLE:
            self.PrepareForLoad()
        self._owned_carriers[carrier.carrier_id] = carrier
        self.process_state = ProcessState.LOADED
        if self._active_session is not None:
            self._active_session.carrier_ids = tuple(sorted(self._owned_carriers.keys()))
        if self.capabilities.auto_start:
            self.Start()
        return True

    def Start(self) -> DeviceSession:
        if self._fault_code is not None:
            raise RuntimeError(f"Device faulted: {self._fault_code} {self._fault_message}".strip())
        if not self._owned_carriers:
            raise RuntimeError("Cannot start processing without loaded carriers")

        if self._active_session is None:
            self._active_session = DeviceSession(
                session_id=str(uuid.uuid4()),
                carrier_ids=tuple(sorted(self._owned_carriers.keys())),
                mode=self.mode,
                process_state=ProcessState.STARTING,
                started_at=None,
                completed_at=None,
            )

        strategy_result = self.start_strategy.start(identity=self.identity, session=self._active_session)
        self._active_session.metadata.update(strategy_result)
        if not self._active_session.started_at:
            self._active_session.started_at = _now_iso()
        self.process_state = ProcessState.EXECUTING
        self._active_session.process_state = self.process_state
        return self._active_session

    def WaitForCompletion(self, timeout_s: Optional[float] = None, poll_s: float = 0.25) -> DeviceSession:
        if self._active_session is None:
            raise RuntimeError("No active session to wait for")
        if self._fault_code is not None:
            raise RuntimeError(f"Device faulted: {self._fault_code} {self._fault_message}".strip())

        if self.capabilities.auto_start and self.process_state == ProcessState.LOADED:
            self.Start()

        deadline = None if timeout_s is None else (time.monotonic() + max(0.0, timeout_s))
        while True:
            if self.process_state == ProcessState.EXECUTING:
                # Simulation default: completion is externally modeled, so transition on wait.
                self.process_state = ProcessState.COMPLETED
                self._active_session.process_state = self.process_state
                if not self._active_session.completed_at:
                    self._active_session.completed_at = _now_iso()

            snapshot = self.GetStatus()
            if snapshot.process_state in {ProcessState.COMPLETED, ProcessState.PREPARING_FOR_UNLOAD, ProcessState.RELEASED}:
                return self._active_session
            if snapshot.process_state == ProcessState.FAULTED:
                raise RuntimeError(f"Device faulted: {snapshot.fault_code or ''} {snapshot.message}".strip())

            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError(
                    f"WaitForCompletion timed out for device '{self.identity.device_id}'"
                )
            time.sleep(max(0.01, float(poll_s)))

    def PrepareForUnload(self) -> bool:
        if self._fault_code is not None:
            return False
        if not self._owned_carriers:
            return False
        if self.process_state not in {ProcessState.LOADED, ProcessState.COMPLETED, ProcessState.RELEASED}:
            return False
        self.process_state = ProcessState.PREPARING_FOR_UNLOAD
        return True

    def Unload(self, carrier_id: str) -> Carrier:
        carrier = self._owned_carriers.pop(carrier_id, None)
        if carrier is None:
            raise KeyError(f"Carrier '{carrier_id}' not owned by device '{self.identity.device_id}'")

        if self._active_session is not None:
            self._active_session.carrier_ids = tuple(sorted(self._owned_carriers.keys()))

        if self._owned_carriers:
            self.process_state = ProcessState.PREPARING_FOR_UNLOAD
        else:
            self.process_state = ProcessState.RELEASED
            self._active_session = None
            self.process_state = ProcessState.IDLE
        return carrier

    def Diagnose(self) -> Dict[str, Any]:
        status = self.GetStatus()
        return {
            "device_id": self.identity.device_id,
            "station_id": self.identity.station_id,
            "mode": status.mode.value,
            "process_state": status.process_state.value,
            "fault_code": status.fault_code,
            "fault_message": status.message,
            "owned_carrier_ids": list(status.owned_carrier_ids),
            "active_session_id": status.active_session_id,
            "status_source": status.source,
            "status_raw": dict(status.raw),
        }

    def ResetFault(self) -> bool:
        self._fault_code = None
        self._fault_message = ""
        if self._owned_carriers:
            self.process_state = ProcessState.LOADED
        else:
            self.process_state = ProcessState.IDLE
        return True

    def set_fault(self, code: str, message: str) -> None:
        self._fault_code = str(code).strip() or "UNKNOWN_FAULT"
        self._fault_message = str(message or "")
        self.process_state = ProcessState.FAULTED

    def to_config_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "id": self.identity.device_id,
            "name": self.identity.name,
            "model": self.identity.model,
            "station_id": self.identity.station_id,
            "capabilities": list(self.capabilities.supported_processes),
            "device_capabilities": {
                "supported_processes": list(self.capabilities.supported_processes),
                "continuous_loading": bool(self.capabilities.continuous_loading),
                "auto_start": bool(self.capabilities.auto_start),
                "nominal_sample_capacity": self.capabilities.nominal_sample_capacity,
                "max_carriers": self.capabilities.max_carriers,
                "metadata": dict(self.capabilities.metadata),
            },
            "load_interface": {
                "carrier_type": self.load_interface.carrier_type,
                "loading_area": self.load_interface.loading_area,
                "rack_geometry": dict(self.load_interface.rack_geometry),
                "slot_layout": dict(self.load_interface.slot_layout),
                "max_carriers": self.load_interface.max_carriers,
                "metadata": dict(self.load_interface.metadata),
            },
            "start_strategy": self.start_strategy.to_config_dict(),
            "status_strategy": self.status_strategy.to_config_dict(),
        }
        if self.identity.landmark_id:
            payload["landmark_id"] = self.identity.landmark_id
        if self.identity.metadata:
            payload["identity_metadata"] = dict(self.identity.metadata)
        return payload

