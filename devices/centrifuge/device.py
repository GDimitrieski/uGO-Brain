from __future__ import annotations

import time
import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Tuple

from .enums import LidState, Mode, ProcessState, RotorState
from .models import (
    BalanceModel,
    DeviceCapabilities,
    DeviceIdentity,
    DeviceStatusSnapshot,
    LoadPlan,
    RotorConfiguration,
    RunSession,
    TubeLoad,
)
from .strategies import (
    ConfigurableLidControlStrategy,
    ConfigurableStartStrategy,
    ConfigurableStatusStrategy,
    LidControlStrategy,
    StartStrategy,
    StatusStrategy,
)

if TYPE_CHECKING:
    from .usage_strategy import CentrifugeUsageProfile


def _now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="milliseconds")


class CentrifugeDevice:
    """Centrifuge device contract focused on tube/rack loading and unloading."""

    def __init__(
        self,
        *,
        identity: DeviceIdentity,
        capabilities: DeviceCapabilities,
        rotor_configuration: RotorConfiguration,
        balance_model: BalanceModel,
        start_strategy: Optional[StartStrategy] = None,
        status_strategy: Optional[StatusStrategy] = None,
        lid_control_strategy: Optional[LidControlStrategy] = None,
        usage_profile: Optional["CentrifugeUsageProfile"] = None,
    ) -> None:
        self.identity = identity
        self.capabilities = capabilities
        self.rotor_configuration = rotor_configuration
        self.balance_model = balance_model
        self.start_strategy = start_strategy or ConfigurableStartStrategy()
        self.status_strategy = status_strategy or ConfigurableStatusStrategy()
        self.lid_control_strategy = lid_control_strategy or ConfigurableLidControlStrategy()
        self.usage_profile = usage_profile

        self.mode: Mode = Mode.AUTOMATIC
        self.lid_state: LidState = LidState.CLOSED
        self.rotor_state: RotorState = RotorState.STANDSTILL
        self.process_state: ProcessState = ProcessState.IDLE
        self._fault_code: Optional[str] = None
        self._fault_message: str = ""
        self._loaded_tubes_by_position: Dict[int, TubeLoad] = {}
        self._active_session: Optional[RunSession] = None
        self._active_plan: Optional[LoadPlan] = None

    def _positions_by_index(self) -> Dict[int, Any]:
        return {int(pos.index): pos for pos in self.rotor_configuration.positions}

    def _position_exists(self, position_index: int) -> bool:
        positions = self._positions_by_index()
        return int(position_index) in positions if positions else int(position_index) > 0

    def _opposite_index(self, position_index: int) -> Optional[int]:
        pos = self._positions_by_index().get(int(position_index))
        if pos is None:
            return None
        if pos.opposite_index is None:
            return None
        return int(pos.opposite_index)

    def _build_snapshot(self) -> DeviceStatusSnapshot:
        loaded_tube_ids = tuple(
            tube.tube_id
            for _, tube in sorted(self._loaded_tubes_by_position.items(), key=lambda x: int(x[0]))
        )
        return DeviceStatusSnapshot(
            timestamp=_now_iso(),
            mode=self.mode,
            lid_state=self.lid_state,
            rotor_state=self.rotor_state,
            process_state=self.process_state,
            is_faulted=self._fault_code is not None,
            fault_code=self._fault_code,
            message=self._fault_message,
            loaded_tube_ids=loaded_tube_ids,
            active_session_id=self._active_session.session_id if self._active_session else None,
            source="device_internal",
            raw={},
        )

    def _all_loaded_as_plan(self) -> LoadPlan:
        loads = tuple(
            tube for _, tube in sorted(self._loaded_tubes_by_position.items(), key=lambda x: int(x[0]))
        )
        return LoadPlan(
            plan_id=f"dynamic-{uuid.uuid4()}",
            rotor_id=self.rotor_configuration.rotor_id,
            tube_loads=loads,
        )

    def GetStatus(self) -> DeviceStatusSnapshot:
        return self.status_strategy.read_status(snapshot=self._build_snapshot())

    def CanAccept(self, tube_load: TubeLoad) -> bool:
        if self._fault_code is not None:
            return False
        if self.rotor_state == RotorState.SPINNING:
            return False
        if self.lid_state != LidState.OPEN:
            return False
        if not self._position_exists(tube_load.position_index):
            return False
        if tube_load.position_index in self._loaded_tubes_by_position:
            return False
        return True

    def OpenLid(self) -> bool:
        if self._fault_code is not None:
            return False
        if self.rotor_state == RotorState.SPINNING:
            return False
        self.lid_control_strategy.open_lid(identity=self.identity)
        self.lid_state = LidState.OPEN
        self.process_state = ProcessState.LID_OPEN
        return True

    def Load(self, tube_load: TubeLoad) -> bool:
        if not self.CanAccept(tube_load):
            return False
        self._loaded_tubes_by_position[int(tube_load.position_index)] = tube_load
        self.process_state = ProcessState.LOADING
        self.process_state = ProcessState.LOADED
        return True

    def _validate_symmetry(self, loads: Dict[int, TubeLoad]) -> bool:
        if not self.balance_model.require_symmetry:
            return True
        for pos_index in sorted(loads.keys()):
            opposite = self._opposite_index(pos_index)
            if opposite is None:
                continue
            if opposite not in loads:
                return False
        return True

    def _validate_mass_balance(self, loads: Dict[int, TubeLoad]) -> bool:
        tolerance = self.balance_model.tolerance_g
        max_imbalance = self.balance_model.max_imbalance_g
        if tolerance is None and max_imbalance is None:
            return True

        checked: set[Tuple[int, int]] = set()
        for pos_index, load in sorted(loads.items(), key=lambda x: int(x[0])):
            opposite = self._opposite_index(pos_index)
            if opposite is None or opposite not in loads:
                continue
            pair = tuple(sorted((int(pos_index), int(opposite))))
            if pair in checked:
                continue
            checked.add(pair)
            other = loads[opposite]
            if load.mass_g is None or other.mass_g is None:
                continue
            diff = abs(float(load.mass_g) - float(other.mass_g))
            if tolerance is not None and diff > float(tolerance):
                return False
            if max_imbalance is not None and diff > float(max_imbalance):
                return False
        return True

    def ValidateBalance(self, load_plan: Optional[LoadPlan] = None) -> bool:
        if self._fault_code is not None:
            return False
        plan = load_plan or self._all_loaded_as_plan()
        loads = {int(t.position_index): t for t in plan.tube_loads}
        if not self._validate_symmetry(loads):
            return False
        if not self._validate_mass_balance(loads):
            return False
        self._active_plan = plan
        self.process_state = ProcessState.BALANCE_VALIDATED
        return True

    def CloseLid(self) -> bool:
        if self._fault_code is not None:
            return False
        if self.lid_state != LidState.OPEN:
            return False
        if not self.ValidateBalance(self._active_plan):
            return False
        self.lid_control_strategy.close_lid(identity=self.identity)
        self.lid_state = LidState.LOCKED if self.capabilities.powered_lid_lock else LidState.CLOSED
        self.process_state = ProcessState.READY_TO_START
        return True

    def Start(self) -> RunSession:
        if self._fault_code is not None:
            raise RuntimeError(f"Device faulted: {self._fault_code} {self._fault_message}".strip())
        if self.lid_state not in {LidState.CLOSED, LidState.LOCKED}:
            raise RuntimeError("Cannot start centrifuge: lid is not closed/locked")
        if not self._loaded_tubes_by_position:
            raise RuntimeError("Cannot start centrifuge without loaded tubes")
        if self.process_state not in {ProcessState.READY_TO_START, ProcessState.BALANCE_VALIDATED, ProcessState.LOADED}:
            raise RuntimeError(f"Cannot start centrifuge from state '{self.process_state.value}'")
        if not self.ValidateBalance(self._active_plan):
            raise RuntimeError("Cannot start centrifuge: balance validation failed")

        if self._active_plan is None:
            self._active_plan = self._all_loaded_as_plan()
        if self._active_session is None:
            self._active_session = RunSession(
                session_id=str(uuid.uuid4()),
                plan_id=self._active_plan.plan_id,
                rotor_id=self._active_plan.rotor_id,
                process_state=ProcessState.RUNNING,
                started_at=None,
                completed_at=None,
            )

        metadata = self.start_strategy.start(identity=self.identity, session=self._active_session)
        self._active_session.metadata.update(metadata)
        if not self._active_session.started_at:
            self._active_session.started_at = _now_iso()
        self.rotor_state = RotorState.SPINNING
        self.process_state = ProcessState.RUNNING
        self._active_session.process_state = self.process_state
        return self._active_session

    def WaitForCompletion(self, timeout_s: Optional[float] = None, poll_s: float = 0.25) -> RunSession:
        if self._active_session is None:
            raise RuntimeError("No active run session")
        if self._fault_code is not None:
            raise RuntimeError(f"Device faulted: {self._fault_code} {self._fault_message}".strip())

        deadline = None if timeout_s is None else (time.monotonic() + max(0.0, timeout_s))
        while True:
            if self.process_state == ProcessState.RUNNING:
                self.rotor_state = RotorState.STANDSTILL
                self.process_state = ProcessState.COMPLETED
                self._active_session.process_state = self.process_state
                if not self._active_session.completed_at:
                    self._active_session.completed_at = _now_iso()

            snapshot = self.GetStatus()
            if snapshot.process_state == ProcessState.COMPLETED and snapshot.rotor_state == RotorState.STANDSTILL:
                return self._active_session
            if snapshot.process_state == ProcessState.FAULTED:
                raise RuntimeError(f"Device faulted: {snapshot.fault_code or ''} {snapshot.message}".strip())
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError(f"WaitForCompletion timed out for '{self.identity.device_id}'")
            time.sleep(max(0.01, float(poll_s)))

    def UnlockOrOpenAfterRun(self) -> bool:
        if self._fault_code is not None:
            return False
        if self.rotor_state != RotorState.STANDSTILL:
            return False
        if self.process_state != ProcessState.COMPLETED:
            return False
        self.lid_control_strategy.open_lid(identity=self.identity)
        self.lid_state = LidState.OPEN
        self.process_state = ProcessState.UNLOADING
        return True

    def Unload(self, position_index: int) -> TubeLoad:
        if self._fault_code is not None:
            raise RuntimeError(f"Device faulted: {self._fault_code} {self._fault_message}".strip())
        if self.lid_state != LidState.OPEN:
            raise RuntimeError("Cannot unload: lid is not open")
        tube = self._loaded_tubes_by_position.pop(int(position_index), None)
        if tube is None:
            raise KeyError(f"No loaded tube at rotor position {position_index}")

        if not self._loaded_tubes_by_position:
            self.process_state = ProcessState.IDLE
            self._active_plan = None
            self._active_session = None
            self.lid_state = LidState.CLOSED
        else:
            self.process_state = ProcessState.UNLOADING
        return tube

    def Diagnose(self) -> Dict[str, Any]:
        status = self.GetStatus()
        return {
            "device_id": self.identity.device_id,
            "station_id": self.identity.station_id,
            "mode": status.mode.value,
            "lid_state": status.lid_state.value,
            "rotor_state": status.rotor_state.value,
            "process_state": status.process_state.value,
            "fault_code": status.fault_code,
            "fault_message": status.message,
            "loaded_tube_ids": list(status.loaded_tube_ids),
            "active_session_id": status.active_session_id,
            "status_source": status.source,
            "status_raw": dict(status.raw),
        }

    def ResetFault(self) -> bool:
        self._fault_code = None
        self._fault_message = ""
        if self._loaded_tubes_by_position:
            self.process_state = ProcessState.LOADED
        else:
            self.process_state = ProcessState.IDLE
            self.rotor_state = RotorState.STANDSTILL
            if self.lid_state == LidState.LOCKED:
                self.lid_state = LidState.CLOSED
        return True

    def set_fault(self, code: str, message: str) -> None:
        self._fault_code = str(code).strip() or "UNKNOWN_FAULT"
        self._fault_message = str(message or "")
        self.process_state = ProcessState.FAULTED
        self.rotor_state = RotorState.STANDSTILL

    def to_config_dict(self) -> Dict[str, Any]:
        rotor_positions = []
        for pos in self.rotor_configuration.positions:
            rotor_positions.append(
                {
                    "index": int(pos.index),
                    "angle_deg": float(pos.angle_deg),
                    "opposite_index": pos.opposite_index,
                    "bucket_id": pos.bucket_id,
                    "metadata": dict(pos.metadata),
                }
            )
        bucket_configs = []
        for bucket in self.rotor_configuration.buckets:
            bucket_configs.append(
                {
                    "bucket_id": bucket.bucket_id,
                    "adapter_ids": list(bucket.adapter_ids),
                    "max_tube_loads": bucket.max_tube_loads,
                    "metadata": dict(bucket.metadata),
                }
            )
        adapter_configs = []
        for adapter in self.rotor_configuration.adapters:
            adapter_configs.append(
                {
                    "adapter_id": adapter.adapter_id,
                    "tube_types": list(adapter.tube_types),
                    "positions_per_bucket": adapter.positions_per_bucket,
                    "metadata": dict(adapter.metadata),
                }
            )
        return {
            "id": self.identity.device_id,
            "name": self.identity.name,
            "model": self.identity.model,
            "station_id": self.identity.station_id,
            "capabilities": list(self.capabilities.supported_processes),
            "device_capabilities": {
                "supported_processes": list(self.capabilities.supported_processes),
                "refrigerated": bool(self.capabilities.refrigerated),
                "automatic_rotor_recognition": bool(self.capabilities.automatic_rotor_recognition),
                "powered_lid_lock": bool(self.capabilities.powered_lid_lock),
                "imbalance_detection": bool(self.capabilities.imbalance_detection),
                "interfaces": list(self.capabilities.interfaces),
                "metadata": dict(self.capabilities.metadata),
            },
            "rotor_configuration": {
                "rotor_id": self.rotor_configuration.rotor_id,
                "rotor_type": self.rotor_configuration.rotor_type,
                "positions": rotor_positions,
                "buckets": bucket_configs,
                "adapters": adapter_configs,
                "metadata": dict(self.rotor_configuration.metadata),
            },
            "balance_model": {
                "rule_type": self.balance_model.rule_type,
                "require_symmetry": bool(self.balance_model.require_symmetry),
                "tolerance_g": self.balance_model.tolerance_g,
                "max_imbalance_g": self.balance_model.max_imbalance_g,
                "metadata": dict(self.balance_model.metadata),
            },
            "start_strategy": self.start_strategy.to_config_dict(),
            "status_strategy": self.status_strategy.to_config_dict(),
            "lid_control_strategy": self.lid_control_strategy.to_config_dict(),
            "usage_profile": (
                self.usage_profile.to_config_dict()  # type: ignore[union-attr]
                if self.usage_profile is not None and hasattr(self.usage_profile, "to_config_dict")
                else None
            ),
        }
