from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple

from .enums import LidState, Mode, ProcessState, RotorState


@dataclass(frozen=True)
class DeviceIdentity:
    device_id: str
    name: str
    model: str
    station_id: str
    landmark_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class DeviceCapabilities:
    supported_processes: Tuple[str, ...] = ()
    refrigerated: bool = False
    automatic_rotor_recognition: bool = False
    powered_lid_lock: bool = False
    imbalance_detection: bool = False
    interfaces: Tuple[str, ...] = ()
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RotorPosition:
    index: int
    angle_deg: float = 0.0
    opposite_index: Optional[int] = None
    bucket_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BucketConfiguration:
    bucket_id: str
    adapter_ids: Tuple[str, ...] = ()
    max_tube_loads: Optional[int] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class AdapterConfiguration:
    adapter_id: str
    tube_types: Tuple[str, ...] = ()
    positions_per_bucket: Optional[int] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RotorConfiguration:
    rotor_id: str
    rotor_type: str
    positions: Tuple[RotorPosition, ...] = ()
    buckets: Tuple[BucketConfiguration, ...] = ()
    adapters: Tuple[AdapterConfiguration, ...] = ()
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TubeLoad:
    tube_id: str
    position_index: int
    sample_id: Optional[str] = None
    bucket_id: Optional[str] = None
    adapter_id: Optional[str] = None
    mass_g: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class LoadPlan:
    plan_id: str
    rotor_id: str
    tube_loads: Tuple[TubeLoad, ...]
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BalanceModel:
    rule_type: str = "OPPOSITE_POSITION"
    require_symmetry: bool = True
    tolerance_g: Optional[float] = None
    max_imbalance_g: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RunSession:
    session_id: str
    plan_id: str
    rotor_id: str
    process_state: ProcessState
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class DeviceStatusSnapshot:
    timestamp: str
    mode: Mode
    lid_state: LidState
    rotor_state: RotorState
    process_state: ProcessState
    is_faulted: bool
    fault_code: Optional[str]
    message: str
    loaded_tube_ids: Tuple[str, ...]
    active_session_id: Optional[str]
    source: str = ""
    raw: Dict[str, Any] = field(default_factory=dict)

