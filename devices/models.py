from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple

from .enums import Mode, ProcessState


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
    continuous_loading: bool = False
    auto_start: bool = False
    nominal_sample_capacity: Optional[int] = None
    max_carriers: Optional[int] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class LoadInterfaceConfig:
    carrier_type: str = "RACK"
    loading_area: str = ""
    rack_geometry: Dict[str, Any] = field(default_factory=dict)
    slot_layout: Dict[str, Any] = field(default_factory=dict)
    max_carriers: Optional[int] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Carrier:
    carrier_id: str
    carrier_type: str = "RACK"
    rack_type: str = ""
    geometry: Dict[str, Any] = field(default_factory=dict)
    layout: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DeviceSession:
    session_id: str
    carrier_ids: Tuple[str, ...]
    mode: Mode
    process_state: ProcessState
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class DeviceStatusSnapshot:
    timestamp: str
    mode: Mode
    process_state: ProcessState
    is_faulted: bool
    fault_code: Optional[str]
    message: str
    owned_carrier_ids: Tuple[str, ...]
    active_session_id: Optional[str]
    source: str = ""
    raw: Dict[str, Any] = field(default_factory=dict)

