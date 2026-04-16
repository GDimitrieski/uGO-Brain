from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple, Union


class StationKind(str, Enum):
    EXTERNAL = "EXTERNAL"
    ON_ROBOT_PLATE = "ON_ROBOT_PLATE"


class SlotKind(str, Enum):
    URG_RACK_SLOT = "URG_RACK_SLOT"
    CENTRIFUGE_RACK_SLOT = "CENTRIFUGE_RACK_SLOT"
    TARA_RACK_SLOT = "TARA_RACK_SLOT"
    BIORAD_IH500_RACK_SLOT = "BIORAD_IH500_RACK_SLOT"
    BIORAD_IH1000_RACK_SLOT = "BIORAD_IH1000_RACK_SLOT"
    INTERMEDIATE_RACK_SLOT = "INTERMEDIATE_RACK_SLOT"
    FRIDGE_URG_RACK_SLOT = "FRIDGE_URG_RACK_SLOT"
    THREE_FINGER_GRIPPER_SAMPLE_SLOT = "THREE_FINGER_GRIPPER_SAMPLE_SLOT"


class RackType(str, Enum):
    URG_RACK = "URG_RACK"
    CENTRIFUGE_RACK = "CENTRIFUGE_RACK"
    TARA_RACK = "TARA_RACK"
    BIORAD_IH500_RACK = "BIORAD_IH500_RACK"
    BIORAD_IH1000_RACK = "BIORAD_IH1000_RACK"
    INTERMEDIATE_RACK = "INTERMEDIATE_RACK"
    FRIDGE_URG_RACK = "FRIDGE_URG_RACK"
    THREE_FINGER_GRIPPER_SAMPLE_HOLDER = "THREE_FINGER_GRIPPER_SAMPLE_HOLDER"


class ProcessType(str, Enum):
    FRIDGE_RACK_PROVISIONING = "FRIDGE_RACK_PROVISIONING"
    CENTRIFUGATION = "CENTRIFUGATION"
    DECAP = "DECAP"
    CAP = "CAP"
    SAMPLE_TYPE_DETECTION = "SAMPLE_TYPE_DETECTION"
    HEMATOLOGY_ANALYSIS = "HEMATOLOGY_ANALYSIS"
    CLINICAL_CHEMISTRY_ANALYSIS = "CLINICAL_CHEMISTRY_ANALYSIS"
    COAGULATION_ANALYSIS = "COAGULATION_ANALYSIS"
    IMMUNOHEMATOLOGY_ANALYSIS = "IMMUNOHEMATOLOGY_ANALYSIS"
    ARCHIVATION = "ARCHIVATION"


class CapState(str, Enum):
    CAPPED = "CAPPED"
    DECAPPED = "DECAPPED"


class CapLocationType(str, Enum):
    STORED = "STORED"
    ON_SAMPLE = "ON_SAMPLE"


class SampleClassificationStatus(str, Enum):
    UNKNOWN = "UNKNOWN"
    RECOGNIZED = "RECOGNIZED"
    UNRECOGNIZED = "UNRECOGNIZED"


DEFAULT_LOADING_STRATEGY_BY_JIG: Dict[int, str] = {
    2: "ROUND_ROBIN",
}


@dataclass(frozen=True)
class Landmark:
    id: str
    code: str
    station_id: str


@dataclass(frozen=True)
class RackSlotConfig:
    slot_id: str
    kind: SlotKind
    jig_id: int
    itm_id: int = 1
    # Jig-level rack-receiver model: one jig can host multiple rack positions.
    rack_capacity: int = 1
    rack_pattern: Optional[str] = None
    rack_rows: Optional[int] = None
    rack_cols: Optional[int] = None
    rack_index: int = 1
    # Offset for mapping per-rack sample slot index to task OBJ_Nbr within a jig.
    obj_nbr_offset: int = 0
    # Optional strategy override. Empty means fallback to JIG defaults.
    loading_strategy: str = ""
    accepted_rack_types: frozenset[RackType] = field(default_factory=frozenset)


@dataclass
class Station:
    id: str
    name: str
    itm_id: int
    kind: StationKind
    amr_pos_target: Optional[str]
    slot_configs: Dict[str, RackSlotConfig]
    landmark_id: Optional[str] = None
    linked_device_ids: Tuple[str, ...] = ()

    def requires_navigation(self) -> bool:
        return self.kind == StationKind.EXTERNAL


@dataclass
class Rack:
    id: str
    rack_type: RackType
    capacity: int
    pattern: str
    pin_obj_type: int
    rows: Optional[int] = None
    cols: Optional[int] = None
    blocked_slots: Set[int] = field(default_factory=set)
    occupied_slots: Dict[int, str] = field(default_factory=dict)
    reserved_slots: Dict[int, str] = field(default_factory=dict)

    def validate_slot(self, slot_index: int) -> None:
        if slot_index < 1 or slot_index > self.capacity:
            raise ValueError(f"Rack '{self.id}' slot {slot_index} out of range 1..{self.capacity}")
        if slot_index in self.blocked_slots:
            raise ValueError(f"Rack '{self.id}' slot {slot_index} is blocked by pin/fixture")

    def available_slots(self) -> List[int]:
        return [i for i in range(1, self.capacity + 1) if i not in self.blocked_slots]


@dataclass(frozen=True)
class Sample:
    id: str
    barcode: str
    obj_type: int
    length_mm: float
    diameter_mm: float
    cap_state: CapState
    required_processes: Tuple[ProcessType, ...] = ()


@dataclass(frozen=True)
class Cap:
    id: str
    obj_type: int = 9014
    assigned_sample_id: str = ""


@dataclass(frozen=True)
class StoredCapLocation:
    station_id: str
    station_slot_id: str
    rack_id: str
    slot_index: int


@dataclass(frozen=True)
class CapOnSampleLocation:
    sample_id: str


CapLocation = Union[StoredCapLocation, CapOnSampleLocation]


@dataclass
class CapStateRecord:
    cap_id: str
    location: CapLocation


@dataclass(frozen=True)
class RackLocation:
    station_id: str
    station_slot_id: str
    rack_id: str
    slot_index: int


@dataclass(frozen=True)
class GripperLocation:
    gripper_id: str = "uLM_GRIPPER"


SampleLocation = Union[RackLocation, GripperLocation]


@dataclass
class SampleState:
    sample_id: str
    location: SampleLocation
    completed_processes: Set[ProcessType] = field(default_factory=set)
    classification_status: SampleClassificationStatus = SampleClassificationStatus.UNKNOWN
    classification_source: str = ""
    assigned_route: str = ""
    assigned_route_station_slot_id: str = ""
    assigned_route_rack_index: Optional[int] = None
    classification_details: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Device:
    id: str
    name: str
    station_id: str
    capabilities: frozenset[ProcessType]
    planner_role: str = "PROCESSOR"
    exclude_station_racks_from_idle_return: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class WorldModel:
    stations: Dict[str, Station]
    landmarks: Dict[str, Landmark]
    racks: Dict[str, Rack]
    devices: Dict[str, Device]
    samples: Dict[str, Sample] = field(default_factory=dict)
    sample_states: Dict[str, SampleState] = field(default_factory=dict)
    caps: Dict[str, Cap] = field(default_factory=dict)
    cap_states: Dict[str, CapStateRecord] = field(default_factory=dict)
    rack_placements: Dict[Tuple[str, str], str] = field(default_factory=dict)
    rack_in_gripper_id: Optional[str] = None
    robot_current_station_id: Optional[str] = None
    _sample_counter: int = 0

    def get_station(self, station_id: str) -> Station:
        st = self.stations.get(station_id)
        if st is None:
            raise KeyError(f"Unknown station '{station_id}'")
        return st

    def get_station_devices(self, station_id: str) -> List[Device]:
        self.get_station(station_id)
        out: List[Device] = []
        for dev_id in sorted(self.devices.keys()):
            dev = self.devices[dev_id]
            if dev.station_id == station_id:
                out.append(dev)
        return out

    def get_slot_config(self, station_id: str, station_slot_id: str) -> RackSlotConfig:
        station = self.get_station(station_id)
        cfg = station.slot_configs.get(station_slot_id)
        if cfg is None:
            raise KeyError(f"Unknown station slot '{station_slot_id}' for station '{station_id}'")
        return cfg

    @staticmethod
    def _normalize_loading_strategy(strategy_raw: Any) -> str:
        txt = str(strategy_raw or "").strip().upper()
        if txt in {"ROUND_ROBIN", "SEQUENTIAL"}:
            return txt
        return ""

    def resolved_loading_strategy(self, station_id: str, station_slot_id: str) -> str:
        cfg = self.get_slot_config(station_id, station_slot_id)
        slot_strategy = self._normalize_loading_strategy(cfg.loading_strategy)
        if slot_strategy:
            return slot_strategy
        return DEFAULT_LOADING_STRATEGY_BY_JIG.get(int(cfg.jig_id), "SEQUENTIAL")

    def slots_for_jig(self, station_id: str, jig_id: int) -> List[RackSlotConfig]:
        station = self.get_station(station_id)
        out: List[RackSlotConfig] = [
            cfg for cfg in station.slot_configs.values()
            if int(cfg.jig_id) == int(jig_id)
        ]
        out.sort(key=lambda cfg: (int(cfg.rack_index), str(cfg.slot_id)))
        return out

    def obj_nbr_for_slot_index(self, station_id: str, station_slot_id: str, slot_index: int) -> int:
        cfg = self.get_slot_config(station_id, station_slot_id)
        return int(cfg.obj_nbr_offset) + int(slot_index)

    def select_next_target_slot_for_jig(
        self,
        station_id: str,
        jig_id: int,
        *,
        strategy: Optional[str] = None,
        preferred_slot_indexes: Optional[Sequence[int]] = None,
    ) -> Tuple[str, int]:
        slot_cfgs = self.slots_for_jig(station_id, jig_id)
        if not slot_cfgs:
            raise ValueError(
                f"No slots configured for station '{station_id}' with JIG_ID={int(jig_id)}"
            )

        preferred_slots: Optional[Set[int]] = None
        if preferred_slot_indexes is not None:
            parsed_slots = {int(x) for x in preferred_slot_indexes if int(x) > 0}
            if not parsed_slots:
                raise ValueError(
                    f"Invalid preferred_slot_indexes for station '{station_id}' JIG_ID={int(jig_id)}"
                )
            preferred_slots = parsed_slots

        normalized_override = self._normalize_loading_strategy(strategy)
        if normalized_override:
            resolved_strategy = normalized_override
        else:
            resolved_strategy = self.resolved_loading_strategy(station_id, slot_cfgs[0].slot_id)

        free_by_slot: Dict[str, List[int]] = {}
        for cfg in slot_cfgs:
            rack_id = self.rack_placements.get((station_id, cfg.slot_id))
            if not rack_id:
                continue
            rack = self.racks.get(rack_id)
            if rack is None:
                continue
            free_slots = [
                idx
                for idx in rack.available_slots()
                if idx not in rack.occupied_slots and (preferred_slots is None or int(idx) in preferred_slots)
            ]
            if free_slots:
                free_by_slot[str(cfg.slot_id)] = free_slots

        if not free_by_slot:
            preferred_msg = (
                f" (preferred_slots={sorted(preferred_slots)})"
                if preferred_slots is not None
                else ""
            )
            raise ValueError(
                f"No free sample slots available for station '{station_id}' JIG_ID={int(jig_id)}"
                f"{preferred_msg}"
            )

        if resolved_strategy == "SEQUENTIAL":
            for cfg in slot_cfgs:
                free_slots = free_by_slot.get(str(cfg.slot_id), [])
                if free_slots:
                    return (str(cfg.slot_id), int(free_slots[0]))
            raise ValueError(
                f"Sequential target resolution failed for station '{station_id}' JIG_ID={int(jig_id)}"
            )

        if resolved_strategy == "ROUND_ROBIN":
            best_slot_id: Optional[str] = None
            best_slot_index: Optional[int] = None
            best_score: Optional[Tuple[int, int, int]] = None
            for cfg in slot_cfgs:
                slot_id = str(cfg.slot_id)
                free_slots = free_by_slot.get(slot_id, [])
                if not free_slots:
                    continue
                rack_id = self.rack_placements.get((station_id, slot_id))
                if not rack_id or rack_id not in self.racks:
                    continue
                rack = self.racks[rack_id]
                score = (
                    len(rack.occupied_slots),
                    int(cfg.rack_index),
                    int(free_slots[0]),
                )
                if best_score is None or score < best_score:
                    best_score = score
                    best_slot_id = slot_id
                    best_slot_index = int(free_slots[0])
            if best_slot_id is not None and best_slot_index is not None:
                return (best_slot_id, best_slot_index)
            raise ValueError(
                f"Round-robin target resolution failed for station '{station_id}' JIG_ID={int(jig_id)}"
            )

        raise ValueError(
            f"Unsupported loading strategy '{resolved_strategy}' for station '{station_id}' "
            f"JIG_ID={int(jig_id)}"
        )

    def set_sample_cap_state(self, sample_id: str, cap_state: CapState) -> None:
        sample = self.samples.get(sample_id)
        if sample is None:
            raise KeyError(f"Unknown sample '{sample_id}'")
        self.samples[sample_id] = Sample(
            id=sample.id,
            barcode=sample.barcode,
            obj_type=sample.obj_type,
            length_mm=sample.length_mm,
            diameter_mm=sample.diameter_mm,
            cap_state=cap_state,
            required_processes=sample.required_processes,
        )
        if cap_state == CapState.CAPPED:
            self.ensure_cap_for_sample(sample_id)

    def _resolve_cap_id_collision(self, preferred_cap_id: str) -> str:
        base = str(preferred_cap_id or "").strip()
        if not base:
            base = "CAP"
        if base not in self.caps:
            return base
        suffix = 2
        while True:
            candidate = f"{base}#{suffix}"
            if candidate not in self.caps:
                return candidate
            suffix += 1

    def _default_cap_id_for_sample(self, sample_id: str) -> str:
        sample_id_txt = str(sample_id).strip()
        barcode = ""
        sample = self.samples.get(sample_id_txt)
        if sample is not None:
            barcode = str(sample.barcode or "").strip()

        # Keep cap IDs stable and filesystem-safe when barcode has separators/symbols.
        label_source = barcode if barcode else sample_id_txt
        label = re.sub(r"[^A-Za-z0-9._-]+", "_", label_source).strip("._-")
        if not label:
            label = sample_id_txt or "UNKNOWN"
        return f"CAP_{label}"

    def cap_id_on_sample(self, sample_id: str) -> Optional[str]:
        sample_id_txt = str(sample_id)
        for cap_id in sorted(self.cap_states.keys()):
            state = self.cap_states[cap_id]
            if isinstance(state.location, CapOnSampleLocation) and str(state.location.sample_id) == sample_id_txt:
                return cap_id
        return None

    def _remove_cap_from_current_location(self, cap_id: str) -> None:
        state = self.cap_states.get(cap_id)
        if state is None:
            return
        loc = state.location
        if isinstance(loc, StoredCapLocation):
            rack = self.racks.get(str(loc.rack_id))
            if rack is not None and rack.occupied_slots.get(int(loc.slot_index)) == cap_id:
                rack.occupied_slots.pop(int(loc.slot_index), None)

    def _ensure_cap_from_stored_location(
        self,
        cap_id: str,
        location: StoredCapLocation,
    ) -> str:
        cap_id_txt = str(cap_id).strip()
        if not cap_id_txt:
            raise ValueError("Cap ID must not be empty")
        if cap_id_txt not in self.caps:
            inferred_sample_id = ""
            if cap_id_txt.upper().startswith("CAP_") and len(cap_id_txt) > 4:
                inferred_sample_id = cap_id_txt[4:]
            self.caps[cap_id_txt] = Cap(
                id=cap_id_txt,
                obj_type=9014,
                assigned_sample_id=inferred_sample_id,
            )
        self.cap_states[cap_id_txt] = CapStateRecord(cap_id=cap_id_txt, location=location)
        return cap_id_txt

    def ensure_cap_for_sample(self, sample_id: str) -> str:
        sample_id_txt = str(sample_id)
        if sample_id_txt not in self.samples:
            raise KeyError(f"Unknown sample '{sample_id_txt}'")

        assigned_cap_ids = sorted(
            cap_id
            for cap_id, cap in self.caps.items()
            if str(cap.assigned_sample_id) == sample_id_txt
        )
        if assigned_cap_ids:
            cap_id = assigned_cap_ids[0]
        else:
            preferred = self._default_cap_id_for_sample(sample_id_txt)
            cap_id = self._resolve_cap_id_collision(preferred)
            self.caps[cap_id] = Cap(id=cap_id, obj_type=9014, assigned_sample_id=sample_id_txt)

        sample = self.samples[sample_id_txt]
        if sample.cap_state == CapState.CAPPED and self.cap_id_on_sample(sample_id_txt) is None:
            self._remove_cap_from_current_location(cap_id)
            self.cap_states[cap_id] = CapStateRecord(
                cap_id=cap_id,
                location=CapOnSampleLocation(sample_id=sample_id_txt),
            )
        return cap_id

    def ensure_cap_tracking_for_capped_samples(self) -> None:
        for sample_id, sample in sorted(self.samples.items()):
            if sample.cap_state != CapState.CAPPED:
                continue
            self.ensure_cap_for_sample(sample_id)

    def store_cap_from_sample_in_jig(
        self,
        sample_id: str,
        *,
        station_id: str,
        jig_id: int,
        target_slot_id: Optional[str] = None,
        target_slot_index: Optional[int] = None,
    ) -> Tuple[str, str, int]:
        sample_id_txt = str(sample_id)
        cap_id = self.cap_id_on_sample(sample_id_txt)
        if cap_id is None:
            self.ensure_cap_for_sample(sample_id_txt)
            cap_id = self.cap_id_on_sample(sample_id_txt)
        if cap_id is None:
            raise ValueError(
                f"No cap is currently tracked on sample '{sample_id_txt}' for decap storage"
            )

        if target_slot_id is None and target_slot_index is None:
            target_slot_id, target_slot_index = self.select_next_target_slot_for_jig(
                station_id=str(station_id),
                jig_id=int(jig_id),
            )
        elif target_slot_id is None or target_slot_index is None:
            raise ValueError("Both target_slot_id and target_slot_index must be provided together")
        else:
            cfg = self.get_slot_config(str(station_id), str(target_slot_id))
            if int(cfg.jig_id) != int(jig_id):
                raise ValueError(
                    f"Target slot '{station_id}.{target_slot_id}' does not belong to JIG_ID={int(jig_id)}"
                )

        rack = self.get_rack_at(str(station_id), str(target_slot_id))
        rack.validate_slot(int(target_slot_index))
        existing = rack.occupied_slots.get(int(target_slot_index))
        if existing is not None and existing != cap_id:
            raise ValueError(
                f"Cannot store cap in '{station_id}.{target_slot_id}' slot {int(target_slot_index)}: "
                f"occupied by '{existing}'"
            )

        self._remove_cap_from_current_location(cap_id)
        rack.occupied_slots[int(target_slot_index)] = cap_id
        self.cap_states[cap_id] = CapStateRecord(
            cap_id=cap_id,
            location=StoredCapLocation(
                station_id=str(station_id),
                station_slot_id=str(target_slot_id),
                rack_id=str(rack.id),
                slot_index=int(target_slot_index),
            ),
        )
        return cap_id, str(target_slot_id), int(target_slot_index)

    def attach_cap_to_sample_from_jig(
        self,
        sample_id: str,
        *,
        station_id: str,
        jig_id: int,
    ) -> Tuple[str, str, int, bool]:
        sample_id_txt = str(sample_id)
        if sample_id_txt not in self.samples:
            raise KeyError(f"Unknown sample '{sample_id_txt}'")

        cap_id, source_slot_id, source_slot_index, assigned_match = self.select_cap_for_sample_from_jig(
            sample_id_txt,
            station_id=str(station_id),
            jig_id=int(jig_id),
        )
        if not source_slot_id:
            return cap_id, source_slot_id, source_slot_index, assigned_match

        source_rack = self.get_rack_at(str(station_id), source_slot_id)
        if source_rack.occupied_slots.get(source_slot_index) != cap_id:
            raise ValueError(
                f"Stored cap '{cap_id}' was not found at expected source slot "
                f"'{station_id}.{source_slot_id}' index {int(source_slot_index)}"
            )
        source_rack.occupied_slots.pop(source_slot_index, None)

        self.cap_states[cap_id] = CapStateRecord(
            cap_id=cap_id,
            location=CapOnSampleLocation(sample_id=sample_id_txt),
        )
        cap = self.caps.get(cap_id)
        if cap is None:
            self.caps[cap_id] = Cap(id=cap_id, obj_type=9014, assigned_sample_id=sample_id_txt)
        elif str(cap.assigned_sample_id) != sample_id_txt:
            self.caps[cap_id] = Cap(
                id=cap.id,
                obj_type=int(cap.obj_type),
                assigned_sample_id=sample_id_txt,
            )
        return cap_id, source_slot_id, source_slot_index, assigned_match

    def select_cap_for_sample_from_jig(
        self,
        sample_id: str,
        *,
        station_id: str,
        jig_id: int,
    ) -> Tuple[str, str, int, bool]:
        sample_id_txt = str(sample_id)
        if sample_id_txt not in self.samples:
            raise KeyError(f"Unknown sample '{sample_id_txt}'")

        existing_cap_id = self.cap_id_on_sample(sample_id_txt)
        if existing_cap_id is not None:
            state = self.cap_states.get(existing_cap_id)
            if isinstance(state.location, CapOnSampleLocation):
                return existing_cap_id, "", 0, True

        candidates: List[Tuple[int, int, int, str, str, int]] = []
        for cfg in self.slots_for_jig(str(station_id), int(jig_id)):
            rack_id = self.rack_placements.get((str(station_id), str(cfg.slot_id)))
            if not rack_id:
                continue
            rack = self.racks.get(str(rack_id))
            if rack is None:
                continue
            for slot_index, occupant_id_raw in sorted(rack.occupied_slots.items()):
                occupant_id = str(occupant_id_raw)
                location = StoredCapLocation(
                    station_id=str(station_id),
                    station_slot_id=str(cfg.slot_id),
                    rack_id=str(rack.id),
                    slot_index=int(slot_index),
                )
                cap_id = self._ensure_cap_from_stored_location(occupant_id, location)
                cap = self.caps.get(cap_id)
                assigned = str(cap.assigned_sample_id) if cap is not None else ""
                is_assigned = 0 if assigned == sample_id_txt else 1
                candidates.append(
                    (
                        int(is_assigned),
                        int(cfg.rack_index),
                        int(slot_index),
                        cap_id,
                        str(cfg.slot_id),
                        int(slot_index),
                    )
                )

        if not candidates:
            raise ValueError(
                f"No stored caps available for station '{station_id}' JIG_ID={int(jig_id)}"
            )

        candidates.sort()
        chosen = candidates[0]
        cap_id = str(chosen[3])
        source_slot_id = str(chosen[4])
        source_slot_index = int(chosen[5])
        assigned_match = bool(chosen[0] == 0)
        return cap_id, source_slot_id, source_slot_index, assigned_match

    def set_robot_station(self, station_id: str) -> None:
        self.get_station(station_id)
        self.robot_current_station_id = station_id

    def needs_navigation(self, station_id: str) -> bool:
        target = self.get_station(station_id)
        if target.kind == StationKind.ON_ROBOT_PLATE:
            return False
        return self.robot_current_station_id != station_id

    def place_rack(self, station_id: str, station_slot_id: str, rack_id: str) -> None:
        cfg = self.get_slot_config(station_id, station_slot_id)
        rack = self.racks.get(rack_id)
        if rack is None:
            raise KeyError(f"Unknown rack '{rack_id}'")

        if cfg.accepted_rack_types and rack.rack_type not in cfg.accepted_rack_types:
            allowed = ", ".join(t.value for t in sorted(cfg.accepted_rack_types, key=lambda x: x.value))
            raise ValueError(
                f"Rack '{rack_id}' type '{rack.rack_type.value}' not allowed in slot "
                f"'{station_id}.{station_slot_id}'. Allowed: {allowed}"
            )

        self.rack_placements[(station_id, station_slot_id)] = rack_id

    def clear_rack_placement(self, station_id: str, station_slot_id: str) -> None:
        self.rack_placements.pop((station_id, station_slot_id), None)

    def pick_rack_to_gripper(
        self,
        source_station_id: str,
        source_station_slot_id: str,
    ) -> str:
        if self.rack_in_gripper_id is not None:
            raise ValueError(f"Gripper already holds rack '{self.rack_in_gripper_id}'")

        src_key = (source_station_id, source_station_slot_id)
        rack_id = self.rack_placements.get(src_key)
        if rack_id is None:
            raise ValueError(f"No rack mounted at '{source_station_id}.{source_station_slot_id}'")

        self.clear_rack_placement(source_station_id, source_station_slot_id)
        self.rack_in_gripper_id = rack_id
        return rack_id

    def place_rack_from_gripper(
        self,
        target_station_id: str,
        target_station_slot_id: str,
    ) -> str:
        rack_id = self.rack_in_gripper_id
        if rack_id is None:
            raise ValueError("No rack currently held by gripper")

        dst_key = (target_station_id, target_station_slot_id)
        if dst_key in self.rack_placements:
            raise ValueError(f"Target slot '{target_station_id}.{target_station_slot_id}' already has a rack")

        self.place_rack(target_station_id, target_station_slot_id, rack_id)
        self.rack_in_gripper_id = None
        return rack_id

    def move_rack(
        self,
        source_station_id: str,
        source_station_slot_id: str,
        target_station_id: str,
        target_station_slot_id: str,
    ) -> str:
        src_key = (source_station_id, source_station_slot_id)
        dst_key = (target_station_id, target_station_slot_id)
        if src_key == dst_key:
            rack_id = self.rack_placements.get(src_key)
            if rack_id is None:
                raise ValueError(f"No rack mounted at '{source_station_id}.{source_station_slot_id}'")
            return rack_id

        picked_rack_id = self.pick_rack_to_gripper(
            source_station_id=source_station_id,
            source_station_slot_id=source_station_slot_id,
        )
        try:
            placed_rack_id = self.place_rack_from_gripper(
                target_station_id=target_station_id,
                target_station_slot_id=target_station_slot_id,
            )
        except Exception:
            # Preserve move_rack's historical atomic behavior on failure.
            try:
                self.place_rack_from_gripper(
                    target_station_id=source_station_id,
                    target_station_slot_id=source_station_slot_id,
                )
            except Exception:
                pass
            raise
        if picked_rack_id != placed_rack_id:
            raise RuntimeError("Rack identity mismatch during move")

        # Keep sample locations in sync when a mounted rack changes station/slot.
        moved_rack = self.racks.get(placed_rack_id)
        if moved_rack is not None:
            for slot_index, occupant_id in moved_rack.occupied_slots.items():
                sample_state = self.sample_states.get(occupant_id)
                if sample_state is not None:
                    sample_state.location = RackLocation(
                        station_id=target_station_id,
                        station_slot_id=target_station_slot_id,
                        rack_id=placed_rack_id,
                        slot_index=int(slot_index),
                    )
                    continue
                cap_state = self.cap_states.get(str(occupant_id))
                if cap_state is None:
                    continue
                if isinstance(cap_state.location, StoredCapLocation):
                    self.cap_states[str(occupant_id)] = CapStateRecord(
                        cap_id=str(occupant_id),
                        location=StoredCapLocation(
                            station_id=str(target_station_id),
                            station_slot_id=str(target_station_slot_id),
                            rack_id=str(placed_rack_id),
                            slot_index=int(slot_index),
                        ),
                    )
        return placed_rack_id

    def get_rack_at(self, station_id: str, station_slot_id: str) -> Rack:
        rack_id = self.rack_placements.get((station_id, station_slot_id))
        if rack_id is None:
            raise KeyError(f"No rack mounted at '{station_id}.{station_slot_id}'")
        rack = self.racks.get(rack_id)
        if rack is None:
            raise KeyError(f"Mounted rack '{rack_id}' does not exist")
        return rack

    def reserve_slot(self, rack_id: str, slot_index: int, sample_id: str) -> None:
        rack = self.racks.get(rack_id)
        if rack is None:
            raise KeyError(f"Unknown rack '{rack_id}'")
        rack.validate_slot(slot_index)

        occupied_by = rack.occupied_slots.get(slot_index)
        if occupied_by is not None and occupied_by != sample_id:
            raise ValueError(f"Rack '{rack_id}' slot {slot_index} already occupied by sample '{occupied_by}'")

        reserved_by = rack.reserved_slots.get(slot_index)
        if reserved_by is not None and reserved_by != sample_id:
            raise ValueError(f"Rack '{rack_id}' slot {slot_index} already reserved by sample '{reserved_by}'")

        rack.reserved_slots[slot_index] = sample_id

    def register_sample(self, sample: Sample, location: SampleLocation) -> None:
        self.samples[sample.id] = sample
        self.sample_states[sample.id] = SampleState(sample_id=sample.id, location=location)
        if sample.cap_state == CapState.CAPPED:
            self.ensure_cap_for_sample(sample.id)

    def _resolve_sample_id_collision(self, preferred_sample_id: str, current_sample_id: str) -> str:
        candidate = str(preferred_sample_id).strip()
        if not candidate:
            raise ValueError("Sample ID must not be empty")
        if candidate == current_sample_id:
            return current_sample_id
        if candidate not in self.samples and candidate not in self.sample_states:
            return candidate
        suffix = 2
        while True:
            alt = f"{candidate}#{suffix}"
            if alt == current_sample_id:
                return current_sample_id
            if alt not in self.samples and alt not in self.sample_states:
                return alt
            suffix += 1

    def reidentify_sample(
        self,
        sample_id: str,
        preferred_sample_id: str,
        *,
        barcode: Optional[str] = None,
    ) -> str:
        sample = self.samples.get(sample_id)
        if sample is None:
            raise KeyError(f"Unknown sample '{sample_id}'")
        state = self.sample_states.get(sample_id)
        if state is None:
            raise KeyError(f"Unknown sample state '{sample_id}'")

        resolved_sample_id = self._resolve_sample_id_collision(preferred_sample_id, sample_id)
        next_barcode = str(barcode).strip() if barcode is not None else sample.barcode
        if not next_barcode:
            next_barcode = sample.barcode

        # Update any rack slot mappings that still reference the old sample ID.
        for rack in self.racks.values():
            for slot_idx, mapped_sample_id in list(rack.occupied_slots.items()):
                if mapped_sample_id == sample_id:
                    rack.occupied_slots[slot_idx] = resolved_sample_id
            for slot_idx, mapped_sample_id in list(rack.reserved_slots.items()):
                if mapped_sample_id == sample_id:
                    rack.reserved_slots[slot_idx] = resolved_sample_id

        if resolved_sample_id == sample_id:
            self.samples[sample_id] = Sample(
                id=sample.id,
                barcode=next_barcode,
                obj_type=sample.obj_type,
                length_mm=sample.length_mm,
                diameter_mm=sample.diameter_mm,
                cap_state=sample.cap_state,
                required_processes=sample.required_processes,
            )
            for cap_id, cap in list(self.caps.items()):
                if str(cap.assigned_sample_id) != str(sample_id):
                    continue
                self.caps[cap_id] = Cap(
                    id=cap.id,
                    obj_type=int(cap.obj_type),
                    assigned_sample_id=str(sample_id),
                )
            return sample_id

        self.samples.pop(sample_id, None)
        self.sample_states.pop(sample_id, None)
        self.samples[resolved_sample_id] = Sample(
            id=resolved_sample_id,
            barcode=next_barcode,
            obj_type=sample.obj_type,
            length_mm=sample.length_mm,
            diameter_mm=sample.diameter_mm,
            cap_state=sample.cap_state,
            required_processes=sample.required_processes,
        )
        state.sample_id = resolved_sample_id
        self.sample_states[resolved_sample_id] = state
        for cap_id, cap in list(self.caps.items()):
            if str(cap.assigned_sample_id) == str(sample_id):
                self.caps[cap_id] = Cap(
                    id=cap.id,
                    obj_type=int(cap.obj_type),
                    assigned_sample_id=str(resolved_sample_id),
                )
            cap_state = self.cap_states.get(cap_id)
            if cap_state is None:
                continue
            if isinstance(cap_state.location, CapOnSampleLocation) and str(cap_state.location.sample_id) == str(sample_id):
                self.cap_states[cap_id] = CapStateRecord(
                    cap_id=cap_id,
                    location=CapOnSampleLocation(sample_id=str(resolved_sample_id)),
                )
        return resolved_sample_id

    def _next_sample_id(self, prefix: str = "SMP") -> str:
        self._sample_counter += 1
        return f"{prefix}_{self._sample_counter:04d}"

    def ensure_placeholder_sample(
        self,
        station_id: str,
        station_slot_id: str,
        slot_index: int,
        obj_type: int,
    ) -> str:
        rack = self.get_rack_at(station_id, station_slot_id)
        rack.validate_slot(slot_index)

        existing = rack.occupied_slots.get(slot_index)
        if existing is not None:
            return existing

        sample_id = self._next_sample_id(prefix="CAM")
        sample = Sample(
            id=sample_id,
            barcode=sample_id,
            obj_type=obj_type,
            length_mm=75.0,
            diameter_mm=13.0,
            cap_state=CapState.CAPPED,
            required_processes=(),
        )
        location = RackLocation(
            station_id=station_id,
            station_slot_id=station_slot_id,
            rack_id=rack.id,
            slot_index=slot_index,
        )
        self.register_sample(sample, location)
        rack.occupied_slots[slot_index] = sample_id
        return sample_id

    def _sample_ids_in_gripper(self) -> List[str]:
        ids: List[str] = []
        for sample_id, state in self.sample_states.items():
            if isinstance(state.location, GripperLocation):
                ids.append(sample_id)
        return sorted(ids)

    def pick_sample_to_gripper(
        self,
        source_station_id: str,
        source_station_slot_id: str,
        source_slot_index: int,
        gripper_id: str = "uLM_GRIPPER",
    ) -> str:
        source_rack = self.get_rack_at(source_station_id, source_station_slot_id)
        source_rack.validate_slot(source_slot_index)

        sample_id = source_rack.occupied_slots.get(source_slot_index)
        if sample_id is None:
            raise ValueError(
                f"Cannot pick sample from '{source_station_id}.{source_station_slot_id}' "
                f"slot {source_slot_index}: slot is empty"
            )

        sample_ids_in_gripper = self._sample_ids_in_gripper()
        if sample_ids_in_gripper and sample_id not in sample_ids_in_gripper:
            raise ValueError(f"Gripper already holds sample '{sample_ids_in_gripper[0]}'")

        state = self.sample_states.get(sample_id)
        if state is None:
            raise KeyError(
                f"Sample state missing for '{sample_id}'. "
                "The occupied object might not be a sample."
            )

        source_rack.occupied_slots.pop(source_slot_index, None)
        state.location = GripperLocation(gripper_id=gripper_id)
        return sample_id

    def place_sample_from_gripper(
        self,
        target_station_id: str,
        target_station_slot_id: str,
        target_slot_index: int,
        sample_id: Optional[str] = None,
    ) -> str:
        sample_ids_in_gripper = self._sample_ids_in_gripper()
        if not sample_ids_in_gripper:
            raise ValueError("No sample currently held by gripper")

        if sample_id is None:
            if len(sample_ids_in_gripper) != 1:
                raise ValueError(
                    "Multiple samples in gripper state; target sample_id is required for placement"
                )
            sample_id = sample_ids_in_gripper[0]
        elif sample_id not in sample_ids_in_gripper:
            raise ValueError(f"Sample '{sample_id}' is not currently in gripper")

        target_rack = self.get_rack_at(target_station_id, target_station_slot_id)
        target_rack.validate_slot(target_slot_index)

        existing = target_rack.occupied_slots.get(target_slot_index)
        if existing is not None and existing != sample_id:
            raise ValueError(
                f"Cannot place sample into '{target_station_id}.{target_station_slot_id}' "
                f"slot {target_slot_index}: occupied by '{existing}'"
            )

        self.reserve_slot(target_rack.id, target_slot_index, sample_id)
        target_rack.occupied_slots[target_slot_index] = sample_id
        target_rack.reserved_slots.pop(target_slot_index, None)

        state = self.sample_states.get(sample_id)
        if state is None:
            raise KeyError(f"Sample state missing for '{sample_id}'")
        state.location = RackLocation(
            station_id=target_station_id,
            station_slot_id=target_station_slot_id,
            rack_id=target_rack.id,
            slot_index=target_slot_index,
        )
        return sample_id

    def move_sample(
        self,
        source_station_id: str,
        source_station_slot_id: str,
        source_slot_index: int,
        target_station_id: str,
        target_station_slot_id: str,
        target_slot_index: int,
    ) -> str:
        picked_sample_id = self.pick_sample_to_gripper(
            source_station_id=source_station_id,
            source_station_slot_id=source_station_slot_id,
            source_slot_index=source_slot_index,
        )
        try:
            placed_sample_id = self.place_sample_from_gripper(
                target_station_id=target_station_id,
                target_station_slot_id=target_station_slot_id,
                target_slot_index=target_slot_index,
                sample_id=picked_sample_id,
            )
        except Exception:
            # Preserve move_sample's historical atomic behavior on failure.
            try:
                self.place_sample_from_gripper(
                    target_station_id=source_station_id,
                    target_station_slot_id=source_station_slot_id,
                    target_slot_index=source_slot_index,
                    sample_id=picked_sample_id,
                )
            except Exception:
                pass
            raise
        if picked_sample_id != placed_sample_id:
            raise RuntimeError("Sample identity mismatch during move")
        return placed_sample_id

    def mark_process_completed(self, sample_id: str, process: ProcessType) -> None:
        state = self.sample_states.get(sample_id)
        if state is None:
            raise KeyError(f"Unknown sample '{sample_id}'")
        state.completed_processes.add(process)

    def pending_processes(self, sample_id: str) -> Tuple[ProcessType, ...]:
        sample = self.samples.get(sample_id)
        if sample is None:
            raise KeyError(f"Unknown sample '{sample_id}'")
        state = self.sample_states.get(sample_id)
        if state is None:
            raise KeyError(f"Unknown sample state '{sample_id}'")
        pending: List[ProcessType] = []
        for proc in sample.required_processes:
            if proc not in state.completed_processes:
                pending.append(proc)
        return tuple(pending)

    def classify_sample(
        self,
        sample_id: str,
        *,
        recognized: bool,
        classification_source: str,
        barcode: Optional[str] = None,
        required_processes: Optional[Sequence[ProcessType]] = None,
        assigned_route: str = "",
        assigned_route_station_slot_id: str = "",
        assigned_route_rack_index: Optional[int] = None,
        classification_details: Optional[Dict[str, Any]] = None,
    ) -> None:
        sample = self.samples.get(sample_id)
        if sample is None:
            raise KeyError(f"Unknown sample '{sample_id}'")
        state = self.sample_states.get(sample_id)
        if state is None:
            raise KeyError(f"Unknown sample state '{sample_id}'")

        next_barcode = str(barcode).strip() if barcode is not None else sample.barcode
        if not next_barcode:
            next_barcode = sample.barcode

        if required_processes is None:
            next_required = sample.required_processes
        else:
            dedup: List[ProcessType] = []
            seen: Set[ProcessType] = set()
            for proc in required_processes:
                if proc in seen:
                    continue
                dedup.append(proc)
                seen.add(proc)
            next_required = tuple(dedup)

        self.samples[sample_id] = Sample(
            id=sample.id,
            barcode=next_barcode,
            obj_type=sample.obj_type,
            length_mm=sample.length_mm,
            diameter_mm=sample.diameter_mm,
            cap_state=sample.cap_state,
            required_processes=next_required,
        )

        state.classification_status = (
            SampleClassificationStatus.RECOGNIZED
            if recognized
            else SampleClassificationStatus.UNRECOGNIZED
        )
        state.classification_source = str(classification_source or "")
        state.assigned_route = str(assigned_route or "")
        state.assigned_route_station_slot_id = str(assigned_route_station_slot_id or "")
        state.assigned_route_rack_index = (
            int(assigned_route_rack_index)
            if assigned_route_rack_index is not None
            else None
        )
        state.classification_details = dict(classification_details or {})

    def occupancy_snapshot(self) -> Dict[str, Dict[str, Any]]:
        snapshot: Dict[str, Dict[str, Any]] = {}
        for (station_id, station_slot_id), rack_id in sorted(self.rack_placements.items()):
            rack = self.racks.get(rack_id)
            if rack is None:
                continue
            key = f"{station_id}.{station_slot_id}"
            snapshot[key] = {
                "station_id": station_id,
                "station_slot_id": station_slot_id,
                "rack_id": rack_id,
                "pattern": rack.pattern,
                "rows": rack.rows,
                "cols": rack.cols,
                "blocked_slots": sorted(rack.blocked_slots),
                "occupied_slots": {str(k): v for k, v in sorted(rack.occupied_slots.items())},
                "reserved_slots": {str(k): v for k, v in sorted(rack.reserved_slots.items())},
            }
        if self.rack_in_gripper_id:
            rack = self.racks.get(self.rack_in_gripper_id)
            if rack is not None:
                snapshot["uLM_GRIPPER.RackGrip"] = {
                    "station_id": "uLM_GRIPPER",
                    "station_slot_id": "RackGrip",
                    "rack_id": rack.id,
                    "pattern": rack.pattern,
                    "rows": rack.rows,
                    "cols": rack.cols,
                    "blocked_slots": sorted(rack.blocked_slots),
                    "occupied_slots": {str(k): v for k, v in sorted(rack.occupied_slots.items())},
                    "reserved_slots": {str(k): v for k, v in sorted(rack.reserved_slots.items())},
                }
        return snapshot


def _as_enum(enum_cls: type[Enum], value: Any) -> Enum:
    if isinstance(value, enum_cls):
        return value
    raw = str(value).strip()
    try:
        return enum_cls(raw)
    except ValueError:
        return enum_cls(raw.upper())


def _as_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return bool(default)
    txt = str(value).strip().lower()
    if txt in {"1", "true", "yes", "on"}:
        return True
    if txt in {"0", "false", "no", "off"}:
        return False
    return bool(default)


def _to_list(section: Any) -> List[Dict[str, Any]]:
    if section is None:
        return []
    if isinstance(section, list):
        return [item for item in section if isinstance(item, dict)]
    if isinstance(section, dict):
        return [item for item in section.values() if isinstance(item, dict)]
    return []


def _location_to_config(location: SampleLocation) -> Dict[str, Any]:
    if isinstance(location, RackLocation):
        return {
            "type": "RACK",
            "station_id": location.station_id,
            "station_slot_id": location.station_slot_id,
            "rack_id": location.rack_id,
            "slot_index": location.slot_index,
        }
    return {"type": "GRIPPER", "gripper_id": location.gripper_id}


def _location_from_config(raw: Dict[str, Any]) -> SampleLocation:
    location_type = str(raw.get("type", "RACK")).upper()
    if location_type == "GRIPPER":
        return GripperLocation(gripper_id=str(raw.get("gripper_id", "uLM_GRIPPER")))
    return RackLocation(
        station_id=str(raw.get("station_id")),
        station_slot_id=str(raw.get("station_slot_id")),
        rack_id=str(raw.get("rack_id")),
        slot_index=int(raw.get("slot_index")),
    )


def _cap_location_to_config(location: CapLocation) -> Dict[str, Any]:
    if isinstance(location, CapOnSampleLocation):
        return {
            "type": CapLocationType.ON_SAMPLE.value,
            "sample_id": str(location.sample_id),
        }
    return {
        "type": CapLocationType.STORED.value,
        "station_id": str(location.station_id),
        "station_slot_id": str(location.station_slot_id),
        "rack_id": str(location.rack_id),
        "slot_index": int(location.slot_index),
    }


def _cap_location_from_config(raw: Dict[str, Any]) -> CapLocation:
    location_type = str(raw.get("type", CapLocationType.ON_SAMPLE.value)).strip().upper()
    if location_type == CapLocationType.STORED.value:
        return StoredCapLocation(
            station_id=str(raw.get("station_id", "")).strip(),
            station_slot_id=str(raw.get("station_slot_id", "")).strip(),
            rack_id=str(raw.get("rack_id", "")).strip(),
            slot_index=int(raw.get("slot_index")),
        )
    return CapOnSampleLocation(sample_id=str(raw.get("sample_id", "")).strip())


def default_world_config() -> Dict[str, Any]:
    return {
        "stations": [
            {
                "id": "InputStation",
                "name": "InputStation",
                "itm_id": 2,
                "kind": "EXTERNAL",
                "amr_pos_target": "2",
                "landmark_id": "LM_INPUT_001",
                "slot_configs": [
                    {
                        "slot_id": "URGRackSlot1",
                        "kind": "URG_RACK_SLOT",
                        "jig_id": 1,
                        "itm_id": 2,
                        "rack_capacity": 1,
                        "rack_pattern": "1x1",
                        "rack_rows": 1,
                        "rack_cols": 1,
                        "rack_index": 1,
                        "obj_nbr_offset": 0,
                        "accepted_rack_types": ["URG_RACK"],
                    },
                    {
                        "slot_id": "URGRackSlot2",
                        "kind": "URG_RACK_SLOT",
                        "jig_id": 1,
                        "itm_id": 2,
                        "rack_capacity": 1,
                        "rack_pattern": "1x1",
                        "rack_rows": 1,
                        "rack_cols": 1,
                        "rack_index": 2,
                        "obj_nbr_offset": 1,
                        "accepted_rack_types": ["URG_RACK"],
                    }
                ],
            },
            {
                "id": "CHARGE",
                "name": "CHARGE",
                "itm_id": 3,
                "kind": "EXTERNAL",
                "amr_pos_target": "3",
                "landmark_id": None,
                "slot_configs": [],
            },
            {
                "id": "CentrifugeStation",
                "name": "CentrifugeStation",
                "itm_id": 5,
                "kind": "EXTERNAL",
                "amr_pos_target": "5",
                "landmark_id": "LM_CENTRIFUGE_001",
                "slot_configs": [
                    {
                        "slot_id": "CentrifugeRacksSlot1",
                        "kind": "CENTRIFUGE_RACK_SLOT",
                        "jig_id": 2,
                        "itm_id": 5,
                        "rack_capacity": 1,
                        "rack_pattern": "1x1",
                        "rack_rows": 1,
                        "rack_cols": 1,
                        "rack_index": 1,
                        "obj_nbr_offset": 0,
                        "accepted_rack_types": ["CENTRIFUGE_RACK"],
                    },
                    {
                        "slot_id": "CentrifugeRacksSlot2",
                        "kind": "CENTRIFUGE_RACK_SLOT",
                        "jig_id": 2,
                        "itm_id": 5,
                        "rack_capacity": 1,
                        "rack_pattern": "1x1",
                        "rack_rows": 1,
                        "rack_cols": 1,
                        "rack_index": 2,
                        "obj_nbr_offset": 0,
                        "accepted_rack_types": ["CENTRIFUGE_RACK"],
                    },
                    {
                        "slot_id": "CentrifugeRacksSlot3",
                        "kind": "CENTRIFUGE_RACK_SLOT",
                        "jig_id": 2,
                        "itm_id": 5,
                        "rack_capacity": 1,
                        "rack_pattern": "1x1",
                        "rack_rows": 1,
                        "rack_cols": 1,
                        "rack_index": 3,
                        "obj_nbr_offset": 0,
                        "accepted_rack_types": ["CENTRIFUGE_RACK"],
                    },
                    {
                        "slot_id": "CentrifugeRacksSlot4",
                        "kind": "CENTRIFUGE_RACK_SLOT",
                        "jig_id": 2,
                        "itm_id": 5,
                        "rack_capacity": 1,
                        "rack_pattern": "1x1",
                        "rack_rows": 1,
                        "rack_cols": 1,
                        "rack_index": 4,
                        "obj_nbr_offset": 0,
                        "accepted_rack_types": ["CENTRIFUGE_RACK"],
                    },
                ],
            },
            {
                "id": "ArchiveStation",
                "name": "ArchiveStation",
                "itm_id": 9,
                "kind": "EXTERNAL",
                "amr_pos_target": "9",
                "landmark_id": "LM_ARCHIVE_001",
                "slot_configs": [
                    {
                        "slot_id": "URGRackSlot",
                        "kind": "URG_RACK_SLOT",
                        "jig_id": 1,
                        "itm_id": 9,
                        "rack_capacity": 1,
                        "rack_pattern": "1x1",
                        "rack_rows": 1,
                        "rack_cols": 1,
                        "rack_index": 1,
                        "obj_nbr_offset": 0,
                        "accepted_rack_types": ["URG_RACK"],
                    }
                ],
            },
            {
                "id": "FridgeStation",
                "name": "FridgeStation",
                "itm_id": 8,
                "kind": "EXTERNAL",
                "amr_pos_target": "8",
                "landmark_id": "LM_FRIDGE_001",
                "slot_configs": [
                    {
                        "slot_id": "URGFridgeRackSlot1",
                        "kind": "FRIDGE_URG_RACK_SLOT",
                        "jig_id": 13,
                        "itm_id": 8,
                        "rack_capacity": 1,
                        "rack_pattern": "1x1",
                        "rack_rows": 1,
                        "rack_cols": 1,
                        "rack_index": 1,
                        "obj_nbr_offset": 0,
                        "accepted_rack_types": ["FRIDGE_URG_RACK"],
                    },
                    {
                        "slot_id": "URGFridgeRackSlot2",
                        "kind": "FRIDGE_URG_RACK_SLOT",
                        "jig_id": 13,
                        "itm_id": 8,
                        "rack_capacity": 1,
                        "rack_pattern": "1x1",
                        "rack_rows": 1,
                        "rack_cols": 1,
                        "rack_index": 2,
                        "obj_nbr_offset": 42,
                        "accepted_rack_types": ["FRIDGE_URG_RACK"],
                    },
                    {
                        "slot_id": "URGFridgeRackSlot3",
                        "kind": "FRIDGE_URG_RACK_SLOT",
                        "jig_id": 13,
                        "itm_id": 8,
                        "rack_capacity": 1,
                        "rack_pattern": "1x1",
                        "rack_rows": 1,
                        "rack_cols": 1,
                        "rack_index": 3,
                        "obj_nbr_offset": 84,
                        "accepted_rack_types": ["FRIDGE_URG_RACK"],
                    },
                ],
            },
            {
                "id": "BioRadIH500Station",
                "name": "BioRadIH500Station",
                "itm_id": 7,
                "kind": "EXTERNAL",
                "amr_pos_target": "7",
                "landmark_id": "LM_IH500_001",
                "slot_configs": [
                    {
                        "slot_id": "BioRadIH500Slot1",
                        "kind": "BIORAD_IH500_RACK_SLOT",
                        "jig_id": 50,
                        "itm_id": 7,
                        "rack_capacity": 3,
                        "rack_pattern": "1x3",
                        "rack_rows": 1,
                        "rack_cols": 3,
                        "rack_index": 1,
                        "obj_nbr_offset": 0,
                        "accepted_rack_types": ["BIORAD_IH500_RACK"],
                    },
                    {
                        "slot_id": "BioRadIH500Slot2",
                        "kind": "BIORAD_IH500_RACK_SLOT",
                        "jig_id": 50,
                        "itm_id": 7,
                        "rack_capacity": 3,
                        "rack_pattern": "1x3",
                        "rack_rows": 1,
                        "rack_cols": 3,
                        "rack_index": 2,
                        "obj_nbr_offset": 12,
                        "accepted_rack_types": ["BIORAD_IH500_RACK"],
                    },
                    {
                        "slot_id": "BioRadIH500Slot3",
                        "kind": "BIORAD_IH500_RACK_SLOT",
                        "jig_id": 50,
                        "itm_id": 7,
                        "rack_capacity": 3,
                        "rack_pattern": "1x3",
                        "rack_rows": 1,
                        "rack_cols": 3,
                        "rack_index": 3,
                        "obj_nbr_offset": 24,
                        "accepted_rack_types": ["BIORAD_IH500_RACK"],
                    },
                ],
            },
            {
                "id": "BioRadIH1000Station",
                "name": "BioRadIH1000Station",
                "itm_id": 6,
                "kind": "EXTERNAL",
                "amr_pos_target": "6",
                "landmark_id": "LM_IH1000_001",
                "slot_configs": [],
            },
            {
                "id": "uLMPlateStation",
                "name": "uLMPlateStation",
                "itm_id": 1,
                "kind": "ON_ROBOT_PLATE",
                "amr_pos_target": None,
                "landmark_id": None,
                "slot_configs": [
                    {
                        "slot_id": "URGRackSlot",
                        "kind": "URG_RACK_SLOT",
                        "jig_id": 1,
                        "itm_id": 1,
                        "rack_capacity": 1,
                        "rack_pattern": "1x1",
                        "rack_rows": 1,
                        "rack_cols": 1,
                        "rack_index": 1,
                        "obj_nbr_offset": 0,
                        "accepted_rack_types": ["URG_RACK"],
                    },
                    {
                        "slot_id": "CentrifugeRacksSlot1",
                        "kind": "CENTRIFUGE_RACK_SLOT",
                        "jig_id": 2,
                        "itm_id": 1,
                        "rack_capacity": 4,
                        "rack_pattern": "1x4",
                        "rack_rows": 1,
                        "rack_cols": 4,
                        "rack_index": 1,
                        "obj_nbr_offset": 0,
                        "accepted_rack_types": ["CENTRIFUGE_RACK"],
                    },
                    {
                        "slot_id": "CentrifugeRacksSlot2",
                        "kind": "CENTRIFUGE_RACK_SLOT",
                        "jig_id": 2,
                        "itm_id": 1,
                        "rack_capacity": 4,
                        "rack_pattern": "1x4",
                        "rack_rows": 1,
                        "rack_cols": 4,
                        "rack_index": 2,
                        "obj_nbr_offset": 9,
                        "accepted_rack_types": ["CENTRIFUGE_RACK"],
                    },
                    {
                        "slot_id": "CentrifugeRacksSlot3",
                        "kind": "CENTRIFUGE_RACK_SLOT",
                        "jig_id": 2,
                        "itm_id": 1,
                        "rack_capacity": 4,
                        "rack_pattern": "1x4",
                        "rack_rows": 1,
                        "rack_cols": 4,
                        "rack_index": 3,
                        "obj_nbr_offset": 18,
                        "accepted_rack_types": ["CENTRIFUGE_RACK"],
                    },
                    {
                        "slot_id": "CentrifugeRacksSlot4",
                        "kind": "CENTRIFUGE_RACK_SLOT",
                        "jig_id": 2,
                        "itm_id": 1,
                        "rack_capacity": 4,
                        "rack_pattern": "1x4",
                        "rack_rows": 1,
                        "rack_cols": 4,
                        "rack_index": 4,
                        "obj_nbr_offset": 27,
                        "accepted_rack_types": ["CENTRIFUGE_RACK"],
                    },
                    {
                        "slot_id": "TaraRacksSlot1",
                        "kind": "TARA_RACK_SLOT",
                        "jig_id": 3,
                        "itm_id": 1,
                        "rack_capacity": 1,
                        "rack_pattern": "1x1",
                        "rack_rows": 1,
                        "rack_cols": 1,
                        "rack_index": 1,
                        "obj_nbr_offset": 0,
                        "accepted_rack_types": ["TARA_RACK"],
                    },
                    {
                        "slot_id": "IntermediateRackSlot1",
                        "kind": "INTERMEDIATE_RACK_SLOT",
                        "jig_id": 4,
                        "itm_id": 1,
                        "rack_capacity": 1,
                        "rack_pattern": "1x1",
                        "rack_rows": 1,
                        "rack_cols": 1,
                        "rack_index": 1,
                        "obj_nbr_offset": 0,
                        "accepted_rack_types": ["INTERMEDIATE_RACK"],
                    },
                    {
                        "slot_id": "URGFridgeRackSlot",
                        "kind": "FRIDGE_URG_RACK_SLOT",
                        "jig_id": 13,
                        "itm_id": 1,
                        "rack_capacity": 1,
                        "rack_pattern": "1x1",
                        "rack_rows": 1,
                        "rack_cols": 1,
                        "rack_index": 1,
                        "obj_nbr_offset": 0,
                        "accepted_rack_types": ["FRIDGE_URG_RACK"],
                    },
                    {
                        "slot_id": "BioRadIH500Slot1",
                        "kind": "BIORAD_IH500_RACK_SLOT",
                        "jig_id": 12,
                        "itm_id": 1,
                        "rack_capacity": 3,
                        "rack_pattern": "1x3",
                        "rack_rows": 1,
                        "rack_cols": 3,
                        "rack_index": 1,
                        "obj_nbr_offset": 0,
                        "accepted_rack_types": ["BIORAD_IH500_RACK"],
                    },
                    {
                        "slot_id": "BioRadIH500Slot2",
                        "kind": "BIORAD_IH500_RACK_SLOT",
                        "jig_id": 12,
                        "itm_id": 1,
                        "rack_capacity": 3,
                        "rack_pattern": "1x3",
                        "rack_rows": 1,
                        "rack_cols": 3,
                        "rack_index": 2,
                        "obj_nbr_offset": 12,
                        "accepted_rack_types": ["BIORAD_IH500_RACK"],
                    },
                    {
                        "slot_id": "BioRadIH500Slot3",
                        "kind": "BIORAD_IH500_RACK_SLOT",
                        "jig_id": 12,
                        "itm_id": 1,
                        "rack_capacity": 3,
                        "rack_pattern": "1x3",
                        "rack_rows": 1,
                        "rack_cols": 3,
                        "rack_index": 3,
                        "obj_nbr_offset": 24,
                        "accepted_rack_types": ["BIORAD_IH500_RACK"],
                    },
                    {
                        "slot_id": "BioRadIH1000Slot1",
                        "kind": "BIORAD_IH1000_RACK_SLOT",
                        "jig_id": 11,
                        "itm_id": 1,
                        "rack_capacity": 3,
                        "rack_pattern": "1x3",
                        "rack_rows": 1,
                        "rack_cols": 3,
                        "rack_index": 1,
                        "obj_nbr_offset": 0,
                        "accepted_rack_types": ["BIORAD_IH1000_RACK"],
                    },
                    {
                        "slot_id": "BioRadIH1000Slot2",
                        "kind": "BIORAD_IH1000_RACK_SLOT",
                        "jig_id": 11,
                        "itm_id": 1,
                        "rack_capacity": 3,
                        "rack_pattern": "1x3",
                        "rack_rows": 1,
                        "rack_cols": 3,
                        "rack_index": 2,
                        "obj_nbr_offset": 12,
                        "accepted_rack_types": ["BIORAD_IH1000_RACK"],
                    },
                    {
                        "slot_id": "BioRadIH1000Slot3",
                        "kind": "BIORAD_IH1000_RACK_SLOT",
                        "jig_id": 11,
                        "itm_id": 1,
                        "rack_capacity": 3,
                        "rack_pattern": "1x3",
                        "rack_rows": 1,
                        "rack_cols": 3,
                        "rack_index": 3,
                        "obj_nbr_offset": 24,
                        "accepted_rack_types": ["BIORAD_IH1000_RACK"],
                    },
                ],
            },
            {
                "id": "3-FingerGripperStation",
                "name": "3-FingerGripperStation",
                "itm_id": 1,
                "kind": "ON_ROBOT_PLATE",
                "amr_pos_target": None,
                "landmark_id": None,
                "slot_configs": [
                    {
                        "slot_id": "SampleSlot1",
                        "kind": "THREE_FINGER_GRIPPER_SAMPLE_SLOT",
                        "jig_id": 10,
                        "itm_id": 1,
                        "rack_capacity": 1,
                        "rack_pattern": "1x1",
                        "rack_rows": 1,
                        "rack_cols": 1,
                        "rack_index": 1,
                        "obj_nbr_offset": 0,
                        "accepted_rack_types": ["THREE_FINGER_GRIPPER_SAMPLE_HOLDER"],
                    },
                    {
                        "slot_id": "RecapCapsSlot",
                        "kind": "THREE_FINGER_GRIPPER_SAMPLE_SLOT",
                        "jig_id": 14,
                        "itm_id": 1,
                        "rack_capacity": 9,
                        "rack_pattern": "3x3",
                        "rack_rows": 3,
                        "rack_cols": 3,
                        "rack_index": 1,
                        "obj_nbr_offset": 0,
                        "accepted_rack_types": ["THREE_FINGER_GRIPPER_SAMPLE_HOLDER"],
                    },
                    {
                        "slot_id": "KreuzprobeRecapCapsSlot",
                        "kind": "THREE_FINGER_GRIPPER_SAMPLE_SLOT",
                        "jig_id": 15,
                        "itm_id": 1,
                        "rack_capacity": 9,
                        "rack_pattern": "3x3",
                        "rack_rows": 3,
                        "rack_cols": 3,
                        "rack_index": 1,
                        "obj_nbr_offset": 0,
                        "accepted_rack_types": ["THREE_FINGER_GRIPPER_SAMPLE_HOLDER"],
                    }
                ],
            },
        ],
        "landmarks": [
            {"id": "LM_INPUT_001", "code": "LM_INPUT_001", "station_id": "InputStation"},
            {"id": "LM_CENTRIFUGE_001", "code": "LM_CENTRIFUGE_001", "station_id": "CentrifugeStation"},
            {"id": "LM_ARCHIVE_001", "code": "LM_ARCHIVE_001", "station_id": "ArchiveStation"},
            {"id": "LM_FRIDGE_001", "code": "LM_FRIDGE_001", "station_id": "FridgeStation"},
            {"id": "LM_IH500_001", "code": "LM_IH500_001", "station_id": "BioRadIH500Station"},
            {"id": "LM_IH1000_001", "code": "LM_IH1000_001", "station_id": "BioRadIH1000Station"},
        ],
        "racks": [
            {
                "id": "RACK_INPUT_URG_01",
                "rack_type": "URG_RACK",
                "capacity": 32,
                "pattern": "URG_4x8_PIN2",
                "pin_obj_type": 9001,
                "rows": 8,
                "cols": 4,
                "blocked_slots": [14, 19],
            },
            {
                "id": "RACK_ULM_CENTRIFUGE_01",
                "rack_type": "CENTRIFUGE_RACK",
                "capacity": 9,
                "pattern": "CENTRIFUGE_3x3_PIN_CENTER",
                "pin_obj_type": 9002,
                "rows": 3,
                "cols": 3,
                "blocked_slots": [5],
            },
            {
                "id": "RACK_ULM_CENTRIFUGE_02",
                "rack_type": "CENTRIFUGE_RACK",
                "capacity": 9,
                "pattern": "CENTRIFUGE_3x3_PIN_CENTER",
                "pin_obj_type": 9002,
                "rows": 3,
                "cols": 3,
                "blocked_slots": [5],
            },
            {
                "id": "RACK_ULM_CENTRIFUGE_03",
                "rack_type": "CENTRIFUGE_RACK",
                "capacity": 9,
                "pattern": "CENTRIFUGE_3x3_PIN_CENTER",
                "pin_obj_type": 9002,
                "rows": 3,
                "cols": 3,
                "blocked_slots": [5],
            },
            {
                "id": "RACK_ULM_CENTRIFUGE_04",
                "rack_type": "CENTRIFUGE_RACK",
                "capacity": 9,
                "pattern": "CENTRIFUGE_3x3_PIN_CENTER",
                "pin_obj_type": 9002,
                "rows": 3,
                "cols": 3,
                "blocked_slots": [5],
            },
            {
                "id": "RACK_ULM_TARA_01",
                "rack_type": "TARA_RACK",
                "capacity": 3,
                "pattern": "1x3",
                "pin_obj_type": 9010,
                "rows": 1,
                "cols": 3,
                "blocked_slots": [],
                "occupied_slots": {
                    1: "DUMMY_0001",
                    2: "DUMMY_0002",
                    3: "DUMMY_0003",
                },
            },
            {
                "id": "RACK_ULM_INTERMEDIATE_01",
                "rack_type": "INTERMEDIATE_RACK",
                "capacity": 24,
                "pattern": "INTERMEDIATE_4x6",
                "pin_obj_type": 9011,
                "rows": 4,
                "cols": 6,
                "blocked_slots": [],
            },
            {
                "id": "RACK_ULM_BIORAD_IH500_01",
                "rack_type": "BIORAD_IH500_RACK",
                "capacity": 13,
                "pattern": "BIORAD_IH500_1x13_PIN3",
                "pin_obj_type": 520,
                "rows": 1,
                "cols": 13,
                "blocked_slots": [3, 7, 10],
            },
            {
                "id": "RACK_ULM_BIORAD_IH500_02",
                "rack_type": "BIORAD_IH500_RACK",
                "capacity": 13,
                "pattern": "BIORAD_IH500_1x13_PIN3",
                "pin_obj_type": 520,
                "rows": 1,
                "cols": 13,
                "blocked_slots": [3, 7, 10],
            },
            {
                "id": "RACK_ULM_BIORAD_IH500_03",
                "rack_type": "BIORAD_IH500_RACK",
                "capacity": 13,
                "pattern": "BIORAD_IH500_1x13_PIN3",
                "pin_obj_type": 520,
                "rows": 1,
                "cols": 13,
                "blocked_slots": [3, 7, 10],
            },
            {
                "id": "RACK_ULM_BIORAD_IH1000_01",
                "rack_type": "BIORAD_IH1000_RACK",
                "capacity": 12,
                "pattern": "BIORAD_IH1000_1x12_PIN2",
                "pin_obj_type": 9013,
                "rows": 1,
                "cols": 12,
                "blocked_slots": [5, 7],
            },
            {
                "id": "RACK_ULM_BIORAD_IH1000_02",
                "rack_type": "BIORAD_IH1000_RACK",
                "capacity": 12,
                "pattern": "BIORAD_IH1000_1x12_PIN2",
                "pin_obj_type": 9013,
                "rows": 1,
                "cols": 12,
                "blocked_slots": [5, 7],
            },
            {
                "id": "RACK_ULM_BIORAD_IH1000_03",
                "rack_type": "BIORAD_IH1000_RACK",
                "capacity": 12,
                "pattern": "BIORAD_IH1000_1x12_PIN2",
                "pin_obj_type": 9013,
                "rows": 1,
                "cols": 12,
                "blocked_slots": [5, 7],
            },
            {
                "id": "RACK_FRIDGE_URG_4x11_01",
                "rack_type": "FRIDGE_URG_RACK",
                "capacity": 44,
                "pattern": "ARCHIVE_4x11_PIN2",
                "pin_obj_type": 9015,
                "rows": 11,
                "cols": 4,
                "blocked_slots": [12, 23],
            },
            {
                "id": "RACK_ARCHIVE_URG_4x8_01",
                "rack_type": "URG_RACK",
                "capacity": 32,
                "pattern": "URG_4x8_PIN2",
                "pin_obj_type": 551,
                "rows": 8,
                "cols": 4,
                "blocked_slots": [14, 19],
            },
            {
                "id": "RACK_3FG_SAMPLE_HOLDER_01",
                "rack_type": "THREE_FINGER_GRIPPER_SAMPLE_HOLDER",
                "capacity": 1,
                "pattern": "THREE_FINGER_GRIPPER_1x1",
                "pin_obj_type": 9014,
                "rows": 1,
                "cols": 1,
                "blocked_slots": [],
            },
            {
                "id": "RACK_3FG_RECAP_CAPS_01",
                "rack_type": "THREE_FINGER_GRIPPER_SAMPLE_HOLDER",
                "capacity": 9,
                "pattern": "THREE_FINGER_GRIPPER_CAP_HOLDER_3x3",
                "pin_obj_type": 9014,
                "rows": 3,
                "cols": 3,
                "blocked_slots": [],
            },
            {
                "id": "RACK_3FG_KREUZ_CAPS_01",
                "rack_type": "THREE_FINGER_GRIPPER_SAMPLE_HOLDER",
                "capacity": 9,
                "pattern": "THREE_FINGER_GRIPPER_CAP_HOLDER_3x3",
                "pin_obj_type": 9014,
                "rows": 3,
                "cols": 3,
                "blocked_slots": [],
            },
        ],
        "devices": [
            {
                "id": "CENTRIFUGE_DEVICE_01",
                "name": "Centrifuge",
                "station_id": "CentrifugeStation",
                "model": "Rotina380R",
                "device_class": "HettichRotina380RDevice",
                "capabilities": ["CENTRIFUGATION"],
                "planner_role": "PROCESSOR",
                "exclude_station_racks_from_idle_return": True,
                "device_capabilities": {
                    "supported_processes": ["CENTRIFUGATION"],
                    "refrigerated": True,
                    "automatic_rotor_recognition": True,
                    "powered_lid_lock": True,
                    "imbalance_detection": True,
                    "interfaces": ["RS232", "LOCAL_UI"],
                },
                "rotor_configuration": {
                    "rotor_id": "GENERIC_ROTOR",
                    "rotor_type": "CONFIGURABLE",
                    "positions": [
                        {"index": 1, "angle_deg": 0.0, "opposite_index": 3},
                        {"index": 2, "angle_deg": 90.0, "opposite_index": 4},
                        {"index": 3, "angle_deg": 180.0, "opposite_index": 1},
                        {"index": 4, "angle_deg": 270.0, "opposite_index": 2},
                    ],
                    "buckets": [],
                    "adapters": [],
                },
                "balance_model": {
                    "rule_type": "OPPOSITE_POSITION",
                    "require_symmetry": True,
                    "tolerance_g": None,
                    "max_imbalance_g": None,
                },
                "lid_control_strategy": {
                    "type": "manual",
                    "method": "local_ui_or_api",
                },
                "start_strategy": {
                    "type": "manual",
                    "method": "local_ui_or_api",
                },
                "status_strategy": {
                    "type": "in_memory",
                    "source": "status_light_or_rs232",
                    "state_map": {},
                },
                "usage_profile": {
                    "type": "Rotina380UsageProfile",
                    "source_station_id": "uLMPlateStation",
                    "centrifuge_station_id": "CentrifugeStation",
                    "fixed_receiver_obj_nbr": 1,
                    "target_loading_jig_id": 2,
                    "tara_probe_jig_id": 3,
                    "enable_tara_balancing": True,
                    "return_tara_probes_on_unload": True,
                },
            },
            {
                "id": "THREE_FINGER_GRIPPER_DEVICE_01",
                "name": "3-FingerGripper",
                "station_id": "3-FingerGripperStation",
                "capabilities": ["CAP", "DECAP", "SAMPLE_TYPE_DETECTION"],
                "planner_role": "PROCESSOR",
                "exclude_station_racks_from_idle_return": True,
            },
            {
                "id": "INPUT_STATION_WISE_DEVICE_01",
                "name": "InputStation Wise Module",
                "station_id": "InputStation",
                "capabilities": [],
                "planner_role": "SENSOR",
                "exclude_station_racks_from_idle_return": False,
                "wise": {
                    "enabled": True,
                    "host": "192.168.137.101",
                    "port": 80,
                    "scheme": "http",
                    "auth": {
                        "username": "root",
                        "password": "12345678",
                    },
                    "di_slot": 0,
                    "di_endpoint_template": "/di_value/slot_{slot}",
                    "timeout_s": 3.0,
                    "poll_interval_s": 1.0,
                    "stale_after_s": 6.0,
                    "verify_tls": True,
                    "slot_ready_channels": {
                        "1": 0,
                    },
                },
            },
            {
                "id": "BIORAD_IH500_DEVICE_01",
                "name": "BioRad IH-500",
                "station_id": "BioRadIH500Station",
                "model": "IH500",
                "device_class": "BioradIh500Device",
                "capabilities": ["IMMUNOHEMATOLOGY_ANALYSIS"],
                "planner_role": "PROCESSOR",
                "exclude_station_racks_from_idle_return": True,
                "device_capabilities": {
                    "supported_processes": ["IMMUNOHEMATOLOGY_ANALYSIS"],
                    "continuous_loading": True,
                    "auto_start": False,
                    "nominal_sample_capacity": 50,
                },
                "load_interface": {
                    "carrier_type": "RACK",
                    "loading_area": "SEPARATE_LOADING_AREA",
                    "rack_geometry": {},
                    "slot_layout": {},
                },
                "start_strategy": {
                    "type": "manual",
                    "method": "status_light_or_interface",
                },
                "status_strategy": {
                    "type": "in_memory",
                    "source": "status_light",
                    "state_map": {},
                },
                "wise": {
                    "enabled": False,
                    "host": "",
                    "port": 80,
                    "scheme": "http",
                    "auth": {
                        "username": "",
                        "password": "",
                    },
                    "di_slot": 0,
                    "di_endpoint_template": "/iocard/{slot}/di",
                    "timeout_s": 1.5,
                    "poll_interval_s": 1.0,
                    "stale_after_s": 5.0,
                    "verify_tls": True,
                    "enforce_ready_for_unload": False,
                    "required_for_selection": False,
                    "required_for_processes": ["IMMUNOHEMATOLOGY_ANALYSIS"],
                    "rack_ready_channels": {
                        "1": 0,
                        "2": 1,
                        "3": 2,
                    },
                },
            },
            {
                "id": "BIORAD_IH1000_DEVICE_01",
                "name": "BioRad IH-1000",
                "station_id": "BioRadIH1000Station",
                "model": "IH1000",
                "device_class": "BioradIh1000Device",
                "capabilities": ["IMMUNOHEMATOLOGY_ANALYSIS"],
                "planner_role": "PROCESSOR",
                "exclude_station_racks_from_idle_return": True,
                "device_capabilities": {
                    "supported_processes": ["IMMUNOHEMATOLOGY_ANALYSIS"],
                    "continuous_loading": True,
                    "auto_start": True,
                    "nominal_sample_capacity": 180,
                },
                "load_interface": {
                    "carrier_type": "RACK",
                    "loading_area": "MAIN_LOADING_AREA",
                    "rack_geometry": {},
                    "slot_layout": {},
                },
                "start_strategy": {
                    "type": "automatic",
                    "method": "auto_start_documented",
                },
                "status_strategy": {
                    "type": "in_memory",
                    "source": "status_light_or_interface",
                    "state_map": {},
                },
            },
        ],
        "rack_placements": [
            {"station_id": "InputStation", "station_slot_id": "URGRackSlot2", "rack_id": "RACK_INPUT_URG_01"},
            {"station_id": "uLMPlateStation", "station_slot_id": "CentrifugeRacksSlot1", "rack_id": "RACK_ULM_CENTRIFUGE_01"},
            {"station_id": "uLMPlateStation", "station_slot_id": "CentrifugeRacksSlot2", "rack_id": "RACK_ULM_CENTRIFUGE_02"},
            {"station_id": "uLMPlateStation", "station_slot_id": "CentrifugeRacksSlot3", "rack_id": "RACK_ULM_CENTRIFUGE_03"},
            {"station_id": "uLMPlateStation", "station_slot_id": "CentrifugeRacksSlot4", "rack_id": "RACK_ULM_CENTRIFUGE_04"},
            {"station_id": "uLMPlateStation", "station_slot_id": "TaraRacksSlot1", "rack_id": "RACK_ULM_TARA_01"},
            {"station_id": "uLMPlateStation", "station_slot_id": "IntermediateRackSlot1", "rack_id": "RACK_ULM_INTERMEDIATE_01"},
            {"station_id": "uLMPlateStation", "station_slot_id": "URGFridgeRackSlot", "rack_id": "RACK_FRIDGE_URG_4x11_01"},
            {"station_id": "ArchiveStation", "station_slot_id": "URGRackSlot", "rack_id": "RACK_ARCHIVE_URG_4x8_01"},
            {"station_id": "uLMPlateStation", "station_slot_id": "BioRadIH500Slot1", "rack_id": "RACK_ULM_BIORAD_IH500_01"},
            {"station_id": "uLMPlateStation", "station_slot_id": "BioRadIH500Slot2", "rack_id": "RACK_ULM_BIORAD_IH500_02"},
            {"station_id": "uLMPlateStation", "station_slot_id": "BioRadIH500Slot3", "rack_id": "RACK_ULM_BIORAD_IH500_03"},
            {"station_id": "uLMPlateStation", "station_slot_id": "BioRadIH1000Slot1", "rack_id": "RACK_ULM_BIORAD_IH1000_01"},
            {"station_id": "uLMPlateStation", "station_slot_id": "BioRadIH1000Slot2", "rack_id": "RACK_ULM_BIORAD_IH1000_02"},
            {"station_id": "uLMPlateStation", "station_slot_id": "BioRadIH1000Slot3", "rack_id": "RACK_ULM_BIORAD_IH1000_03"},
            {"station_id": "3-FingerGripperStation", "station_slot_id": "SampleSlot1", "rack_id": "RACK_3FG_SAMPLE_HOLDER_01"},
            {"station_id": "3-FingerGripperStation", "station_slot_id": "RecapCapsSlot", "rack_id": "RACK_3FG_RECAP_CAPS_01"},
            {"station_id": "3-FingerGripperStation", "station_slot_id": "KreuzprobeRecapCapsSlot", "rack_id": "RACK_3FG_KREUZ_CAPS_01"},
        ],
        "robot_current_station_id": "CHARGE",
        "samples": [
            {
                "id": "DUMMY_0001",
                "barcode": "DUMMY_0001",
                "obj_type": 810,
                "length_mm": 75.0,
                "diameter_mm": 13.0,
                "cap_state": "CAPPED",
                "required_processes": [],
            },
            {
                "id": "DUMMY_0002",
                "barcode": "DUMMY_0002",
                "obj_type": 810,
                "length_mm": 75.0,
                "diameter_mm": 13.0,
                "cap_state": "CAPPED",
                "required_processes": [],
            },
            {
                "id": "DUMMY_0003",
                "barcode": "DUMMY_0003",
                "obj_type": 810,
                "length_mm": 75.0,
                "diameter_mm": 13.0,
                "cap_state": "CAPPED",
                "required_processes": [],
            },
        ],
        "sample_states": [
            {
                "sample_id": "DUMMY_0001",
                "location": {
                    "type": "RACK",
                    "station_id": "uLMPlateStation",
                    "station_slot_id": "TaraRacksSlot1",
                    "rack_id": "RACK_ULM_TARA_01",
                    "slot_index": 1,
                },
                "completed_processes": [],
            },
            {
                "sample_id": "DUMMY_0002",
                "location": {
                    "type": "RACK",
                    "station_id": "uLMPlateStation",
                    "station_slot_id": "TaraRacksSlot1",
                    "rack_id": "RACK_ULM_TARA_01",
                    "slot_index": 2,
                },
                "completed_processes": [],
            },
            {
                "sample_id": "DUMMY_0003",
                "location": {
                    "type": "RACK",
                    "station_id": "uLMPlateStation",
                    "station_slot_id": "TaraRacksSlot1",
                    "rack_id": "RACK_ULM_TARA_01",
                    "slot_index": 3,
                },
                "completed_processes": [],
            },
        ],
        "caps": [],
        "cap_states": [],
    }


def world_from_config(config: Dict[str, Any]) -> WorldModel:
    stations: Dict[str, Station] = {}
    for raw_station in _to_list(config.get("stations")):
        station_id = str(raw_station["id"])
        station_itm_id = int(raw_station.get("itm_id", 1))
        station_kind = _as_enum(StationKind, raw_station["kind"])  # type: ignore[arg-type]
        raw_amr_pos_target = raw_station.get("amr_pos_target")
        amr_pos_target = None if raw_amr_pos_target is None else str(raw_amr_pos_target).strip()
        if amr_pos_target == "":
            amr_pos_target = None
        if station_kind != StationKind.ON_ROBOT_PLATE and amr_pos_target is None:
            raise ValueError(
                f"Station '{station_id}' must define non-empty numeric 'amr_pos_target' "
                f"because kind='{station_kind.value}'"
            )
        if station_kind != StationKind.ON_ROBOT_PLATE and amr_pos_target is not None:
            if not amr_pos_target.isdigit():
                raise ValueError(
                    f"Station '{station_id}' has invalid amr_pos_target='{amr_pos_target}'. "
                    f"Expected numeric string matching itm_id={station_itm_id}."
                )
            if int(amr_pos_target) != station_itm_id:
                raise ValueError(
                    f"Station '{station_id}' has amr_pos_target='{amr_pos_target}' "
                    f"but itm_id={station_itm_id}. They must match."
                )

        raw_slots = _to_list(raw_station.get("slot_configs"))
        slot_configs: Dict[str, RackSlotConfig] = {}
        for raw_slot in raw_slots:
            slot_id = str(raw_slot["slot_id"])
            accepted = frozenset(
                _as_enum(RackType, t) for t in raw_slot.get("accepted_rack_types", []) if t is not None
            )
            slot_configs[slot_id] = RackSlotConfig(
                slot_id=slot_id,
                kind=_as_enum(SlotKind, raw_slot["kind"]),  # type: ignore[arg-type]
                jig_id=int(raw_slot["jig_id"]),
                itm_id=int(raw_slot.get("itm_id", 1)),
                rack_capacity=int(raw_slot.get("rack_capacity", 1)),
                rack_pattern=None if raw_slot.get("rack_pattern") is None else str(raw_slot.get("rack_pattern")),
                rack_rows=None if raw_slot.get("rack_rows") is None else int(raw_slot.get("rack_rows")),
                rack_cols=None if raw_slot.get("rack_cols") is None else int(raw_slot.get("rack_cols")),
                rack_index=int(raw_slot.get("rack_index", 1)),
                obj_nbr_offset=int(raw_slot.get("obj_nbr_offset", 0)),
                loading_strategy=str(raw_slot.get("loading_strategy", "") or "").strip(),
                accepted_rack_types=accepted,
            )

        stations[station_id] = Station(
            id=station_id,
            name=str(raw_station.get("name", station_id)),
            itm_id=station_itm_id,
            kind=station_kind,
            amr_pos_target=amr_pos_target,
            slot_configs=slot_configs,
            landmark_id=None if raw_station.get("landmark_id") is None else str(raw_station.get("landmark_id")),
            linked_device_ids=tuple(
                sorted(
                    str(x)
                    for x in raw_station.get("linked_device_ids", [])
                    if str(x).strip()
                )
            ),
        )

    landmarks: Dict[str, Landmark] = {}
    for raw_landmark in _to_list(config.get("landmarks")):
        landmark = Landmark(
            id=str(raw_landmark["id"]),
            code=str(raw_landmark.get("code", raw_landmark["id"])),
            station_id=str(raw_landmark["station_id"]),
        )
        landmarks[landmark.id] = landmark

    racks: Dict[str, Rack] = {}
    for raw_rack in _to_list(config.get("racks")):
        rack = Rack(
            id=str(raw_rack["id"]),
            rack_type=_as_enum(RackType, raw_rack["rack_type"]),  # type: ignore[arg-type]
            capacity=int(raw_rack["capacity"]),
            pattern=str(raw_rack["pattern"]),
            pin_obj_type=int(raw_rack["pin_obj_type"]),
            rows=None if raw_rack.get("rows") is None else int(raw_rack.get("rows")),
            cols=None if raw_rack.get("cols") is None else int(raw_rack.get("cols")),
            blocked_slots={int(x) for x in raw_rack.get("blocked_slots", [])},
            occupied_slots={int(k): str(v) for k, v in dict(raw_rack.get("occupied_slots", {})).items()},
            reserved_slots={int(k): str(v) for k, v in dict(raw_rack.get("reserved_slots", {})).items()},
        )
        racks[rack.id] = rack

    devices: Dict[str, Device] = {}
    for raw_device in _to_list(config.get("devices")):
        caps = frozenset(_as_enum(ProcessType, c) for c in raw_device.get("capabilities", []))
        planner_role_raw = str(raw_device.get("planner_role", "")).strip().upper()
        planner_role = planner_role_raw if planner_role_raw else ("PROCESSOR" if caps else "SENSOR")
        default_exclude = planner_role == "PROCESSOR"
        if "exclude_station_racks_from_idle_return" in raw_device:
            exclude_station_racks = _as_bool(
                raw_device.get("exclude_station_racks_from_idle_return"),
                default=default_exclude,
            )
        else:
            exclude_station_racks = default_exclude
        metadata = {
            str(k): v
            for k, v in raw_device.items()
            if str(k)
            not in {
                "id",
                "name",
                "station_id",
                "capabilities",
                "planner_role",
                "exclude_station_racks_from_idle_return",
            }
        }
        device = Device(
            id=str(raw_device["id"]),
            name=str(raw_device.get("name", raw_device["id"])),
            station_id=str(raw_device["station_id"]),
            capabilities=caps,
            planner_role=planner_role,
            exclude_station_racks_from_idle_return=exclude_station_racks,
            metadata=metadata,
        )
        devices[device.id] = device
        if device.station_id not in stations:
            raise ValueError(
                f"Device '{device.id}' references unknown station '{device.station_id}'"
            )

    station_device_links: Dict[str, Set[str]] = {
        station_id: set(st.linked_device_ids) for station_id, st in stations.items()
    }
    for dev in devices.values():
        if dev.station_id in station_device_links:
            station_device_links[dev.station_id].add(dev.id)
    for station_id, linked_ids in station_device_links.items():
        stations[station_id].linked_device_ids = tuple(sorted(linked_ids))

    samples: Dict[str, Sample] = {}
    for raw_sample in _to_list(config.get("samples")):
        req_proc = tuple(_as_enum(ProcessType, p) for p in raw_sample.get("required_processes", []))
        sample = Sample(
            id=str(raw_sample["id"]),
            barcode=str(raw_sample.get("barcode", raw_sample["id"])),
            obj_type=int(raw_sample["obj_type"]),
            length_mm=float(raw_sample["length_mm"]),
            diameter_mm=float(raw_sample["diameter_mm"]),
            cap_state=_as_enum(CapState, raw_sample.get("cap_state", CapState.CAPPED.value)),  # type: ignore[arg-type]
            required_processes=req_proc,
        )
        samples[sample.id] = sample

    sample_states: Dict[str, SampleState] = {}
    for raw_state in _to_list(config.get("sample_states")):
        sample_id = str(raw_state["sample_id"])
        location = _location_from_config(dict(raw_state.get("location", {})))
        completed = {_as_enum(ProcessType, p) for p in raw_state.get("completed_processes", [])}
        raw_status = raw_state.get("classification_status", SampleClassificationStatus.UNKNOWN.value)
        try:
            classification_status = _as_enum(SampleClassificationStatus, raw_status)
        except Exception:
            classification_status = SampleClassificationStatus.UNKNOWN
        assigned_route_rack_index_raw = raw_state.get("assigned_route_rack_index")
        assigned_route_rack_index: Optional[int] = None
        if assigned_route_rack_index_raw is not None:
            try:
                assigned_route_rack_index = int(assigned_route_rack_index_raw)
            except Exception:
                assigned_route_rack_index = None
        details_raw = raw_state.get("classification_details", {})
        classification_details = details_raw if isinstance(details_raw, dict) else {}
        sample_states[sample_id] = SampleState(
            sample_id=sample_id,
            location=location,
            completed_processes=completed,
            classification_status=classification_status,
            classification_source=str(raw_state.get("classification_source", "")),
            assigned_route=str(raw_state.get("assigned_route", "")),
            assigned_route_station_slot_id=str(raw_state.get("assigned_route_station_slot_id", "")),
            assigned_route_rack_index=assigned_route_rack_index,
            classification_details=classification_details,
        )

    caps: Dict[str, Cap] = {}
    for raw_cap in _to_list(config.get("caps")):
        cap_id = str(raw_cap.get("id", "")).strip()
        if not cap_id:
            continue
        try:
            cap_obj_type = int(raw_cap.get("obj_type", 9014))
        except Exception:
            cap_obj_type = 9014
        caps[cap_id] = Cap(
            id=cap_id,
            obj_type=cap_obj_type,
            assigned_sample_id=str(raw_cap.get("assigned_sample_id", "")).strip(),
        )

    cap_states: Dict[str, CapStateRecord] = {}
    for raw_state in _to_list(config.get("cap_states")):
        cap_id = str(raw_state.get("cap_id", raw_state.get("id", ""))).strip()
        if not cap_id:
            continue
        raw_location = raw_state.get("location", {})
        if not isinstance(raw_location, dict):
            continue
        try:
            cap_location = _cap_location_from_config(dict(raw_location))
        except Exception:
            continue
        cap_states[cap_id] = CapStateRecord(
            cap_id=cap_id,
            location=cap_location,
        )

    world = WorldModel(
        stations=stations,
        landmarks=landmarks,
        racks=racks,
        devices=devices,
        samples=samples,
        sample_states=sample_states,
        caps=caps,
        cap_states=cap_states,
    )

    for placement in _to_list(config.get("rack_placements")):
        world.place_rack(
            station_id=str(placement["station_id"]),
            station_slot_id=str(placement["station_slot_id"]),
            rack_id=str(placement["rack_id"]),
        )

    robot_station = config.get("robot_current_station_id")
    if robot_station is not None:
        world.set_robot_station(str(robot_station))

    rack_in_gripper_id = config.get("rack_in_gripper_id")
    if isinstance(rack_in_gripper_id, str) and rack_in_gripper_id in world.racks:
        world.rack_in_gripper_id = rack_in_gripper_id
        for key, placed_rack_id in list(world.rack_placements.items()):
            if placed_rack_id == rack_in_gripper_id:
                world.rack_placements.pop(key, None)

    for cap_id, cap_state in list(world.cap_states.items()):
        cap = world.caps.get(cap_id)
        if cap is None:
            inferred_sample_id = ""
            if cap_id.upper().startswith("CAP_") and len(cap_id) > 4:
                inferred_sample_id = cap_id[4:]
            world.caps[cap_id] = Cap(
                id=cap_id,
                obj_type=9014,
                assigned_sample_id=inferred_sample_id,
            )
        if isinstance(cap_state.location, StoredCapLocation):
            loc = cap_state.location
            rack = world.racks.get(str(loc.rack_id))
            if rack is not None:
                existing = rack.occupied_slots.get(int(loc.slot_index))
                if existing is None:
                    rack.occupied_slots[int(loc.slot_index)] = cap_id

    for (station_id, station_slot_id), rack_id in sorted(world.rack_placements.items()):
        rack = world.racks.get(rack_id)
        if rack is None:
            continue
        for slot_index, occupant_id in sorted(rack.occupied_slots.items()):
            if occupant_id in world.samples:
                continue
            if occupant_id in world.cap_states:
                continue
            if occupant_id in world.caps or str(occupant_id).upper().startswith("CAP_"):
                cap_id = str(occupant_id)
                world._ensure_cap_from_stored_location(
                    cap_id,
                    StoredCapLocation(
                        station_id=str(station_id),
                        station_slot_id=str(station_slot_id),
                        rack_id=str(rack_id),
                        slot_index=int(slot_index),
                    ),
                )

    world.ensure_cap_tracking_for_capped_samples()
    world._sample_counter = len(world.samples)
    return world


def world_to_config(world: WorldModel) -> Dict[str, Any]:
    stations_out: List[Dict[str, Any]] = []
    for station_id in sorted(world.stations.keys()):
        station = world.stations[station_id]
        slots_out = []
        for slot_id in sorted(station.slot_configs.keys()):
            slot = station.slot_configs[slot_id]
            slots_out.append(
                {
                    "slot_id": slot.slot_id,
                    "kind": slot.kind.value,
                    "jig_id": slot.jig_id,
                    "itm_id": slot.itm_id,
                    "rack_capacity": slot.rack_capacity,
                    "rack_pattern": slot.rack_pattern,
                    "rack_rows": slot.rack_rows,
                    "rack_cols": slot.rack_cols,
                    "rack_index": slot.rack_index,
                    "obj_nbr_offset": slot.obj_nbr_offset,
                    "loading_strategy": slot.loading_strategy,
                    "accepted_rack_types": sorted(t.value for t in slot.accepted_rack_types),
                }
            )
        stations_out.append(
            {
                "id": station.id,
                "name": station.name,
                "itm_id": station.itm_id,
                "kind": station.kind.value,
                "amr_pos_target": station.amr_pos_target,
                "landmark_id": station.landmark_id,
                "linked_device_ids": list(station.linked_device_ids),
                "slot_configs": slots_out,
            }
        )

    landmarks_out = [
        {"id": lm.id, "code": lm.code, "station_id": lm.station_id}
        for _, lm in sorted(world.landmarks.items(), key=lambda item: item[0])
    ]

    racks_out = []
    for rack_id in sorted(world.racks.keys()):
        rack = world.racks[rack_id]
        racks_out.append(
            {
                "id": rack.id,
                "rack_type": rack.rack_type.value,
                "capacity": rack.capacity,
                "pattern": rack.pattern,
                "pin_obj_type": rack.pin_obj_type,
                "rows": rack.rows,
                "cols": rack.cols,
                "blocked_slots": sorted(rack.blocked_slots),
                "occupied_slots": {str(k): v for k, v in sorted(rack.occupied_slots.items())},
                "reserved_slots": {str(k): v for k, v in sorted(rack.reserved_slots.items())},
            }
        )

    devices_out = []
    for dev_id in sorted(world.devices.keys()):
        dev = world.devices[dev_id]
        metadata_payload = {
            str(k): v
            for k, v in dict(dev.metadata).items()
            if str(k) not in {"planner_role", "exclude_station_racks_from_idle_return"}
        }
        payload: Dict[str, Any] = {
            "id": dev.id,
            "name": dev.name,
            "station_id": dev.station_id,
            "capabilities": sorted(p.value for p in dev.capabilities),
            "planner_role": str(getattr(dev, "planner_role", "PROCESSOR")).strip().upper() or "PROCESSOR",
            "exclude_station_racks_from_idle_return": bool(
                getattr(dev, "exclude_station_racks_from_idle_return", True)
            ),
        }
        payload.update(metadata_payload)
        devices_out.append(payload)

    samples_out = []
    for sample_id in sorted(world.samples.keys()):
        sample = world.samples[sample_id]
        samples_out.append(
            {
                "id": sample.id,
                "barcode": sample.barcode,
                "obj_type": sample.obj_type,
                "length_mm": sample.length_mm,
                "diameter_mm": sample.diameter_mm,
                "cap_state": sample.cap_state.value,
                "required_processes": [p.value for p in sample.required_processes],
            }
        )

    sample_states_out = []
    for sample_id in sorted(world.sample_states.keys()):
        state = world.sample_states[sample_id]
        sample_states_out.append(
            {
                "sample_id": state.sample_id,
                "location": _location_to_config(state.location),
                "completed_processes": sorted(p.value for p in state.completed_processes),
                "classification_status": state.classification_status.value,
                "classification_source": state.classification_source,
                "assigned_route": state.assigned_route,
                "assigned_route_station_slot_id": state.assigned_route_station_slot_id,
                "assigned_route_rack_index": state.assigned_route_rack_index,
                "classification_details": state.classification_details,
            }
        )

    caps_out = []
    for cap_id in sorted(world.caps.keys()):
        cap = world.caps[cap_id]
        caps_out.append(
            {
                "id": cap.id,
                "obj_type": int(cap.obj_type),
                "assigned_sample_id": str(cap.assigned_sample_id),
            }
        )

    cap_states_out = []
    for cap_id in sorted(world.cap_states.keys()):
        cap_state = world.cap_states[cap_id]
        cap_states_out.append(
            {
                "cap_id": cap_state.cap_id,
                "location": _cap_location_to_config(cap_state.location),
            }
        )

    placements_out = []
    for (station_id, slot_id), rack_id in sorted(world.rack_placements.items()):
        placements_out.append(
            {"station_id": station_id, "station_slot_id": slot_id, "rack_id": rack_id}
        )

    return {
        "stations": stations_out,
        "landmarks": landmarks_out,
        "racks": racks_out,
        "devices": devices_out,
        "rack_placements": placements_out,
        "rack_in_gripper_id": world.rack_in_gripper_id,
        "robot_current_station_id": world.robot_current_station_id,
        "samples": samples_out,
        "sample_states": sample_states_out,
        "caps": caps_out,
        "cap_states": cap_states_out,
    }


def _load_yaml(path: Path) -> Dict[str, Any]:
    try:
        import yaml  # type: ignore[import-untyped]
    except Exception as exc:
        raise RuntimeError("YAML support requires PyYAML. Install with: pip install pyyaml") from exc

    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ValueError(f"Config root must be a dictionary, got {type(data)}")
    return data


def _dump_yaml(path: Path, data: Dict[str, Any]) -> None:
    try:
        import yaml  # type: ignore[import-untyped]
    except Exception as exc:
        raise RuntimeError("YAML support requires PyYAML. Install with: pip install pyyaml") from exc

    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, sort_keys=False)


def load_world_config_file(path: Union[str, Path]) -> Dict[str, Any]:
    cfg_path = Path(path)
    suffix = cfg_path.suffix.lower()
    if suffix in {".yaml", ".yml"}:
        return _load_yaml(cfg_path)
    with open(cfg_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"Config root must be a dictionary, got {type(data)}")
    return data


def save_world_config_file(path: Union[str, Path], data: Dict[str, Any]) -> None:
    cfg_path = Path(path)
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    suffix = cfg_path.suffix.lower()
    if suffix in {".yaml", ".yml"}:
        _dump_yaml(cfg_path, data)
        return
    with open(cfg_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def load_world_from_file(path: Union[str, Path]) -> WorldModel:
    return world_from_config(load_world_config_file(path))


def save_world_to_file(world: WorldModel, path: Union[str, Path]) -> None:
    save_world_config_file(path, world_to_config(world))


def ensure_world_config_file(path: Union[str, Path]) -> WorldModel:
    cfg_path = Path(path)
    if not cfg_path.exists():
        save_world_config_file(cfg_path, default_world_config())
    return load_world_from_file(cfg_path)


def build_default_world() -> WorldModel:
    return world_from_config(default_world_config())


def _upsert_dict_item(items: List[Dict[str, Any]], key: str, key_value: str, item: Dict[str, Any]) -> None:
    for idx, cur in enumerate(items):
        if str(cur.get(key)) == key_value:
            items[idx] = item
            return
    items.append(item)


_UNSET = object()


class WorldConfigManager:
    """Config editing interface for JSON/YAML world files."""

    def __init__(self, path: Union[str, Path], create_if_missing: bool = True) -> None:
        self.path = Path(path)
        if create_if_missing and not self.path.exists():
            save_world_config_file(self.path, default_world_config())
        self.data: Dict[str, Any] = load_world_config_file(self.path)
        self._ensure_sections()

    def _ensure_sections(self) -> None:
        self.data.setdefault("stations", [])
        self.data.setdefault("landmarks", [])
        self.data.setdefault("racks", [])
        self.data.setdefault("devices", [])
        self.data.setdefault("rack_placements", [])
        self.data.setdefault("samples", [])
        self.data.setdefault("sample_states", [])
        self.data.setdefault("caps", [])
        self.data.setdefault("cap_states", [])

    def reload(self) -> None:
        self.data = load_world_config_file(self.path)
        self._ensure_sections()

    def save(self) -> None:
        save_world_config_file(self.path, self.data)

    def to_world(self) -> WorldModel:
        return world_from_config(self.data)

    def summary(self) -> Dict[str, int]:
        return {
            "stations": len(_to_list(self.data.get("stations"))),
            "landmarks": len(_to_list(self.data.get("landmarks"))),
            "racks": len(_to_list(self.data.get("racks"))),
            "devices": len(_to_list(self.data.get("devices"))),
            "rack_placements": len(_to_list(self.data.get("rack_placements"))),
            "samples": len(_to_list(self.data.get("samples"))),
            "sample_states": len(_to_list(self.data.get("sample_states"))),
            "caps": len(_to_list(self.data.get("caps"))),
            "cap_states": len(_to_list(self.data.get("cap_states"))),
        }

    def _find_station(self, station_id: str) -> Optional[Dict[str, Any]]:
        for st in _to_list(self.data.get("stations")):
            if str(st.get("id")) == station_id:
                return st
        return None

    def get_station_config(self, station_id: str) -> Optional[Dict[str, Any]]:
        station = self._find_station(station_id)
        return dict(station) if station is not None else None

    def upsert_station(
        self,
        station_id: str,
        name: Optional[str] = None,
        itm_id: Optional[int] = None,
        kind: Optional[Union[StationKind, str]] = None,
        amr_pos_target: Any = _UNSET,
        landmark_id: Any = _UNSET,
        linked_device_ids: Any = _UNSET,
    ) -> None:
        stations = _to_list(self.data.get("stations"))
        existing = None
        for st in stations:
            if str(st.get("id")) == station_id:
                existing = dict(st)
                break

        if existing is None:
            itm_value = int(itm_id) if itm_id is not None else 1
            kind_value = _as_enum(StationKind, kind).value if kind is not None else StationKind.EXTERNAL.value
            if amr_pos_target is _UNSET:
                resolved_amr_pos_target = None if kind_value == StationKind.ON_ROBOT_PLATE.value else str(itm_value)
            else:
                resolved_amr_pos_target = amr_pos_target
            existing = {
                "id": station_id,
                "name": name or station_id,
                "itm_id": itm_value,
                "kind": kind_value,
                "amr_pos_target": resolved_amr_pos_target,
                "landmark_id": None if landmark_id is _UNSET else landmark_id,
                "linked_device_ids": [] if linked_device_ids is _UNSET else list(linked_device_ids or []),
                "slot_configs": [],
            }
        else:
            if name is not None:
                existing["name"] = name
            if itm_id is not None:
                existing["itm_id"] = int(itm_id)
            elif "itm_id" not in existing:
                existing["itm_id"] = 1
            if kind is not None:
                existing["kind"] = _as_enum(StationKind, kind).value
            if amr_pos_target is not _UNSET:
                existing["amr_pos_target"] = amr_pos_target
            elif "amr_pos_target" not in existing:
                if str(existing.get("kind", StationKind.EXTERNAL.value)) == StationKind.ON_ROBOT_PLATE.value:
                    existing["amr_pos_target"] = None
                else:
                    existing["amr_pos_target"] = str(int(existing.get("itm_id", 1)))
            if landmark_id is not _UNSET:
                existing["landmark_id"] = landmark_id
            elif "landmark_id" not in existing:
                existing["landmark_id"] = None
            if linked_device_ids is not _UNSET:
                existing["linked_device_ids"] = list(linked_device_ids or [])
            elif "linked_device_ids" not in existing:
                existing["linked_device_ids"] = []
            existing.setdefault("slot_configs", [])

        _upsert_dict_item(stations, "id", station_id, existing)
        self.data["stations"] = stations

    def upsert_station_slot(
        self,
        station_id: str,
        slot_id: str,
        kind: Union[SlotKind, str],
        jig_id: int,
        itm_id: int = 1,
        rack_capacity: int = 1,
        rack_pattern: Optional[str] = None,
        rack_rows: Optional[int] = None,
        rack_cols: Optional[int] = None,
        rack_index: int = 1,
        obj_nbr_offset: int = 0,
        loading_strategy: str = "",
        accepted_rack_types: Optional[Iterable[Union[RackType, str]]] = None,
    ) -> None:
        station = self._find_station(station_id)
        if station is None:
            raise KeyError(f"Station '{station_id}' does not exist in config")

        slots = _to_list(station.get("slot_configs"))
        existing_slot = None
        for s in slots:
            if str(s.get("slot_id")) == slot_id:
                existing_slot = dict(s)
                break

        final_rack_pattern = rack_pattern
        if final_rack_pattern is None and existing_slot is not None:
            final_rack_pattern = existing_slot.get("rack_pattern")

        final_rack_rows = rack_rows
        if final_rack_rows is None and existing_slot is not None and "rack_rows" in existing_slot:
            final_rack_rows = existing_slot.get("rack_rows")

        final_rack_cols = rack_cols
        if final_rack_cols is None and existing_slot is not None and "rack_cols" in existing_slot:
            final_rack_cols = existing_slot.get("rack_cols")

        item = {
            "slot_id": slot_id,
            "kind": _as_enum(SlotKind, kind).value,
            "jig_id": int(jig_id),
            "itm_id": int(itm_id),
            "rack_capacity": int(rack_capacity),
            "rack_pattern": final_rack_pattern,
            "rack_rows": (None if final_rack_rows is None else int(final_rack_rows)),
            "rack_cols": (None if final_rack_cols is None else int(final_rack_cols)),
            "rack_index": int(rack_index),
            "obj_nbr_offset": int(obj_nbr_offset),
            "loading_strategy": str(loading_strategy or "").strip(),
            "accepted_rack_types": sorted(_as_enum(RackType, t).value for t in (accepted_rack_types or [])),
        }
        _upsert_dict_item(slots, "slot_id", slot_id, item)
        station["slot_configs"] = slots

    def upsert_landmark(self, landmark_id: str, code: str, station_id: str) -> None:
        landmarks = _to_list(self.data.get("landmarks"))
        item = {"id": landmark_id, "code": code, "station_id": station_id}
        _upsert_dict_item(landmarks, "id", landmark_id, item)
        self.data["landmarks"] = landmarks

    def upsert_rack(
        self,
        rack_id: str,
        rack_type: Union[RackType, str],
        capacity: int,
        pattern: str,
        pin_obj_type: int,
        rows: Optional[int] = None,
        cols: Optional[int] = None,
        blocked_slots: Optional[Iterable[int]] = None,
    ) -> None:
        racks = _to_list(self.data.get("racks"))
        existing = None
        for r in racks:
            if str(r.get("id")) == rack_id:
                existing = dict(r)
                break
        item = {
            "id": rack_id,
            "rack_type": _as_enum(RackType, rack_type).value,
            "capacity": int(capacity),
            "pattern": pattern,
            "pin_obj_type": int(pin_obj_type),
        }
        if rows is not None:
            item["rows"] = int(rows)
        elif existing is not None and "rows" in existing:
            item["rows"] = existing["rows"]

        if cols is not None:
            item["cols"] = int(cols)
        elif existing is not None and "cols" in existing:
            item["cols"] = existing["cols"]

        if blocked_slots is not None:
            item["blocked_slots"] = sorted(int(x) for x in blocked_slots)
        elif existing is not None and "blocked_slots" in existing:
            item["blocked_slots"] = existing["blocked_slots"]

        _upsert_dict_item(racks, "id", rack_id, item)
        self.data["racks"] = racks

    def upsert_device(
        self,
        device_id: str,
        name: str,
        station_id: str,
        capabilities: Iterable[Union[ProcessType, str]],
        planner_role: str = "PROCESSOR",
        exclude_station_racks_from_idle_return: bool = True,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        devices = _to_list(self.data.get("devices"))
        item = {
            "id": device_id,
            "name": name,
            "station_id": station_id,
            "capabilities": sorted(_as_enum(ProcessType, c).value for c in capabilities),
            "planner_role": str(planner_role).strip().upper() or "PROCESSOR",
            "exclude_station_racks_from_idle_return": bool(exclude_station_racks_from_idle_return),
        }
        if metadata:
            item.update(dict(metadata))
        _upsert_dict_item(devices, "id", device_id, item)
        self.data["devices"] = devices

    def set_rack_placement(self, station_id: str, station_slot_id: str, rack_id: str) -> None:
        placements = _to_list(self.data.get("rack_placements"))
        updated = False
        for placement in placements:
            if (
                str(placement.get("station_id")) == station_id
                and str(placement.get("station_slot_id")) == station_slot_id
            ):
                placement["rack_id"] = rack_id
                updated = True
                break
        if not updated:
            placements.append(
                {"station_id": station_id, "station_slot_id": station_slot_id, "rack_id": rack_id}
            )
        self.data["rack_placements"] = placements

    def clear_rack_placement(self, station_id: str, station_slot_id: str) -> None:
        placements = _to_list(self.data.get("rack_placements"))
        self.data["rack_placements"] = [
            p
            for p in placements
            if not (
                str(p.get("station_id")) == station_id
                and str(p.get("station_slot_id")) == station_slot_id
            )
        ]

    def set_robot_station(self, station_id: Optional[str]) -> None:
        self.data["robot_current_station_id"] = station_id
