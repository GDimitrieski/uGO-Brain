from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Dict, List, Tuple

from world.lab_world import WorldModel

_DEVICES_PREFIX_RE = re.compile(r"^\s*devices\s*:\s*", re.IGNORECASE)
_ENTRY_RE = re.compile(r"^\s*(?P<itm_id>\d+)\s*-\s*(?P<packml_state>[A-Za-z_][A-Za-z0-9_]*)\s*$")
_SPLIT_RE = re.compile(r"[;,]")


@dataclass(frozen=True)
class DevicePackmlStatus:
    itm_id: int
    packml_state: str


@dataclass(frozen=True)
class DevicePackmlAssignment:
    itm_id: int
    packml_state: str
    station_id: str
    device_id: str


@dataclass(frozen=True)
class UnmappedDevicePackmlStatus:
    itm_id: int
    packml_state: str
    reason: str


@dataclass(frozen=True)
class DevicePackmlMappingResult:
    assignments: Tuple[DevicePackmlAssignment, ...]
    unmapped: Tuple[UnmappedDevicePackmlStatus, ...]

    def by_device_id(self) -> Dict[str, str]:
        out: Dict[str, str] = {}
        for assignment in self.assignments:
            existing = out.get(assignment.device_id)
            if existing is not None and existing != assignment.packml_state:
                raise ValueError(
                    "Conflicting PACKML states mapped for device "
                    f"'{assignment.device_id}': '{existing}' vs '{assignment.packml_state}'"
                )
            out[assignment.device_id] = assignment.packml_state
        return out


def parse_update_world_device_statuses(raw_devices: str) -> Tuple[DevicePackmlStatus, ...]:
    text = str(raw_devices or "").strip()
    if not text:
        return ()

    text = _DEVICES_PREFIX_RE.sub("", text, count=1).strip()
    if text.startswith("(") and text.endswith(")"):
        text = text[1:-1].strip()
    if not text:
        return ()

    statuses: List[DevicePackmlStatus] = []
    for raw_chunk in _SPLIT_RE.split(text):
        chunk = str(raw_chunk).strip().strip("()").strip()
        if not chunk:
            continue
        match = _ENTRY_RE.match(chunk)
        if match is None:
            raise ValueError(
                "Invalid device state token in UpdateWorldState_From_uLM output: "
                f"'{chunk}'. Expected format '<ITM_ID>-<PACKML_STATE>'."
            )
        statuses.append(
            DevicePackmlStatus(
                itm_id=int(match.group("itm_id")),
                packml_state=str(match.group("packml_state")).strip().upper(),
            )
        )
    return tuple(statuses)


def map_update_world_devices_to_assigned_world_devices(
    world: WorldModel,
    raw_devices: str,
) -> DevicePackmlMappingResult:
    statuses = parse_update_world_device_statuses(raw_devices)
    itm_to_station_ids: Dict[int, List[str]] = {}
    for station_id in sorted(world.stations.keys()):
        station = world.stations[station_id]
        itm_to_station_ids.setdefault(int(station.itm_id), []).append(station_id)

    assignments: List[DevicePackmlAssignment] = []
    unmapped: List[UnmappedDevicePackmlStatus] = []
    for status in statuses:
        station_ids = itm_to_station_ids.get(int(status.itm_id), [])
        if not station_ids:
            unmapped.append(
                UnmappedDevicePackmlStatus(
                    itm_id=int(status.itm_id),
                    packml_state=status.packml_state,
                    reason=f"No station found with ITM_ID={int(status.itm_id)}",
                )
            )
            continue

        mapped = False
        for station_id in station_ids:
            station_devices = world.get_station_devices(station_id)
            if not station_devices:
                unmapped.append(
                    UnmappedDevicePackmlStatus(
                        itm_id=int(status.itm_id),
                        packml_state=status.packml_state,
                        reason=f"Station '{station_id}' has no assigned devices",
                    )
                )
                continue
            for device in station_devices:
                assignments.append(
                    DevicePackmlAssignment(
                        itm_id=int(status.itm_id),
                        packml_state=status.packml_state,
                        station_id=str(station_id),
                        device_id=str(device.id),
                    )
                )
                mapped = True
        if not mapped and not any(u.itm_id == status.itm_id for u in unmapped):
            unmapped.append(
                UnmappedDevicePackmlStatus(
                    itm_id=int(status.itm_id),
                    packml_state=status.packml_state,
                    reason="No device assignment resolved for this ITM_ID",
                )
            )

    assignment_map: Dict[Tuple[int, str, str, str], DevicePackmlAssignment] = {}
    for item in assignments:
        key = (item.itm_id, item.station_id, item.device_id, item.packml_state)
        assignment_map[key] = item

    unmapped_map: Dict[Tuple[int, str, str], UnmappedDevicePackmlStatus] = {}
    for item in unmapped:
        key = (item.itm_id, item.packml_state, item.reason)
        unmapped_map[key] = item

    assignments_out = tuple(
        sorted(
            assignment_map.values(),
            key=lambda x: (int(x.itm_id), str(x.station_id), str(x.device_id), str(x.packml_state)),
        )
    )
    unmapped_out = tuple(
        sorted(
            unmapped_map.values(),
            key=lambda x: (int(x.itm_id), str(x.packml_state), str(x.reason)),
        )
    )
    return DevicePackmlMappingResult(assignments=assignments_out, unmapped=unmapped_out)


def mapped_packml_state_by_device_id(world: WorldModel, raw_devices: str) -> Dict[str, str]:
    return map_update_world_devices_to_assigned_world_devices(world, raw_devices).by_device_id()


__all__ = [
    "DevicePackmlStatus",
    "DevicePackmlAssignment",
    "UnmappedDevicePackmlStatus",
    "DevicePackmlMappingResult",
    "parse_update_world_device_statuses",
    "map_update_world_devices_to_assigned_world_devices",
    "mapped_packml_state_by_device_id",
]
