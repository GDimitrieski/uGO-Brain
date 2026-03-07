from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from world.lab_world import GripperLocation, RackLocation, WorldModel


def local_now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="milliseconds")


def export_trace(records: List[Dict[str, Any]], path: Union[str, Path]) -> None:
    if not records:
        return

    out_path = Path(path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    param_keys = sorted(
        {
            k
            for rec in records
            for k in rec.keys()
            if k
            not in {
                "timestamp_sent",
                "command_sent",
                "result",
                "task_id",
                "receiver",
                "dispatch_path",
                "message",
                "state_path",
                "state_timeline",
                "timestamp_returned",
            }
        }
    )
    fieldnames = [
        "timestamp_sent",
        "command_sent",
        *param_keys,
        "result",
        "task_id",
        "receiver",
        "dispatch_path",
        "message",
        "state_path",
        "state_timeline",
        "timestamp_returned",
    ]

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for rec in records:
            writer.writerow(rec)


def export_state_changes(records: List[Dict[str, Any]], path: Union[str, Path]) -> None:
    out_path = Path(path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = ["task_id", "command_sent", "change_index", "state", "timestamp"]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for rec in records:
            writer.writerow(rec)


def _world_state_snapshot(world: WorldModel) -> Dict[str, Any]:
    racks = []
    station_slots = []
    sample_ids_in_gripper: List[str] = []

    for station_id in sorted(world.stations.keys()):
        station = world.stations[station_id]
        for station_slot_id in sorted(station.slot_configs.keys()):
            slot_cfg = station.slot_configs[station_slot_id]
            mounted_rack_id = world.rack_placements.get((station_id, station_slot_id))
            mounted_rack = world.racks.get(mounted_rack_id) if mounted_rack_id else None
            station_slots.append(
                {
                    "station_id": station_id,
                    "station_slot_id": station_slot_id,
                    "slot_kind": slot_cfg.kind.value,
                    "jig_id": slot_cfg.jig_id,
                    "itm_id": slot_cfg.itm_id,
                    "accepted_rack_types": sorted(t.value for t in slot_cfg.accepted_rack_types),
                    "mounted_rack_id": mounted_rack_id,
                    "mounted_rack_type": mounted_rack.rack_type.value if mounted_rack else None,
                    "slot_state": "RACK_PRESENT" if mounted_rack_id else "EMPTY",
                }
            )

    for sample_id, sample_state in sorted(world.sample_states.items()):
        if isinstance(sample_state.location, GripperLocation):
            sample_ids_in_gripper.append(sample_id)

    accepted_rack_types = sorted({rack.rack_type.value for rack in world.racks.values()})
    station_slots.append(
        {
            "station_id": "uLM_GRIPPER",
            "station_slot_id": "RackGrip",
            "slot_kind": "VIRTUAL_GRIPPER_RACK_SLOT",
            "jig_id": -1,
            "itm_id": -1,
            "accepted_rack_types": accepted_rack_types,
            "mounted_rack_id": world.rack_in_gripper_id,
            "mounted_rack_type": (
                world.racks[world.rack_in_gripper_id].rack_type.value
                if world.rack_in_gripper_id and world.rack_in_gripper_id in world.racks
                else None
            ),
            "slot_state": "RACK_PRESENT" if world.rack_in_gripper_id else "EMPTY",
        }
    )
    station_slots.append(
        {
            "station_id": "uLM_GRIPPER",
            "station_slot_id": "SampleGrip",
            "slot_kind": "VIRTUAL_GRIPPER_SAMPLE_SLOT",
            "jig_id": -1,
            "itm_id": -1,
            "accepted_rack_types": [],
            "mounted_rack_id": None,
            "mounted_rack_type": None,
            "mounted_sample_ids": sample_ids_in_gripper,
            "slot_state": "SAMPLE_PRESENT" if sample_ids_in_gripper else "EMPTY",
        }
    )

    for (station_id, station_slot_id), rack_id in sorted(world.rack_placements.items()):
        rack = world.racks.get(rack_id)
        if rack is None:
            continue
        racks.append(
            {
                "station_id": station_id,
                "station_slot_id": station_slot_id,
                "rack_id": rack_id,
                "rack_type": rack.rack_type.value,
                "pattern": rack.pattern,
                "rows": rack.rows,
                "cols": rack.cols,
                "blocked_slots": sorted(rack.blocked_slots),
                "occupied_slots": {str(k): v for k, v in sorted(rack.occupied_slots.items())},
                "reserved_slots": {str(k): v for k, v in sorted(rack.reserved_slots.items())},
            }
        )

    if world.rack_in_gripper_id:
        rack = world.racks.get(world.rack_in_gripper_id)
        if rack is not None:
            racks.append(
                {
                    "station_id": "uLM_GRIPPER",
                    "station_slot_id": "RackGrip",
                    "rack_id": rack.id,
                    "rack_type": rack.rack_type.value,
                    "pattern": rack.pattern,
                    "rows": rack.rows,
                    "cols": rack.cols,
                    "blocked_slots": sorted(rack.blocked_slots),
                    "occupied_slots": {str(k): v for k, v in sorted(rack.occupied_slots.items())},
                    "reserved_slots": {str(k): v for k, v in sorted(rack.reserved_slots.items())},
                }
            )

    sample_locations = []
    for sample_id, sample_state in sorted(world.sample_states.items()):
        loc = sample_state.location
        if isinstance(loc, RackLocation):
            sample_locations.append(
                {
                    "sample_id": sample_id,
                    "location_type": "RACK",
                    "station_id": loc.station_id,
                    "station_slot_id": loc.station_slot_id,
                    "rack_id": loc.rack_id,
                    "slot_index": loc.slot_index,
                }
            )
        elif isinstance(loc, GripperLocation):
            sample_locations.append(
                {
                    "sample_id": sample_id,
                    "location_type": "GRIPPER",
                    "gripper_id": loc.gripper_id,
                }
            )
    return {
        "robot_current_station_id": world.robot_current_station_id,
        "rack_in_gripper_id": world.rack_in_gripper_id,
        "station_slots": station_slots,
        "racks": racks,
        "sample_locations": sample_locations,
    }


def append_world_event(
    records: List[Dict[str, Any]],
    world: WorldModel,
    event_type: str,
    entity_type: str,
    entity_id: str,
    source: Optional[Dict[str, Any]] = None,
    target: Optional[Dict[str, Any]] = None,
    details: Optional[Dict[str, Any]] = None,
) -> None:
    records.append(
        {
            "timestamp": local_now_iso(),
            "event_type": event_type,
            "entity_type": entity_type,
            "entity_id": entity_id,
            "source": source or {},
            "target": target or {},
            "details": details or {},
            "state_after": _world_state_snapshot(world),
        }
    )


def export_occupancy_trace(records: List[Dict[str, Any]], path: Union[str, Path]) -> None:
    out_path = Path(path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Keep historical filename, but write JSONL records for digital-twin/event replay use.
    with open(out_path, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=True) + "\n")


def export_occupancy_events_jsonl(records: List[Dict[str, Any]], path: Union[str, Path]) -> None:
    out_path = Path(path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with open(out_path, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=True) + "\n")


__all__ = [
    "local_now_iso",
    "export_trace",
    "export_state_changes",
    "append_world_event",
    "export_occupancy_trace",
    "export_occupancy_events_jsonl",
]
