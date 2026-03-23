from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from world.lab_world import (
    CapOnSampleLocation,
    GripperLocation,
    StoredCapLocation,
    WorldModel,
    load_world_from_file,
)

WORLD_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG_PATH = WORLD_DIR / "world_config.json"
DEFAULT_OUT_PATH = WORLD_DIR / "world_snapshot.jsonl"


def _now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="milliseconds")


def _index_to_row_col(slot_index: int, cols: Optional[int]) -> Dict[str, Optional[int]]:
    if cols is None or cols <= 0:
        return {"row": None, "col": None}
    return {"row": ((slot_index - 1) // cols) + 1, "col": ((slot_index - 1) % cols) + 1}


def _occupant_kind(world: WorldModel, occupant_id: Optional[str]) -> str:
    if occupant_id is None:
        return "NONE"
    if occupant_id in world.samples:
        return "SAMPLE"
    if occupant_id in world.caps or occupant_id in world.cap_states or str(occupant_id).strip().upper().startswith("CAP_"):
        return "CAP"
    return "UNKNOWN"


def build_snapshot_records(world: WorldModel, config_path: Path) -> List[Dict[str, Any]]:
    ts = _now_iso()
    snapshot_id = f"WORLD_SNAPSHOT@{ts}"
    records: List[Dict[str, Any]] = []

    records.append(
        {
            "timestamp": ts,
            "snapshot_id": snapshot_id,
            "record_type": "WORLD",
            "config_path": str(config_path.resolve()),
            "robot_current_station_id": world.robot_current_station_id,
            "counts": {
                "stations": len(world.stations),
                "virtual_stations": 1,
                "racks": len(world.racks),
                "rack_placements": len(world.rack_placements),
                "rack_in_gripper": 1 if world.rack_in_gripper_id else 0,
                "samples": len(world.samples),
                "caps": len(world.caps),
                "cap_states": len(world.cap_states),
            },
        }
    )

    for station_id in sorted(world.stations.keys()):
        station = world.stations[station_id]
        records.append(
            {
                "timestamp": ts,
                "snapshot_id": snapshot_id,
                "record_type": "STATION",
                "station_id": station.id,
                "station_name": station.name,
                "station_kind": station.kind.value,
                "amr_pos_target": station.amr_pos_target,
                "landmark_id": station.landmark_id,
                "requires_navigation": station.requires_navigation(),
            }
        )

        for slot_id in sorted(station.slot_configs.keys()):
            slot_cfg = station.slot_configs[slot_id]
            mounted_rack_id = world.rack_placements.get((station_id, slot_id))
            records.append(
                {
                    "timestamp": ts,
                    "snapshot_id": snapshot_id,
                    "record_type": "SLOT",
                    "station_id": station_id,
                    "station_slot_id": slot_id,
                    "slot_kind": slot_cfg.kind.value,
                    "jig_id": slot_cfg.jig_id,
                    "itm_id": slot_cfg.itm_id,
                    "accepted_rack_types": sorted(t.value for t in slot_cfg.accepted_rack_types),
                    "mounted_rack_id": mounted_rack_id,
                    "slot_state": "RACK_PRESENT" if mounted_rack_id else "EMPTY",
                }
            )

            if mounted_rack_id is None:
                continue

            rack = world.racks[mounted_rack_id]
            occupied_count = len(rack.occupied_slots)
            pin_count = len(rack.blocked_slots)
            free_count = len(rack.available_slots()) - occupied_count

            records.append(
                {
                    "timestamp": ts,
                    "snapshot_id": snapshot_id,
                    "record_type": "RACK",
                    "rack_id": rack.id,
                    "rack_type": rack.rack_type.value,
                    "pattern": rack.pattern,
                    "rows": rack.rows,
                    "cols": rack.cols,
                    "capacity": rack.capacity,
                    "blocked_slots": sorted(rack.blocked_slots),
                    "station_id": station_id,
                    "station_slot_id": slot_id,
                    "counts": {
                        "occupied_positions": occupied_count,
                        "pin_positions": pin_count,
                        "free_positions": free_count,
                    },
                }
            )

            for pos in range(1, rack.capacity + 1):
                row_col = _index_to_row_col(pos, rack.cols)
                if pos in rack.blocked_slots:
                    pos_state = "PIN"
                    sample_id = None
                    cap_id = None
                    occupant_id = None
                    occupant_kind = "NONE"
                else:
                    occupant_id = rack.occupied_slots.get(pos)
                    pos_state = "OCCUPIED" if occupant_id else "FREE"
                    occupant_kind = _occupant_kind(world, occupant_id)
                    sample_id = occupant_id if occupant_kind == "SAMPLE" else None
                    cap_id = occupant_id if occupant_kind == "CAP" else None

                records.append(
                    {
                        "timestamp": ts,
                        "snapshot_id": snapshot_id,
                        "record_type": "RACK_POSITION",
                        "rack_id": rack.id,
                        "rack_type": rack.rack_type.value,
                        "station_id": station_id,
                        "station_slot_id": slot_id,
                        "position_index": pos,
                        "row": row_col["row"],
                        "col": row_col["col"],
                        "position_state": pos_state,
                        "occupied_object_id": occupant_id if pos_state == "OCCUPIED" else None,
                        "occupied_object_kind": occupant_kind if pos_state == "OCCUPIED" else "NONE",
                        "sample_id": sample_id,
                        "cap_id": cap_id,
                    }
                )

    sample_ids_in_gripper: List[str] = []
    for sample_id, sample_state in sorted(world.sample_states.items()):
        if isinstance(sample_state.location, GripperLocation):
            sample_ids_in_gripper.append(sample_id)

    accepted_rack_types = sorted({rack.rack_type.value for rack in world.racks.values()})

    records.append(
        {
            "timestamp": ts,
            "snapshot_id": snapshot_id,
            "record_type": "STATION",
            "station_id": "uLM_GRIPPER",
            "station_name": "uLM_GRIPPER",
            "station_kind": "VIRTUAL",
            "amr_pos_target": None,
            "landmark_id": None,
            "requires_navigation": False,
        }
    )
    records.append(
        {
            "timestamp": ts,
            "snapshot_id": snapshot_id,
            "record_type": "SLOT",
            "station_id": "uLM_GRIPPER",
            "station_slot_id": "RackGrip",
            "slot_kind": "VIRTUAL_GRIPPER_RACK_SLOT",
            "jig_id": -1,
            "itm_id": -1,
            "accepted_rack_types": accepted_rack_types,
            "mounted_rack_id": world.rack_in_gripper_id,
            "slot_state": "RACK_PRESENT" if world.rack_in_gripper_id else "EMPTY",
        }
    )
    records.append(
        {
            "timestamp": ts,
            "snapshot_id": snapshot_id,
            "record_type": "SLOT",
            "station_id": "uLM_GRIPPER",
            "station_slot_id": "SampleGrip",
            "slot_kind": "VIRTUAL_GRIPPER_SAMPLE_SLOT",
            "jig_id": -1,
            "itm_id": -1,
            "accepted_rack_types": [],
            "mounted_rack_id": None,
            "mounted_sample_ids": sample_ids_in_gripper,
            "slot_state": "SAMPLE_PRESENT" if sample_ids_in_gripper else "EMPTY",
        }
    )

    if world.rack_in_gripper_id:
        rack = world.racks.get(world.rack_in_gripper_id)
        if rack is not None:
            occupied_count = len(rack.occupied_slots)
            pin_count = len(rack.blocked_slots)
            free_count = len(rack.available_slots()) - occupied_count
            records.append(
                {
                    "timestamp": ts,
                    "snapshot_id": snapshot_id,
                    "record_type": "RACK",
                    "rack_id": rack.id,
                    "rack_type": rack.rack_type.value,
                    "pattern": rack.pattern,
                    "rows": rack.rows,
                    "cols": rack.cols,
                    "capacity": rack.capacity,
                    "blocked_slots": sorted(rack.blocked_slots),
                    "station_id": "uLM_GRIPPER",
                    "station_slot_id": "RackGrip",
                    "counts": {
                        "occupied_positions": occupied_count,
                        "pin_positions": pin_count,
                        "free_positions": free_count,
                    },
                }
            )
            for pos in range(1, rack.capacity + 1):
                row_col = _index_to_row_col(pos, rack.cols)
                if pos in rack.blocked_slots:
                    pos_state = "PIN"
                    sample_id = None
                    cap_id = None
                    occupant_id = None
                    occupant_kind = "NONE"
                else:
                    occupant_id = rack.occupied_slots.get(pos)
                    pos_state = "OCCUPIED" if occupant_id else "FREE"
                    occupant_kind = _occupant_kind(world, occupant_id)
                    sample_id = occupant_id if occupant_kind == "SAMPLE" else None
                    cap_id = occupant_id if occupant_kind == "CAP" else None
                records.append(
                    {
                        "timestamp": ts,
                        "snapshot_id": snapshot_id,
                        "record_type": "RACK_POSITION",
                        "rack_id": rack.id,
                        "rack_type": rack.rack_type.value,
                        "station_id": "uLM_GRIPPER",
                        "station_slot_id": "RackGrip",
                        "position_index": pos,
                        "row": row_col["row"],
                        "col": row_col["col"],
                        "position_state": pos_state,
                        "occupied_object_id": occupant_id if pos_state == "OCCUPIED" else None,
                        "occupied_object_kind": occupant_kind if pos_state == "OCCUPIED" else "NONE",
                        "sample_id": sample_id,
                        "cap_id": cap_id,
                    }
                )

    for cap_id in sorted(world.caps.keys()):
        cap = world.caps[cap_id]
        cap_state = world.cap_states.get(cap_id)
        payload: Dict[str, Any] = {
            "timestamp": ts,
            "snapshot_id": snapshot_id,
            "record_type": "CAP",
            "cap_id": cap_id,
            "obj_type": int(cap.obj_type),
            "assigned_sample_id": str(cap.assigned_sample_id),
            "location_type": "UNKNOWN",
        }
        if cap_state is not None:
            loc = cap_state.location
            if isinstance(loc, CapOnSampleLocation):
                payload.update(
                    {
                        "location_type": "ON_SAMPLE",
                        "sample_id": str(loc.sample_id),
                    }
                )
            elif isinstance(loc, StoredCapLocation):
                payload.update(
                    {
                        "location_type": "STORED",
                        "station_id": str(loc.station_id),
                        "station_slot_id": str(loc.station_slot_id),
                        "rack_id": str(loc.rack_id),
                        "slot_index": int(loc.slot_index),
                    }
                )
        records.append(payload)

    return records


def write_jsonl(path: Path, records: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=True) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Export world snapshot to JSONL records.")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH), help="Path to world config file")
    parser.add_argument("--out", default=str(DEFAULT_OUT_PATH), help="Output JSONL path")
    args = parser.parse_args()

    config_path = Path(args.config)
    out_path = Path(args.out)
    world = load_world_from_file(config_path)
    records = build_snapshot_records(world, config_path=config_path)
    write_jsonl(out_path, records)

    print(f"Snapshot written: {out_path.resolve()}")
    print(f"Records: {len(records)}")


if __name__ == "__main__":
    main()
