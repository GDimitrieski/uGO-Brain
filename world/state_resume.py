from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, Optional, Set, Tuple

from world.lab_world import (
    CapState,
    GripperLocation,
    ProcessType,
    RackLocation,
    RackType,
    Sample,
    SampleState,
    WorldModel,
    ensure_world_config_file,
)
from world.jig_rack_strategy import is_tara_probe_sample_id

INPUT_STATION_ID = "InputStation"
INPUT_SLOT_ID = "URGRackSlot"
PLATE_STATION_ID = "uLMPlateStation"
PLATE_RACK_SLOT_ID = "URGRackSlot"
OBJ_TYPE_PROBE = 810

RESUME_FROM_LAST_WORLD_SNAPSHOT = os.getenv("UGO_RESUME_FROM_LAST_WORLD_SNAPSHOT", "1").strip().lower() in {
    "1",
    "true",
    "yes",
}
FORCE_INPUT_RACK_AT_INPUT_ON_START = os.getenv("UGO_FORCE_INPUT_RACK_AT_INPUT", "").strip().lower() in {
    "1",
    "true",
    "yes",
}


def _slot_map_from_raw(raw: Any) -> Dict[int, str]:
    if not isinstance(raw, dict):
        return {}
    out: Dict[int, str] = {}
    for key, value in raw.items():
        if value is None:
            continue
        try:
            slot_index = int(key)
        except Exception:
            continue
        out[slot_index] = str(value)
    return out


def _ensure_sample_exists(world: WorldModel, sample_id: str) -> None:
    if sample_id in world.samples:
        return
    # Tara probes are balancing helpers, not workflow samples.
    required_processes = () if is_tara_probe_sample_id(sample_id) else (ProcessType.CENTRIFUGATION,)
    world.samples[sample_id] = Sample(
        id=sample_id,
        barcode=sample_id,
        obj_type=OBJ_TYPE_PROBE,
        length_mm=75.0,
        diameter_mm=13.0,
        cap_state=CapState.CAPPED,
        required_processes=required_processes,
    )


def _sample_counter_from_ids(sample_ids: Set[str]) -> int:
    max_counter = 0
    for sample_id in sample_ids:
        tail = sample_id.rsplit("_", 1)[-1]
        if tail.isdigit():
            max_counter = max(max_counter, int(tail))
    return max(max_counter, len(sample_ids))


def load_last_world_state(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None

    last_state: Optional[Dict[str, Any]] = None
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            txt = line.strip()
            if not txt:
                continue
            try:
                rec = json.loads(txt)
            except Exception:
                continue
            if not isinstance(rec, dict):
                continue
            state_after = rec.get("state_after")
            if isinstance(state_after, dict):
                last_state = state_after
    return last_state


def restore_world_from_state(world: WorldModel, state: Dict[str, Any]) -> None:
    world.rack_placements.clear()
    world.rack_in_gripper_id = None
    for rack in world.racks.values():
        rack.occupied_slots.clear()
        rack.reserved_slots.clear()

    world.samples.clear()
    world.sample_states.clear()

    robot_station = state.get("robot_current_station_id")
    if isinstance(robot_station, str) and robot_station in world.stations:
        world.set_robot_station(robot_station)
    else:
        world.robot_current_station_id = None

    sample_ids: Set[str] = set()
    for raw_rack in state.get("racks", []) if isinstance(state.get("racks", []), list) else []:
        if not isinstance(raw_rack, dict):
            continue
        station_id = str(raw_rack.get("station_id", ""))
        station_slot_id = str(raw_rack.get("station_slot_id", ""))
        rack_id = str(raw_rack.get("rack_id", ""))
        if not station_id or not station_slot_id or not rack_id:
            continue
        if rack_id not in world.racks:
            continue

        try:
            world.place_rack(station_id=station_id, station_slot_id=station_slot_id, rack_id=rack_id)
        except Exception:
            continue

        rack = world.racks[rack_id]
        rack.occupied_slots = _slot_map_from_raw(raw_rack.get("occupied_slots", {}))
        rack.reserved_slots = _slot_map_from_raw(raw_rack.get("reserved_slots", {}))
        sample_ids.update(rack.occupied_slots.values())
        sample_ids.update(rack.reserved_slots.values())

    for sample_id in sorted(sample_ids):
        _ensure_sample_exists(world, sample_id)

    rack_in_gripper_raw = state.get("rack_in_gripper_id")
    if isinstance(rack_in_gripper_raw, str) and rack_in_gripper_raw in world.racks:
        world.rack_in_gripper_id = rack_in_gripper_raw
        # Keep single-location invariant if the same rack is also listed in a station slot.
        for key, placed_rack_id in list(world.rack_placements.items()):
            if placed_rack_id == rack_in_gripper_raw:
                world.rack_placements.pop(key, None)

    raw_locations = state.get("sample_locations", [])
    if isinstance(raw_locations, list):
        for raw_loc in raw_locations:
            if not isinstance(raw_loc, dict):
                continue
            sample_id = str(raw_loc.get("sample_id", "")).strip()
            if not sample_id:
                continue
            _ensure_sample_exists(world, sample_id)

            location_type = str(raw_loc.get("location_type", "RACK")).upper()
            if location_type == "GRIPPER":
                location = GripperLocation(gripper_id=str(raw_loc.get("gripper_id", "uLM_GRIPPER")))
            else:
                station_id = str(raw_loc.get("station_id", "")).strip()
                station_slot_id = str(raw_loc.get("station_slot_id", "")).strip()
                rack_id = str(raw_loc.get("rack_id", "")).strip()
                slot_index_raw = raw_loc.get("slot_index")
                if not station_id or not station_slot_id or not rack_id or slot_index_raw is None:
                    continue
                try:
                    slot_index = int(slot_index_raw)
                except Exception:
                    continue
                location = RackLocation(
                    station_id=station_id,
                    station_slot_id=station_slot_id,
                    rack_id=rack_id,
                    slot_index=slot_index,
                )

            world.sample_states[sample_id] = SampleState(sample_id=sample_id, location=location)

    for (station_id, station_slot_id), rack_id in sorted(world.rack_placements.items()):
        rack = world.racks[rack_id]
        for slot_index, sample_id in sorted(rack.occupied_slots.items()):
            _ensure_sample_exists(world, sample_id)
            if sample_id not in world.sample_states:
                world.sample_states[sample_id] = SampleState(
                    sample_id=sample_id,
                    location=RackLocation(
                        station_id=station_id,
                        station_slot_id=station_slot_id,
                        rack_id=rack_id,
                        slot_index=slot_index,
                    ),
                )

    world._sample_counter = _sample_counter_from_ids(set(world.samples.keys()))


def _rack_id_at(world: WorldModel, station_id: str, station_slot_id: str) -> Optional[str]:
    rack_id = world.rack_placements.get((station_id, station_slot_id))
    if rack_id is None:
        return None
    if rack_id not in world.racks:
        return None
    return rack_id


def _find_rack_location(world: WorldModel, rack_id: str) -> Optional[Tuple[str, str]]:
    for (station_id, station_slot_id), rid in world.rack_placements.items():
        if rid == rack_id:
            return (station_id, station_slot_id)
    return None


def prepare_input_rack_for_new_batch(world: WorldModel) -> None:
    rack_id = _rack_id_at(world, INPUT_STATION_ID, INPUT_SLOT_ID)
    if rack_id is None:
        rack_id = _rack_id_at(world, PLATE_STATION_ID, PLATE_RACK_SLOT_ID)

    if rack_id is None and world.rack_in_gripper_id:
        gripped_rack = world.racks.get(world.rack_in_gripper_id)
        if gripped_rack and gripped_rack.rack_type == RackType.URG_RACK:
            rack_id = gripped_rack.id

    if rack_id is None:
        for candidate_id, rack in world.racks.items():
            if rack.rack_type == RackType.URG_RACK:
                rack_id = candidate_id
                break

    if rack_id is None:
        print("New-batch mode: no URG rack found to prepare")
        return

    rack = world.racks[rack_id]
    if rack.rack_type != RackType.URG_RACK:
        print(f"New-batch mode: rack '{rack_id}' is not URG_RACK, skipping")
        return

    location = _find_rack_location(world, rack_id)
    if location is None:
        if world.rack_in_gripper_id == rack_id:
            world.rack_in_gripper_id = None
        try:
            world.place_rack(INPUT_STATION_ID, INPUT_SLOT_ID, rack_id)
            location = (INPUT_STATION_ID, INPUT_SLOT_ID)
        except Exception as exc:
            print(f"New-batch mode: cannot place rack '{rack_id}' at input station: {exc}")
            return

    if location != (INPUT_STATION_ID, INPUT_SLOT_ID):
        if _rack_id_at(world, INPUT_STATION_ID, INPUT_SLOT_ID) is not None:
            print(
                "New-batch mode: InputStation.URGRackSlot already occupied; "
                "cannot relocate input rack automatically"
            )
            return
        try:
            world.move_rack(
                source_station_id=location[0],
                source_station_slot_id=location[1],
                target_station_id=INPUT_STATION_ID,
                target_station_slot_id=INPUT_SLOT_ID,
            )
            location = (INPUT_STATION_ID, INPUT_SLOT_ID)
        except Exception as exc:
            print(f"New-batch mode: failed to move rack '{rack_id}' to input station: {exc}")
            return

    removed_sample_ids = set(rack.occupied_slots.values()) | set(rack.reserved_slots.values())
    rack.occupied_slots.clear()
    rack.reserved_slots.clear()

    for sample_id in removed_sample_ids:
        state = world.sample_states.get(sample_id)
        if isinstance(state.location, RackLocation) and state.location.rack_id == rack_id:
            world.sample_states.pop(sample_id, None)
        if sample_id not in world.sample_states:
            world.samples.pop(sample_id, None)

    world._sample_counter = _sample_counter_from_ids(set(world.samples.keys()))
    print(
        "New-batch mode: input rack prepared at InputStation; "
        "previous samples on the input rack were cleared from world state"
    )


def load_world_with_resume(world_config_file: Path, occupancy_events_file: Path) -> WorldModel:
    world = ensure_world_config_file(world_config_file)
    if not RESUME_FROM_LAST_WORLD_SNAPSHOT:
        if FORCE_INPUT_RACK_AT_INPUT_ON_START:
            prepare_input_rack_for_new_batch(world)
        return world

    last_state = load_last_world_state(occupancy_events_file)
    if last_state is None:
        print("World resume: no previous snapshot found, using world_config baseline")
        if FORCE_INPUT_RACK_AT_INPUT_ON_START:
            prepare_input_rack_for_new_batch(world)
        return world

    try:
        restore_world_from_state(world, last_state)
        print(f"World resume: restored from {occupancy_events_file.resolve()}")
    except Exception as exc:
        print(f"World resume failed ({exc}), using world_config baseline")
    if FORCE_INPUT_RACK_AT_INPUT_ON_START:
        prepare_input_rack_for_new_batch(world)
    return world


__all__ = [
    "load_last_world_state",
    "restore_world_from_state",
    "load_world_with_resume",
    "prepare_input_rack_for_new_batch",
]
