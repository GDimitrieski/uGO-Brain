import csv
import json
import os
import re
import shutil
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from engine.bt_nodes import ActionNode, Blackboard, ConditionNode, ForEachNode, RetryNode, SequenceNode, Status
from engine.command_layer import CommandSender
from planning.planner import Goal, PlanStep, RulePlanner
from routing.sample_routing import (
    ChainedSampleRouter,
    HardRuleRoutingProvider,
    LisRoutingProvider,
    RuleBasedRoutingProvider,
    SampleRoutingRequest,
    TrainingCatalogRoutingProvider,
)
from Device.centrifuge_usage_strategy import (
    CentrifugeUsagePlan,
    DeviceActionStep,
    RackTransferStep,
    RunningValidationStep,
    SampleTransferStep,
    ValidationStep,
    compile_centrifuge_usage_plan,
)
from Device.registry import build_device_registry_from_world
from engine.sender import build_sender
from world.export_world_snapshot_jsonl import build_snapshot_records, write_jsonl
from world.lab_world import (
    CapState,
    GripperLocation,
    ProcessType,
    RackLocation,
    RackType,
    Sample,
    SampleState,
    SlotKind,
    WorldModel,
    ensure_world_config_file,
)

ACTION_PICK = 1
ACTION_PLACE = 2
DEVICE_ACTION_OPEN_HATCH = 1
DEVICE_ACTION_START_CENTRIFUGE = 2
DEVICE_ACTION_CLOSE_HATCH = 3
DEVICE_ACTION_MOVE_ROTOR = 4
DEVICE_ACTION_SCAN_LANDMARK = 30
OBJ_TYPE_PROBE = 101
RACK_SLOT_INDEX = 1

INPUT_STATION_ID = "InputStation"
INPUT_SLOT_ID = "URGRackSlot"
PLATE_STATION_ID = "uLMPlateStation"
PLATE_RACK_SLOT_ID = "URGRackSlot"
THREE_FINGER_STATION_ID = "3-FingerGripperStation"
THREE_FINGER_SLOT_ID = "SampleSlot1"
CENTRIFUGE_STATION_ID = "CentrifugeStation"
BASE_DIR = PROJECT_ROOT
WORLD_DIR = PROJECT_ROOT / "world"
TRACE_DIR = PROJECT_ROOT / "tracing"
WORLD_CONFIG_FILE = WORLD_DIR / "world_config.json"
TRACE_FILE = TRACE_DIR / "tree_execution_trace.csv"
STATE_CHANGES_FILE = TRACE_DIR / "tree_state_changes.csv"
OCCUPANCY_TRACE_FILE = WORLD_DIR / "world_occupancy_trace.csv"
OCCUPANCY_EVENTS_FILE = WORLD_DIR / "world_occupancy_trace.jsonl"
WORLD_SNAPSHOT_FILE = WORLD_DIR / "world_snapshot.jsonl"
TRACE_WIP_FILE = TRACE_DIR / "tree_execution_trace.wip.csv"
STATE_CHANGES_WIP_FILE = TRACE_DIR / "tree_state_changes.wip.csv"
OCCUPANCY_TRACE_WIP_FILE = WORLD_DIR / "world_occupancy_trace.wip.csv"
OCCUPANCY_EVENTS_WIP_FILE = WORLD_DIR / "world_occupancy_trace.wip.jsonl"
WORLD_SNAPSHOT_WIP_FILE = WORLD_DIR / "world_snapshot.wip.jsonl"
WORLD_BACKUP_DIR = WORLD_DIR / "versions"
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
FINAL_PLATE_TARGET = os.getenv("UGO_FINAL_PLATE_TARGET", "").strip().upper()
WORKFLOW_MODE = os.getenv("UGO_WORKFLOW_MODE", "GETTING_NEW_SAMPLES").strip().upper()
CENTRIFUGE_MODE = os.getenv("UGO_CENTRIFUGE_MODE", "AUTO").strip().upper()
SAMPLE_ROUTING_RULES_FILE = Path(
    os.getenv("UGO_SAMPLE_ROUTING_RULES_FILE", str(PROJECT_ROOT / "routing" / "sample_routing_rules.json"))
).resolve()
TRAINING_WORKFLOWS_FILE = Path(
    os.getenv(
        "UGO_TRAINING_WORKFLOWS_FILE",
        str(PROJECT_ROOT / "TrainingData" / "Workflows" / "Workflows_Training.xlsx"),
    )
).resolve()
ENABLE_TRAINING_CATALOG_ROUTING = os.getenv("UGO_ENABLE_TRAINING_CATALOG_ROUTING", "0").strip().lower() in {
    "1",
    "true",
    "yes",
}
INVALID_SAMPLE_TARGET_SLOT_ID = os.getenv("UGO_INVALID_SAMPLE_TARGET_SLOT_ID", "IntermediateRackSlot1").strip()
ENABLE_RULES_DEFAULT = os.getenv("UGO_ENABLE_RULES_DEFAULT", "1").strip().lower() in {
    "1",
    "true",
    "yes",
}
ENABLE_LIS_ROUTING = os.getenv("UGO_ENABLE_LIS_ROUTING", "").strip().lower() in {"1", "true", "yes"}
LIS_ROUTING_URL = os.getenv("UGO_LIS_ROUTING_URL", "").strip()
LIS_ROUTING_TOKEN = os.getenv("UGO_LIS_ROUTING_TOKEN", "").strip()
try:
    LIS_ROUTING_TIMEOUT_S = float(os.getenv("UGO_LIS_ROUTING_TIMEOUT_S", "2.0").strip())
except Exception:
    LIS_ROUTING_TIMEOUT_S = 2.0
_WORLD_FILE_BACKUPS_DONE: Set[Path] = set()
STATE_CHANGE_FIELDNAMES = [
    "task_id",
    "command_sent",
    "change_index",
    "state",
    "timestamp",
    "task_outputs",
    "task_output_results",
    "task_output_position",
    "task_data",
]


def _try_parse_string_list(value: str) -> Optional[List[int]]:
    txt = value.strip()
    if not txt:
        return []

    # JSON-style list string, e.g. "[1,2,3]"
    if txt.startswith("[") and txt.endswith("]"):
        try:
            parsed = json.loads(txt)
            if isinstance(parsed, list):
                out: List[int] = []
                for item in parsed:
                    out.append(int(item))
                return out
        except Exception:
            pass

    # Comma-separated string, e.g. "1,2,3"
    try:
        return [int(part.strip()) for part in txt.split(",") if part.strip()]
    except Exception:
        return None


def extract_positions(result: Dict[str, Any]) -> List[int]:
    raw = result.get("raw", {})
    data = raw.get("data", {}) if isinstance(raw, dict) else {}
    outputs = data.get("outputs", {}) if isinstance(data, dict) else {}

    candidates = [
        data.get("positions"),
        data.get("detectedPositions"),
        data.get("samplePositions"),
        data.get("samples"),
        outputs.get("Results") if isinstance(outputs, dict) else None,
        outputs.get("results") if isinstance(outputs, dict) else None,
        outputs.get("Detected") if isinstance(outputs, dict) else None,
        outputs.get("detected") if isinstance(outputs, dict) else None,
        outputs.get("Positions") if isinstance(outputs, dict) else None,
        outputs.get("positions") if isinstance(outputs, dict) else None,
        raw.get("positions") if isinstance(raw, dict) else None,
        raw.get("detectedPositions") if isinstance(raw, dict) else None,
    ]

    for candidate in candidates:
        if isinstance(candidate, list):
            out: List[int] = []
            for item in candidate:
                if isinstance(item, int):
                    out.append(item)
                elif isinstance(item, str):
                    out.append(int(item))
                elif isinstance(item, dict):
                    value = item.get("position", item.get("slot", item.get("index")))
                    if value is not None:
                        out.append(int(value))
            if out:
                return out
        elif isinstance(candidate, str):
            parsed = _try_parse_string_list(candidate)
            if parsed is not None:
                return parsed

    # Last resort: task may return list as plain message string
    msg = str(result.get("message", "")).strip()
    parsed_msg = _try_parse_string_list(msg)
    if parsed_msg is not None:
        return parsed_msg

    return []


def extract_sample_type(result: Dict[str, Any]) -> Optional[int]:
    raw = result.get("raw", {})
    data = raw.get("data", {}) if isinstance(raw, dict) else {}
    outputs = data.get("outputs", {}) if isinstance(data, dict) else {}

    candidates = [
        outputs.get("SampleType") if isinstance(outputs, dict) else None,
        outputs.get("sampleType") if isinstance(outputs, dict) else None,
        outputs.get("Type") if isinstance(outputs, dict) else None,
        outputs.get("type") if isinstance(outputs, dict) else None,
        outputs.get("Results") if isinstance(outputs, dict) else None,
        outputs.get("results") if isinstance(outputs, dict) else None,
        data.get("sampleType"),
        data.get("type"),
        result.get("message"),
    ]

    for candidate in candidates:
        if candidate is None:
            continue
        if isinstance(candidate, int):
            if 1 <= candidate <= 4:
                return candidate
            continue

        txt = str(candidate).strip()
        if not txt:
            continue

        if txt.isdigit():
            value = int(txt)
            if 1 <= value <= 4:
                return value
            continue

        match = re.search(r"\b([1-4])\b", txt)
        if match:
            return int(match.group(1))

    return None


def extract_sample_barcode(result: Dict[str, Any]) -> Optional[str]:
    raw = result.get("raw", {})
    data = raw.get("data", {}) if isinstance(raw, dict) else {}
    outputs = data.get("outputs", {}) if isinstance(data, dict) else {}

    candidates: List[Tuple[Any, str]] = []
    if isinstance(outputs, dict):
        candidates.extend(
            [
                (outputs.get("Barcode"), "barcode_field"),
                (outputs.get("barcode"), "barcode_field"),
                (outputs.get("SampleBarcode"), "barcode_field"),
                (outputs.get("sampleBarcode"), "barcode_field"),
                (outputs.get("SampleId"), "barcode_field"),
                (outputs.get("sampleId"), "barcode_field"),
                (outputs.get("Results"), "barcode_field"),
                (outputs.get("results"), "barcode_field"),
            ]
        )
    if isinstance(data, dict):
        candidates.extend(
            [
                (data.get("Barcode"), "barcode_field"),
                (data.get("barcode"), "barcode_field"),
                (data.get("SampleBarcode"), "barcode_field"),
                (data.get("sampleBarcode"), "barcode_field"),
            ]
        )
    candidates.append((result.get("message"), "message"))

    for candidate, source_kind in candidates:
        if candidate is None:
            continue
        txt = str(candidate).strip()
        if not txt:
            continue
        if txt.lower() in {"none", "null", "n/a"}:
            continue
        if source_kind == "message" and txt.isdigit() and len(txt) <= 2:
            continue
        return txt
    return None


def build_sample_router() -> ChainedSampleRouter:
    providers: List[Any] = []
    providers.append(
        HardRuleRoutingProvider(
            invalid_target_station_slot_id=INVALID_SAMPLE_TARGET_SLOT_ID or "IntermediateRackSlot1"
        )
    )
    try:
        providers.append(
            RuleBasedRoutingProvider.from_file(
                SAMPLE_ROUTING_RULES_FILE,
                apply_default=ENABLE_RULES_DEFAULT,
            )
        )
    except Exception as exc:
        print(f"Sample routing rules load failed ({SAMPLE_ROUTING_RULES_FILE}): {exc}")
    if ENABLE_TRAINING_CATALOG_ROUTING:
        try:
            providers.append(
                TrainingCatalogRoutingProvider.from_xlsx(
                    TRAINING_WORKFLOWS_FILE,
                )
            )
        except Exception as exc:
            print(f"Training workflow catalog load failed ({TRAINING_WORKFLOWS_FILE}): {exc}")
    if ENABLE_LIS_ROUTING and LIS_ROUTING_URL:
        providers.append(
            LisRoutingProvider(
                endpoint=LIS_ROUTING_URL,
                token=LIS_ROUTING_TOKEN,
                timeout_s=LIS_ROUTING_TIMEOUT_S,
            )
        )
    return ChainedSampleRouter(providers=providers)


def _local_now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="milliseconds")


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
    world.samples[sample_id] = Sample(
        id=sample_id,
        barcode=sample_id,
        obj_type=OBJ_TYPE_PROBE,
        length_mm=75.0,
        diameter_mm=13.0,
        cap_state=CapState.CAPPED,
        required_processes=(ProcessType.CENTRIFUGATION,),
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


def export_trace(records: List[Dict[str, Any]], path: Path) -> None:
    if not records:
        return

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

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for rec in records:
            writer.writerow(rec)


def export_state_changes(records: List[Dict[str, Any]], path: Path) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=STATE_CHANGE_FIELDNAMES)
        writer.writeheader()
        for rec in records:
            writer.writerow(rec)


def _trace_fieldnames_from_catalog(sender: CommandSender) -> List[str]:
    tasks_raw = sender.catalog.raw.get("Available_Tasks", {})
    param_keys: Set[str] = set()
    if isinstance(tasks_raw, dict):
        for task_def in tasks_raw.values():
            if not isinstance(task_def, dict):
                continue
            payload_template = task_def.get("payload_template", {})
            if isinstance(payload_template, dict):
                param_keys.update(str(k) for k in payload_template.keys())
            parameters = task_def.get("parameters", {})
            if isinstance(parameters, dict):
                param_keys.update(str(k) for k in parameters.keys())
    param_keys.update(
        {
            "task_outputs",
            "task_output_results",
            "task_output_position",
            "task_data",
        }
    )

    excluded = {
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
    ordered_param_keys = sorted(k for k in param_keys if k not in excluded)
    return [
        "timestamp_sent",
        "command_sent",
        *ordered_param_keys,
        "result",
        "task_id",
        "receiver",
        "dispatch_path",
        "message",
        "state_path",
        "state_timeline",
        "timestamp_returned",
    ]


def _init_live_trace_files(trace_fieldnames: List[str]) -> None:
    TRACE_DIR.mkdir(parents=True, exist_ok=True)
    with open(TRACE_WIP_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=trace_fieldnames)
        writer.writeheader()
    with open(STATE_CHANGES_WIP_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=STATE_CHANGE_FIELDNAMES)
        writer.writeheader()


def _append_live_trace_record(record: Dict[str, Any], trace_fieldnames: List[str]) -> None:
    with open(TRACE_WIP_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=trace_fieldnames, extrasaction="ignore")
        writer.writerow(record)


def _append_live_state_change(record: Dict[str, Any]) -> None:
    with open(STATE_CHANGES_WIP_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=STATE_CHANGE_FIELDNAMES, extrasaction="ignore")
        writer.writerow(record)


def _init_live_world_files() -> None:
    WORLD_DIR.mkdir(parents=True, exist_ok=True)
    with open(OCCUPANCY_TRACE_WIP_FILE, "w", encoding="utf-8") as f:
        f.write("")
    with open(OCCUPANCY_EVENTS_WIP_FILE, "w", encoding="utf-8") as f:
        f.write("")


def _append_live_world_event(event: Dict[str, Any]) -> None:
    line = json.dumps(event, ensure_ascii=True)
    with open(OCCUPANCY_TRACE_WIP_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")
    with open(OCCUPANCY_EVENTS_WIP_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


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


TRACE_RECORD_META_KEYS = {
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
    "task_outputs",
    "task_output_results",
    "task_output_position",
    "task_data",
}


def _parse_iso_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    txt = str(value).strip()
    if not txt:
        return None
    if txt.endswith("Z"):
        txt = txt[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(txt)
    except Exception:
        return None


def _parse_json_maybe(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list, int, float, bool)):
        return value
    txt = str(value).strip()
    if not txt:
        return None
    if txt.lower() in {"none", "null"}:
        return None
    if (txt.startswith("{") and txt.endswith("}")) or (txt.startswith("[") and txt.endswith("]")):
        try:
            return json.loads(txt)
        except Exception:
            return txt
    return txt


def _normalize_dispatch_path(value: Any) -> List[str]:
    if isinstance(value, list):
        return [str(part).strip() for part in value if str(part).strip()]
    txt = str(value or "").strip()
    if not txt:
        return []
    return [part.strip() for part in txt.split(">") if part.strip()]


def _task_parameters_from_trace_record(record: Dict[str, Any]) -> Dict[str, Any]:
    params: Dict[str, Any] = {}
    for key in sorted(record.keys()):
        if key in TRACE_RECORD_META_KEYS:
            continue
        parsed = _parse_json_maybe(record.get(key))
        if parsed is None:
            continue
        params[key] = parsed
    return params


def _task_outputs_from_trace_record(record: Dict[str, Any]) -> Dict[str, Any]:
    outputs: Dict[str, Any] = {}
    parsed_outputs = _parse_json_maybe(record.get("task_outputs"))
    if parsed_outputs is not None:
        outputs["outputs"] = parsed_outputs

    parsed_results = _parse_json_maybe(record.get("task_output_results"))
    if parsed_results is not None:
        outputs["results"] = parsed_results

    parsed_position = _parse_json_maybe(record.get("task_output_position"))
    if parsed_position is not None:
        outputs["position"] = parsed_position

    parsed_task_data = _parse_json_maybe(record.get("task_data"))
    if parsed_task_data is not None:
        outputs["task_data"] = parsed_task_data
    return outputs


def _task_context_from_trace_record(record: Dict[str, Any]) -> Dict[str, Any]:
    context: Dict[str, Any] = {
        "task_id": str(record.get("task_id") or ""),
        "task_key": str(record.get("command_sent") or ""),
        "status": str(record.get("result") or ""),
        "receiver": str(record.get("receiver") or ""),
        "dispatch_path": _normalize_dispatch_path(record.get("dispatch_path")),
        "timestamps": {
            "sent": str(record.get("timestamp_sent") or ""),
            "returned": str(record.get("timestamp_returned") or ""),
        },
        "parameters": _task_parameters_from_trace_record(record),
        "outputs": _task_outputs_from_trace_record(record),
    }
    message = str(record.get("message") or "").strip()
    if message:
        context["message"] = message

    state_path = str(record.get("state_path") or "").strip()
    if state_path:
        context["state_path"] = state_path

    state_timeline = str(record.get("state_timeline") or "").strip()
    if state_timeline:
        context["state_timeline"] = state_timeline
    return context


def _match_task_context_for_event(
    event_ts: datetime, trace_rows: List[Dict[str, Any]], max_previous_gap_s: float = 5.0
) -> Optional[Dict[str, Any]]:
    covering: Optional[Dict[str, Any]] = None
    covering_delta_s: Optional[float] = None
    for row in trace_rows:
        sent_ts = row.get("sent_ts")
        returned_ts = row.get("returned_ts")
        if sent_ts is None and returned_ts is None:
            continue
        if sent_ts is None:
            sent_ts = returned_ts
        if returned_ts is None:
            returned_ts = sent_ts
        if sent_ts is None or returned_ts is None:
            continue
        if sent_ts <= event_ts <= (returned_ts + timedelta(seconds=0.75)):
            delta_s = abs((event_ts - returned_ts).total_seconds())
            if covering is None or covering_delta_s is None or delta_s < covering_delta_s:
                covering = row
                covering_delta_s = delta_s
    if covering is not None:
        return dict(covering.get("context") or {})

    best_previous: Optional[Dict[str, Any]] = None
    best_gap_s: Optional[float] = None
    for row in trace_rows:
        anchor_ts = row.get("returned_ts") or row.get("sent_ts")
        if anchor_ts is None or anchor_ts > event_ts:
            continue
        gap_s = (event_ts - anchor_ts).total_seconds()
        if best_previous is None or best_gap_s is None or gap_s < best_gap_s:
            best_previous = row
            best_gap_s = gap_s

    if best_previous is None or best_gap_s is None or best_gap_s > max_previous_gap_s:
        return None
    return dict(best_previous.get("context") or {})


def enrich_occupancy_records_with_task_context(
    occupancy_records: List[Dict[str, Any]],
    trace_records: List[Dict[str, Any]],
) -> None:
    if not occupancy_records or not trace_records:
        return

    trace_rows: List[Dict[str, Any]] = []
    for trace_record in trace_records:
        if not isinstance(trace_record, dict):
            continue
        sent_ts = _parse_iso_datetime(trace_record.get("timestamp_sent"))
        returned_ts = _parse_iso_datetime(trace_record.get("timestamp_returned"))
        if sent_ts is None and returned_ts is None:
            continue
        trace_rows.append(
            {
                "sent_ts": sent_ts,
                "returned_ts": returned_ts or sent_ts,
                "context": _task_context_from_trace_record(trace_record),
            }
        )

    if not trace_rows:
        return

    for event in occupancy_records:
        if not isinstance(event, dict):
            continue
        if isinstance(event.get("task_context"), dict):
            continue
        if str(event.get("event_type") or "").upper() == "WORLD_SNAPSHOT":
            continue
        event_ts = _parse_iso_datetime(event.get("timestamp"))
        if event_ts is None:
            continue
        context = _match_task_context_for_event(event_ts, trace_rows)
        if context:
            event["task_context"] = context


def append_world_event(
    records: List[Dict[str, Any]],
    world: WorldModel,
    event_type: str,
    entity_type: str,
    entity_id: str,
    source: Optional[Dict[str, Any]] = None,
    target: Optional[Dict[str, Any]] = None,
    details: Optional[Dict[str, Any]] = None,
    task_context: Optional[Dict[str, Any]] = None,
) -> None:
    event = {
        "timestamp": _local_now_iso(),
        "event_type": event_type,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "source": source or {},
        "target": target or {},
        "details": details or {},
        "state_after": _world_state_snapshot(world),
    }
    if task_context:
        event["task_context"] = dict(task_context)
    records.append(event)
    _append_live_world_event(event)
    _sync_world_snapshot_file(world)


def _sync_world_snapshot_file(world: WorldModel) -> None:
    try:
        records = build_snapshot_records(world, config_path=WORLD_CONFIG_FILE)
        write_jsonl(WORLD_SNAPSHOT_WIP_FILE, records)
    except Exception as exc:
        print(f"World snapshot sync failed ({WORLD_SNAPSHOT_WIP_FILE}): {exc}")


def _finalize_world_snapshot_file(world: WorldModel) -> None:
    try:
        _backup_world_file_once(WORLD_SNAPSHOT_FILE)
        records = build_snapshot_records(world, config_path=WORLD_CONFIG_FILE)
        write_jsonl(WORLD_SNAPSHOT_FILE, records)
    except Exception as exc:
        print(f"World snapshot finalize failed ({WORLD_SNAPSHOT_FILE}): {exc}")


def export_occupancy_trace(
    records: List[Dict[str, Any]],
    path: Path,
    trace_records: Optional[List[Dict[str, Any]]] = None,
) -> None:
    if trace_records:
        enrich_occupancy_records_with_task_context(records, trace_records)
    # Keep historical filename, but write JSONL records for digital-twin/event replay use.
    _backup_world_file_once(path)
    with open(path, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=True) + "\n")


def export_occupancy_events_jsonl(
    records: List[Dict[str, Any]],
    path: Path,
    trace_records: Optional[List[Dict[str, Any]]] = None,
) -> None:
    if trace_records:
        enrich_occupancy_records_with_task_context(records, trace_records)
    _backup_world_file_once(path)
    with open(path, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=True) + "\n")


def _backup_world_file_once(path: Path) -> None:
    """Create one timestamped backup per file per process before overwrite."""
    canonical = path.resolve()
    if canonical in _WORLD_FILE_BACKUPS_DONE:
        return
    _WORLD_FILE_BACKUPS_DONE.add(canonical)

    if not path.exists():
        return
    try:
        if path.stat().st_size <= 0:
            return
    except Exception:
        return

    stamp = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S_%f")[:-3]
    backup_name = f"{path.stem}.{stamp}{path.suffix}.bak"
    backup_path = WORLD_BACKUP_DIR / backup_name

    try:
        WORLD_BACKUP_DIR.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, backup_path)
        print(f"World file backup created: {backup_path.resolve()}")
    except Exception as exc:
        print(f"World file backup failed for {path}: {exc}")


def build_tree(
    sender: CommandSender,
    world: WorldModel,
    trace_records: List[Dict[str, Any]],
    state_change_records: List[Dict[str, Any]],
    occupancy_records: List[Dict[str, Any]],
    trace_fieldnames: List[str],
) -> SequenceNode:
    """Build workflow tree with scaffold steps and executable centrifuge cycle phase."""

    workflow_mode = str(WORKFLOW_MODE).strip().upper()
    required_station_ids = {
        INPUT_STATION_ID,
        PLATE_STATION_ID,
        THREE_FINGER_STATION_ID,
        "CHARGE",
    }
    required_task_keys = {
        "SingleTask",
        "ProcessAt3FingerStation",
        "InspectRackAtStation",
    }
    scaffold_steps = [
        "NavigateToStation",
        "ScanStationLandmark",
        "TransferRackBetweenStations",
        "ChargeAtStation",
        "InspectRackAtStation",
        "RouteUrgVia3Finger",
        "CentrifugeCycle",
    ]
    centrifuge_modes = {
        "CENTRIFUGE_DEVICE_SERVICE",
        "CENTRIFUGE_SERVICE",
        "CENTRIFUGE",
        "ROTINA380_SERVICE",
    }
    planner_goal = Goal(
        name="GETTING_NEW_SAMPLES",
        options={"workflow_mode": "GETTING_NEW_SAMPLES"},
    )
    planner_plan_steps: List[PlanStep] = []
    planner_plan_error = ""
    if workflow_mode == "GETTING_NEW_SAMPLES":
        try:
            planner_plan_steps = RulePlanner().build_plan(world, planner_goal)
        except Exception as exc:
            planner_plan_error = str(exc)
        required_station_ids.update({INPUT_STATION_ID, PLATE_STATION_ID, THREE_FINGER_STATION_ID, "CHARGE"})
    elif workflow_mode in centrifuge_modes:
        required_station_ids.add(CENTRIFUGE_STATION_ID)
        required_task_keys.update({"Navigate", "Charge", "SingleTask", "SingleDeviceAction"})

    plan_step_by_id = {step.step_id: step for step in planner_plan_steps}
    active_step_names = (
        [step.step_id for step in planner_plan_steps]
        if workflow_mode == "GETTING_NEW_SAMPLES" and planner_plan_steps
        else list(scaffold_steps)
    )
    runtime_devices = build_device_registry_from_world(world)

    def validate_scaffold_prerequisites(bb: Blackboard) -> bool:
        missing_stations = [sid for sid in sorted(required_station_ids) if sid not in world.stations]
        if missing_stations:
            print(
                "Workflow prerequisite failed: missing stations in world config: "
                f"{missing_stations}"
            )
            return False

        available_tasks_raw = sender.catalog.raw.get("Available_Tasks", {})
        available_task_keys = set(available_tasks_raw.keys()) if isinstance(available_tasks_raw, dict) else set()
        required_task_keys_local = set(required_task_keys)
        if workflow_mode == "GETTING_NEW_SAMPLES":
            if planner_plan_error:
                print(f"Planner prerequisite failed: cannot build GETTING_NEW_SAMPLES plan ({planner_plan_error})")
                return False
            if not planner_plan_steps:
                print("Planner prerequisite failed: GETTING_NEW_SAMPLES plan is empty")
                return False
            required_task_keys_local.update(RulePlanner.task_keys(planner_plan_steps))
        missing_task_keys = [key for key in sorted(required_task_keys_local) if key not in available_task_keys]
        if missing_task_keys:
            print(
                "Workflow prerequisite failed: missing task definitions in Available_Tasks.json: "
                f"{missing_task_keys}"
            )
            return False

        bb["workflow_mode"] = WORKFLOW_MODE
        bb["centrifuge_mode"] = CENTRIFUGE_MODE or "AUTO"
        bb["final_plate_target"] = FINAL_PLATE_TARGET or None
        if workflow_mode == "GETTING_NEW_SAMPLES":
            bb["tree_profile"] = "planner_getting_new_samples_v1"
            bb["planned_steps"] = [step.step_id for step in planner_plan_steps]
        else:
            bb["tree_profile"] = "blank_scaffold_v1"
        bb["scaffold_steps"] = list(active_step_names)
        if workflow_mode in centrifuge_modes:
            runtime_centrifuges = runtime_devices.get_centrifuges_at_station(CENTRIFUGE_STATION_ID)
            if not runtime_centrifuges:
                print(
                    "Centrifuge prerequisite failed: no runtime Device.Centrifuge available at "
                    "CentrifugeStation"
                )
                return False
        return True

    def _csv_value(value: Any) -> Any:
        if isinstance(value, (dict, list, tuple, set)):
            return json.dumps(value, ensure_ascii=True)
        return value

    def _append_trace_and_state_changes(
        *,
        task_key: str,
        payload: Dict[str, Any],
        result: Dict[str, Any],
        sent_ts: str,
        returned_ts: str,
    ) -> None:
        raw = result.get("raw", {}) if isinstance(result, dict) else {}
        data = raw.get("data", {}) if isinstance(raw, dict) else {}
        outputs = data.get("outputs", {}) if isinstance(data, dict) else {}
        state_history = result.get("state_history", []) if isinstance(result, dict) else []
        if not isinstance(state_history, list):
            state_history = []

        state_path = " > ".join(
            str(item.get("state", "")).strip()
            for item in state_history
            if isinstance(item, dict) and str(item.get("state", "")).strip()
        )
        state_timeline = " | ".join(
            f"{str(item.get('timestamp', '')).strip()}:{str(item.get('state', '')).strip()}"
            for item in state_history
            if isinstance(item, dict)
            and str(item.get("timestamp", "")).strip()
            and str(item.get("state", "")).strip()
        )
        dispatch_path_raw = result.get("dispatch_path", []) if isinstance(result, dict) else []
        if isinstance(dispatch_path_raw, list):
            dispatch_path_txt = " > ".join(str(x).strip() for x in dispatch_path_raw if str(x).strip())
        else:
            dispatch_path_txt = str(dispatch_path_raw or "")

        record: Dict[str, Any] = {
            "timestamp_sent": sent_ts,
            "command_sent": task_key,
            "result": str(result.get("status", "")),
            "task_id": str(result.get("task_id", "")),
            "receiver": str(result.get("receiver", "")),
            "dispatch_path": dispatch_path_txt,
            "message": str(result.get("message", "")),
            "state_path": state_path,
            "state_timeline": state_timeline,
            "timestamp_returned": returned_ts,
            "task_outputs": _csv_value(outputs),
            "task_output_results": _csv_value(outputs.get("Results")),
            "task_output_position": _csv_value(outputs.get("Position")),
            "task_data": _csv_value(data),
        }
        for key, value in payload.items():
            record[str(key)] = _csv_value(value)
        trace_records.append(record)
        _append_live_trace_record(record, trace_fieldnames)

        for idx, item in enumerate(state_history, start=1):
            if not isinstance(item, dict):
                continue
            sc_record = {
                "task_id": str(result.get("task_id", "")),
                "command_sent": task_key,
                "change_index": idx,
                "state": str(item.get("state", "")),
                "timestamp": str(item.get("timestamp", "")),
                "task_outputs": _csv_value(outputs),
                "task_output_results": _csv_value(outputs.get("Results")),
                "task_output_position": _csv_value(outputs.get("Position")),
                "task_data": _csv_value(data),
            }
            state_change_records.append(sc_record)
            _append_live_state_change(sc_record)

    def _run_task(task_key: str, overrides: Dict[str, Any], task_name: str) -> Tuple[bool, Dict[str, Any]]:
        sent_ts = _local_now_iso()
        try:
            payload = sender.catalog.build_payload(task_key, overrides=overrides)
        except Exception as exc:
            result = {
                "status": "failed",
                "message": f"{task_name} payload build failed: {exc}",
                "task_id": "",
                "receiver": "",
                "dispatch_path": [],
                "state_history": [],
                "raw": {},
            }
            returned_ts = _local_now_iso()
            _append_trace_and_state_changes(
                task_key=task_key,
                payload={"taskName": task_key, **dict(overrides)},
                result=result,
                sent_ts=sent_ts,
                returned_ts=returned_ts,
            )
            print(result["message"])
            return False, result

        result = sender.run(task_key, overrides=overrides, task_name=task_name)
        returned_ts = _local_now_iso()
        _append_trace_and_state_changes(
            task_key=task_key,
            payload=payload,
            result=result,
            sent_ts=sent_ts,
            returned_ts=returned_ts,
        )
        ok = str(result.get("status", "")).strip().lower() == "succeeded"
        if not ok:
            print(f"{task_name} failed: {result.get('message', '')}")
        return ok, result

    def _first_free_slot_index(station_id: str, station_slot_id: str) -> int:
        rack = world.get_rack_at(station_id, station_slot_id)
        occupied = {int(idx) for idx in rack.occupied_slots.keys()}
        for idx in rack.available_slots():
            if int(idx) not in occupied:
                return int(idx)
        raise ValueError(f"No free slot available at {station_id}.{station_slot_id}")

    def _default_target_jig_for_processes(processes: Sequence[ProcessType]) -> int:
        process_to_jig: Dict[ProcessType, int] = {
            ProcessType.CENTRIFUGATION: 2,
            ProcessType.IMMUNOANALYSIS: 10,
            ProcessType.HEMATOLOGY_ANALYSIS: 11,
        }

        # Route by the first rack-bound process in the declared process sequence.
        # This avoids skipping prerequisite handling (e.g. centrifugation before immunoanalysis).
        for proc in processes:
            jig = process_to_jig.get(proc)
            if jig is not None:
                return int(jig)

        return 4

    def _resolve_routing_target(
        decision_processes: Sequence[ProcessType],
        target_station_slot_id: Optional[str],
        target_rack_index: Optional[int],
    ) -> Tuple[str, str, int]:
        target_station_id = PLATE_STATION_ID
        explicit_slot_id = str(target_station_slot_id or "").strip()
        if explicit_slot_id:
            return target_station_id, explicit_slot_id, _first_free_slot_index(target_station_id, explicit_slot_id)

        target_jig_id = _default_target_jig_for_processes(decision_processes)
        if target_rack_index is not None:
            slot_cfgs = [
                cfg
                for cfg in world.slots_for_jig(target_station_id, int(target_jig_id))
                if int(getattr(cfg, "rack_index", 1)) == int(target_rack_index)
            ]
            if not slot_cfgs:
                raise ValueError(
                    f"No slot config found for station '{target_station_id}', "
                    f"JIG_ID={int(target_jig_id)}, rack_index={int(target_rack_index)}"
                )
            for cfg in sorted(slot_cfgs, key=lambda c: int(getattr(c, "rack_index", 1))):
                try:
                    slot_idx = _first_free_slot_index(target_station_id, str(cfg.slot_id))
                    return target_station_id, str(cfg.slot_id), int(slot_idx)
                except Exception:
                    continue
            raise ValueError(
                f"No free slot available for station '{target_station_id}', "
                f"JIG_ID={int(target_jig_id)}, rack_index={int(target_rack_index)}"
            )

        slot_id, slot_idx = world.select_next_target_slot_for_jig(
            station_id=target_station_id,
            jig_id=int(target_jig_id),
        )
        return target_station_id, str(slot_id), int(slot_idx)

    def _move_sample_between_slots(
        *,
        source_station_id: str,
        source_station_slot_id: str,
        source_slot_index: int,
        target_station_id: str,
        target_station_slot_id: str,
        target_slot_index: int,
        task_prefix: str,
        phase: str,
        reason: str = "",
        expected_sample_id: Optional[str] = None,
    ) -> Tuple[bool, Optional[str]]:
        source_rack = world.get_rack_at(source_station_id, source_station_slot_id)
        sample_id = source_rack.occupied_slots.get(int(source_slot_index))
        if sample_id is None:
            print(
                f"{task_prefix} failed: source slot is empty "
                f"({source_station_id}.{source_station_slot_id}[{int(source_slot_index)}])"
            )
            return False, None
        sample_id_txt = str(sample_id)
        sample = world.samples.get(sample_id_txt)
        obj_type = int(getattr(sample, "obj_type", OBJ_TYPE_PROBE))

        source_cfg = world.get_slot_config(source_station_id, source_station_slot_id)
        target_cfg = world.get_slot_config(target_station_id, target_station_slot_id)
        pick_overrides = {
            "ITM_ID": int(source_cfg.itm_id),
            "JIG_ID": int(source_cfg.jig_id),
            "OBJ_Nbr": int(world.obj_nbr_for_slot_index(source_station_id, source_station_slot_id, source_slot_index)),
            "ACTION": ACTION_PICK,
            "OBJ_Type": int(obj_type),
        }
        ok, _ = _run_task("SingleTask", pick_overrides, f"{task_prefix}.PickSample")
        if not ok:
            return False, None

        place_overrides = {
            "ITM_ID": int(target_cfg.itm_id),
            "JIG_ID": int(target_cfg.jig_id),
            "OBJ_Nbr": int(world.obj_nbr_for_slot_index(target_station_id, target_station_slot_id, target_slot_index)),
            "ACTION": ACTION_PLACE,
            "OBJ_Type": int(obj_type),
        }
        ok, _ = _run_task("SingleTask", place_overrides, f"{task_prefix}.PlaceSample")
        if not ok:
            return False, None

        try:
            moved_sample_id = world.move_sample(
                source_station_id=source_station_id,
                source_station_slot_id=source_station_slot_id,
                source_slot_index=int(source_slot_index),
                target_station_id=target_station_id,
                target_station_slot_id=target_station_slot_id,
                target_slot_index=int(target_slot_index),
            )
        except Exception as exc:
            print(f"{task_prefix} world move failed: {exc}")
            return False, None

        if expected_sample_id and str(moved_sample_id) != str(expected_sample_id):
            print(
                f"{task_prefix} sample identity mismatch: "
                f"expected={expected_sample_id}, moved={moved_sample_id}"
            )
            return False, None

        append_world_event(
            occupancy_records,
            world,
            event_type="SAMPLE_MOVED",
            entity_type="SAMPLE",
            entity_id=str(moved_sample_id),
            source={
                "station_id": source_station_id,
                "station_slot_id": source_station_slot_id,
                "slot_index": int(source_slot_index),
            },
            target={
                "station_id": target_station_id,
                "station_slot_id": target_station_slot_id,
                "slot_index": int(target_slot_index),
            },
            details={
                "phase": phase,
                "reason": str(reason or ""),
            },
        )
        return True, str(moved_sample_id)

    def _execute_getting_new_samples_phase(step_id: str, bb: Blackboard) -> bool:
        if step_id == "await_input_rack_present":
            rack_id = _rack_id_at(world, INPUT_STATION_ID, INPUT_SLOT_ID)
            if not rack_id:
                print(
                    "GettingNewSamples prerequisite failed: no rack at "
                    f"{INPUT_STATION_ID}.{INPUT_SLOT_ID}"
                )
                return False
            bb["input_rack_id"] = str(rack_id)
            return True

        if step_id == "transfer_input_rack":
            if not bool(bb.get("input_landmark_scanned", False)):
                print(
                    "GettingNewSamples transfer prerequisite failed: InputStation landmark "
                    "scan must complete before non-plate handling"
                )
                return False
            source_rack_id = _rack_id_at(world, INPUT_STATION_ID, INPUT_SLOT_ID)
            if source_rack_id is None:
                print(
                    "GettingNewSamples transfer failed: no rack at "
                    f"{INPUT_STATION_ID}.{INPUT_SLOT_ID}"
                )
                return False
            if _rack_id_at(world, PLATE_STATION_ID, PLATE_RACK_SLOT_ID) is not None:
                print(
                    "GettingNewSamples transfer failed: target slot already occupied "
                    f"({PLATE_STATION_ID}.{PLATE_RACK_SLOT_ID})"
                )
                return False

            source_cfg = world.get_slot_config(INPUT_STATION_ID, INPUT_SLOT_ID)
            target_cfg = world.get_slot_config(PLATE_STATION_ID, PLATE_RACK_SLOT_ID)
            rack = world.racks.get(source_rack_id)
            if rack is None:
                print(f"GettingNewSamples transfer failed: unknown source rack '{source_rack_id}'")
                return False
            obj_type = int(rack.pin_obj_type)
            pick_overrides = {
                "ITM_ID": int(source_cfg.itm_id),
                "JIG_ID": int(source_cfg.jig_id),
                "OBJ_Nbr": int(source_cfg.rack_index),
                "ACTION": ACTION_PICK,
                "OBJ_Type": int(obj_type),
            }
            ok, _ = _run_task("SingleTask", pick_overrides, "GettingNewSamples.TransferInputRack.Pick")
            if not ok:
                return False

            place_overrides = {
                "ITM_ID": int(target_cfg.itm_id),
                "JIG_ID": int(target_cfg.jig_id),
                "OBJ_Nbr": int(target_cfg.rack_index),
                "ACTION": ACTION_PLACE,
                "OBJ_Type": int(obj_type),
            }
            ok, _ = _run_task("SingleTask", place_overrides, "GettingNewSamples.TransferInputRack.Place")
            if not ok:
                return False

            try:
                moved_rack_id = world.move_rack(
                    source_station_id=INPUT_STATION_ID,
                    source_station_slot_id=INPUT_SLOT_ID,
                    target_station_id=PLATE_STATION_ID,
                    target_station_slot_id=PLATE_RACK_SLOT_ID,
                )
            except Exception as exc:
                print(f"GettingNewSamples transfer failed: world move failed ({exc})")
                return False

            append_world_event(
                occupancy_records,
                world,
                event_type="RACK_MOVED",
                entity_type="RACK",
                entity_id=str(moved_rack_id),
                source={"station_id": INPUT_STATION_ID, "station_slot_id": INPUT_SLOT_ID},
                target={"station_id": PLATE_STATION_ID, "station_slot_id": PLATE_RACK_SLOT_ID},
                details={
                    "phase": "GettingNewSamples",
                    "step_id": step_id,
                },
            )
            bb["input_rack_id"] = str(moved_rack_id)
            return True

        if step_id == "camera_inspect_urg_for_new_samples":
            rack_id = _rack_id_at(world, PLATE_STATION_ID, PLATE_RACK_SLOT_ID)
            if rack_id is None:
                print(
                    "GettingNewSamples inspect failed: no URG rack on plate "
                    f"({PLATE_STATION_ID}.{PLATE_RACK_SLOT_ID})"
                )
                return False

            inspect_overrides = {
                "STATION": PLATE_STATION_ID,
                "JIG_ID": int(world.get_slot_config(PLATE_STATION_ID, PLATE_RACK_SLOT_ID).jig_id),
                "CAMERA": "WRIST",
            }
            ok, result = _run_task(
                "InspectRackAtStation",
                inspect_overrides,
                "GettingNewSamples.CameraInspectUrgRack",
            )
            if not ok:
                return False

            rack = world.get_rack_at(PLATE_STATION_ID, PLATE_RACK_SLOT_ID)
            detected_positions: List[int] = []
            for pos in extract_positions(result):
                try:
                    pos_int = int(pos)
                    rack.validate_slot(pos_int)
                    detected_positions.append(pos_int)
                except Exception:
                    continue
            detected_positions = sorted(set(detected_positions))
            detected_sample_ids: List[str] = []
            for pos in detected_positions:
                sample_id = world.ensure_placeholder_sample(
                    PLATE_STATION_ID,
                    PLATE_RACK_SLOT_ID,
                    int(pos),
                    OBJ_TYPE_PROBE,
                )
                detected_sample_ids.append(str(sample_id))
                append_world_event(
                    occupancy_records,
                    world,
                    event_type="SAMPLE_DETECTED",
                    entity_type="SAMPLE",
                    entity_id=str(sample_id),
                    source={
                        "station_id": PLATE_STATION_ID,
                        "station_slot_id": PLATE_RACK_SLOT_ID,
                        "slot_index": int(pos),
                    },
                    details={
                        "phase": "GettingNewSamples",
                        "step_id": step_id,
                    },
                )

            if not detected_sample_ids:
                detected_sample_ids = [
                    str(sid) for _, sid in sorted(rack.occupied_slots.items(), key=lambda item: int(item[0]))
                ]

            bb["detected_urg_positions"] = list(detected_positions)
            bb["detected_sample_ids"] = list(detected_sample_ids)
            return True

        if step_id == "urg_sort_via_3fg_router":
            rack_id = _rack_id_at(world, PLATE_STATION_ID, PLATE_RACK_SLOT_ID)
            if rack_id is None:
                print(
                    "GettingNewSamples routing failed: no URG rack on plate "
                    f"({PLATE_STATION_ID}.{PLATE_RACK_SLOT_ID})"
                )
                return False

            router = build_sample_router()
            sample_ids_raw = bb.get("detected_sample_ids", [])
            sample_ids: List[str] = []
            if isinstance(sample_ids_raw, list):
                sample_ids = [str(x) for x in sample_ids_raw if str(x).strip()]
            if not sample_ids:
                source_rack = world.get_rack_at(PLATE_STATION_ID, PLATE_RACK_SLOT_ID)
                sample_ids = [
                    str(sid) for _, sid in sorted(source_rack.occupied_slots.items(), key=lambda item: int(item[0]))
                ]
            if not sample_ids:
                print("GettingNewSamples routing: no samples to route from URG rack")
                bb["routed_sample_ids"] = []
                return True

            routed_sample_ids: List[str] = []
            three_fg_cfg = world.get_slot_config(THREE_FINGER_STATION_ID, THREE_FINGER_SLOT_ID)
            process_overrides = {
                "ITM_ID": int(three_fg_cfg.itm_id),
                "JIG_ID": int(three_fg_cfg.jig_id),
                "ACTION": 3,
            }
            for sample_id in sample_ids:
                state = world.sample_states.get(sample_id)
                if state is None or not isinstance(state.location, RackLocation):
                    print(f"GettingNewSamples routing failed: sample '{sample_id}' has no rack location")
                    return False
                if (
                    state.location.station_id != PLATE_STATION_ID
                    or state.location.station_slot_id != PLATE_RACK_SLOT_ID
                ):
                    continue
                source_slot_index = int(state.location.slot_index)

                ok, moved_sample_id = _move_sample_between_slots(
                    source_station_id=PLATE_STATION_ID,
                    source_station_slot_id=PLATE_RACK_SLOT_ID,
                    source_slot_index=source_slot_index,
                    target_station_id=THREE_FINGER_STATION_ID,
                    target_station_slot_id=THREE_FINGER_SLOT_ID,
                    target_slot_index=1,
                    task_prefix=f"GettingNewSamples.RouteSample.{sample_id}.To3FG",
                    phase="GettingNewSamples",
                    reason="to_3fg",
                    expected_sample_id=sample_id,
                )
                if not ok or moved_sample_id is None:
                    return False

                ok, process_result = _run_task(
                    "ProcessAt3FingerStation",
                    process_overrides,
                    f"GettingNewSamples.RouteSample.{sample_id}.DetermineSampleType",
                )
                if not ok:
                    return False

                sample_type = extract_sample_type(process_result)
                barcode = extract_sample_barcode(process_result)
                decision = router.route(
                    SampleRoutingRequest(
                        sample_id=sample_id,
                        barcode=barcode,
                        sample_type=sample_type,
                    )
                )
                decision_processes = tuple(step.process for step in decision.process_steps)
                try:
                    target_station_id, target_slot_id, target_slot_index = _resolve_routing_target(
                        decision_processes,
                        decision.target_station_slot_id,
                        decision.target_rack_index,
                    )
                except Exception as exc:
                    print(f"GettingNewSamples routing failed for sample '{sample_id}': {exc}")
                    return False

                ok, moved_sample_id = _move_sample_between_slots(
                    source_station_id=THREE_FINGER_STATION_ID,
                    source_station_slot_id=THREE_FINGER_SLOT_ID,
                    source_slot_index=1,
                    target_station_id=target_station_id,
                    target_station_slot_id=target_slot_id,
                    target_slot_index=target_slot_index,
                    task_prefix=f"GettingNewSamples.RouteSample.{sample_id}.ToDestination",
                    phase="GettingNewSamples",
                    reason=decision.classification,
                    expected_sample_id=sample_id,
                )
                if not ok or moved_sample_id is None:
                    return False

                try:
                    world.classify_sample(
                        sample_id=sample_id,
                        recognized=bool(decision.recognized),
                        classification_source=str(decision.source),
                        barcode=barcode,
                        required_processes=decision_processes or None,
                        assigned_route=str(decision.classification),
                        assigned_route_station_slot_id=str(target_slot_id),
                        assigned_route_rack_index=(
                            int(decision.target_rack_index)
                            if decision.target_rack_index is not None
                            else None
                        ),
                        classification_details={
                            "provider": str(decision.source),
                            "recognized": bool(decision.recognized),
                            "sample_type": decision.sample_type,
                            "target_station_slot_id": decision.target_station_slot_id,
                            "target_rack_index": decision.target_rack_index,
                            "details": dict(decision.details or {}),
                        },
                    )
                except Exception as exc:
                    print(f"GettingNewSamples classification failed for sample '{sample_id}': {exc}")
                    return False

                resolved_sample_id = str(sample_id)
                normalized_barcode = str(barcode).strip() if barcode is not None else ""
                if normalized_barcode:
                    try:
                        resolved_sample_id = str(
                            world.reidentify_sample(
                                sample_id=sample_id,
                                preferred_sample_id=normalized_barcode,
                                barcode=normalized_barcode,
                            )
                        )
                    except Exception as exc:
                        print(
                            "GettingNewSamples re-identification failed for sample "
                            f"'{sample_id}' with barcode '{normalized_barcode}': {exc}"
                        )
                        return False

                append_world_event(
                    occupancy_records,
                    world,
                    event_type="SAMPLE_CLASSIFIED",
                    entity_type="SAMPLE",
                    entity_id=str(resolved_sample_id),
                    target={
                        "station_id": target_station_id,
                        "station_slot_id": target_slot_id,
                        "slot_index": int(target_slot_index),
                    },
                    details={
                        "phase": "GettingNewSamples",
                        "step_id": step_id,
                        "classification": str(decision.classification),
                        "provider": str(decision.source),
                        "original_sample_id": str(sample_id),
                        "resolved_sample_id": str(resolved_sample_id),
                    },
                )
                routed_sample_ids.append(str(resolved_sample_id))

            bb["routed_sample_ids"] = list(routed_sample_ids)
            return True

        if step_id == "handoff_to_state_driven_planning":
            bb["state_driven_planning_requested"] = True
            bb["state_driven_planning_handoff_ts"] = _local_now_iso()
            append_world_event(
                occupancy_records,
                world,
                event_type="WORKFLOW_PHASE_COMPLETED",
                entity_type="WORKFLOW",
                entity_id="GETTING_NEW_SAMPLES",
                details={
                    "phase": "GettingNewSamples",
                    "step_id": step_id,
                    "next_phase": "state_driven_planning",
                },
            )
            return True

        print(f"GettingNewSamples failed: unsupported phase '{step_id}'")
        return False

    def _execute_getting_new_samples_task(step: PlanStep, bb: Blackboard) -> bool:
        if not step.task_key:
            print(f"GettingNewSamples failed: task step '{step.step_id}' has no task_key")
            return False
        overrides = dict(step.overrides or {})
        ok, _ = _run_task(step.task_key, overrides, f"GettingNewSamples.{step.step_id}")
        if not ok:
            return False

        if step.task_key in {"Navigate", "Charge"} and step.station_id:
            try:
                world.set_robot_station(step.station_id)
            except Exception:
                pass
        if step.step_id == "scan_input_landmark":
            bb["input_landmark_scanned"] = True
        return True

    def _is_centrifuge_ready(device: Any) -> Tuple[bool, Dict[str, Any], str]:
        try:
            diag = device.diagnose()
        except Exception as exc:
            return False, {}, f"Centrifuge diagnose failed: {exc}"
        if str(diag.get("fault_code", "")).strip():
            return False, diag, f"Centrifuge has fault: {diag.get('fault_code')} {diag.get('fault_message', '')}"
        if bool(diag.get("rotor_spinning", False)):
            return False, diag, "Centrifuge not ready: rotor is spinning"
        if str(diag.get("packml_state", "")).strip().upper() == "FAULTED":
            return False, diag, "Centrifuge not ready: packml state is FAULTED"
        return True, diag, ""

    def _execute_sample_transfer(op: SampleTransferStep) -> bool:
        pick_overrides = {
            "ITM_ID": int(op.source_itm_id),
            "JIG_ID": int(op.source_jig_id),
            "OBJ_Nbr": int(op.source_obj_nbr),
            "ACTION": ACTION_PICK,
            "OBJ_Type": int(op.obj_type),
        }
        ok, _ = _run_task("SingleTask", pick_overrides, f"CentrifugeCycle.{op.name}.PickSample")
        if not ok:
            return False

        place_overrides = {
            "ITM_ID": int(op.target_itm_id),
            "JIG_ID": int(op.target_jig_id),
            "OBJ_Nbr": int(op.target_obj_nbr),
            "ACTION": ACTION_PLACE,
            "OBJ_Type": int(op.obj_type),
        }
        ok, _ = _run_task("SingleTask", place_overrides, f"CentrifugeCycle.{op.name}.PlaceSample")
        if not ok:
            return False

        try:
            moved_sample_id = world.move_sample(
                source_station_id=op.source_station_id,
                source_station_slot_id=op.source_station_slot_id,
                source_slot_index=int(op.source_slot_index),
                target_station_id=op.target_station_id,
                target_station_slot_id=op.target_station_slot_id,
                target_slot_index=int(op.target_slot_index),
            )
        except Exception as exc:
            print(f"CentrifugeCycle sample move failed ({op.name}): {exc}")
            return False

        if str(moved_sample_id) != str(op.sample_id):
            print(
                "CentrifugeCycle sample identity mismatch "
                f"({op.name}): expected={op.sample_id}, moved={moved_sample_id}"
            )
            return False

        append_world_event(
            occupancy_records,
            world,
            event_type="SAMPLE_MOVED",
            entity_type="SAMPLE",
            entity_id=str(moved_sample_id),
            source={
                "station_id": op.source_station_id,
                "station_slot_id": op.source_station_slot_id,
                "slot_index": int(op.source_slot_index),
            },
            target={
                "station_id": op.target_station_id,
                "station_slot_id": op.target_station_slot_id,
                "slot_index": int(op.target_slot_index),
            },
            details={
                "phase": "CentrifugeCycle",
                "operation": str(op.name),
                "reason": str(op.reason or ""),
            },
        )
        return True

    def _execute_rack_transfer(
        plan: CentrifugeUsagePlan,
        op: RackTransferStep,
        runtime_device: Any,
    ) -> bool:
        pick_overrides = {
            "ITM_ID": int(op.source_itm_id),
            "JIG_ID": int(op.source_jig_id),
            "OBJ_Nbr": int(op.source_obj_nbr),
            "ACTION": ACTION_PICK,
            "OBJ_Type": int(op.obj_type),
        }
        ok, _ = _run_task("SingleTask", pick_overrides, f"CentrifugeCycle.{op.name}.Pick")
        if not ok:
            return False

        place_overrides = {
            "ITM_ID": int(op.target_itm_id),
            "JIG_ID": int(op.target_jig_id),
            "OBJ_Nbr": int(op.target_obj_nbr),
            "ACTION": ACTION_PLACE,
            "OBJ_Type": int(op.obj_type),
        }
        ok, _ = _run_task("SingleTask", place_overrides, f"CentrifugeCycle.{op.name}.Place")
        if not ok:
            return False

        try:
            moved_rack_id = world.move_rack(
                source_station_id=op.source_station_id,
                source_station_slot_id=op.source_station_slot_id,
                target_station_id=op.target_station_id,
                target_station_slot_id=op.target_station_slot_id,
            )
        except Exception as exc:
            print(f"CentrifugeCycle world move failed ({op.name}): {exc}")
            return False

        if plan.mode == "LOAD":
            try:
                runtime_loaded = runtime_device.load_rack(str(moved_rack_id), "CENTRIFUGE_RACK")
            except Exception as exc:
                print(f"CentrifugeCycle runtime device load sync failed ({op.name}): {exc}")
                return False
            if not runtime_loaded:
                print(f"CentrifugeCycle runtime device load sync rejected ({op.name})")
                return False
        elif plan.mode == "UNLOAD" and op.source_station_id == plan.centrifuge_station_id:
            try:
                runtime_unloaded = runtime_device.unload_rack(str(moved_rack_id))
            except Exception as exc:
                print(f"CentrifugeCycle runtime device unload sync failed ({op.name}): {exc}")
                return False
            if not runtime_unloaded:
                print(f"CentrifugeCycle runtime device unload sync rejected ({op.name})")
                return False

        append_world_event(
            occupancy_records,
            world,
            event_type="RACK_MOVED",
            entity_type="RACK",
            entity_id=str(moved_rack_id),
            source={"station_id": op.source_station_id, "station_slot_id": op.source_station_slot_id},
            target={"station_id": op.target_station_id, "station_slot_id": op.target_station_slot_id},
            details={
                "phase": "CentrifugeCycle",
                "mode": plan.mode,
                "transfer_index": int(op.transfer_index),
            },
        )
        return True

    def _execute_centrifuge_cycle(bb: Blackboard) -> bool:
        mode = str(bb.get("centrifuge_mode", CENTRIFUGE_MODE or "AUTO")).strip().upper() or "AUTO"
        runtime_device = runtime_devices.get_first_centrifuge_at_station(CENTRIFUGE_STATION_ID)
        if runtime_device is None:
            print("CentrifugeCycle failed: no runtime Device.Centrifuge at CentrifugeStation")
            return False
        bb["active_runtime_centrifuge_id"] = str(runtime_device.identity.device_id)

        station = world.get_station(CENTRIFUGE_STATION_ID)
        scan_overrides = {"ITM_ID": int(station.itm_id), "ACT": DEVICE_ACTION_SCAN_LANDMARK}
        ok, _ = _run_task("SingleDeviceAction", scan_overrides, "CentrifugeCycle.ScanLandmark")
        if not ok:
            return False
        try:
            if not runtime_device.apply_single_device_action(DEVICE_ACTION_SCAN_LANDMARK):
                print("CentrifugeCycle sync failed: runtime device rejected ScanLandmark")
                return False
        except Exception as exc:
            print(f"CentrifugeCycle runtime device sync failed (ScanLandmark): {exc}")
            return False

        try:
            plan = compile_centrifuge_usage_plan(world=world, device=runtime_device, mode=mode)
        except Exception as exc:
            print(f"CentrifugeCycle failed to compile usage plan: {exc}")
            return False
        bb["centrifuge_mode_resolved"] = str(plan.mode)
        if plan.mode == "UNLOAD":
            for slot_id in plan.centrifuge_slot_ids:
                rack_id = world.rack_placements.get((plan.centrifuge_station_id, slot_id))
                if not rack_id:
                    continue
                try:
                    loaded = runtime_device.load_rack(str(rack_id), "CENTRIFUGE_RACK")
                except Exception as exc:
                    print(f"CentrifugeCycle runtime preload failed ({slot_id}): {exc}")
                    return False
                if not loaded:
                    print(f"CentrifugeCycle runtime preload rejected ({slot_id}, rack={rack_id})")
                    return False

        for op in plan.operations:
            if isinstance(op, ValidationStep):
                ready, diag, reason = _is_centrifuge_ready(runtime_device)
                if not ready:
                    print(f"CentrifugeCycle validation failed: {reason}")
                    return False
                source_occupied = sum(
                    1 for sid in plan.source_slot_ids if world.rack_placements.get((plan.source_station_id, sid))
                )
                centrifuge_occupied = sum(
                    1
                    for sid in plan.centrifuge_slot_ids
                    if world.rack_placements.get((plan.centrifuge_station_id, sid))
                )
                if plan.mode == "LOAD":
                    if source_occupied != len(plan.source_slot_ids) or centrifuge_occupied != 0:
                        print(
                            "CentrifugeCycle load validation failed: expected source full and centrifuge empty. "
                            f"Observed source={source_occupied}/{len(plan.source_slot_ids)}, "
                            f"centrifuge={centrifuge_occupied}/{len(plan.centrifuge_slot_ids)}; diag={diag}"
                        )
                        return False
                elif plan.mode == "UNLOAD":
                    if source_occupied != 0 or centrifuge_occupied != len(plan.centrifuge_slot_ids):
                        print(
                            "CentrifugeCycle unload validation failed: expected source empty and centrifuge full. "
                            f"Observed source={source_occupied}/{len(plan.source_slot_ids)}, "
                            f"centrifuge={centrifuge_occupied}/{len(plan.centrifuge_slot_ids)}; diag={diag}"
                        )
                        return False
                continue

            if isinstance(op, DeviceActionStep):
                ok, _ = _run_task(op.task_key, dict(op.overrides), f"CentrifugeCycle.{op.name}")
                if not ok:
                    return False
                act = int(op.overrides.get("ACT", 0))
                try:
                    if not runtime_device.apply_single_device_action(act):
                        print(f"CentrifugeCycle runtime device sync rejected ({op.name})")
                        return False
                except Exception as exc:
                    print(f"CentrifugeCycle runtime device sync failed ({op.name}): {exc}")
                    return False
                continue

            if isinstance(op, SampleTransferStep):
                if not _execute_sample_transfer(op):
                    return False
                continue

            if isinstance(op, RackTransferStep):
                if not _execute_rack_transfer(plan, op, runtime_device):
                    return False
                continue

            if isinstance(op, RunningValidationStep):
                ready, diag, reason = _is_centrifuge_ready(runtime_device)
                if ready:
                    print(
                        "CentrifugeCycle running validation failed: runtime Device reports ready state "
                        f"instead of running; diag={diag}"
                    )
                    return False
                runtime_state = str(diag.get("packml_state", "")).strip().upper()
                runtime_spinning = bool(diag.get("rotor_spinning", False))
                if runtime_state != "EXECUTE" or not runtime_spinning:
                    print(
                        "CentrifugeCycle running validation failed: runtime Device interface expected "
                        f"EXECUTE + spinning, got state={runtime_state}, spinning={runtime_spinning}; reason={reason}"
                    )
                    return False
                continue

            print(f"CentrifugeCycle failed: unsupported operation '{type(op).__name__}'")
            return False

        bb["last_centrifuge_cycle_mode"] = str(plan.mode)
        bb["last_centrifuge_cycle_ts"] = _local_now_iso()
        return True

    def make_step(step_name: str):
        def _run(bb: Blackboard) -> bool:
            if workflow_mode == "GETTING_NEW_SAMPLES":
                step = plan_step_by_id.get(step_name)
                if step is None:
                    print(f"GettingNewSamples failed: planner step '{step_name}' not found")
                    return False
                bb["last_plan_step"] = str(step.step_id)
                if str(step.step_type).strip().upper() == "TASK":
                    return _execute_getting_new_samples_task(step, bb)
                if str(step.step_type).strip().upper() == "PHASE":
                    return _execute_getting_new_samples_phase(step.step_id, bb)
                print(f"GettingNewSamples failed: unsupported step_type '{step.step_type}'")
                return False

            bb["last_blank_step"] = step_name
            if step_name == "CentrifugeCycle" and workflow_mode in centrifuge_modes:
                return _execute_centrifuge_cycle(bb)
            print(f"[BLANK_NODE] {step_name}: no behavior implemented yet")
            return True

        return _run

    nodes = [
        RetryNode(
            "ValidateScaffoldPrerequisites",
            ConditionNode("ValidateScaffoldPrerequisites", validate_scaffold_prerequisites),
            max_attempts=1,
        )
    ]
    for step_name in active_step_names:
        nodes.append(
            RetryNode(
                step_name,
                ConditionNode(step_name, make_step(step_name)),
                max_attempts=1,
            )
        )

    return SequenceNode("RackAndProbeTransferFlow", nodes)

def main() -> None:
    TRACE_DIR.mkdir(parents=True, exist_ok=True)
    WORLD_DIR.mkdir(parents=True, exist_ok=True)

    sender = build_sender()
    trace_fieldnames = _trace_fieldnames_from_catalog(sender)
    _init_live_trace_files(trace_fieldnames)
    world = load_world_with_resume(WORLD_CONFIG_FILE, OCCUPANCY_EVENTS_FILE)
    _init_live_world_files()
    trace_records: List[Dict[str, Any]] = []
    state_change_records: List[Dict[str, Any]] = []
    occupancy_records: List[Dict[str, Any]] = []
    bb = Blackboard()
    append_world_event(
        occupancy_records,
        world,
        event_type="WORLD_SNAPSHOT",
        entity_type="WORLD",
        entity_id="WORLD",
        details={"reason": "run_start"},
    )
    tree = build_tree(
        sender,
        world,
        trace_records,
        state_change_records,
        occupancy_records,
        trace_fieldnames,
    )

    final_status = Status.FAILURE
    try:
        while True:
            st = tree.tick(bb)
            print("TREE:", st)
            if st in (Status.SUCCESS, Status.FAILURE):
                break
            time.sleep(0.1)
        final_status = st
        print("Final blackboard:", dict(bb))
        print("Final sample states:", {k: str(v.location) for k, v in world.sample_states.items()})
        print(f"World config loaded from: {WORLD_CONFIG_FILE.resolve()}")
        append_world_event(
            occupancy_records,
            world,
            event_type="WORLD_SNAPSHOT",
            entity_type="WORLD",
            entity_id="WORLD",
            details={"reason": f"run_end_{str(st)}"},
        )
    finally:
        if final_status == Status.SUCCESS:
            export_trace(trace_records, TRACE_FILE)
            export_state_changes(state_change_records, STATE_CHANGES_FILE)
            export_occupancy_trace(occupancy_records, OCCUPANCY_TRACE_FILE, trace_records=trace_records)
            export_occupancy_events_jsonl(occupancy_records, OCCUPANCY_EVENTS_FILE, trace_records=trace_records)
            _finalize_world_snapshot_file(world)
            print(f"Trace written to {TRACE_FILE.resolve()}")
            print(f"State transitions written to {STATE_CHANGES_FILE.resolve()}")
            print(f"Occupancy trace written to {OCCUPANCY_TRACE_FILE.resolve()}")
            print(f"Occupancy events written to {OCCUPANCY_EVENTS_FILE.resolve()}")
            print(f"World snapshot written to {WORLD_SNAPSHOT_FILE.resolve()}")
        else:
            print(
                "Workflow did not complete successfully; "
                "canonical final files were not updated."
            )
            print(f"WIP trace: {TRACE_WIP_FILE.resolve()}")
            print(f"WIP state transitions: {STATE_CHANGES_WIP_FILE.resolve()}")
            print(f"WIP occupancy trace: {OCCUPANCY_TRACE_WIP_FILE.resolve()}")
            print(f"WIP occupancy events: {OCCUPANCY_EVENTS_WIP_FILE.resolve()}")
            print(f"WIP world snapshot: {WORLD_SNAPSHOT_WIP_FILE.resolve()}")


if __name__ == "__main__":
    main()
