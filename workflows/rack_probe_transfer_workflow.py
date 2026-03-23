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
from planning.planner import DynamicPlanAction, DynamicStatePlanner, Goal, PlanStep, RulePlanner
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
from Device.wise_adapter import wise_snapshot_to_metadata
from engine.sender import build_sender
from world.export_world_snapshot_jsonl import build_snapshot_records, write_jsonl
from world.lab_world import (
    Cap,
    CapOnSampleLocation,
    CapState,
    CapStateRecord,
    Device,
    GripperLocation,
    ProcessType,
    RackLocation,
    RackType,
    Sample,
    SampleState,
    SlotKind,
    StoredCapLocation,
    StationKind,
    WorldModel,
    ensure_world_config_file,
    load_world_config_file,
)
from world.update_world_mapper import map_update_world_devices_to_assigned_world_devices

ACTION_PICK = 1
ACTION_PLACE = 2
ACTION_PULL_RACK_OUT = 3
ACTION_PUSH_RACK_IN = 4
DEVICE_ACTION_OPEN_HATCH = 1
DEVICE_ACTION_START_CENTRIFUGE = 2
DEVICE_ACTION_CLOSE_HATCH = 3
DEVICE_ACTION_MOVE_ROTOR = 4
DEVICE_ACTION_SCAN_LANDMARK = 30
OBJ_TYPE_PROBE = 810
RACK_SLOT_INDEX = 1

INPUT_STATION_ID = "InputStation"
INPUT_SLOT_ID = "URGRackSlot"
PLATE_STATION_ID = "uLMPlateStation"
PLATE_RACK_SLOT_ID = "URGRackSlot"
THREE_FINGER_STATION_ID = "3-FingerGripperStation"
THREE_FINGER_SLOT_ID = "SampleSlot1"
THREE_FINGER_RECAP_JIG_ID = 14
THREE_FINGER_KREUZ_RECAP_JIG_ID = 15
CENTRIFUGE_STATION_ID = "CentrifugeStation"
IH500_STATION_ID = "BioRadIH500Station"
IH500_SOURCE_JIG_ID = 12
IH500_DEVICE_JIG_ID = 50
CHARGE_STATION_ID = "CHARGE"
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
RUNTIME_DIR = PROJECT_ROOT / "runtime"
PAUSE_REQUEST_FILE = Path(
    os.getenv("UGO_PLANNER_PAUSE_REQUEST_FILE", str(RUNTIME_DIR / "planner_workflow_pause.request"))
).resolve()
PAUSE_ACK_FILE = Path(
    os.getenv("UGO_PLANNER_PAUSE_ACK_FILE", str(RUNTIME_DIR / "planner_workflow_paused.ack"))
).resolve()
try:
    PAUSE_POLL_S = max(0.1, float(os.getenv("UGO_PLANNER_PAUSE_POLL_S", "0.2").strip()))
except Exception:
    PAUSE_POLL_S = 0.2
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
IMMUNO_KREUZPROBE_MAP_FILE = Path(
    os.getenv(
        "UGO_IMMUNO_KREUZPROBE_MAP_FILE",
        str(PROJECT_ROOT / "routing" / "immuno_kreuzprobe_map.json"),
    )
).resolve()
PROCESS_POLICIES_FILE = Path(
    os.getenv("UGO_PROCESS_POLICIES_FILE", str(PROJECT_ROOT / "planning" / "process_policies.json"))
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
try:
    STATE_DRIVEN_MAX_ACTIONS = int(os.getenv("UGO_STATE_DRIVEN_MAX_ACTIONS", "200").strip())
except Exception:
    STATE_DRIVEN_MAX_ACTIONS = 200
try:
    STATE_DRIVEN_WAIT_POLL_S = float(os.getenv("UGO_STATE_DRIVEN_WAIT_POLL_S", "1.0").strip())
except Exception:
    STATE_DRIVEN_WAIT_POLL_S = 1.0
ENABLE_WISE_POLLING = os.getenv("UGO_ENABLE_WISE_POLLING", "1").strip().lower() in {
    "1",
    "true",
    "yes",
}
PLANNER_USE_WISE_READINESS = os.getenv("UGO_PLANNER_USE_WISE_READINESS", "0").strip().lower() in {
    "1",
    "true",
    "yes",
}
WAIT_READY_PACKML_STATES = {"COMPLETE", "IDLE", "STOPPED"}
SAMPLE_HOLD_STATUS_RUNNING = "RUNNING"
SAMPLE_HOLD_STATUS_READY_TO_UNLOAD = "READY_TO_UNLOAD"
try:
    CENTRIFUGE_ASYNC_MAX_RUNTIME_S = max(
        60.0,
        float(os.getenv("UGO_CENTRIFUGE_ASYNC_MAX_RUNTIME_S", "1200").strip() or "1200"),
    )
except Exception:
    CENTRIFUGE_ASYNC_MAX_RUNTIME_S = 1200.0
POST_CONTEXT_FOR_EACH_TASK = os.getenv("UGO_PLANNER_CONTEXT_EACH_TASK", "1").strip().lower() in {
    "1",
    "true",
    "yes",
}
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


def _normalize_barcode_key(value: Any) -> str:
    return str(value or "").strip().upper()


def _load_immuno_kreuzprobe_map(path: Path) -> Dict[str, str]:
    """Load immuno barcode -> Kreuzprobe sample-id mapping from JSON."""
    if not path.exists():
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except Exception:
        return {}

    mapping: Dict[str, str] = {}

    if isinstance(raw, dict):
        forward = raw.get("immuno_to_kreuzprobe")
        reverse = raw.get("kreuzprobe_to_immuno")

        if isinstance(forward, dict):
            for barcode, kreuz_id in forward.items():
                key = _normalize_barcode_key(barcode)
                value = str(kreuz_id or "").strip()
                if key and value:
                    mapping[key] = value

        if isinstance(reverse, dict):
            for kreuz_id, barcode in reverse.items():
                key = _normalize_barcode_key(barcode)
                value = str(kreuz_id or "").strip()
                if key and value:
                    mapping[key] = value

        # Backward-compatible flat object: {"<barcode>": "KREUZPROBE_0001"}
        if not mapping:
            for barcode, kreuz_id in raw.items():
                if not isinstance(barcode, str):
                    continue
                if isinstance(kreuz_id, (str, int, float)):
                    key = _normalize_barcode_key(barcode)
                    value = str(kreuz_id).strip()
                    if key and value:
                        mapping[key] = value

    return mapping


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


def _try_parse_presence_mask_positions(value: Any) -> Optional[List[int]]:
    """Parse camera mask formats like "[P,F,P,...]" and return 1-based occupied positions.

    Convention:
    - F => sample present
    - P => no sample
    """
    tokens: List[str] = []

    if isinstance(value, list):
        for item in value:
            txt = str(item).strip().upper()
            if txt:
                tokens.append(txt)
    elif isinstance(value, str):
        txt = value.strip()
        if not txt:
            return []
        parsed = None
        if txt.startswith("[") and txt.endswith("]"):
            try:
                parsed = json.loads(txt)
            except Exception:
                parsed = None
        if isinstance(parsed, list):
            for item in parsed:
                item_txt = str(item).strip().upper()
                if item_txt:
                    tokens.append(item_txt)
        else:
            core = txt[1:-1] if txt.startswith("[") and txt.endswith("]") else txt
            parts = [part.strip().strip("'\"").upper() for part in core.split(",")]
            tokens = [part for part in parts if part]
    else:
        return None

    if not tokens:
        return []

    valid = {"F", "P"}
    if any(token not in valid for token in tokens):
        return None

    # Camera mask is 1-based when mapped to rack slot positions.
    return [idx for idx, token in enumerate(tokens, start=1) if token == "F"]


def _to_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(int(value))
    txt = str(value).strip().lower()
    if not txt:
        return bool(default)
    if txt in {"1", "true", "yes", "y", "on"}:
        return True
    if txt in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


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
        mask_positions = _try_parse_presence_mask_positions(candidate)
        if mask_positions is not None:
            return mask_positions

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
    parsed_mask_msg = _try_parse_presence_mask_positions(msg)
    if parsed_mask_msg is not None:
        return parsed_mask_msg
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
        # Keep the full barcode as returned by 3FG.
        return txt
    return None


def _classification_key_from_barcode(barcode: Optional[str]) -> Optional[str]:
    txt = str(barcode or "").strip()
    if not txt:
        return None
    if txt.lower() in {"none", "null", "n/a"}:
        return None
    if len(txt) < 2:
        return txt
    # Sample class routing is based on the last two characters.
    return txt[-2:]


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


def _pause_requested() -> bool:
    return PAUSE_REQUEST_FILE.exists()


def _set_pause_ack(paused: bool) -> None:
    try:
        PAUSE_ACK_FILE.parent.mkdir(parents=True, exist_ok=True)
        if paused:
            with open(PAUSE_ACK_FILE, "w", encoding="utf-8") as f:
                f.write(_local_now_iso() + "\n")
        else:
            PAUSE_ACK_FILE.unlink(missing_ok=True)
    except Exception as exc:
        print(f"Pause ack update warning: {exc}")


def _wait_if_pause_requested(task_name: str) -> None:
    if not _pause_requested():
        _set_pause_ack(False)
        return

    _set_pause_ack(True)
    printed_wait = False
    try:
        while _pause_requested():
            if not printed_wait:
                print(
                    "Workflow pause requested; holding before next action dispatch "
                    f"('{task_name}')."
                )
                printed_wait = True
            time.sleep(PAUSE_POLL_S)
    finally:
        _set_pause_ack(False)

    if printed_wait:
        print(f"Workflow resume requested; continuing with '{task_name}'.")


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


def _looks_like_cap_id(entity_id: str) -> bool:
    return str(entity_id).strip().upper().startswith("CAP_")


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
        required_processes=(),
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
    world.caps.clear()
    world.cap_states.clear()

    robot_station = state.get("robot_current_station_id")
    if isinstance(robot_station, str) and robot_station in world.stations:
        world.set_robot_station(robot_station)
    else:
        world.robot_current_station_id = None

    occupant_ids: Set[str] = set()
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
        occupant_ids.update(rack.occupied_slots.values())
        occupant_ids.update(rack.reserved_slots.values())

    explicit_cap_ids: Set[str] = set()
    raw_cap_locations = state.get("cap_locations", [])
    if isinstance(raw_cap_locations, list):
        for raw_cap_loc in raw_cap_locations:
            if not isinstance(raw_cap_loc, dict):
                continue
            cap_id = str(raw_cap_loc.get("cap_id", "")).strip()
            if cap_id:
                explicit_cap_ids.add(cap_id)

    cap_like_ids: Set[str] = {sid for sid in occupant_ids if _looks_like_cap_id(sid)}
    cap_like_ids.update(explicit_cap_ids)
    sample_ids: Set[str] = {sid for sid in occupant_ids if sid not in cap_like_ids}

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
            if sample_id in cap_like_ids:
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
            if sample_id in cap_like_ids:
                continue
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

    if isinstance(raw_cap_locations, list) and raw_cap_locations:
        for raw_cap_loc in raw_cap_locations:
            if not isinstance(raw_cap_loc, dict):
                continue
            cap_id = str(raw_cap_loc.get("cap_id", "")).strip()
            if not cap_id:
                continue
            assigned_sample_id = str(raw_cap_loc.get("assigned_sample_id", "")).strip()
            if assigned_sample_id and assigned_sample_id not in world.samples:
                _ensure_sample_exists(world, assigned_sample_id)
            try:
                cap_obj_type = int(raw_cap_loc.get("obj_type", 9014))
            except Exception:
                cap_obj_type = 9014
            world.caps[cap_id] = Cap(
                id=cap_id,
                obj_type=cap_obj_type,
                assigned_sample_id=assigned_sample_id,
            )
            location_type = str(raw_cap_loc.get("location_type", "ON_SAMPLE")).strip().upper()
            if location_type == "STORED":
                station_id = str(raw_cap_loc.get("station_id", "")).strip()
                station_slot_id = str(raw_cap_loc.get("station_slot_id", "")).strip()
                rack_id = str(raw_cap_loc.get("rack_id", "")).strip()
                try:
                    slot_index = int(raw_cap_loc.get("slot_index"))
                except Exception:
                    continue
                world.cap_states[cap_id] = CapStateRecord(
                    cap_id=cap_id,
                    location=StoredCapLocation(
                        station_id=station_id,
                        station_slot_id=station_slot_id,
                        rack_id=rack_id,
                        slot_index=slot_index,
                    ),
                )
                rack = world.racks.get(rack_id)
                if rack is not None and rack.occupied_slots.get(slot_index) is None:
                    rack.occupied_slots[slot_index] = cap_id
            else:
                sample_id = str(raw_cap_loc.get("sample_id", "")).strip()
                if not sample_id and assigned_sample_id:
                    sample_id = assigned_sample_id
                if sample_id and sample_id not in world.samples:
                    _ensure_sample_exists(world, sample_id)
                world.cap_states[cap_id] = CapStateRecord(
                    cap_id=cap_id,
                    location=CapOnSampleLocation(sample_id=sample_id),
                )
    else:
        for (station_id, station_slot_id), rack_id in sorted(world.rack_placements.items()):
            rack = world.racks[rack_id]
            for slot_index, occupant_id in sorted(rack.occupied_slots.items()):
                if not _looks_like_cap_id(occupant_id):
                    continue
                cap_id = str(occupant_id)
                inferred_sample_id = cap_id[4:] if cap_id.upper().startswith("CAP_") and len(cap_id) > 4 else ""
                world.caps[cap_id] = Cap(
                    id=cap_id,
                    obj_type=9014,
                    assigned_sample_id=inferred_sample_id,
                )
                world.cap_states[cap_id] = CapStateRecord(
                    cap_id=cap_id,
                    location=StoredCapLocation(
                        station_id=station_id,
                        station_slot_id=station_slot_id,
                        rack_id=rack_id,
                        slot_index=slot_index,
                    ),
                )

    samples_with_cap_on_sample: Set[str] = set()
    samples_with_stored_cap: Set[str] = set()
    for cap_id, cap_state in sorted(world.cap_states.items()):
        loc = cap_state.location
        if isinstance(loc, CapOnSampleLocation):
            sample_id = str(loc.sample_id).strip()
            if sample_id:
                samples_with_cap_on_sample.add(sample_id)
            continue
        cap = world.caps.get(cap_id)
        assigned_sample_id = str(cap.assigned_sample_id).strip() if cap is not None else ""
        if assigned_sample_id:
            samples_with_stored_cap.add(assigned_sample_id)

    for sample_id in sorted(samples_with_stored_cap):
        if sample_id in samples_with_cap_on_sample:
            continue
        if sample_id in world.samples:
            world.set_sample_cap_state(sample_id, CapState.DECAPPED)
    for sample_id in sorted(samples_with_cap_on_sample):
        if sample_id in world.samples:
            world.set_sample_cap_state(sample_id, CapState.CAPPED)

    world.ensure_cap_tracking_for_capped_samples()
    world._sample_counter = _sample_counter_from_ids(set(world.samples.keys()))


def load_world_with_resume(world_config_file: Path, occupancy_events_file: Path) -> Tuple[WorldModel, bool]:
    world = ensure_world_config_file(world_config_file)
    if not RESUME_FROM_LAST_WORLD_SNAPSHOT:
        if FORCE_INPUT_RACK_AT_INPUT_ON_START:
            prepare_input_rack_for_new_batch(world)
        return world, False

    last_state = load_last_world_state(occupancy_events_file)
    if last_state is None:
        print("World resume: no previous snapshot found, using world_config baseline")
        if FORCE_INPUT_RACK_AT_INPUT_ON_START:
            prepare_input_rack_for_new_batch(world)
        return world, False

    try:
        restore_world_from_state(world, last_state)
        print(f"World resume: restored from {occupancy_events_file.resolve()}")
        resumed = True
    except Exception as exc:
        print(f"World resume failed ({exc}), using world_config baseline")
        resumed = False
    if FORCE_INPUT_RACK_AT_INPUT_ON_START:
        prepare_input_rack_for_new_batch(world)
    return world, resumed


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


def _read_csv_header(path: Path) -> List[str]:
    if not path.exists():
        return []
    try:
        with open(path, "r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            row = next(reader, [])
            if not isinstance(row, list):
                return []
            return [str(x) for x in row if str(x)]
    except Exception:
        return []


def export_trace(records: List[Dict[str, Any]], path: Path, *, append: bool = False) -> None:
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

    path.parent.mkdir(parents=True, exist_ok=True)
    use_append = False
    writer_fieldnames = list(fieldnames)
    if append and path.exists() and path.stat().st_size > 0:
        existing_header = _read_csv_header(path)
        if existing_header:
            writer_fieldnames = existing_header
            use_append = True

    with open(path, "a" if use_append else "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=writer_fieldnames, extrasaction="ignore")
        if not use_append:
            writer.writeheader()
        for rec in records:
            writer.writerow(rec)


def export_state_changes(records: List[Dict[str, Any]], path: Path, *, append: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    use_append = bool(append and path.exists() and path.stat().st_size > 0)
    with open(path, "a" if use_append else "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=STATE_CHANGE_FIELDNAMES, extrasaction="ignore")
        if not use_append:
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


def _init_live_trace_files(trace_fieldnames: List[str], *, reset: bool) -> None:
    TRACE_DIR.mkdir(parents=True, exist_ok=True)

    def _ensure_csv(path: Path, fieldnames: List[str]) -> None:
        if not reset and path.exists() and path.stat().st_size > 0:
            return
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

    _ensure_csv(TRACE_WIP_FILE, trace_fieldnames)
    _ensure_csv(STATE_CHANGES_WIP_FILE, STATE_CHANGE_FIELDNAMES)


def _append_live_trace_record(record: Dict[str, Any], trace_fieldnames: List[str]) -> None:
    with open(TRACE_WIP_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=trace_fieldnames, extrasaction="ignore")
        writer.writerow(record)


def _append_live_state_change(record: Dict[str, Any]) -> None:
    with open(STATE_CHANGES_WIP_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=STATE_CHANGE_FIELDNAMES, extrasaction="ignore")
        writer.writerow(record)


def _init_live_world_files(*, reset: bool) -> None:
    WORLD_DIR.mkdir(parents=True, exist_ok=True)
    if reset or not OCCUPANCY_TRACE_WIP_FILE.exists():
        with open(OCCUPANCY_TRACE_WIP_FILE, "w", encoding="utf-8") as f:
            f.write("")
    if reset or not OCCUPANCY_EVENTS_WIP_FILE.exists():
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
    cap_locations = []
    for cap_id in sorted(world.caps.keys()):
        cap = world.caps[cap_id]
        cap_state = world.cap_states.get(cap_id)
        if cap_state is None:
            cap_locations.append(
                {
                    "cap_id": cap_id,
                    "obj_type": int(cap.obj_type),
                    "assigned_sample_id": str(cap.assigned_sample_id),
                    "location_type": "UNKNOWN",
                }
            )
            continue
        loc = cap_state.location
        if isinstance(loc, StoredCapLocation):
            cap_locations.append(
                {
                    "cap_id": cap_id,
                    "obj_type": int(cap.obj_type),
                    "assigned_sample_id": str(cap.assigned_sample_id),
                    "location_type": "STORED",
                    "station_id": str(loc.station_id),
                    "station_slot_id": str(loc.station_slot_id),
                    "rack_id": str(loc.rack_id),
                    "slot_index": int(loc.slot_index),
                }
            )
        elif isinstance(loc, CapOnSampleLocation):
            cap_locations.append(
                {
                    "cap_id": cap_id,
                    "obj_type": int(cap.obj_type),
                    "assigned_sample_id": str(cap.assigned_sample_id),
                    "location_type": "ON_SAMPLE",
                    "sample_id": str(loc.sample_id),
                }
            )
    return {
        "robot_current_station_id": world.robot_current_station_id,
        "rack_in_gripper_id": world.rack_in_gripper_id,
        "station_slots": station_slots,
        "racks": racks,
        "sample_locations": sample_locations,
        "cap_locations": cap_locations,
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
    *,
    append: bool = False,
) -> None:
    if trace_records:
        enrich_occupancy_records_with_task_context(records, trace_records)
    # Keep historical filename, but write JSONL records for digital-twin/event replay use.
    path.parent.mkdir(parents=True, exist_ok=True)
    use_append = bool(append and path.exists() and path.stat().st_size > 0)
    if not use_append:
        _backup_world_file_once(path)
    with open(path, "a" if use_append else "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=True) + "\n")


def export_occupancy_events_jsonl(
    records: List[Dict[str, Any]],
    path: Path,
    trace_records: Optional[List[Dict[str, Any]]] = None,
    *,
    append: bool = False,
) -> None:
    if trace_records:
        enrich_occupancy_records_with_task_context(records, trace_records)
    path.parent.mkdir(parents=True, exist_ok=True)
    use_append = bool(append and path.exists() and path.stat().st_size > 0)
    if not use_append:
        _backup_world_file_once(path)
    with open(path, "a" if use_append else "w", encoding="utf-8") as f:
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
    dynamic_state_planner: Optional[DynamicStatePlanner] = None
    dynamic_state_planner_error = ""
    if workflow_mode == "GETTING_NEW_SAMPLES":
        try:
            planner_plan_steps = RulePlanner().build_plan(world, planner_goal)
        except Exception as exc:
            planner_plan_error = str(exc)
        try:
            dynamic_state_planner = DynamicStatePlanner.from_file(
                PROCESS_POLICIES_FILE,
                use_wise_readiness=PLANNER_USE_WISE_READINESS,
            )
        except Exception as exc:
            dynamic_state_planner_error = str(exc)
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
    active_context: Dict[str, Blackboard] = {}
    baseline_rack_home_by_id: Dict[str, Tuple[str, str]] = {}
    baseline_rack_home_error = ""
    try:
        baseline_cfg = load_world_config_file(WORLD_CONFIG_FILE)
        raw_placements = baseline_cfg.get("rack_placements", [])
        placements: List[Dict[str, Any]] = []
        if isinstance(raw_placements, list):
            placements = [x for x in raw_placements if isinstance(x, dict)]
        elif isinstance(raw_placements, dict):
            placements = [x for x in raw_placements.values() if isinstance(x, dict)]
        for placement in placements:
            rack_id = str(placement.get("rack_id", "")).strip()
            station_id = str(placement.get("station_id", "")).strip()
            station_slot_id = str(placement.get("station_slot_id", "")).strip()
            if not rack_id or not station_id or not station_slot_id:
                continue
            if rack_id in baseline_rack_home_by_id:
                continue
            baseline_rack_home_by_id[rack_id] = (station_id, station_slot_id)
    except Exception as exc:
        baseline_rack_home_error = str(exc)

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
            if dynamic_state_planner_error:
                print(
                    "Planner prerequisite failed: cannot load dynamic process policies "
                    f"({PROCESS_POLICIES_FILE}): {dynamic_state_planner_error}"
                )
                return False
            if dynamic_state_planner is None:
                print(
                    "Planner prerequisite failed: dynamic process policy loader "
                    "did not return an active planner instance"
                )
                return False
            if baseline_rack_home_error:
                print(
                    "Planner prerequisite warning: unable to load baseline rack-home map "
                    f"from world config ({baseline_rack_home_error}). Rack auto-return will be skipped."
                )
            elif not baseline_rack_home_by_id:
                print(
                    "Planner prerequisite warning: baseline rack-home map is empty in world config. "
                    "Rack auto-return will be skipped."
                )
            required_task_keys_local.add("UpdateWorldState_From_uLM")
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
            bb["dynamic_process_policies_file"] = str(PROCESS_POLICIES_FILE)
            bb["planner_use_wise_readiness"] = bool(PLANNER_USE_WISE_READINESS)
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

    def _context_message_for_plan_step(step: PlanStep) -> str:
        step_id = str(step.step_id).strip()
        messages_by_step_id = {
            "await_input_rack_present": "Waiting for Input Rack at Input Station",
            "nav_input": "Navigation to the Input Station",
            "scan_input_landmark": "Referencing robot coordinates at Input Station",
            "transfer_input_rack": "Transferring Input Rack to uLM Plate",
            "charge": "Charging at Charge Station",
            "camera_inspect_urg_for_new_samples": "Identifying samples from the Input Rack",
            "urg_sort_via_3fg_router": "Routing identified samples",
            "handoff_to_state_driven_planning": "State-driven planning in progress",
        }
        default_message = str(step.label).strip() or f"Executing step {step_id}"
        return str(messages_by_step_id.get(step_id, default_message)).strip()

    def _context_message_for_task_execution(task_name: str) -> str:
        name = str(task_name).strip()
        if not name:
            return ""
        return f"Executing {name}"

    def _post_workflow_context_message(bb: Blackboard, message: str) -> None:
        msg = str(message).strip()
        if not msg:
            return
        last_msg = str(bb.get("workflow_context_message", "")).strip()
        if last_msg == msg:
            return

        # Workflow status/step context is internal-only; no planner error posting.
        bb["workflow_context_message"] = msg
        bb["workflow_context_ts"] = _local_now_iso()

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
        _wait_if_pause_requested(task_name)
        sent_ts = _local_now_iso()
        if POST_CONTEXT_FOR_EACH_TASK:
            bb_ctx = active_context.get("bb")
            if bb_ctx is not None:
                try:
                    task_context = _context_message_for_task_execution(task_name)
                    if task_context:
                        _post_workflow_context_message(bb_ctx, task_context)
                except Exception as exc:
                    print(f"Workflow context task-post warning: {exc}")
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

    def _single_task_overrides(
        *,
        itm_id: int,
        jig_id: int,
        obj_nbr: int,
        action: int,
        obj_type: int,
    ) -> Dict[str, int]:
        return {
            "ITM_ID": int(itm_id),
            "JIG_ID": int(jig_id),
            "OBJ_Nbr": int(obj_nbr),
            "ACTION": int(action),
            "OBJ_Type": int(obj_type),
        }

    def _run_single_task_action(
        *,
        itm_id: int,
        jig_id: int,
        obj_nbr: int,
        action: int,
        obj_type: int,
        task_name: str,
    ) -> bool:
        overrides = _single_task_overrides(
            itm_id=int(itm_id),
            jig_id=int(jig_id),
            obj_nbr=int(obj_nbr),
            action=int(action),
            obj_type=int(obj_type),
        )
        ok, _ = _run_task("SingleTask", overrides, task_name)
        return bool(ok)

    def _extract_update_world_devices_payload(result: Dict[str, Any]) -> str:
        raw = result.get("raw", {})
        data = raw.get("data", {}) if isinstance(raw, dict) else {}
        outputs = data.get("outputs", {}) if isinstance(data, dict) else {}

        candidates = [
            outputs.get("Devices") if isinstance(outputs, dict) else None,
            outputs.get("devices") if isinstance(outputs, dict) else None,
            outputs.get("Results") if isinstance(outputs, dict) else None,
            outputs.get("results") if isinstance(outputs, dict) else None,
            data.get("Devices") if isinstance(data, dict) else None,
            data.get("devices") if isinstance(data, dict) else None,
            data.get("Results") if isinstance(data, dict) else None,
            data.get("results") if isinstance(data, dict) else None,
            result.get("message"),
        ]
        for candidate in candidates:
            if candidate is None:
                continue
            txt = str(candidate).strip()
            if not txt or txt.lower() in {"none", "null"}:
                continue
            if "-" not in txt:
                continue
            return txt
        return ""

    def _set_world_device_packml_state(
        device_id: str,
        packml_state: str,
        *,
        source: str = "UpdateWorldState_From_uLM",
    ) -> None:
        dev = world.devices.get(str(device_id))
        if dev is None:
            raise KeyError(f"Unknown world device '{device_id}'")
        metadata = dict(dev.metadata) if isinstance(dev.metadata, dict) else {}
        metadata["packml_state"] = str(packml_state).strip().upper()
        metadata["packml_state_ts"] = _local_now_iso()
        metadata["packml_state_source"] = str(source or "UpdateWorldState_From_uLM")
        world.devices[str(device_id)] = Device(
            id=str(dev.id),
            name=str(dev.name),
            station_id=str(dev.station_id),
            capabilities=dev.capabilities,
            metadata=metadata,
        )

    def _get_world_device_packml_state(device_id: str) -> str:
        dev = world.devices.get(str(device_id))
        if dev is None:
            return ""
        metadata = dict(dev.metadata) if isinstance(dev.metadata, dict) else {}
        return str(metadata.get("packml_state", "")).strip().upper()

    def _get_world_device_metadata(device_id: str) -> Dict[str, Any]:
        dev = world.devices.get(str(device_id))
        if dev is None:
            raise KeyError(f"Unknown world device '{device_id}'")
        return dict(dev.metadata) if isinstance(dev.metadata, dict) else {}

    def _replace_world_device_metadata(device_id: str, metadata: Dict[str, Any]) -> None:
        dev = world.devices.get(str(device_id))
        if dev is None:
            raise KeyError(f"Unknown world device '{device_id}'")
        world.devices[str(device_id)] = Device(
            id=str(dev.id),
            name=str(dev.name),
            station_id=str(dev.station_id),
            capabilities=dev.capabilities,
            metadata=dict(metadata),
        )

    def _set_world_device_wise_state(
        device_id: str,
        wise_payload: Dict[str, Any],
        *,
        source: str = "WiseModulePoll",
    ) -> None:
        metadata = _get_world_device_metadata(device_id)
        payload = dict(wise_payload) if isinstance(wise_payload, dict) else {}
        metadata["wise_state"] = payload
        metadata["wise_state_ts"] = _local_now_iso()
        metadata["wise_state_source"] = str(source)
        metadata["wise_online"] = bool(_to_bool(payload.get("online"), False))
        metadata["wise_stale"] = bool(_to_bool(payload.get("stale"), True))
        metadata["wise_error"] = str(payload.get("error", "")).strip()
        if metadata["wise_online"] and (not metadata["wise_stale"]):
            metadata["wise_last_ok_ts"] = _local_now_iso()
        _replace_world_device_metadata(device_id, metadata)

    def _parse_wise_slot_ready_channel_map(device_id: str) -> Dict[int, int]:
        metadata = _get_world_device_metadata(device_id)
        wise_cfg = metadata.get("wise")
        if not isinstance(wise_cfg, dict):
            return {}

        out: Dict[int, int] = {}
        raw_mapping = (
            wise_cfg.get("rack_ready_channels")
            or wise_cfg.get("slot_ready_channels")
            or wise_cfg.get("slot_to_channel")
        )
        if isinstance(raw_mapping, dict):
            for raw_slot, raw_channel in raw_mapping.items():
                match = re.search(r"(\d+)", str(raw_slot))
                if match is None:
                    continue
                try:
                    out[int(match.group(1))] = int(raw_channel)
                except Exception:
                    continue
        elif isinstance(raw_mapping, (list, tuple)):
            for idx, raw_channel in enumerate(raw_mapping, start=1):
                try:
                    out[int(idx)] = int(raw_channel)
                except Exception:
                    continue

        if out:
            return out

        channel_map = wise_cfg.get("channel_map")
        if isinstance(channel_map, dict):
            for raw_name, raw_channel in channel_map.items():
                name = str(raw_name).strip().lower().replace("-", "_")
                match = re.search(r"slot[_]?(\d+)", name)
                if match is None:
                    continue
                if "ready" not in name:
                    continue
                try:
                    out[int(match.group(1))] = int(raw_channel)
                except Exception:
                    continue
        return out

    def _wise_ready_details_for_device(
        device_id: str,
        *,
        required_slots: Optional[Sequence[int]] = None,
    ) -> Dict[str, Any]:
        metadata = _get_world_device_metadata(device_id)
        wise_cfg = metadata.get("wise")
        if not isinstance(wise_cfg, dict):
            return {
                "configured": False,
                "enabled": False,
                "online": False,
                "stale": True,
                "required_slots": [],
                "ready_slots": [],
                "missing_slots": [],
                "ready_by_slot": {},
                "all_ready": False,
                "error": "Wise configuration missing",
            }

        enabled = bool(_to_bool(wise_cfg.get("enabled"), False))
        slot_map = _parse_wise_slot_ready_channel_map(device_id)
        required: List[int]
        if required_slots is None:
            required = sorted(slot_map.keys())
        else:
            required = sorted({int(x) for x in required_slots if int(x) > 0})

        wise_state = metadata.get("wise_state")
        state_dict = dict(wise_state) if isinstance(wise_state, dict) else {}
        online = bool(_to_bool(state_dict.get("online"), False))
        stale = bool(_to_bool(state_dict.get("stale"), True))
        error = str(state_dict.get("error", "")).strip()
        channels_raw = state_dict.get("channels")
        channels: Dict[int, bool] = {}
        if isinstance(channels_raw, dict):
            for raw_key, raw_val in channels_raw.items():
                try:
                    channels[int(raw_key)] = bool(_to_bool(raw_val, False))
                except Exception:
                    continue

        ready_by_slot: Dict[int, Optional[bool]] = {}
        for slot_index in required:
            channel_index = slot_map.get(int(slot_index))
            if channel_index is None:
                ready_by_slot[int(slot_index)] = None
                continue
            ready_by_slot[int(slot_index)] = bool(channels.get(int(channel_index), False))

        ready_slots = sorted([slot for slot, is_ready in ready_by_slot.items() if is_ready is True])
        missing_slots = sorted([slot for slot, is_ready in ready_by_slot.items() if is_ready is not True])
        configured = bool(enabled and slot_map and required)
        all_ready = bool(configured and online and (not stale) and (not missing_slots))
        return {
            "configured": bool(configured),
            "enabled": bool(enabled),
            "online": bool(online),
            "stale": bool(stale),
            "required_slots": [int(x) for x in required],
            "ready_slots": [int(x) for x in ready_slots],
            "missing_slots": [int(x) for x in missing_slots],
            "ready_by_slot": {str(int(k)): v for k, v in sorted(ready_by_slot.items())},
            "all_ready": bool(all_ready),
            "error": str(error),
        }

    def _sync_runtime_device_packml_state(device_id: str, packml_state: str) -> None:
        desired = str(packml_state or "").strip().upper()
        if not desired:
            return
        try:
            runtime_centrifuge = runtime_devices.get_centrifuge(str(device_id))
        except Exception:
            return

        # Controller-backed centrifuges must not be force-transitioned by mirrored
        # world snapshots; the remote device state is authoritative.
        if getattr(runtime_centrifuge, "controller", None) is not None:
            return

        try:
            current = str(runtime_centrifuge.diagnose().get("packml_state", "")).strip().upper()
        except Exception:
            current = ""
        if current == desired:
            return

        try:
            if desired == "COMPLETE" and current == "EXECUTE":
                runtime_centrifuge.complete_cycle()
            elif desired == "STOPPED" and current == "EXECUTE":
                runtime_centrifuge.stop_centrifuge()
            elif desired == "IDLE":
                if current == "EXECUTE":
                    runtime_centrifuge.complete_cycle()
                    current = str(runtime_centrifuge.diagnose().get("packml_state", "")).strip().upper()
                if current in {"COMPLETE", "STOPPED", "ABORTED", "FAULTED"}:
                    runtime_centrifuge.transition("RESET")
        except Exception as exc:
            print(
                "StateDriven runtime sync warning: failed to align runtime device "
                f"'{device_id}' to PACKML '{desired}' ({exc})"
            )

    def _resolve_wait_ready_states(raw_states: Any) -> Set[str]:
        if isinstance(raw_states, (list, tuple, set)):
            parsed = {str(x).strip().upper() for x in raw_states if str(x).strip()}
            if parsed:
                return parsed
        txt = str(raw_states or "").strip()
        if txt:
            tokens = [str(x).strip().upper() for x in txt.split(",") if str(x).strip()]
            if tokens:
                return set(tokens)
        return set(WAIT_READY_PACKML_STATES)

    def _is_external_wait_satisfied(bb: Blackboard, loop_index: int) -> bool:
        wait_device_id = str(bb.get("state_driven_wait_device_id", "")).strip()
        wait_reason = str(bb.get("state_driven_waiting_reason", "")).strip()
        wait_process = str(bb.get("state_driven_wait_process", "")).strip().upper()
        wait_source = str(bb.get("state_driven_wait_source", "PACKML")).strip().upper() or "PACKML"
        ready_states = _resolve_wait_ready_states(bb.get("state_driven_wait_ready_states", ()))

        if not wait_device_id:
            print(
                "StateDriven waiting: no wait device id configured; continuing UpdateWorldState polling. "
                f"reason='{wait_reason or 'unspecified'}'"
            )
            return False

        if wait_source == "WISE" and wait_process == ProcessType.IMMUNOHEMATOLOGY_ANALYSIS.value:
            required_slots_raw = bb.get("state_driven_wait_wise_required_slots", [])
            required_slots: List[int] = []
            if isinstance(required_slots_raw, (list, tuple, set)):
                for item in required_slots_raw:
                    try:
                        required_slots.append(int(item))
                    except Exception:
                        continue
            elif str(required_slots_raw).strip():
                for token in str(required_slots_raw).split(","):
                    token_txt = str(token).strip()
                    if not token_txt:
                        continue
                    try:
                        required_slots.append(int(token_txt))
                    except Exception:
                        continue

            details = _wise_ready_details_for_device(
                wait_device_id,
                required_slots=required_slots,
            )
            bb["state_driven_wait_last_wise_state"] = dict(details)
            if bool(details.get("all_ready", False)):
                bb["state_driven_waiting_external_completion"] = False
                bb["state_driven_waiting_reason"] = ""
                bb["state_driven_wait_satisfied_ts"] = _local_now_iso()
                bb.pop("state_driven_wait_process", None)
                bb.pop("state_driven_wait_device_id", None)
                bb.pop("state_driven_wait_ready_states", None)
                bb.pop("state_driven_wait_source", None)
                bb.pop("state_driven_wait_wise_required_slots", None)
                bb.pop("state_driven_wait_last_packml_state", None)
                append_world_event(
                    occupancy_records,
                    world,
                    event_type="STATE_DRIVEN_WAIT_SATISFIED",
                    entity_type="WORKFLOW",
                    entity_id="STATE_DRIVEN_PLANNING",
                    details={
                        "phase": "StateDrivenPlanning",
                        "loop_index": int(loop_index),
                        "process": str(wait_process),
                        "device_id": str(wait_device_id),
                        "source": "WISE",
                        "ready_slots": list(details.get("ready_slots", [])),
                        "required_slots": list(details.get("required_slots", [])),
                    },
                )
                return True

            print(
                "StateDriven waiting: Wise readiness pending "
                f"(process={wait_process}, device={wait_device_id}, source=WISE, "
                f"ready_slots={details.get('ready_slots', [])}, "
                f"missing_slots={details.get('missing_slots', [])}, "
                f"online={details.get('online', False)}, stale={details.get('stale', True)})"
            )
            return False

        current_state = _get_world_device_packml_state(wait_device_id)
        bb["state_driven_wait_last_packml_state"] = str(current_state)
        if current_state in ready_states:
            bb["state_driven_waiting_external_completion"] = False
            bb["state_driven_waiting_reason"] = ""
            bb["state_driven_wait_satisfied_ts"] = _local_now_iso()
            bb.pop("state_driven_wait_process", None)
            bb.pop("state_driven_wait_device_id", None)
            bb.pop("state_driven_wait_ready_states", None)
            bb.pop("state_driven_wait_source", None)
            bb.pop("state_driven_wait_wise_required_slots", None)
            append_world_event(
                occupancy_records,
                world,
                event_type="STATE_DRIVEN_WAIT_SATISFIED",
                entity_type="WORKFLOW",
                entity_id="STATE_DRIVEN_PLANNING",
                details={
                    "phase": "StateDrivenPlanning",
                    "loop_index": int(loop_index),
                    "process": str(wait_process),
                    "device_id": str(wait_device_id),
                    "packml_state": str(current_state),
                    "ready_states": sorted(ready_states),
                },
            )
            return True

        print(
            "StateDriven waiting: external completion pending "
            f"(process={wait_process or 'UNKNOWN'}, device={wait_device_id}, "
            f"packml_state={current_state or 'UNKNOWN'}, ready_states={sorted(ready_states)})"
        )
        return False

    def _refresh_world_device_states_from_ulm(bb: Blackboard, loop_index: int) -> bool:
        task_key = "UpdateWorldState_From_uLM"
        if task_key not in sender.catalog.raw.get("Available_Tasks", {}):
            print(f"StateDriven prerequisite failed: missing task '{task_key}' in Available_Tasks.json")
            return False

        task_name = f"StateDriven.UpdateWorldState.loop{int(loop_index)}"
        ok, result = _run_task(task_key, {}, task_name)
        if not ok:
            return False

        raw_devices = _extract_update_world_devices_payload(result)
        if not raw_devices:
            print(
                f"{task_name} failed: no device status payload found in response outputs. "
                "Expected e.g. 'Devices: (5-COMPLETE; 7-COMPLETE)'."
            )
            return False

        try:
            mapping_result = map_update_world_devices_to_assigned_world_devices(world, raw_devices)
        except Exception as exc:
            print(f"{task_name} failed: cannot map device payload to world devices ({exc})")
            return False

        for assignment in mapping_result.assignments:
            try:
                _set_world_device_packml_state(assignment.device_id, assignment.packml_state)
                _sync_runtime_device_packml_state(assignment.device_id, assignment.packml_state)
            except Exception as exc:
                print(
                    f"{task_name} failed: cannot update device '{assignment.device_id}' "
                    f"PACKML state '{assignment.packml_state}' ({exc})"
                )
                return False

        if mapping_result.unmapped:
            print(
                f"{task_name}: unmapped statuses detected: "
                f"{[{'itm_id': x.itm_id, 'packml_state': x.packml_state, 'reason': x.reason} for x in mapping_result.unmapped]}"
            )

        bb["device_status_update_raw"] = str(raw_devices)
        bb["device_status_update_assignments"] = [
            {
                "itm_id": int(x.itm_id),
                "packml_state": str(x.packml_state),
                "station_id": str(x.station_id),
                "device_id": str(x.device_id),
            }
            for x in mapping_result.assignments
        ]
        bb["device_status_update_unmapped"] = [
            {"itm_id": int(x.itm_id), "packml_state": str(x.packml_state), "reason": str(x.reason)}
            for x in mapping_result.unmapped
        ]
        if ENABLE_WISE_POLLING:
            if not _refresh_world_device_states_from_wise(bb, loop_index):
                return False
        else:
            bb["wise_status_update_assignments"] = []

        append_world_event(
            occupancy_records,
            world,
            event_type="DEVICE_STATUS_UPDATED",
            entity_type="WORKFLOW",
            entity_id="STATE_DRIVEN_PLANNING",
            details={
                "phase": "StateDrivenPlanning",
                "source_task": task_key,
                "raw_devices": str(raw_devices),
                "assignments": bb["device_status_update_assignments"],
                "unmapped": bb["device_status_update_unmapped"],
                "wise_assignments": bb.get("wise_status_update_assignments", []),
                "loop_index": int(loop_index),
            },
        )
        return True

    def _refresh_world_device_states_from_wise(bb: Blackboard, loop_index: int) -> bool:
        try:
            wise_modules = runtime_devices.get_wise_modules()
        except Exception as exc:
            print(f"StateDriven Wise refresh failed: cannot access Wise registry ({exc})")
            return False
        if not wise_modules:
            bb["wise_status_update_assignments"] = []
            return True

        assignments: List[Dict[str, Any]] = []
        for device_id in sorted(wise_modules.keys()):
            module = wise_modules[device_id]
            try:
                snapshot = module.poll_inputs()
            except Exception as exc:
                print(
                    "StateDriven Wise refresh failed: poll exception "
                    f"(device='{device_id}', loop={int(loop_index)}, error={exc})"
                )
                return False

            payload = wise_snapshot_to_metadata(snapshot)
            try:
                _set_world_device_wise_state(device_id, payload, source="WiseModulePoll")
            except Exception as exc:
                print(
                    "StateDriven Wise refresh failed: cannot write world metadata "
                    f"(device='{device_id}', error={exc})"
                )
                return False

            try:
                ready_details = _wise_ready_details_for_device(device_id)
            except Exception as exc:
                print(
                    "StateDriven Wise refresh failed: cannot derive ready slots "
                    f"(device='{device_id}', error={exc})"
                )
                return False

            metadata = _get_world_device_metadata(device_id)
            metadata["wise_ready_slots"] = list(ready_details.get("ready_slots", []))
            metadata["wise_missing_ready_slots"] = list(ready_details.get("missing_slots", []))
            metadata["wise_all_ready"] = bool(ready_details.get("all_ready", False))
            _replace_world_device_metadata(device_id, metadata)

            assignments.append(
                {
                    "device_id": str(device_id),
                    "online": bool(payload.get("online", False)),
                    "stale": bool(payload.get("stale", True)),
                    "error": str(payload.get("error", "")),
                    "ready_slots": list(ready_details.get("ready_slots", [])),
                    "missing_slots": list(ready_details.get("missing_slots", [])),
                    "all_ready": bool(ready_details.get("all_ready", False)),
                }
            )

        bb["wise_status_update_assignments"] = assignments
        append_world_event(
            occupancy_records,
            world,
            event_type="WISE_STATUS_UPDATED",
            entity_type="WORKFLOW",
            entity_id="STATE_DRIVEN_PLANNING",
            details={
                "phase": "StateDrivenPlanning",
                "source_task": "WiseModulePoll",
                "assignments": assignments,
                "loop_index": int(loop_index),
            },
        )
        return True

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
            ProcessType.IMMUNOHEMATOLOGY_ANALYSIS: 12,
            ProcessType.HEMATOLOGY_ANALYSIS: 11,
            ProcessType.ARCHIVATION: 13,
        }

        # Route by the first rack-bound process in the declared process sequence.
        # This avoids skipping prerequisite handling (e.g. centrifugation before immunohematology analysis).
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

    def _pair_immuno_sample_with_kreuzprobe(
        *,
        primary_sample_id: str,
        immuno_barcode: str,
        decision_processes: Sequence[ProcessType],
        decision_source: str,
        decision_classification: str,
        target_slot_id: str,
    ) -> Tuple[bool, Optional[str], str]:
        barcode_key = _normalize_barcode_key(immuno_barcode)
        if not barcode_key:
            return False, None, "immuno sample has no barcode; pairing requires barcode label"

        mapping = _load_immuno_kreuzprobe_map(IMMUNO_KREUZPROBE_MAP_FILE)
        if not mapping:
            return (
                False,
                None,
                f"pairing map is missing/empty ({IMMUNO_KREUZPROBE_MAP_FILE})",
            )

        kreuz_sample_id = str(mapping.get(barcode_key, "")).strip()
        if not kreuz_sample_id:
            return (
                False,
                None,
                f"no Kreuzprobe mapping for immuno barcode '{barcode_key}' in {IMMUNO_KREUZPROBE_MAP_FILE}",
            )
        if kreuz_sample_id == str(primary_sample_id):
            return False, None, "mapped Kreuzprobe sample equals primary sample id"

        primary_state = world.sample_states.get(str(primary_sample_id))
        if primary_state is None:
            return False, None, f"unknown primary sample state '{primary_sample_id}'"
        primary_details = (
            dict(primary_state.classification_details)
            if isinstance(primary_state.classification_details, dict)
            else {}
        )
        existing_pair = ""
        pairing_raw = primary_details.get("pairing")
        if isinstance(pairing_raw, dict):
            existing_pair = str(pairing_raw.get("paired_sample_id", "")).strip()
        if existing_pair:
            if existing_pair == kreuz_sample_id:
                return True, kreuz_sample_id, "already paired"
            return (
                False,
                None,
                f"primary sample already paired with '{existing_pair}' (requested '{kreuz_sample_id}')",
            )

        kreuz_sample = world.samples.get(kreuz_sample_id)
        if kreuz_sample is None:
            return False, None, f"mapped Kreuzprobe sample '{kreuz_sample_id}' not found in world samples"
        kreuz_state = world.sample_states.get(kreuz_sample_id)
        if kreuz_state is None or not isinstance(kreuz_state.location, RackLocation):
            return False, None, f"mapped Kreuzprobe sample '{kreuz_sample_id}' has no rack location"
        if (
            str(kreuz_state.location.station_id) != "FridgeStation"
            or str(kreuz_state.location.station_slot_id) != "URGFridgeRackSlot1"
        ):
            return (
                False,
                None,
                "mapped Kreuzprobe sample is not staged in FridgeStation.URGFridgeRackSlot1",
            )

        if kreuz_sample.cap_state != CapState.CAPPED:
            return False, None, f"mapped Kreuzprobe sample '{kreuz_sample_id}' is not capped"

        if kreuz_state.completed_processes:
            return (
                False,
                None,
                f"mapped Kreuzprobe sample '{kreuz_sample_id}' already has completed processes",
            )

        kreuz_details = (
            dict(kreuz_state.classification_details)
            if isinstance(kreuz_state.classification_details, dict)
            else {}
        )
        kreuz_pairing_raw = kreuz_details.get("pairing")
        if isinstance(kreuz_pairing_raw, dict):
            paired_primary = str(kreuz_pairing_raw.get("paired_sample_id", "")).strip()
            if paired_primary and paired_primary != str(primary_sample_id):
                return (
                    False,
                    None,
                    f"mapped Kreuzprobe sample '{kreuz_sample_id}' is already paired with '{paired_primary}'",
                )

        try:
            pending = world.pending_processes(kreuz_sample_id)
        except Exception:
            pending = ()
        if pending:
            return (
                False,
                None,
                f"mapped Kreuzprobe sample '{kreuz_sample_id}' is already active with pending processes {list(pending)}",
            )

        pair_id = f"PAIR::{barcode_key}::{kreuz_sample_id}"
        pair_payload_primary = {
            "pair_id": pair_id,
            "role": "PRIMARY",
            "paired_sample_id": kreuz_sample_id,
            "barcode_key": barcode_key,
            "mapping_file": str(IMMUNO_KREUZPROBE_MAP_FILE),
            "mapping_source": "IMMUNO_KREUZPROBE_MAP",
        }
        pair_payload_kreuz = {
            "pair_id": pair_id,
            "role": "KREUZPROBE",
            "paired_sample_id": str(primary_sample_id),
            "barcode_key": barcode_key,
            "mapping_file": str(IMMUNO_KREUZPROBE_MAP_FILE),
            "mapping_source": "IMMUNO_KREUZPROBE_MAP",
        }

        try:
            world.classify_sample(
                sample_id=kreuz_sample_id,
                recognized=True,
                classification_source="IMMUNO_KREUZPROBE_MAP",
                barcode=str(kreuz_sample.barcode or kreuz_sample_id),
                required_processes=(
                    ProcessType.FRIDGE_RACK_PROVISIONING,
                    *tuple(decision_processes),
                ),
                assigned_route=f"PairedKreuzprobe:{decision_classification}",
                assigned_route_station_slot_id=str(target_slot_id),
                assigned_route_rack_index=None,
                classification_details={
                    "provider": "IMMUNO_KREUZPROBE_MAP",
                    "recognized": True,
                    "paired_by": str(decision_source),
                    "paired_for_sample_id": str(primary_sample_id),
                    "source_fridge_location": {
                        "station_id": str(kreuz_state.location.station_id),
                        "station_slot_id": str(kreuz_state.location.station_slot_id),
                        "rack_id": str(kreuz_state.location.rack_id),
                        "slot_index": int(kreuz_state.location.slot_index),
                    },
                    "rack_provisioning_policy": {
                        "process": ProcessType.FRIDGE_RACK_PROVISIONING.value,
                        "source_station_id": "FridgeStation",
                        "source_station_slot_id": "URGFridgeRackSlot1",
                        "target_station_id": PLATE_STATION_ID,
                        "target_station_slot_id": "URGFridgeRackSlot",
                    },
                    "terminal_return_policy": {
                        "process": ProcessType.ARCHIVATION.value,
                        "mode": "RETURN_TO_SOURCE_FRIDGE_SLOT",
                        "source_station_id": str(kreuz_state.location.station_id),
                        "source_station_slot_id": str(kreuz_state.location.station_slot_id),
                        "required_rack_id": str(kreuz_state.location.rack_id),
                        "target_station_id": PLATE_STATION_ID,
                        "target_station_slot_id": "URGFridgeRackSlot",
                        "target_slot_index": int(kreuz_state.location.slot_index),
                    },
                    "pairing": pair_payload_kreuz,
                },
            )
        except Exception as exc:
            return (
                False,
                None,
                f"failed to classify paired Kreuzprobe sample '{kreuz_sample_id}': {exc}",
            )

        primary_details["pairing"] = pair_payload_primary
        primary_state.classification_details = primary_details
        return True, kreuz_sample_id, "paired"

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
        if not _run_single_task_action(
            itm_id=int(source_cfg.itm_id),
            jig_id=int(source_cfg.jig_id),
            obj_nbr=int(
                world.obj_nbr_for_slot_index(
                    source_station_id,
                    source_station_slot_id,
                    source_slot_index,
                )
            ),
            action=ACTION_PICK,
            obj_type=int(obj_type),
            task_name=f"{task_prefix}.PickSample",
        ):
            return False, None

        if not _run_single_task_action(
            itm_id=int(target_cfg.itm_id),
            jig_id=int(target_cfg.jig_id),
            obj_nbr=int(
                world.obj_nbr_for_slot_index(
                    target_station_id,
                    target_station_slot_id,
                    target_slot_index,
                )
            ),
            action=ACTION_PLACE,
            obj_type=int(obj_type),
            task_name=f"{task_prefix}.PlaceSample",
        ):
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

    def _requires_station_reference_scan(station_id: str) -> bool:
        if str(station_id) == CHARGE_STATION_ID:
            return False
        station = world.get_station(station_id)
        return station.kind != StationKind.ON_ROBOT_PLATE

    def _ensure_station_reference(station_id: str, task_prefix: str) -> bool:
        if not _requires_station_reference_scan(station_id):
            return True

        station = world.get_station(station_id)
        if world.needs_navigation(station_id):
            if not station.amr_pos_target:
                print(
                    f"{task_prefix} prerequisite failed: station '{station_id}' has no AMR position target"
                )
                return False
            navigate_overrides = {
                "AMR_PosTarget": str(station.amr_pos_target),
                "AMR_Footprint": "1",
                "AMR_DOCK": "1",
            }
            ok, _ = _run_task("Navigate", navigate_overrides, f"{task_prefix}.Navigate.{station_id}")
            if not ok:
                return False
            try:
                world.set_robot_station(station_id)
            except Exception:
                pass

        scan_overrides = {
            "ITM_ID": int(station.itm_id),
            "ACT": DEVICE_ACTION_SCAN_LANDMARK,
        }
        ok, _ = _run_task(
            "SingleDeviceAction",
            scan_overrides,
            f"{task_prefix}.ScanLandmark.{station_id}",
        )
        return ok

    def _set_sample_cap_state(sample_id: str, cap_state: CapState) -> None:
        world.set_sample_cap_state(sample_id, cap_state)

    def _resolve_recap_jig_id(sample_id: str, default_jig_id: int) -> int:
        """Resolve recap-cap JIG by sample role.

        - PRIMARY / non-paired samples -> JIG 14 (general recap caps)
        - KREUZPROBE samples -> JIG 15 (Kreuzprobe recap caps)
        """
        state = world.sample_states.get(str(sample_id))
        details = state.classification_details if isinstance(getattr(state, "classification_details", None), dict) else {}
        pairing = details.get("pairing") if isinstance(details, dict) else None
        role = str(pairing.get("role", "")).strip().upper() if isinstance(pairing, dict) else ""
        target_jig_id = (
            int(THREE_FINGER_KREUZ_RECAP_JIG_ID)
            if role == "KREUZPROBE"
            else int(THREE_FINGER_RECAP_JIG_ID)
        )
        # Guard against config drift: if the requested jig is not configured on 3FG, keep prior behavior.
        try:
            slot_cfgs = world.slots_for_jig(THREE_FINGER_STATION_ID, int(target_jig_id))
            if slot_cfgs:
                return int(target_jig_id)
        except Exception:
            pass
        return int(default_jig_id)

    def _append_process_completed_event(sample_id: str, process: ProcessType, details: Optional[Dict[str, Any]] = None) -> None:
        payload = {
            "phase": "StateDrivenPlanning",
            "process": process.value,
        }
        if isinstance(details, dict):
            payload.update({str(k): v for k, v in details.items()})
        append_world_event(
            occupancy_records,
            world,
            event_type="SAMPLE_PROCESS_COMPLETED",
            entity_type="SAMPLE",
            entity_id=str(sample_id),
            details=payload,
        )

    def _policy_for_process(process: ProcessType) -> Optional[Any]:
        if dynamic_state_planner is None:
            return None
        return dynamic_state_planner.policies.get(process)

    def _locked_racks(bb: Blackboard) -> Dict[str, Dict[str, Any]]:
        raw = bb.get("state_driven_locked_racks", {})
        if isinstance(raw, dict):
            out: Dict[str, Dict[str, Any]] = {}
            for key, payload in raw.items():
                if not isinstance(payload, dict):
                    continue
                out[str(key)] = dict(payload)
            return out
        return {}

    def _set_rack_lock(bb: Blackboard, rack_id: str, payload: Dict[str, Any]) -> None:
        locks = _locked_racks(bb)
        locks[str(rack_id)] = dict(payload)
        bb["state_driven_locked_racks"] = locks

    def _clear_rack_lock(bb: Blackboard, rack_id: str) -> None:
        locks = _locked_racks(bb)
        locks.pop(str(rack_id), None)
        bb["state_driven_locked_racks"] = locks

    def _is_rack_locked(bb: Blackboard, rack_id: str) -> bool:
        locks = _locked_racks(bb)
        return str(rack_id) in locks

    def _sample_ids_with_pending_process(process: ProcessType) -> List[str]:
        pending_ids: List[str] = []
        for sample_id in sorted(world.sample_states.keys()):
            try:
                pending = world.pending_processes(str(sample_id))
            except Exception:
                continue
            if process in pending:
                pending_ids.append(str(sample_id))
        return pending_ids

    def _execute_state_driven_provision_rack_action(action: DynamicPlanAction, bb: Blackboard) -> bool:
        task_prefix = f"StateDriven.{action.sample_id}.{action.process.value}.ProvisionRack"

        source_station_id = str(action.source_station_id)
        source_station_slot_id = str(action.source_station_slot_id)
        target_station_id = str(action.target_station_id)
        target_station_slot_id = str(action.target_station_slot_id)

        source_rack_id = _rack_id_at(world, source_station_id, source_station_slot_id)
        if source_rack_id is None:
            print(
                f"{task_prefix} prerequisite failed: no rack at source "
                f"{source_station_id}.{source_station_slot_id}"
            )
            return False
        if _rack_id_at(world, target_station_id, target_station_slot_id) is not None:
            print(
                f"{task_prefix} prerequisite failed: target slot already has a rack "
                f"({target_station_id}.{target_station_slot_id})"
            )
            return False

        rack = world.racks.get(str(source_rack_id))
        if rack is None:
            print(f"{task_prefix} prerequisite failed: unknown source rack '{source_rack_id}'")
            return False

        source_cfg = world.get_slot_config(source_station_id, source_station_slot_id)
        target_cfg = world.get_slot_config(target_station_id, target_station_slot_id)
        obj_type = int(rack.pin_obj_type)

        if not _ensure_station_reference(source_station_id, task_prefix):
            return False

        if not _run_single_task_action(
            itm_id=int(source_cfg.itm_id),
            jig_id=int(source_cfg.jig_id),
            obj_nbr=int(source_cfg.rack_index),
            action=ACTION_PICK,
            obj_type=int(obj_type),
            task_name=f"{task_prefix}.Pick",
        ):
            return False

        if not _ensure_station_reference(target_station_id, task_prefix):
            return False

        if not _run_single_task_action(
            itm_id=int(target_cfg.itm_id),
            jig_id=int(target_cfg.jig_id),
            obj_nbr=int(target_cfg.rack_index),
            action=ACTION_PLACE,
            obj_type=int(obj_type),
            task_name=f"{task_prefix}.Place",
        ):
            return False

        try:
            moved_rack_id = world.move_rack(
                source_station_id=source_station_id,
                source_station_slot_id=source_station_slot_id,
                target_station_id=target_station_id,
                target_station_slot_id=target_station_slot_id,
            )
        except Exception as exc:
            print(f"{task_prefix} world move failed: {exc}")
            return False

        append_world_event(
            occupancy_records,
            world,
            event_type="RACK_MOVED",
            entity_type="RACK",
            entity_id=str(moved_rack_id),
            source={"station_id": source_station_id, "station_slot_id": source_station_slot_id},
            target={"station_id": target_station_id, "station_slot_id": target_station_slot_id},
            details={
                "phase": "StateDrivenPlanning",
                "action_type": "PROVISION_RACK",
                "process": action.process.value,
            },
        )
        append_world_event(
            occupancy_records,
            world,
            event_type="STATE_DRIVEN_ACTION_EXECUTED",
            entity_type="RACK",
            entity_id=str(moved_rack_id),
            details={
                "phase": "StateDrivenPlanning",
                "action_type": "PROVISION_RACK",
                "process": action.process.value,
            },
        )

        provisioned = bb.get("state_driven_provisioned_racks", {})
        if not isinstance(provisioned, dict):
            provisioned = {}
        provisioned[str(moved_rack_id)] = {
            "process": action.process.value,
            "source_station_id": source_station_id,
            "source_station_slot_id": source_station_slot_id,
            "target_station_id": target_station_id,
            "target_station_slot_id": target_station_slot_id,
            "target_jig_id": int(action.target_jig_id),
            "provisioned_ts": _local_now_iso(),
        }
        bb["state_driven_provisioned_racks"] = provisioned
        _set_rack_lock(
            bb,
            str(moved_rack_id),
            {
                "lock_type": "PROCESS",
                "process": action.process.value,
                "reason": "provisioned_for_process",
                "timestamp": _local_now_iso(),
            },
        )
        bb["state_driven_last_action"] = action.to_dict()
        return True

    def _maybe_return_provisioned_racks_after_process(process: ProcessType, bb: Blackboard) -> bool:
        policy = _policy_for_process(process)
        if policy is None:
            return True
        if not bool(getattr(policy, "return_provisioned_rack_after_process", False)):
            return True

        # Return decisions are made per provisioned rack, based on pending work
        # of samples currently mounted in that rack. A global "any sample has this
        # process pending" guard can block valid rack swaps when two flows share
        # one receiver slot (for example Archive vs. Fridge transit racks).
        provisioned = bb.get("state_driven_provisioned_racks", {})
        if not isinstance(provisioned, dict) or not provisioned:
            return True

        for rack_id, payload in list(provisioned.items()):
            if not isinstance(payload, dict):
                continue
            if str(payload.get("process", "")).strip().upper() != process.value:
                continue

            source_station_id = str(payload.get("source_station_id", "")).strip()
            source_station_slot_id = str(payload.get("source_station_slot_id", "")).strip()
            target_station_id = str(payload.get("target_station_id", "")).strip()
            target_station_slot_id = str(payload.get("target_station_slot_id", "")).strip()
            if not source_station_id or not source_station_slot_id or not target_station_id or not target_station_slot_id:
                continue

            mounted_rack_id = _rack_id_at(world, target_station_id, target_station_slot_id)
            if str(mounted_rack_id or "") != str(rack_id):
                provisioned.pop(str(rack_id), None)
                _clear_rack_lock(bb, str(rack_id))
                continue

            # Keep rack on plate if any sample inside still has pending work.
            # This prevents returning transit racks while an active sample is
            # still in-flight (for example after FRIDGE_RACK_PROVISIONING).
            try:
                mounted_rack = world.get_rack_at(target_station_id, target_station_slot_id)
            except Exception as exc:
                print(
                    "StateDriven return-rack prerequisite failed: cannot resolve mounted rack "
                    f"at {target_station_id}.{target_station_slot_id} ({exc})"
                )
                return False
            for sid in mounted_rack.occupied_slots.values():
                try:
                    if world.pending_processes(str(sid)):
                        return True
                except Exception:
                    continue

            source_slot_rack_id = _rack_id_at(world, source_station_id, source_station_slot_id)
            if source_slot_rack_id is not None and str(source_slot_rack_id) != str(rack_id):
                print(
                    "StateDriven return-rack prerequisite failed: source slot is occupied "
                    f"({source_station_id}.{source_station_slot_id} -> {source_slot_rack_id})"
                )
                return False

            task_prefix = f"StateDriven.{process.value}.ReturnProvisionedRack.{rack_id}"
            source_cfg = world.get_slot_config(source_station_id, source_station_slot_id)
            target_cfg = world.get_slot_config(target_station_id, target_station_slot_id)
            rack = world.racks.get(str(rack_id))
            if rack is None:
                print(f"{task_prefix} prerequisite failed: unknown rack '{rack_id}'")
                return False

            if not _ensure_station_reference(target_station_id, task_prefix):
                return False
            if not _run_single_task_action(
                itm_id=int(target_cfg.itm_id),
                jig_id=int(target_cfg.jig_id),
                obj_nbr=int(target_cfg.rack_index),
                action=ACTION_PICK,
                obj_type=int(rack.pin_obj_type),
                task_name=f"{task_prefix}.Pick",
            ):
                return False

            if not _ensure_station_reference(source_station_id, task_prefix):
                return False
            if not _run_single_task_action(
                itm_id=int(source_cfg.itm_id),
                jig_id=int(source_cfg.jig_id),
                obj_nbr=int(source_cfg.rack_index),
                action=ACTION_PLACE,
                obj_type=int(rack.pin_obj_type),
                task_name=f"{task_prefix}.Place",
            ):
                return False

            try:
                moved_rack_id = world.move_rack(
                    source_station_id=target_station_id,
                    source_station_slot_id=target_station_slot_id,
                    target_station_id=source_station_id,
                    target_station_slot_id=source_station_slot_id,
                )
            except Exception as exc:
                print(f"{task_prefix} world move failed: {exc}")
                return False

            append_world_event(
                occupancy_records,
                world,
                event_type="RACK_MOVED",
                entity_type="RACK",
                entity_id=str(moved_rack_id),
                source={"station_id": target_station_id, "station_slot_id": target_station_slot_id},
                target={"station_id": source_station_id, "station_slot_id": source_station_slot_id},
                details={
                    "phase": "StateDrivenPlanning",
                    "process": process.value,
                    "action": "RETURN_PROVISIONED_RACK",
                },
            )
            provisioned.pop(str(rack_id), None)
            _clear_rack_lock(bb, str(rack_id))

        bb["state_driven_provisioned_racks"] = provisioned
        return True

    def _baseline_home_for_rack(rack_id: str) -> Optional[Tuple[str, str]]:
        home = baseline_rack_home_by_id.get(str(rack_id))
        if not home:
            return None
        station_id = str(home[0]).strip()
        station_slot_id = str(home[1]).strip()
        if not station_id or not station_slot_id:
            return None
        try:
            world.get_slot_config(station_id, station_slot_id)
        except Exception:
            return None
        return station_id, station_slot_id

    def _rack_has_pending_sample_processes(rack_id: str) -> bool:
        rack = world.racks.get(str(rack_id))
        if rack is None:
            return True
        for sample_id in rack.occupied_slots.values():
            try:
                if world.pending_processes(str(sample_id)):
                    return True
            except Exception:
                # Unknown sample linkage should never trigger an implicit rack return.
                return True
        return False

    def _rack_type_name_for_rack_id(rack_id: str) -> str:
        rack = world.racks.get(str(rack_id))
        if rack is None:
            return ""
        rack_type = str(getattr(getattr(rack, "rack_type", ""), "value", getattr(rack, "rack_type", "")))
        return rack_type.strip().upper()

    def _device_managed_rack_types() -> Set[str]:
        rack_types: Set[str] = set()
        for station_id in sorted(world.stations.keys()):
            try:
                station_devices = world.get_station_devices(station_id)
            except Exception:
                station_devices = []
            if not station_devices:
                continue
            try:
                station = world.get_station(station_id)
            except Exception:
                continue
            for slot_cfg in station.slot_configs.values():
                for rack_type in slot_cfg.accepted_rack_types:
                    rack_type_txt = str(getattr(rack_type, "value", rack_type)).strip().upper()
                    if rack_type_txt:
                        rack_types.add(rack_type_txt)
        return rack_types

    def _is_device_managed_rack(
        rack_id: str,
        *,
        device_rack_types: Optional[Set[str]] = None,
    ) -> bool:
        rack_type = _rack_type_name_for_rack_id(rack_id)
        if not rack_type:
            return False
        tracked_types = device_rack_types if device_rack_types is not None else _device_managed_rack_types()
        return rack_type in tracked_types

    def _is_centrifuge_rack(rack_id: str) -> bool:
        rack_type = _rack_type_name_for_rack_id(rack_id)
        return rack_type.strip().upper() == RackType.CENTRIFUGE_RACK.value

    def _next_idle_rack_return_candidate(bb: Blackboard) -> Optional[Dict[str, Any]]:
        if not baseline_rack_home_by_id:
            return None

        device_rack_types = _device_managed_rack_types()
        for (station_id, station_slot_id), rack_id in sorted(world.rack_placements.items()):
            rack_id_txt = str(rack_id)
            # Device racks are process-controlled and must not be auto-returned on IDLE.
            if _is_device_managed_rack(rack_id_txt, device_rack_types=device_rack_types):
                continue
            if _is_rack_locked(bb, rack_id_txt):
                continue
            home = _baseline_home_for_rack(rack_id_txt)
            if home is None:
                continue
            home_station_id, home_station_slot_id = home
            if str(station_id) == home_station_id and str(station_slot_id) == home_station_slot_id:
                continue
            if _rack_has_pending_sample_processes(rack_id_txt):
                continue

            home_slot_rack_id = _rack_id_at(world, home_station_id, home_station_slot_id)
            if home_slot_rack_id is not None and str(home_slot_rack_id) != rack_id_txt:
                continue

            rack = world.racks.get(rack_id_txt)
            if rack is None:
                continue
            reason = (
                "rack_empty_return_to_baseline"
                if not rack.occupied_slots
                else "rack_idle_return_to_baseline"
            )
            return {
                "rack_id": rack_id_txt,
                "source_station_id": str(station_id),
                "source_station_slot_id": str(station_slot_id),
                "target_station_id": home_station_id,
                "target_station_slot_id": home_station_slot_id,
                "reason": reason,
            }
        return None

    def _return_rack_action_payload(candidate: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "action_type": "RETURN_RACK_HOME",
            "rack_id": str(candidate.get("rack_id", "")),
            "source_station_id": str(candidate.get("source_station_id", "")),
            "source_station_slot_id": str(candidate.get("source_station_slot_id", "")),
            "target_station_id": str(candidate.get("target_station_id", "")),
            "target_station_slot_id": str(candidate.get("target_station_slot_id", "")),
            "reason": str(candidate.get("reason", "")),
        }

    def _execute_return_rack_home(candidate: Dict[str, Any], bb: Blackboard) -> bool:
        rack_id = str(candidate.get("rack_id", "")).strip()
        source_station_id = str(candidate.get("source_station_id", "")).strip()
        source_station_slot_id = str(candidate.get("source_station_slot_id", "")).strip()
        target_station_id = str(candidate.get("target_station_id", "")).strip()
        target_station_slot_id = str(candidate.get("target_station_slot_id", "")).strip()
        reason = str(candidate.get("reason", "")).strip()
        if not rack_id or not source_station_id or not source_station_slot_id or not target_station_id or not target_station_slot_id:
            print(
                "StateDriven rack-return failed: invalid candidate payload "
                f"({candidate})"
            )
            return False

        mounted_rack_id = _rack_id_at(world, source_station_id, source_station_slot_id)
        if str(mounted_rack_id or "") != rack_id:
            return False
        home_slot_rack_id = _rack_id_at(world, target_station_id, target_station_slot_id)
        if home_slot_rack_id is not None and str(home_slot_rack_id) != rack_id:
            print(
                "StateDriven rack-return prerequisite failed: baseline slot occupied "
                f"({target_station_id}.{target_station_slot_id} -> {home_slot_rack_id})"
            )
            return False

        rack = world.racks.get(rack_id)
        if rack is None:
            print(f"StateDriven rack-return prerequisite failed: unknown rack '{rack_id}'")
            return False
        if _is_device_managed_rack(rack_id):
            print(
                "StateDriven rack-return skipped: device racks are process-controlled "
                f"(rack_id='{rack_id}')"
            )
            return True
        source_cfg = world.get_slot_config(source_station_id, source_station_slot_id)
        target_cfg = world.get_slot_config(target_station_id, target_station_slot_id)

        task_prefix = f"StateDriven.RackReturn.{rack_id}"
        if not _ensure_station_reference(source_station_id, task_prefix):
            return False
        if not _run_single_task_action(
            itm_id=int(source_cfg.itm_id),
            jig_id=int(source_cfg.jig_id),
            obj_nbr=int(source_cfg.rack_index),
            action=ACTION_PICK,
            obj_type=int(rack.pin_obj_type),
            task_name=f"{task_prefix}.Pick",
        ):
            return False

        if not _ensure_station_reference(target_station_id, task_prefix):
            return False
        if not _run_single_task_action(
            itm_id=int(target_cfg.itm_id),
            jig_id=int(target_cfg.jig_id),
            obj_nbr=int(target_cfg.rack_index),
            action=ACTION_PLACE,
            obj_type=int(rack.pin_obj_type),
            task_name=f"{task_prefix}.Place",
        ):
            return False

        try:
            moved_rack_id = world.move_rack(
                source_station_id=source_station_id,
                source_station_slot_id=source_station_slot_id,
                target_station_id=target_station_id,
                target_station_slot_id=target_station_slot_id,
            )
        except Exception as exc:
            print(f"{task_prefix} world move failed: {exc}")
            return False

        append_world_event(
            occupancy_records,
            world,
            event_type="RACK_MOVED",
            entity_type="RACK",
            entity_id=str(moved_rack_id),
            source={"station_id": source_station_id, "station_slot_id": source_station_slot_id},
            target={"station_id": target_station_id, "station_slot_id": target_station_slot_id},
            details={
                "phase": "StateDrivenPlanning",
                "action_type": "RETURN_RACK_HOME",
                "reason": reason,
            },
        )
        append_world_event(
            occupancy_records,
            world,
            event_type="STATE_DRIVEN_ACTION_EXECUTED",
            entity_type="RACK",
            entity_id=str(moved_rack_id),
            details={
                "phase": "StateDrivenPlanning",
                "action_type": "RETURN_RACK_HOME",
                "reason": reason,
            },
        )
        bb["state_driven_last_action"] = _return_rack_action_payload(candidate)

        executed = bb.get("state_driven_rack_returns_executed", [])
        if not isinstance(executed, list):
            executed = []
        executed.append(
            {
                "rack_id": str(moved_rack_id),
                "source_station_id": source_station_id,
                "source_station_slot_id": source_station_slot_id,
                "target_station_id": target_station_id,
                "target_station_slot_id": target_station_slot_id,
                "reason": reason,
                "timestamp": _local_now_iso(),
            }
        )
        bb["state_driven_rack_returns_executed"] = executed
        return True

    def _sample_ids_in_jig(station_id: str, jig_id: int) -> List[str]:
        ids: Set[str] = set()
        for cfg in world.slots_for_jig(station_id, int(jig_id)):
            rack_id = world.rack_placements.get((station_id, str(cfg.slot_id)))
            if not rack_id:
                continue
            rack = world.racks.get(str(rack_id))
            if rack is None:
                continue
            for sample_id in rack.occupied_slots.values():
                ids.add(str(sample_id))
        return sorted(ids)

    def _get_sample_process_holds(bb: Blackboard) -> Dict[str, Dict[str, Any]]:
        raw = bb.get("state_driven_sample_process_holds", {})
        out: Dict[str, Dict[str, Any]] = {}
        if isinstance(raw, dict):
            for sample_id, payload in raw.items():
                sid = str(sample_id).strip()
                if not sid or not isinstance(payload, dict):
                    continue
                out[sid] = dict(payload)
        bb["state_driven_sample_process_holds"] = out
        return out

    def _set_sample_process_holds(bb: Blackboard, holds: Dict[str, Dict[str, Any]]) -> None:
        cleaned: Dict[str, Dict[str, Any]] = {}
        for sample_id, payload in holds.items():
            sid = str(sample_id).strip()
            if not sid or not isinstance(payload, dict):
                continue
            cleaned[sid] = dict(payload)
        bb["state_driven_sample_process_holds"] = cleaned

    def _get_active_device_jobs(bb: Blackboard) -> Dict[str, Dict[str, Any]]:
        raw = bb.get("state_driven_active_device_jobs", {})
        out: Dict[str, Dict[str, Any]] = {}
        if isinstance(raw, dict):
            for device_id, payload in raw.items():
                did = str(device_id).strip()
                if not did or not isinstance(payload, dict):
                    continue
                out[did] = dict(payload)
        bb["state_driven_active_device_jobs"] = out
        return out

    def _set_active_device_jobs(bb: Blackboard, jobs: Dict[str, Dict[str, Any]]) -> None:
        cleaned: Dict[str, Dict[str, Any]] = {}
        for device_id, payload in jobs.items():
            did = str(device_id).strip()
            if not did or not isinstance(payload, dict):
                continue
            cleaned[did] = dict(payload)
        bb["state_driven_active_device_jobs"] = cleaned

    def _pending_sample_ids_for_process_at_jig(
        station_id: str,
        jig_id: int,
        process: ProcessType,
    ) -> List[str]:
        sample_ids: Set[str] = set()
        for sample_id in _sample_ids_in_jig(station_id, int(jig_id)):
            try:
                pending = world.pending_processes(str(sample_id))
            except Exception:
                continue
            if process in pending:
                sample_ids.add(str(sample_id))
        return sorted(sample_ids)

    def _register_centrifuge_running_job(
        bb: Blackboard,
        *,
        device_id: str,
        sample_ids: Sequence[str],
        reason: str,
    ) -> None:
        did = str(device_id).strip()
        if not did:
            return
        unique_samples = sorted({str(x).strip() for x in sample_ids if str(x).strip()})
        if not unique_samples:
            return

        now_ts = _local_now_iso()
        jobs = _get_active_device_jobs(bb)
        existing = dict(jobs.get(did, {}))
        merged_samples = sorted(
            set(unique_samples)
            | {str(x).strip() for x in existing.get("sample_ids", []) if str(x).strip()}
        )
        started_ts = str(existing.get("started_ts", "")).strip() or now_ts
        jobs[did] = {
            "process": ProcessType.CENTRIFUGATION.value,
            "status": SAMPLE_HOLD_STATUS_RUNNING,
            "started_ts": started_ts,
            "updated_ts": now_ts,
            "sample_ids": merged_samples,
            "reason": str(reason),
        }
        _set_active_device_jobs(bb, jobs)

        holds = _get_sample_process_holds(bb)
        for sample_id in merged_samples:
            hold = dict(holds.get(sample_id, {}))
            hold["process"] = ProcessType.CENTRIFUGATION.value
            hold["device_id"] = did
            hold["status"] = SAMPLE_HOLD_STATUS_RUNNING
            hold["reason"] = str(reason)
            hold["updated_ts"] = now_ts
            hold["created_ts"] = str(hold.get("created_ts", "")).strip() or started_ts
            holds[sample_id] = hold
        _set_sample_process_holds(bb, holds)

    def _mark_centrifuge_job_ready_to_unload(bb: Blackboard, *, device_id: str, reason: str) -> None:
        did = str(device_id).strip()
        if not did:
            return
        jobs = _get_active_device_jobs(bb)
        job = dict(jobs.get(did, {}))
        if not job:
            return
        if str(job.get("status", "")).strip().upper() == SAMPLE_HOLD_STATUS_READY_TO_UNLOAD:
            return

        now_ts = _local_now_iso()
        job["status"] = SAMPLE_HOLD_STATUS_READY_TO_UNLOAD
        job["ready_ts"] = now_ts
        job["updated_ts"] = now_ts
        job["reason"] = str(reason)
        jobs[did] = job
        _set_active_device_jobs(bb, jobs)

        holds = _get_sample_process_holds(bb)
        for sample_id in [str(x).strip() for x in job.get("sample_ids", []) if str(x).strip()]:
            hold = dict(holds.get(sample_id, {}))
            hold["process"] = ProcessType.CENTRIFUGATION.value
            hold["device_id"] = did
            hold["status"] = SAMPLE_HOLD_STATUS_READY_TO_UNLOAD
            hold["reason"] = str(reason)
            hold["updated_ts"] = now_ts
            hold["created_ts"] = str(hold.get("created_ts", "")).strip() or now_ts
            holds[sample_id] = hold
        _set_sample_process_holds(bb, holds)

    def _clear_centrifuge_job_and_holds(
        bb: Blackboard,
        *,
        device_id: str,
        sample_ids: Optional[Sequence[str]] = None,
    ) -> None:
        did = str(device_id).strip()
        jobs = _get_active_device_jobs(bb)
        job = dict(jobs.get(did, {})) if did else {}

        release_ids: Set[str] = set()
        if sample_ids is not None:
            release_ids |= {str(x).strip() for x in sample_ids if str(x).strip()}
        if job:
            release_ids |= {str(x).strip() for x in job.get("sample_ids", []) if str(x).strip()}

        holds = _get_sample_process_holds(bb)
        for sample_id in release_ids:
            holds.pop(sample_id, None)
        _set_sample_process_holds(bb, holds)

        jobs_changed = False
        if did and did in jobs:
            jobs.pop(did, None)
            jobs_changed = True
        elif release_ids:
            for candidate_id, payload in list(jobs.items()):
                process = str(payload.get("process", "")).strip().upper()
                if process != ProcessType.CENTRIFUGATION.value:
                    continue
                job_sample_ids = {
                    str(x).strip()
                    for x in payload.get("sample_ids", [])
                    if str(x).strip()
                }
                if job_sample_ids & release_ids:
                    jobs.pop(candidate_id, None)
                    jobs_changed = True
        if jobs_changed:
            _set_active_device_jobs(bb, jobs)

    def _running_hold_sample_ids(bb: Blackboard) -> Set[str]:
        holds = _get_sample_process_holds(bb)
        out: Set[str] = set()
        for sample_id, payload in holds.items():
            process = str(payload.get("process", "")).strip().upper()
            status = str(payload.get("status", "")).strip().upper()
            if process == ProcessType.CENTRIFUGATION.value and status == SAMPLE_HOLD_STATUS_RUNNING:
                out.add(str(sample_id))
        return out

    def _has_running_centrifuge_jobs(bb: Blackboard) -> bool:
        jobs = _get_active_device_jobs(bb)
        for payload in jobs.values():
            process = str(payload.get("process", "")).strip().upper()
            status = str(payload.get("status", "")).strip().upper()
            if process == ProcessType.CENTRIFUGATION.value and status == SAMPLE_HOLD_STATUS_RUNNING:
                return True
        return False

    def _active_centrifuge_job_sample_ids(bb: Blackboard, device_id: str) -> List[str]:
        did = str(device_id).strip()
        if not did:
            return []
        jobs = _get_active_device_jobs(bb)
        job = jobs.get(did)
        if not isinstance(job, dict):
            return []
        return sorted({str(x).strip() for x in job.get("sample_ids", []) if str(x).strip()})

    def _sync_active_centrifuge_jobs(bb: Blackboard, loop_index: int) -> bool:
        try:
            runtime_centrifuges = runtime_devices.get_centrifuges_at_station(CENTRIFUGE_STATION_ID)
        except Exception:
            return True

        for runtime_centrifuge in runtime_centrifuges:
            device_id = str(runtime_centrifuge.identity.device_id)
            try:
                diag = dict(runtime_centrifuge.diagnose())
            except Exception as exc:
                print(
                    "StateDriven centrifuge background sync warning: diagnose failed "
                    f"(device='{device_id}', loop={int(loop_index)}, error={exc})"
                )
                continue

            runtime_packml = str(diag.get("packml_state", "")).strip().upper()
            runtime_fault = str(diag.get("fault_code", "")).strip()
            runtime_spinning = bool(diag.get("rotor_spinning", False))
            if runtime_packml:
                _set_world_device_packml_state(
                    device_id,
                    runtime_packml,
                    source="RuntimeDeviceDiagnose",
                )

            pending_sample_ids = _pending_sample_ids_for_process_at_jig(
                CENTRIFUGE_STATION_ID,
                2,
                ProcessType.CENTRIFUGATION,
            )

            if (runtime_spinning or runtime_packml == "EXECUTE") and pending_sample_ids:
                _register_centrifuge_running_job(
                    bb,
                    device_id=device_id,
                    sample_ids=pending_sample_ids,
                    reason="Centrifuge cycle running.",
                )

            if runtime_fault and runtime_fault != "00000000":
                active_samples = _active_centrifuge_job_sample_ids(bb, device_id)
                if active_samples:
                    print(
                        "StateDriven centrifuge background sync failed: active centrifuge job is faulted "
                        f"(device='{device_id}', fault='{runtime_fault}', samples={active_samples})"
                    )
                    return False

            jobs = _get_active_device_jobs(bb)
            job = jobs.get(device_id)
            if not isinstance(job, dict):
                continue

            job_status = str(job.get("status", "")).strip().upper()
            started_ts = str(job.get("started_ts", "")).strip()
            if job_status == SAMPLE_HOLD_STATUS_RUNNING and started_ts:
                try:
                    started_dt = datetime.fromisoformat(started_ts)
                    elapsed_s = max(
                        0.0,
                        (datetime.now().astimezone() - started_dt).total_seconds(),
                    )
                    if elapsed_s > float(CENTRIFUGE_ASYNC_MAX_RUNTIME_S):
                        print(
                            "StateDriven centrifuge background sync failed: max runtime exceeded "
                            f"(device='{device_id}', elapsed_s={elapsed_s:.1f}, "
                            f"limit_s={float(CENTRIFUGE_ASYNC_MAX_RUNTIME_S):.1f})"
                        )
                        return False
                except Exception:
                    pass

            if (
                job_status == SAMPLE_HOLD_STATUS_RUNNING
                and not runtime_spinning
                and runtime_packml in WAIT_READY_PACKML_STATES
            ):
                _mark_centrifuge_job_ready_to_unload(
                    bb,
                    device_id=device_id,
                    reason=(
                        "Centrifuge cycle finished; unload can continue "
                        f"(packml_state={runtime_packml})"
                    ),
                )
                append_world_event(
                    occupancy_records,
                    world,
                    event_type="STATE_DRIVEN_WAIT_SATISFIED",
                    entity_type="WORKFLOW",
                    entity_id="STATE_DRIVEN_PLANNING",
                    details={
                        "phase": "StateDrivenPlanning",
                        "loop_index": int(loop_index),
                        "process": ProcessType.CENTRIFUGATION.value,
                        "device_id": device_id,
                        "packml_state": runtime_packml,
                        "ready_states": sorted(WAIT_READY_PACKML_STATES),
                        "source": "RuntimeDeviceDiagnose",
                    },
                )
        return True

    def _slot_cfg_by_rack_index_for_jig(station_id: str, jig_id: int) -> Dict[int, Any]:
        out: Dict[int, Any] = {}
        for cfg in world.slots_for_jig(station_id, int(jig_id)):
            idx = int(getattr(cfg, "rack_index", 0))
            if idx <= 0:
                raise ValueError(
                    f"Invalid rack_index for station '{station_id}', JIG_ID={int(jig_id)}, "
                    f"slot='{getattr(cfg, 'slot_id', '?')}'"
                )
            if idx in out:
                raise ValueError(
                    f"Duplicate rack_index={idx} for station '{station_id}', JIG_ID={int(jig_id)}"
                )
            out[idx] = cfg
        return out

    def _ih500_slot_pairs() -> List[Tuple[int, Any, Any]]:
        source_by_idx = _slot_cfg_by_rack_index_for_jig(PLATE_STATION_ID, IH500_SOURCE_JIG_ID)
        target_by_idx = _slot_cfg_by_rack_index_for_jig(IH500_STATION_ID, IH500_DEVICE_JIG_ID)
        source_idx = set(source_by_idx.keys())
        target_idx = set(target_by_idx.keys())
        if not source_idx:
            raise ValueError(
                f"No source rack slots configured for '{PLATE_STATION_ID}' JIG_ID={IH500_SOURCE_JIG_ID}"
            )
        if not target_idx:
            raise ValueError(
                f"No device rack slots configured for '{IH500_STATION_ID}' JIG_ID={IH500_DEVICE_JIG_ID}"
            )
        if source_idx != target_idx:
            missing_on_target = sorted(source_idx - target_idx)
            missing_on_source = sorted(target_idx - source_idx)
            raise ValueError(
                "IH500 slot mapping mismatch by rack_index "
                f"(missing_on_target={missing_on_target}, missing_on_source={missing_on_source})"
            )
        return [(idx, source_by_idx[idx], target_by_idx[idx]) for idx in sorted(source_idx)]

    def _resolve_immuno_device_id(preferred_device_id: str = "") -> str:
        preferred = str(preferred_device_id or "").strip()
        if preferred:
            dev = world.devices.get(preferred)
            if dev is not None:
                if str(dev.station_id) == IH500_STATION_ID and ProcessType.IMMUNOHEMATOLOGY_ANALYSIS in dev.capabilities:
                    return preferred
        candidates: List[str] = []
        for device_id in sorted(world.devices.keys()):
            dev = world.devices[device_id]
            if str(dev.station_id) != IH500_STATION_ID:
                continue
            if ProcessType.IMMUNOHEMATOLOGY_ANALYSIS not in dev.capabilities:
                continue
            candidates.append(str(device_id))
        if not candidates:
            return ""
        return str(candidates[0])

    def _arm_ih500_wait_for_wise(
        bb: Blackboard,
        *,
        device_id: str,
        required_slots: Sequence[int],
        reason: str,
    ) -> None:
        bb["state_driven_waiting_external_completion"] = True
        bb["state_driven_waiting_reason"] = str(reason)
        bb["state_driven_wait_process"] = ProcessType.IMMUNOHEMATOLOGY_ANALYSIS.value
        bb["state_driven_wait_device_id"] = str(device_id)
        bb["state_driven_wait_source"] = "WISE"
        bb["state_driven_wait_wise_required_slots"] = [int(x) for x in sorted({int(x) for x in required_slots})]
        bb["state_driven_wait_armed_ts"] = _local_now_iso()

    def _check_ih500_wise_unload_gate(
        task_prefix: str,
        bb: Blackboard,
        *,
        device_id: str,
        required_slots: Sequence[int],
    ) -> Tuple[bool, bool, List[int], List[int]]:
        required: List[int] = sorted({int(x) for x in required_slots if int(x) > 0})
        did = str(device_id).strip()
        if not did:
            print(f"{task_prefix} prerequisite failed: no IH500 device id available for Wise unload gate")
            return False, False, [], required

        metadata = _get_world_device_metadata(did)
        wise_cfg = metadata.get("wise")
        if not isinstance(wise_cfg, dict):
            return True, False, list(required), []
        if not _to_bool(wise_cfg.get("enabled"), False):
            return True, False, list(required), []
        if not _to_bool(wise_cfg.get("enforce_ready_for_unload"), False):
            return True, False, list(required), []

        details = _wise_ready_details_for_device(did, required_slots=required)
        bb["last_ih500_wise_ready"] = dict(details)
        if not bool(details.get("configured", False)):
            print(
                f"{task_prefix} prerequisite failed: Wise unload gate is enabled but slot mapping is missing "
                f"(device='{did}')"
            )
            return False, False, [], required
        if not bool(details.get("online", False)) or bool(details.get("stale", True)):
            print(
                f"{task_prefix} prerequisite failed: Wise state unavailable for unload decision "
                f"(device='{did}', online={details.get('online', False)}, stale={details.get('stale', True)}, "
                f"error='{details.get('error', '')}')"
            )
            return False, False, [], required

        ready_slots_raw = details.get("ready_slots", [])
        missing_slots_raw = details.get("missing_slots", [])
        ready_slots: List[int] = []
        missing_slots: List[int] = []
        required_set = set(required)
        if isinstance(ready_slots_raw, (list, tuple, set)):
            for item in ready_slots_raw:
                try:
                    idx = int(item)
                except Exception:
                    continue
                if idx in required_set:
                    ready_slots.append(idx)
        if isinstance(missing_slots_raw, (list, tuple, set)):
            for item in missing_slots_raw:
                try:
                    idx = int(item)
                except Exception:
                    continue
                if idx in required_set:
                    missing_slots.append(idx)
        ready_slots = sorted({int(x) for x in ready_slots})
        missing_slots = sorted({int(x) for x in missing_slots})

        if ready_slots:
            if missing_slots:
                print(
                    "IH500 partial unload: releasing ready racks while waiting for remaining Wise-ready slots "
                    f"(device='{did}', ready_slots={ready_slots}, missing_slots={missing_slots})"
                )
                return True, True, ready_slots, missing_slots
            return True, False, ready_slots, []

        wait_reason = (
            "IH500 unload deferred: waiting for Wise rack-ready sensors "
            f"(device='{did}', ready_slots={details.get('ready_slots', [])}, "
            f"missing_slots={details.get('missing_slots', [])})"
        )
        _arm_ih500_wait_for_wise(
            bb,
            device_id=did,
            required_slots=[int(x) for x in required],
            reason=wait_reason,
        )
        print(wait_reason)
        return False, True, [], list(required)

    def _execute_ih500_immuno_cycle(
        sample_id: str,
        bb: Blackboard,
        *,
        selected_device_id: str = "",
    ) -> bool:
        task_prefix = f"StateDriven.{sample_id}.{ProcessType.IMMUNOHEMATOLOGY_ANALYSIS.value}.Process"
        sample_state = world.sample_states.get(str(sample_id))
        if sample_state is None:
            print(f"{task_prefix} prerequisite failed: unknown sample '{sample_id}'")
            return False
        sample = world.samples.get(str(sample_id))
        if sample is None:
            print(f"{task_prefix} prerequisite failed: unknown sample payload '{sample_id}'")
            return False
        if sample.cap_state != CapState.DECAPPED:
            print(
                f"{task_prefix} prerequisite failed: sample is not decapped "
                f"(sample='{sample_id}', cap_state='{sample.cap_state.value}')"
            )
            return False
        try:
            slot_pairs = _ih500_slot_pairs()
        except Exception as exc:
            print(f"{task_prefix} prerequisite failed: {exc}")
            return False

        pair_state: List[Tuple[int, Any, Any, Optional[str], Optional[str]]] = []
        source_count = 0
        target_count = 0
        for idx, source_cfg, target_cfg in slot_pairs:
            source_rack_id = _rack_id_at(world, PLATE_STATION_ID, str(source_cfg.slot_id))
            target_rack_id = _rack_id_at(world, IH500_STATION_ID, str(target_cfg.slot_id))
            if source_rack_id:
                source_count += 1
            if target_rack_id:
                target_count += 1
            pair_state.append((idx, source_cfg, target_cfg, source_rack_id, target_rack_id))

        pair_count = len(pair_state)
        mode = "UNLOAD_ONLY" if target_count > 0 else "LOAD_UNLOAD"

        active_pairs: List[Tuple[int, Any, Any, Optional[str], Optional[str]]] = []
        if mode == "LOAD_UNLOAD":
            for idx, source_cfg, target_cfg, source_rack_id, target_rack_id in pair_state:
                if not source_rack_id:
                    print(
                        f"{task_prefix} load failed: expected source rack at "
                        f"{PLATE_STATION_ID}.{source_cfg.slot_id}"
                    )
                    return False
                rack = world.racks.get(str(source_rack_id))
                if rack is None:
                    print(f"{task_prefix} load failed: unknown rack '{source_rack_id}'")
                    return False
                if rack.occupied_slots:
                    active_pairs.append((idx, source_cfg, target_cfg, source_rack_id, target_rack_id))
        else:
            for idx, source_cfg, target_cfg, source_rack_id, target_rack_id in pair_state:
                if not target_rack_id:
                    # Mixed occupancy is valid for IH500; skip slots with no rack.
                    continue
                rack = world.racks.get(str(target_rack_id))
                if rack is None:
                    print(f"{task_prefix} unload failed: unknown rack '{target_rack_id}'")
                    return False
                if rack.occupied_slots:
                    active_pairs.append((idx, source_cfg, target_cfg, source_rack_id, target_rack_id))

        if not active_pairs:
            print(f"{task_prefix}: no racks with samples; skipping IH500 rack movement.")
            bb["state_driven_waiting_external_completion"] = False
            bb["state_driven_waiting_reason"] = ""
            bb["last_ih500_cycle_mode"] = str(mode)
            bb["last_ih500_cycle_ts"] = _local_now_iso()
            return True

        if not _ensure_station_reference(IH500_STATION_ID, task_prefix):
            return False

        immuno_device_id = _resolve_immuno_device_id(selected_device_id)
        active_slot_indexes = [int(idx) for idx, *_ in active_pairs]
        unload_ready_slots: List[int] = list(active_slot_indexes)
        pending_unload_slots: List[int] = []

        if mode == "LOAD_UNLOAD":
            for idx, source_cfg, target_cfg, source_rack_id, _ in active_pairs:
                rack = world.racks.get(str(source_rack_id))
                if rack is None:
                    print(f"{task_prefix} load failed: unknown rack '{source_rack_id}'")
                    return False

                # Load transfer must explicitly pick from source jig before pushing into target jig.
                if not _run_single_task_action(
                    itm_id=int(source_cfg.itm_id),
                    jig_id=int(source_cfg.jig_id),
                    obj_nbr=int(source_cfg.rack_index),
                    action=ACTION_PICK,
                    obj_type=int(rack.pin_obj_type),
                    task_name=f"{task_prefix}.LoadRack{int(idx)}.PickFromSource",
                ):
                    return False

                if not _run_single_task_action(
                    itm_id=int(target_cfg.itm_id),
                    jig_id=int(target_cfg.jig_id),
                    obj_nbr=int(target_cfg.rack_index),
                    action=ACTION_PUSH_RACK_IN,
                    obj_type=int(rack.pin_obj_type),
                    task_name=f"{task_prefix}.LoadRack{int(idx)}.PushRackIn",
                ):
                    return False

                try:
                    moved_rack_id = world.move_rack(
                        source_station_id=PLATE_STATION_ID,
                        source_station_slot_id=str(source_cfg.slot_id),
                        target_station_id=IH500_STATION_ID,
                        target_station_slot_id=str(target_cfg.slot_id),
                    )
                except Exception as exc:
                    print(f"{task_prefix} load world move failed (rack_index={int(idx)}): {exc}")
                    return False

                append_world_event(
                    occupancy_records,
                    world,
                    event_type="RACK_MOVED",
                    entity_type="RACK",
                    entity_id=str(moved_rack_id),
                    source={"station_id": PLATE_STATION_ID, "station_slot_id": str(source_cfg.slot_id)},
                    target={"station_id": IH500_STATION_ID, "station_slot_id": str(target_cfg.slot_id)},
                    details={
                        "phase": "StateDrivenPlanning",
                        "process": ProcessType.IMMUNOHEMATOLOGY_ANALYSIS.value,
                        "mode": "LOAD",
                        "transfer_index": int(idx),
                        "action": "PushRackIn",
                    },
                )

            allow_unload, waiting, unload_ready_slots, pending_unload_slots = _check_ih500_wise_unload_gate(
                task_prefix,
                bb,
                device_id=immuno_device_id,
                required_slots=active_slot_indexes,
            )
            if not allow_unload:
                if waiting:
                    bb["last_ih500_cycle_mode"] = "LOAD_WAIT_READY"
                    bb["last_ih500_cycle_ts"] = _local_now_iso()
                    return True
                return False
        else:
            allow_unload, waiting, unload_ready_slots, pending_unload_slots = _check_ih500_wise_unload_gate(
                task_prefix,
                bb,
                device_id=immuno_device_id,
                required_slots=active_slot_indexes,
            )
            if not allow_unload:
                if waiting:
                    bb["last_ih500_cycle_mode"] = "WAIT_READY"
                    bb["last_ih500_cycle_ts"] = _local_now_iso()
                    return True
                return False

        unload_ready_set = {int(x) for x in unload_ready_slots if int(x) > 0}
        unload_pairs = (
            [pair for pair in active_pairs if int(pair[0]) in unload_ready_set]
            if unload_ready_set
            else list(active_pairs)
        )
        if not unload_pairs:
            if pending_unload_slots:
                wait_reason = (
                    "IH500 unload deferred: waiting for remaining Wise rack-ready sensors "
                    f"(device='{immuno_device_id}', ready_slots={sorted(unload_ready_set)}, "
                    f"missing_slots={sorted({int(x) for x in pending_unload_slots if int(x) > 0})})"
                )
                _arm_ih500_wait_for_wise(
                    bb,
                    device_id=immuno_device_id,
                    required_slots=[int(x) for x in pending_unload_slots],
                    reason=wait_reason,
                )
                print(wait_reason)
                bb["last_ih500_cycle_mode"] = f"{mode}_WAIT_READY"
                bb["last_ih500_cycle_ts"] = _local_now_iso()
                return True
            print(f"{task_prefix}: no eligible IH500 racks selected for unload.")
            bb["last_ih500_cycle_mode"] = str(mode)
            bb["last_ih500_cycle_ts"] = _local_now_iso()
            return True

        for idx, source_cfg, target_cfg, _, _ in unload_pairs:
            device_rack_id = _rack_id_at(world, IH500_STATION_ID, str(target_cfg.slot_id))
            if not device_rack_id:
                print(
                    f"{task_prefix} unload failed: expected device rack at "
                    f"{IH500_STATION_ID}.{target_cfg.slot_id}"
                )
                return False
            rack = world.racks.get(str(device_rack_id))
            if rack is None:
                print(f"{task_prefix} unload failed: unknown rack '{device_rack_id}'")
                return False

            if not _run_single_task_action(
                itm_id=int(target_cfg.itm_id),
                jig_id=int(target_cfg.jig_id),
                obj_nbr=int(target_cfg.rack_index),
                action=ACTION_PULL_RACK_OUT,
                obj_type=int(rack.pin_obj_type),
                task_name=f"{task_prefix}.UnloadRack{int(idx)}.PullRackOut",
            ):
                return False

            if not _run_single_task_action(
                itm_id=int(source_cfg.itm_id),
                jig_id=int(source_cfg.jig_id),
                obj_nbr=int(source_cfg.rack_index),
                action=ACTION_PLACE,
                obj_type=int(rack.pin_obj_type),
                task_name=f"{task_prefix}.UnloadRack{int(idx)}.PlaceOnPlate",
            ):
                return False

            try:
                moved_rack_id = world.move_rack(
                    source_station_id=IH500_STATION_ID,
                    source_station_slot_id=str(target_cfg.slot_id),
                    target_station_id=PLATE_STATION_ID,
                    target_station_slot_id=str(source_cfg.slot_id),
                )
            except Exception as exc:
                print(f"{task_prefix} unload world move failed (rack_index={int(idx)}): {exc}")
                return False

            append_world_event(
                occupancy_records,
                world,
                event_type="RACK_MOVED",
                entity_type="RACK",
                entity_id=str(moved_rack_id),
                source={"station_id": IH500_STATION_ID, "station_slot_id": str(target_cfg.slot_id)},
                target={"station_id": PLATE_STATION_ID, "station_slot_id": str(source_cfg.slot_id)},
                details={
                    "phase": "StateDrivenPlanning",
                    "process": ProcessType.IMMUNOHEMATOLOGY_ANALYSIS.value,
                    "mode": "UNLOAD",
                    "transfer_index": int(idx),
                    "action": "PullRackOut",
                },
            )

        completed_sample_ids = _sample_ids_in_jig(PLATE_STATION_ID, IH500_SOURCE_JIG_ID)
        for sid in completed_sample_ids:
            try:
                world.mark_process_completed(sid, ProcessType.IMMUNOHEMATOLOGY_ANALYSIS)
            except Exception:
                continue
            _append_process_completed_event(
                sid,
                ProcessType.IMMUNOHEMATOLOGY_ANALYSIS,
                {"mode": mode, "source_jig_id": IH500_SOURCE_JIG_ID, "device_jig_id": IH500_DEVICE_JIG_ID},
            )

        pending_after_unload = sorted({int(x) for x in pending_unload_slots if int(x) > 0})
        if pending_after_unload:
            wait_reason = (
                "IH500 unload deferred: waiting for remaining Wise rack-ready sensors "
                f"(device='{immuno_device_id}', ready_slots={sorted(unload_ready_set)}, "
                f"missing_slots={pending_after_unload})"
            )
            _arm_ih500_wait_for_wise(
                bb,
                device_id=immuno_device_id,
                required_slots=pending_after_unload,
                reason=wait_reason,
            )
            print(wait_reason)
            bb["last_ih500_cycle_mode"] = f"{mode}_PARTIAL_WAIT_READY"
            bb["last_ih500_cycle_ts"] = _local_now_iso()
            return True

        bb["state_driven_waiting_external_completion"] = False
        bb["state_driven_waiting_reason"] = ""
        bb.pop("state_driven_wait_source", None)
        bb.pop("state_driven_wait_wise_required_slots", None)
        bb.pop("state_driven_wait_last_wise_state", None)
        bb["last_ih500_cycle_mode"] = str(mode)
        bb["last_ih500_cycle_ts"] = _local_now_iso()
        return True

    def _execute_state_driven_stage_action(action: DynamicPlanAction, bb: Blackboard) -> bool:
        sample_id = str(action.sample_id)
        state = world.sample_states.get(sample_id)
        if state is None or not isinstance(state.location, RackLocation):
            print(
                "StateDriven stage failed: sample has no rack location "
                f"(sample='{sample_id}')"
            )
            return False

        source_station_id = str(state.location.station_id)
        source_station_slot_id = str(state.location.station_slot_id)
        source_slot_index = int(state.location.slot_index)
        target_station_id = str(action.target_station_id)
        target_station_slot_id = str(action.target_station_slot_id)
        target_slot_index = int(action.target_slot_index)
        task_prefix = f"StateDriven.{sample_id}.{action.process.value}.Stage"

        if not _ensure_station_reference(source_station_id, task_prefix):
            return False

        source_rack = world.get_rack_at(source_station_id, source_station_slot_id)
        sample_id_at_source = source_rack.occupied_slots.get(int(source_slot_index))
        if sample_id_at_source is None:
            print(
                f"{task_prefix} failed: source slot is empty "
                f"({source_station_id}.{source_station_slot_id}[{int(source_slot_index)}])"
            )
            return False
        if str(sample_id_at_source) != sample_id:
            print(
                f"{task_prefix} failed: source sample mismatch "
                f"(expected={sample_id}, found={sample_id_at_source})"
            )
            return False

        sample = world.samples.get(sample_id)
        obj_type = int(getattr(sample, "obj_type", OBJ_TYPE_PROBE))
        source_cfg = world.get_slot_config(source_station_id, source_station_slot_id)
        target_cfg = world.get_slot_config(target_station_id, target_station_slot_id)

        if not _run_single_task_action(
            itm_id=int(source_cfg.itm_id),
            jig_id=int(source_cfg.jig_id),
            obj_nbr=int(
                world.obj_nbr_for_slot_index(
                    source_station_id,
                    source_station_slot_id,
                    source_slot_index,
                )
            ),
            action=ACTION_PICK,
            obj_type=int(obj_type),
            task_name=f"{task_prefix}.PickSample",
        ):
            return False

        if target_station_id != source_station_id:
            if not _ensure_station_reference(target_station_id, task_prefix):
                return False

        if not _run_single_task_action(
            itm_id=int(target_cfg.itm_id),
            jig_id=int(target_cfg.jig_id),
            obj_nbr=int(
                world.obj_nbr_for_slot_index(
                    target_station_id,
                    target_station_slot_id,
                    target_slot_index,
                )
            ),
            action=ACTION_PLACE,
            obj_type=int(obj_type),
            task_name=f"{task_prefix}.PlaceSample",
        ):
            return False

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
            return False
        if str(moved_sample_id) != sample_id:
            print(
                f"{task_prefix} sample identity mismatch: "
                f"expected={sample_id}, moved={moved_sample_id}"
            )
            return False

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
                "phase": "StateDrivenPlanning",
                "reason": f"stage_for_{action.process.value}",
            },
        )
        append_world_event(
            occupancy_records,
            world,
            event_type="STATE_DRIVEN_ACTION_EXECUTED",
            entity_type="SAMPLE",
            entity_id=str(moved_sample_id),
            details={
                "phase": "StateDrivenPlanning",
                "action_type": str(action.action_type),
                "process": action.process.value,
            },
        )
        if not _maybe_return_provisioned_racks_after_process(ProcessType.FRIDGE_RACK_PROVISIONING, bb):
            return False
        bb["state_driven_last_action"] = action.to_dict()
        return True

    def _execute_state_driven_process_action(action: DynamicPlanAction, bb: Blackboard) -> bool:
        sample_id = str(action.sample_id)
        process = action.process
        task_prefix = f"StateDriven.{sample_id}.{process.value}.Process"

        if process in {ProcessType.CAP, ProcessType.DECAP, ProcessType.SAMPLE_TYPE_DETECTION}:
            state = world.sample_states.get(sample_id)
            if state is None or not isinstance(state.location, RackLocation):
                print(
                    "StateDriven process failed: sample has no rack location "
                    f"(sample='{sample_id}')"
                )
                return False
            if str(state.location.station_id) != THREE_FINGER_STATION_ID:
                print(
                    "StateDriven process prerequisite failed: sample is not at 3-Finger station "
                    f"(sample='{sample_id}', station='{state.location.station_id}')"
                )
                return False

            if not _ensure_station_reference(str(state.location.station_id), task_prefix):
                return False

            action_code_by_process = {
                ProcessType.CAP: 1,
                ProcessType.DECAP: 2,
                ProcessType.SAMPLE_TYPE_DETECTION: 3,
            }
            slot_cfg = world.get_slot_config(str(state.location.station_id), str(state.location.station_slot_id))
            sample_process_jig_id = int(slot_cfg.jig_id)
            recap_storage_jig_id = int(slot_cfg.jig_id)
            if process in {ProcessType.DECAP, ProcessType.CAP}:
                recap_storage_jig_id = _resolve_recap_jig_id(sample_id, int(slot_cfg.jig_id))

            decap_target_slot_id = ""
            decap_target_slot_index = 0
            decap_target_cfg = None
            decap_cap_obj_type = 9014
            if process == ProcessType.DECAP:
                try:
                    cap_id_on_sample = world.cap_id_on_sample(sample_id)
                    if cap_id_on_sample is None:
                        world.ensure_cap_for_sample(sample_id)
                        cap_id_on_sample = world.cap_id_on_sample(sample_id)
                    if cap_id_on_sample is None:
                        raise ValueError("No cap tracked on sample before decap")
                    decap_target_slot_id, decap_target_slot_index = world.select_next_target_slot_for_jig(
                        station_id=str(state.location.station_id),
                        jig_id=int(recap_storage_jig_id),
                    )
                    decap_target_cfg = world.get_slot_config(
                        str(state.location.station_id),
                        str(decap_target_slot_id),
                    )
                    cap_obj = world.caps.get(str(cap_id_on_sample))
                    decap_cap_obj_type = int(getattr(cap_obj, "obj_type", 9014))
                except Exception as exc:
                    print(
                        "StateDriven decap failed while planning cap placement in recap jig "
                        f"(sample='{sample_id}', station='{state.location.station_id}', "
                        f"jig_id={int(recap_storage_jig_id)}): {exc}"
                    )
                    return False

            if process == ProcessType.CAP:
                try:
                    cap_id_to_pick, cap_source_slot_id, cap_source_slot_index, _ = world.select_cap_for_sample_from_jig(
                        sample_id,
                        station_id=str(state.location.station_id),
                        jig_id=int(recap_storage_jig_id),
                    )
                except Exception as exc:
                    print(
                        "StateDriven recap failed while selecting cap from recap jig "
                        f"(sample='{sample_id}', station='{state.location.station_id}', "
                        f"jig_id={int(recap_storage_jig_id)}): {exc}"
                    )
                    return False

                # Physical precondition for capping: pick the selected cap from the recap rack first.
                if str(cap_source_slot_id).strip():
                    cap_source_cfg = world.get_slot_config(str(state.location.station_id), str(cap_source_slot_id))
                    cap_obj_nbr = int(
                        world.obj_nbr_for_slot_index(
                            str(state.location.station_id),
                            str(cap_source_slot_id),
                            int(cap_source_slot_index),
                        )
                    )
                    cap_obj = world.caps.get(str(cap_id_to_pick))
                    cap_obj_type = int(getattr(cap_obj, "obj_type", 9014))
                    if not _run_single_task_action(
                        itm_id=int(cap_source_cfg.itm_id),
                        jig_id=int(cap_source_cfg.jig_id),
                        obj_nbr=int(cap_obj_nbr),
                        action=ACTION_PICK,
                        obj_type=int(cap_obj_type),
                        task_name=f"{task_prefix}.PickCap",
                    ):
                        return False

            overrides = {
                "ITM_ID": int(slot_cfg.itm_id),
                "JIG_ID": int(sample_process_jig_id),
                "ACT": int(action_code_by_process[process]),
            }
            ok, _ = _run_task("ProcessAt3FingerStation", overrides, task_prefix)
            if not ok:
                return False
            if process == ProcessType.DECAP:
                if decap_target_cfg is None:
                    print(
                        "StateDriven decap failed: missing recap target configuration "
                        f"(sample='{sample_id}', station='{state.location.station_id}', "
                        f"jig_id={int(recap_storage_jig_id)})"
                    )
                    return False

                decap_target_obj_nbr = int(
                    world.obj_nbr_for_slot_index(
                        str(state.location.station_id),
                        str(decap_target_slot_id),
                        int(decap_target_slot_index),
                    )
                )
                if not _run_single_task_action(
                    itm_id=int(decap_target_cfg.itm_id),
                    jig_id=int(decap_target_cfg.jig_id),
                    obj_nbr=int(decap_target_obj_nbr),
                    action=ACTION_PLACE,
                    obj_type=int(decap_cap_obj_type),
                    task_name=f"{task_prefix}.PlaceCap",
                ):
                    return False

                try:
                    cap_id, target_slot_id, target_slot_index = world.store_cap_from_sample_in_jig(
                        sample_id,
                        station_id=str(state.location.station_id),
                        jig_id=int(recap_storage_jig_id),
                        target_slot_id=str(decap_target_slot_id),
                        target_slot_index=int(decap_target_slot_index),
                    )
                    target_rack_id = world.get_rack_at(
                        str(state.location.station_id),
                        str(target_slot_id),
                    ).id
                except Exception as exc:
                    print(
                        "StateDriven decap failed while storing cap in recap jig "
                        f"(sample='{sample_id}', station='{state.location.station_id}', "
                        f"jig_id={int(recap_storage_jig_id)}): {exc}"
                    )
                    return False
                _set_sample_cap_state(sample_id, CapState.DECAPPED)
                append_world_event(
                    occupancy_records,
                    world,
                    event_type="CAP_MOVED",
                    entity_type="CAP",
                    entity_id=str(cap_id),
                    source={
                        "location_type": "ON_SAMPLE",
                        "sample_id": str(sample_id),
                    },
                    target={
                        "location_type": "RACK",
                        "station_id": str(state.location.station_id),
                        "station_slot_id": str(target_slot_id),
                        "rack_id": str(target_rack_id),
                        "slot_index": int(target_slot_index),
                    },
                    details={
                        "phase": "StateDrivenPlanning",
                        "process": ProcessType.DECAP.value,
                        "process_jig_id": int(sample_process_jig_id),
                        "recap_jig_id": int(recap_storage_jig_id),
                    },
                )
            elif process == ProcessType.CAP:
                try:
                    cap_id, source_slot_id, source_slot_index, assigned_match = world.attach_cap_to_sample_from_jig(
                        sample_id,
                        station_id=str(state.location.station_id),
                        jig_id=int(recap_storage_jig_id),
                    )
                except Exception as exc:
                    print(
                        "StateDriven recap failed while attaching cap from recap jig "
                        f"(sample='{sample_id}', station='{state.location.station_id}', "
                        f"jig_id={int(recap_storage_jig_id)}): {exc}"
                    )
                    return False
                _set_sample_cap_state(sample_id, CapState.CAPPED)
                if source_slot_id:
                    source_rack_id = world.get_rack_at(
                        str(state.location.station_id),
                        str(source_slot_id),
                    ).id
                    append_world_event(
                        occupancy_records,
                        world,
                        event_type="CAP_MOVED",
                        entity_type="CAP",
                        entity_id=str(cap_id),
                        source={
                            "location_type": "RACK",
                            "station_id": str(state.location.station_id),
                            "station_slot_id": str(source_slot_id),
                            "rack_id": str(source_rack_id),
                            "slot_index": int(source_slot_index),
                        },
                        target={
                            "location_type": "ON_SAMPLE",
                            "sample_id": str(sample_id),
                        },
                        details={
                            "phase": "StateDrivenPlanning",
                            "process": ProcessType.CAP.value,
                            "process_jig_id": int(sample_process_jig_id),
                            "recap_jig_id": int(recap_storage_jig_id),
                            "assigned_match": bool(assigned_match),
                        },
                    )
            world.mark_process_completed(sample_id, process)
            _append_process_completed_event(sample_id, process)
            bb["state_driven_last_action"] = action.to_dict()
            return True

        if process == ProcessType.CENTRIFUGATION:
            hold_payload = _get_sample_process_holds(bb).get(sample_id, {})
            hold_status = str(hold_payload.get("status", "")).strip().upper()
            if hold_status == SAMPLE_HOLD_STATUS_RUNNING:
                print(
                    "StateDriven centrifugation deferred: sample is still in active centrifuge cycle "
                    f"(sample='{sample_id}', hold_status={hold_status})"
                )
                bb["state_driven_last_action"] = action.to_dict()
                return True

            ok = _execute_centrifuge_cycle(bb)
            if not ok:
                return False
            resolved_mode = str(bb.get("centrifuge_mode_resolved", "")).strip().upper()
            wait_device_id = str(
                action.selected_device_id or bb.get("active_runtime_centrifuge_id", "")
            ).strip()
            if resolved_mode == "UNLOAD":
                completed_sample_ids: Set[str] = set(_active_centrifuge_job_sample_ids(bb, wait_device_id))
                completed_sample_ids.update(_sample_ids_in_jig(PLATE_STATION_ID, 2))
                for sid in sorted(completed_sample_ids):
                    try:
                        if ProcessType.CENTRIFUGATION not in world.pending_processes(sid):
                            continue
                        world.mark_process_completed(sid, ProcessType.CENTRIFUGATION)
                    except Exception:
                        continue
                    _append_process_completed_event(
                        sid,
                        ProcessType.CENTRIFUGATION,
                        {"mode": "UNLOAD"},
                    )
                _clear_centrifuge_job_and_holds(
                    bb,
                    device_id=wait_device_id,
                    sample_ids=sorted(completed_sample_ids),
                )
            elif resolved_mode == "LOAD":
                running_sample_ids = _pending_sample_ids_for_process_at_jig(
                    CENTRIFUGE_STATION_ID,
                    2,
                    ProcessType.CENTRIFUGATION,
                )
                if sample_id and sample_id not in running_sample_ids:
                    try:
                        if ProcessType.CENTRIFUGATION in world.pending_processes(sample_id):
                            running_sample_ids.append(sample_id)
                    except Exception:
                        pass
                if wait_device_id and running_sample_ids:
                    _register_centrifuge_running_job(
                        bb,
                        device_id=wait_device_id,
                        sample_ids=running_sample_ids,
                        reason=(
                            "Centrifuge started in LOAD mode. Samples are held until "
                            "runtime reports cycle completion."
                        ),
                    )
            bb["state_driven_waiting_external_completion"] = False
            bb["state_driven_waiting_reason"] = ""
            bb.pop("state_driven_wait_process", None)
            bb.pop("state_driven_wait_device_id", None)
            bb.pop("state_driven_wait_ready_states", None)
            bb.pop("state_driven_wait_last_packml_state", None)
            bb.pop("state_driven_wait_source", None)
            bb.pop("state_driven_wait_wise_required_slots", None)
            bb.pop("state_driven_wait_last_wise_state", None)
            bb["state_driven_last_action"] = action.to_dict()
            return True

        if process == ProcessType.ARCHIVATION:
            world.mark_process_completed(sample_id, ProcessType.ARCHIVATION)
            _append_process_completed_event(sample_id, ProcessType.ARCHIVATION)
            if not _maybe_return_provisioned_racks_after_process(ProcessType.ARCHIVATION, bb):
                return False
            bb["state_driven_last_action"] = action.to_dict()
            return True

        if process == ProcessType.FRIDGE_RACK_PROVISIONING:
            world.mark_process_completed(sample_id, ProcessType.FRIDGE_RACK_PROVISIONING)
            _append_process_completed_event(
                sample_id,
                ProcessType.FRIDGE_RACK_PROVISIONING,
                {"mode": "RACK_PROVISION_ONLY"},
            )
            if not _maybe_return_provisioned_racks_after_process(ProcessType.FRIDGE_RACK_PROVISIONING, bb):
                return False
            bb["state_driven_last_action"] = action.to_dict()
            return True

        if process == ProcessType.IMMUNOHEMATOLOGY_ANALYSIS:
            ok = _execute_ih500_immuno_cycle(
                sample_id,
                bb,
                selected_device_id=str(action.selected_device_id or ""),
            )
            if not ok:
                return False
            bb["state_driven_last_action"] = action.to_dict()
            return True

        if process in {
            ProcessType.HEMATOLOGY_ANALYSIS,
            ProcessType.CLINICAL_CHEMISTRY_ANALYSIS,
            ProcessType.COAGULATION_ANALYSIS,
        }:
            print(
                "StateDriven process prerequisite failed: no execution handler configured for "
                f"process '{process.value}'. Add task mapping and executor implementation first."
            )
            return False

        print(f"StateDriven process failed: unsupported process '{process.value}'")
        return False

    def _execute_state_driven_action(action: DynamicPlanAction, bb: Blackboard) -> bool:
        action_type = str(action.action_type).strip().upper()
        if action_type == "STAGE_SAMPLE":
            return _execute_state_driven_stage_action(action, bb)
        if action_type == "PROCESS_SAMPLE":
            return _execute_state_driven_process_action(action, bb)
        if action_type == "PROVISION_RACK":
            return _execute_state_driven_provision_rack_action(action, bb)
        print(f"StateDriven action failed: unsupported action_type '{action.action_type}'")
        return False

    def _run_state_driven_planning_loop(bb: Blackboard) -> bool:
        if dynamic_state_planner is None:
            print(
                "StateDriven planning failed: dynamic planner is unavailable "
                f"(policies file: {PROCESS_POLICIES_FILE})"
            )
            return False

        max_actions = max(1, int(STATE_DRIVEN_MAX_ACTIONS))
        executed_actions: List[Dict[str, Any]] = []
        bb["state_driven_waiting_external_completion"] = False
        bb["state_driven_waiting_reason"] = ""
        loop_index = 0
        executed_action_count = 0

        while executed_action_count < max_actions:
            loop_index += 1
            if not _refresh_world_device_states_from_ulm(bb, loop_index):
                return False
            if not _sync_active_centrifuge_jobs(bb, loop_index):
                return False
            if bool(bb.get("state_driven_waiting_external_completion", False)):
                if not _is_external_wait_satisfied(bb, loop_index):
                    if STATE_DRIVEN_WAIT_POLL_S > 0:
                        time.sleep(float(STATE_DRIVEN_WAIT_POLL_S))
                    continue
            excluded_sample_ids = _running_hold_sample_ids(bb)
            bb["state_driven_excluded_sample_ids"] = sorted(excluded_sample_ids)
            try:
                dynamic_result = dynamic_state_planner.plan_next(
                    world,
                    excluded_sample_ids=excluded_sample_ids,
                )
            except Exception as exc:
                print(f"StateDriven planning failed: planner error ({exc})")
                return False

            dynamic_payload = dynamic_result.to_dict()
            status = str(dynamic_result.status).strip().upper()
            bb["state_driven_plan_next"] = dynamic_payload
            bb["state_driven_plan_status"] = status
            bb["state_driven_loop_iterations"] = int(loop_index)

            event_type = "STATE_DRIVEN_PLAN_IDLE"
            if status == "READY":
                event_type = "STATE_DRIVEN_PLAN_READY"
            elif status == "BLOCKED":
                event_type = "STATE_DRIVEN_PLAN_BLOCKED"

            append_world_event(
                occupancy_records,
                world,
                event_type=event_type,
                entity_type="WORKFLOW",
                entity_id="STATE_DRIVEN_PLANNING",
                details={
                    "phase": "StateDrivenPlanning",
                    "loop_index": int(loop_index),
                    "dynamic_plan": dynamic_payload,
                },
            )

            if status == "IDLE":
                return_candidate = _next_idle_rack_return_candidate(bb)
                if return_candidate is not None:
                    if not _execute_return_rack_home(return_candidate, bb):
                        return False
                    executed_actions.append(_return_rack_action_payload(return_candidate))
                    executed_action_count += 1
                    bb["state_driven_actions_executed"] = list(executed_actions)
                    continue
                if _has_running_centrifuge_jobs(bb):
                    if STATE_DRIVEN_WAIT_POLL_S > 0:
                        time.sleep(float(STATE_DRIVEN_WAIT_POLL_S))
                    continue
                bb["state_driven_actions_executed"] = list(executed_actions)
                return True

            if status == "BLOCKED":
                if _has_running_centrifuge_jobs(bb):
                    if STATE_DRIVEN_WAIT_POLL_S > 0:
                        time.sleep(float(STATE_DRIVEN_WAIT_POLL_S))
                    continue
                blocked_reason = ""
                if dynamic_result.blocked:
                    blocked_reason = str(dynamic_result.blocked[0].get("reason", ""))
                print(
                    "StateDriven planning blocked: no actionable step found. "
                    f"First blocked reason: {blocked_reason or 'unknown'}"
                )
                return False

            action = dynamic_result.action
            if action is None:
                print("StateDriven planning failed: READY status returned without action payload")
                return False

            bb["state_driven_plan_action"] = action.to_dict()
            if not _execute_state_driven_action(action, bb):
                return False
            executed_actions.append(action.to_dict())
            executed_action_count += 1

            if bool(bb.get("state_driven_waiting_external_completion", False)):
                bb["state_driven_actions_executed"] = list(executed_actions)
                if STATE_DRIVEN_WAIT_POLL_S > 0:
                    time.sleep(float(STATE_DRIVEN_WAIT_POLL_S))
                continue

        print(
            "StateDriven planning failed: maximum action limit reached "
            f"({max_actions}). Check for planning loop conditions."
        )
        return False

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
            if not _run_single_task_action(
                itm_id=int(source_cfg.itm_id),
                jig_id=int(source_cfg.jig_id),
                obj_nbr=int(source_cfg.rack_index),
                action=ACTION_PICK,
                obj_type=int(obj_type),
                task_name="GettingNewSamples.TransferInputRack.Pick",
            ):
                return False

            if not _run_single_task_action(
                itm_id=int(target_cfg.itm_id),
                jig_id=int(target_cfg.jig_id),
                obj_nbr=int(target_cfg.rack_index),
                action=ACTION_PLACE,
                obj_type=int(obj_type),
                task_name="GettingNewSamples.TransferInputRack.Place",
            ):
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
                "ACT": 3,
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
                classification_key = _classification_key_from_barcode(barcode)
                decision = router.route(
                    SampleRoutingRequest(
                        sample_id=sample_id,
                        barcode=classification_key,
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
                        required_processes=decision_processes,
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
                            "classification_key": str(classification_key or ""),
                            "full_barcode": str(barcode or ""),
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
                normalized_classification_key = (
                    str(classification_key).strip() if classification_key is not None else ""
                )
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

                paired_kreuzprobe_sample_id = ""
                if ProcessType.IMMUNOHEMATOLOGY_ANALYSIS in set(decision_processes):
                    pair_ok, paired_kreuzprobe_sample_id, pair_reason = _pair_immuno_sample_with_kreuzprobe(
                        primary_sample_id=str(resolved_sample_id),
                        immuno_barcode=normalized_barcode,
                        decision_processes=decision_processes,
                        decision_source=str(decision.source),
                        decision_classification=str(decision.classification),
                        target_slot_id=str(target_slot_id),
                    )
                    if not pair_ok:
                        print(
                            "GettingNewSamples pairing failed for immuno sample "
                            f"'{resolved_sample_id}': {pair_reason}"
                        )
                        return False
                    append_world_event(
                        occupancy_records,
                        world,
                        event_type="SAMPLES_PAIRED",
                        entity_type="SAMPLE",
                        entity_id=str(resolved_sample_id),
                        source={"sample_id": str(resolved_sample_id)},
                        target={"sample_id": str(paired_kreuzprobe_sample_id or "")},
                        details={
                            "phase": "GettingNewSamples",
                            "step_id": step_id,
                            "classification": str(decision.classification),
                            "provider": str(decision.source),
                            "barcode": normalized_barcode,
                            "classification_key": normalized_classification_key,
                            "paired_kreuzprobe_sample_id": str(paired_kreuzprobe_sample_id or ""),
                            "mapping_file": str(IMMUNO_KREUZPROBE_MAP_FILE),
                            "pairing_status": str(pair_reason),
                        },
                    )

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
                        "barcode": normalized_barcode,
                        "classification_key": normalized_classification_key,
                        "paired_kreuzprobe_sample_id": str(paired_kreuzprobe_sample_id or ""),
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
            return _run_state_driven_planning_loop(bb)

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
        if not _run_single_task_action(
            itm_id=int(op.source_itm_id),
            jig_id=int(op.source_jig_id),
            obj_nbr=int(op.source_obj_nbr),
            action=ACTION_PICK,
            obj_type=int(op.obj_type),
            task_name=f"CentrifugeCycle.{op.name}.PickSample",
        ):
            return False

        if not _run_single_task_action(
            itm_id=int(op.target_itm_id),
            jig_id=int(op.target_jig_id),
            obj_nbr=int(op.target_obj_nbr),
            action=ACTION_PLACE,
            obj_type=int(op.obj_type),
            task_name=f"CentrifugeCycle.{op.name}.PlaceSample",
        ):
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
        if not _run_single_task_action(
            itm_id=int(op.source_itm_id),
            jig_id=int(op.source_jig_id),
            obj_nbr=int(op.source_obj_nbr),
            action=ACTION_PICK,
            obj_type=int(op.obj_type),
            task_name=f"CentrifugeCycle.{op.name}.Pick",
        ):
            return False

        if not _run_single_task_action(
            itm_id=int(op.target_itm_id),
            jig_id=int(op.target_jig_id),
            obj_nbr=int(op.target_obj_nbr),
            action=ACTION_PLACE,
            obj_type=int(op.obj_type),
            task_name=f"CentrifugeCycle.{op.name}.Place",
        ):
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
            plan = compile_centrifuge_usage_plan(world=world, device=runtime_device, mode=mode)
        except Exception as exc:
            print(f"CentrifugeCycle failed to compile usage plan: {exc}")
            return False
        bb["centrifuge_mode_resolved"] = str(plan.mode)
        plan_mode = str(plan.mode).strip().upper()
        if plan_mode == "LOAD":
            _post_workflow_context_message(bb, "Loading the Centrifuge")
        elif plan_mode == "UNLOAD":
            _post_workflow_context_message(bb, "Unloading the Centrifuge")
        else:
            _post_workflow_context_message(bb, "Running the Centrifuge")
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
                action_overrides = dict(op.overrides or {})
                act = int(action_overrides.get("ACT", 0))
                rotor_slot_index = int(
                    getattr(op, "rotor_slot_index", 0)
                    or action_overrides.get("OBJ_Nbr", 0)
                    or 0
                )
                try:
                    if not runtime_device.apply_single_device_action(
                        act,
                        rotor_slot_index=rotor_slot_index,
                    ):
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
            active_context["bb"] = bb
            if workflow_mode == "GETTING_NEW_SAMPLES":
                step = plan_step_by_id.get(step_name)
                if step is None:
                    print(f"GettingNewSamples failed: planner step '{step_name}' not found")
                    return False
                bb["last_plan_step"] = str(step.step_id)
                _post_workflow_context_message(bb, _context_message_for_plan_step(step))
                if str(step.step_type).strip().upper() == "TASK":
                    return _execute_getting_new_samples_task(step, bb)
                if str(step.step_type).strip().upper() == "PHASE":
                    return _execute_getting_new_samples_phase(step.step_id, bb)
                print(f"GettingNewSamples failed: unsupported step_type '{step.step_type}'")
                return False

            bb["last_blank_step"] = step_name
            if step_name == "CentrifugeCycle" and workflow_mode in centrifuge_modes:
                _post_workflow_context_message(bb, "Preparing Centrifuge Cycle")
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
    PAUSE_ACK_FILE.parent.mkdir(parents=True, exist_ok=True)
    _set_pause_ack(False)

    sender = build_sender()
    trace_fieldnames = _trace_fieldnames_from_catalog(sender)
    world, world_resumed = load_world_with_resume(WORLD_CONFIG_FILE, OCCUPANCY_EVENTS_FILE)
    reset_trace_session = not world_resumed
    if reset_trace_session:
        print("Trace session: reset (world loaded from baseline)")
    else:
        print("Trace session: append (world resumed from previous snapshot)")
    _init_live_trace_files(trace_fieldnames, reset=reset_trace_session)
    _init_live_world_files(reset=reset_trace_session)
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
        _set_pause_ack(False)
        # Persist physical world-state artifacts even when the workflow fails.
        export_occupancy_trace(
            occupancy_records,
            OCCUPANCY_TRACE_FILE,
            trace_records=trace_records,
            append=world_resumed,
        )
        export_occupancy_events_jsonl(
            occupancy_records,
            OCCUPANCY_EVENTS_FILE,
            trace_records=trace_records,
            append=world_resumed,
        )
        _finalize_world_snapshot_file(world)

        if final_status == Status.SUCCESS:
            export_trace(trace_records, TRACE_FILE, append=world_resumed)
            export_state_changes(state_change_records, STATE_CHANGES_FILE, append=world_resumed)
            print(f"Trace written to {TRACE_FILE.resolve()}")
            print(f"State transitions written to {STATE_CHANGES_FILE.resolve()}")
            print(f"Occupancy trace written to {OCCUPANCY_TRACE_FILE.resolve()}")
            print(f"Occupancy events written to {OCCUPANCY_EVENTS_FILE.resolve()}")
            print(f"World snapshot written to {WORLD_SNAPSHOT_FILE.resolve()}")
        else:
            print(
                "Workflow did not complete successfully; "
                "execution trace/state transition canonical files were not updated."
            )
            print(f"WIP trace: {TRACE_WIP_FILE.resolve()}")
            print(f"WIP state transitions: {STATE_CHANGES_WIP_FILE.resolve()}")
            print(f"Occupancy trace written to {OCCUPANCY_TRACE_FILE.resolve()}")
            print(f"Occupancy events written to {OCCUPANCY_EVENTS_FILE.resolve()}")
            print(f"World snapshot written to {WORLD_SNAPSHOT_FILE.resolve()}")
            print(f"WIP occupancy trace mirror: {OCCUPANCY_TRACE_WIP_FILE.resolve()}")
            print(f"WIP occupancy events mirror: {OCCUPANCY_EVENTS_WIP_FILE.resolve()}")
            print(f"WIP world snapshot mirror: {WORLD_SNAPSHOT_WIP_FILE.resolve()}")


if __name__ == "__main__":
    main()
