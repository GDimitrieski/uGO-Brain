from __future__ import annotations

import argparse
import contextlib
import io
import json
import random
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from engine.command_layer import TaskCatalog
from engine.sender import build_sender


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_WORLD_CONFIG = PROJECT_ROOT / "world" / "world_config.json"
DEFAULT_OCCUPANCY_TRACE = PROJECT_ROOT / "world" / "world_occupancy_trace.jsonl"
DEFAULT_TASK_CATALOG = PROJECT_ROOT / "Available_Tasks.json"
ULM_PLATE_STATION_ID = "uLMPlateStation"
LANDMARK_SCAN_ACTION = 30


@dataclass(frozen=True)
class WorldPools:
    station_ids: Sequence[str]
    station_itm_ids: Dict[str, int]
    amr_pos_targets: Sequence[str]
    slot_tuples: Sequence[Tuple[str, str, int, int, int]]
    jig_ids: Sequence[int]
    obj_nbr_values: Sequence[int]
    obj_types: Sequence[int]
    landmark_station_ids: Sequence[str]
    landmark_itm_ids: Sequence[int]
    plate_itm_id: int


def _load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _latest_jsonl_entry(path: Path) -> Dict[str, Any]:
    last_line = ""
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                last_line = line
    if not last_line:
        raise ValueError(f"Trace file is empty: {path}")
    return json.loads(last_line)


def _collect_world_pools(world_cfg: Dict[str, Any]) -> WorldPools:
    stations_raw = world_cfg.get("stations", [])
    if not isinstance(stations_raw, list) or not stations_raw:
        raise ValueError("world_config has no stations")

    station_ids: List[str] = []
    station_itm_ids: Dict[str, int] = {}
    amr_pos_targets: List[str] = []
    slot_tuples: List[Tuple[str, str, int, int, int]] = []
    jig_ids: List[int] = []
    obj_nbr_values: List[int] = []

    for st in stations_raw:
        station_id = str(st.get("id"))
        if not station_id:
            continue
        itm_id = int(st.get("itm_id", 1))
        station_ids.append(station_id)
        station_itm_ids[station_id] = itm_id

        amr_pos_target = st.get("amr_pos_target")
        if amr_pos_target not in (None, ""):
            amr_pos_targets.append(str(amr_pos_target))

        slot_configs = st.get("slot_configs", [])
        if not isinstance(slot_configs, list):
            continue
        for slot in slot_configs:
            slot_id = str(slot.get("slot_id", ""))
            if not slot_id:
                continue
            jig_id = int(slot.get("jig_id", 0))
            slot_itm_id = int(slot.get("itm_id", itm_id))
            capacity = int(slot.get("rack_capacity", 1))
            offset = int(slot.get("obj_nbr_offset", 0))
            slot_tuples.append((station_id, slot_id, slot_itm_id, jig_id, capacity))
            jig_ids.append(jig_id)
            for idx in range(1, max(1, capacity) + 1):
                obj_nbr_values.append(offset + idx)

    if not station_itm_ids:
        raise ValueError("world_config has no valid station ITM_ID values")

    landmarks_raw = world_cfg.get("landmarks", [])
    landmark_station_ids = []
    landmark_itm_ids = []
    if isinstance(landmarks_raw, list):
        for lm in landmarks_raw:
            station_id = str(lm.get("station_id", ""))
            if station_id:
                landmark_station_ids.append(station_id)
                if station_id in station_itm_ids:
                    landmark_itm_ids.append(station_itm_ids[station_id])

    obj_types_set = set()
    for sample in world_cfg.get("samples", []) if isinstance(world_cfg.get("samples"), list) else []:
        try:
            obj_types_set.add(int(sample.get("obj_type")))
        except Exception:
            pass
    for rack in world_cfg.get("racks", []) if isinstance(world_cfg.get("racks"), list) else []:
        try:
            obj_types_set.add(int(rack.get("pin_obj_type")))
        except Exception:
            pass

    obj_types = sorted(obj_types_set) if obj_types_set else [101]
    amr_targets_unique = sorted(set(amr_pos_targets)) if amr_pos_targets else ["1"]
    jig_ids_unique = sorted(set(jig_ids)) if jig_ids else [1]
    obj_nbr_unique = sorted(set(obj_nbr_values)) if obj_nbr_values else [1]
    landmark_itm_unique = sorted(set(landmark_itm_ids))

    plate_itm_id = station_itm_ids.get(ULM_PLATE_STATION_ID)
    if plate_itm_id is None:
        raise ValueError(f"world_config missing required station '{ULM_PLATE_STATION_ID}'")

    return WorldPools(
        station_ids=tuple(sorted(set(station_ids))),
        station_itm_ids=station_itm_ids,
        amr_pos_targets=tuple(amr_targets_unique),
        slot_tuples=tuple(slot_tuples),
        jig_ids=tuple(jig_ids_unique),
        obj_nbr_values=tuple(obj_nbr_unique),
        obj_types=tuple(obj_types),
        landmark_station_ids=tuple(sorted(set(landmark_station_ids))),
        landmark_itm_ids=tuple(landmark_itm_unique),
        plate_itm_id=int(plate_itm_id),
    )


def _random_choice(rng: random.Random, values: Sequence[Any], fallback: Any) -> Any:
    if values:
        return values[rng.randrange(0, len(values))]
    return fallback


def _pick_slot_for_station(
    rng: random.Random,
    pools: WorldPools,
    station_id: str,
) -> Optional[Tuple[str, str, int, int, int]]:
    candidates = [slot for slot in pools.slot_tuples if slot[0] == station_id]
    if not candidates:
        return None
    return candidates[rng.randrange(0, len(candidates))]


def _build_random_overrides(
    task_key: str,
    task_def: Dict[str, Any],
    pools: WorldPools,
    rng: random.Random,
) -> Dict[str, Any]:
    params_def = task_def.get("parameters", {})
    if not isinstance(params_def, dict):
        params_def = {}

    overrides: Dict[str, Any] = {}
    enum_cache: Dict[str, List[Any]] = {}
    for p, meta in params_def.items():
        if isinstance(meta, dict):
            enum_vals = meta.get("enum")
            if isinstance(enum_vals, list):
                enum_cache[p] = list(enum_vals)

    if task_key == "Navigate":
        target = str(_random_choice(rng, pools.amr_pos_targets, "1"))
        overrides["AMR_PosTarget"] = target
        overrides["AMR_Footprint"] = str(_random_choice(rng, pools.amr_pos_targets, "1"))
        overrides["AMR_DOCK"] = str(_random_choice(rng, pools.amr_pos_targets, "1"))
        return overrides

    if task_key == "Charge":
        return overrides

    if task_key == "InspectRackAtStation":
        station_id = str(_random_choice(rng, pools.station_ids, ULM_PLATE_STATION_ID))
        slot = _pick_slot_for_station(rng, pools, station_id)
        jig_id = int(slot[3]) if slot else int(_random_choice(rng, pools.jig_ids, 1))
        overrides["STATION"] = station_id
        overrides["JIG_ID"] = jig_id
        camera_default = "WRIST"
        camera_enum = enum_cache.get("CAMERA", [])
        if camera_enum:
            overrides["CAMERA"] = str(_random_choice(rng, [str(v) for v in camera_enum], camera_default))
        else:
            overrides["CAMERA"] = camera_default
        return overrides

    if task_key == "ProcessAt3FingerStation":
        slot = _pick_slot_for_station(rng, pools, "3-FingerGripperStation")
        if slot:
            overrides["ITM_ID"] = int(slot[2])
            overrides["JIG_ID"] = int(slot[3])
        else:
            overrides["ITM_ID"] = int(_random_choice(rng, list(pools.station_itm_ids.values()), pools.plate_itm_id))
            overrides["JIG_ID"] = int(_random_choice(rng, pools.jig_ids, 1))
        action_choices = [int(v) for v in enum_cache.get("ACTION", [1, 2, 3])]
        overrides["ACTION"] = int(_random_choice(rng, action_choices, 3))
        return overrides

    if task_key == "SingleDeviceAction":
        action_choices = [int(v) for v in enum_cache.get("ACT", [1, 2, 3, 4, 30])]
        act = int(_random_choice(rng, action_choices, 30))
        overrides["ACT"] = act
        if act == LANDMARK_SCAN_ACTION and pools.landmark_itm_ids:
            overrides["ITM_ID"] = int(_random_choice(rng, pools.landmark_itm_ids, pools.plate_itm_id))
        else:
            centrifuge_itm = pools.station_itm_ids.get("CentrifugeStation")
            if act in {1, 2, 3, 4} and centrifuge_itm is not None:
                overrides["ITM_ID"] = int(centrifuge_itm)
            else:
                overrides["ITM_ID"] = int(
                    _random_choice(rng, list(pools.station_itm_ids.values()), pools.plate_itm_id)
                )
        return overrides

    if task_key == "SingleTask":
        slot = _random_choice(rng, pools.slot_tuples, None)
        if slot:
            station_id, _, itm_id, jig_id, capacity = slot
            overrides["ITM_ID"] = int(itm_id)
            overrides["JIG_ID"] = int(jig_id)
            overrides["OBJ_Nbr"] = int(rng.randint(1, max(1, int(capacity))))
            # If station is not uLM plate and we have known obj offsets in world config,
            # use one of those world-derived OBJ_Nbr values for wider test coverage.
            if station_id != ULM_PLATE_STATION_ID and pools.obj_nbr_values:
                overrides["OBJ_Nbr"] = int(_random_choice(rng, pools.obj_nbr_values, overrides["OBJ_Nbr"]))
        else:
            overrides["ITM_ID"] = int(_random_choice(rng, list(pools.station_itm_ids.values()), pools.plate_itm_id))
            overrides["JIG_ID"] = int(_random_choice(rng, pools.jig_ids, 1))
            overrides["OBJ_Nbr"] = int(_random_choice(rng, pools.obj_nbr_values, 1))
        action_choices = [int(v) for v in enum_cache.get("ACTION", [1])]
        overrides["ACTION"] = int(_random_choice(rng, action_choices, 1))
        overrides["OBJ_Type"] = int(_random_choice(rng, pools.obj_types, 101))
        return overrides

    for p, meta in params_def.items():
        if not isinstance(meta, dict):
            continue
        p_type = str(meta.get("type", "")).lower()
        enum_vals = meta.get("enum")
        if isinstance(enum_vals, list) and enum_vals:
            value = _random_choice(rng, enum_vals, enum_vals[0])
        elif "default" in meta:
            value = meta.get("default")
        elif p_type == "integer":
            value = 1
        elif p_type == "string":
            value = "1"
        else:
            continue
        overrides[p] = value
    return overrides


def _station_itm_from_payload(task_key: str, payload: Dict[str, Any], pools: WorldPools) -> Optional[int]:
    if "ITM_ID" in payload:
        try:
            return int(payload["ITM_ID"])
        except Exception:
            return None
    if task_key == "InspectRackAtStation":
        station = str(payload.get("STATION", ""))
        if station and station in pools.station_itm_ids:
            return int(pools.station_itm_ids[station])
    return None


def _send_and_wait(
    sender: Any,
    task_key: str,
    payload: Dict[str, Any],
    timeout_s: float,
    dry_run: bool,
) -> Dict[str, Any]:
    if dry_run:
        return {
            "task_key": task_key,
            "task_id": "DRY_RUN",
            "status": "succeeded",
            "message": "dry-run",
            "payload": payload,
            "raw": {},
            "state_history": [],
        }

    with contextlib.redirect_stdout(io.StringIO()):
        task_id = sender.robot.send_task(payload)
    if not task_id:
        return {
            "task_key": task_key,
            "task_id": None,
            "status": "failed",
            "message": "task_post_send returned empty RequestId",
            "payload": payload,
            "raw": {},
            "state_history": [],
        }

    with contextlib.redirect_stdout(io.StringIO()):
        wait_result = sender.robot.wait_task(task_id, timeout_s=timeout_s, poll_s=1.0)
    return {
        "task_key": task_key,
        "task_id": task_id,
        "payload": payload,
        **wait_result,
    }


def run_random_task_sweep(
    world_config_path: Path,
    occupancy_trace_path: Path,
    task_catalog_path: Path,
    seed: Optional[int],
    timeout_s: float,
    dry_run: bool,
    stop_on_failure: bool,
) -> int:
    for required_path in (world_config_path, occupancy_trace_path, task_catalog_path):
        if not required_path.exists():
            raise FileNotFoundError(f"Required file not found: {required_path}")

    world_cfg = _load_json(world_config_path)
    latest_trace_entry = _latest_jsonl_entry(occupancy_trace_path)
    task_catalog_raw = _load_json(task_catalog_path)

    available_tasks = task_catalog_raw.get("Available_Tasks", {})
    if not isinstance(available_tasks, dict) or not available_tasks:
        raise ValueError("Available_Tasks.json has no 'Available_Tasks' definitions")

    pools = _collect_world_pools(world_cfg)
    trace_timestamp = str(latest_trace_entry.get("timestamp", "UNKNOWN"))
    trace_event = str(latest_trace_entry.get("event_type", "UNKNOWN"))

    rng_seed = int(seed if seed is not None else time.time_ns() % 2_147_483_647)
    rng = random.Random(rng_seed)

    with contextlib.redirect_stdout(io.StringIO()):
        sender = build_sender(task_catalog_path=task_catalog_path, max_attempts=1)
    sender.post_error_on_fail = False
    sender.clear_error_immediately = False
    catalog = TaskCatalog.from_file(str(task_catalog_path))

    print("=== Random Task Sweep ===")
    print(f"seed={rng_seed}")
    print(f"world_config={world_config_path}")
    print(f"occupancy_trace={occupancy_trace_path}")
    print(f"latest_trace_timestamp={trace_timestamp}")
    print(f"latest_trace_event={trace_event}")
    print(f"task_catalog={task_catalog_path}")
    print(f"dry_run={dry_run}")
    print("")

    failures = 0
    executed = 0

    for task_key in available_tasks.keys():
        task_def = available_tasks.get(task_key, {})
        if not isinstance(task_def, dict):
            print(f"[SKIP] {task_key}: invalid task definition")
            continue

        overrides = _build_random_overrides(task_key, task_def, pools, rng)
        payload = catalog.build_payload(task_key, overrides=overrides)

        station_itm = _station_itm_from_payload(task_key, payload, pools)
        is_scan_task = task_key == "SingleDeviceAction" and int(payload.get("ACT", -1)) == LANDMARK_SCAN_ACTION

        if station_itm is not None and station_itm != pools.plate_itm_id and not is_scan_task:
            prereq_payload = catalog.build_payload(
                "SingleDeviceAction",
                overrides={"ITM_ID": int(station_itm), "ACT": LANDMARK_SCAN_ACTION},
            )
            print(f"[PREREQ] ScanLandmark before {task_key}: {json.dumps(prereq_payload)}")
            prereq_result = _send_and_wait(
                sender=sender,
                task_key="SingleDeviceAction",
                payload=prereq_payload,
                timeout_s=timeout_s,
                dry_run=dry_run,
            )
            executed += 1
            print(
                f"[RESULT] prereq status={prereq_result.get('status')} "
                f"task_id={prereq_result.get('task_id')} message={prereq_result.get('message', '')}"
            )
            if prereq_result.get("status") != "succeeded":
                failures += 1
                print(f"[STOP] prerequisite failed for {task_key}")
                if stop_on_failure:
                    break

        print(f"[SEND] {task_key}: {json.dumps(payload)}")
        result = _send_and_wait(
            sender=sender,
            task_key=task_key,
            payload=payload,
            timeout_s=timeout_s,
            dry_run=dry_run,
        )
        executed += 1
        print(
            f"[RESULT] {task_key} status={result.get('status')} "
            f"task_id={result.get('task_id')} message={result.get('message', '')}"
        )

        if result.get("status") != "succeeded":
            failures += 1
            if stop_on_failure:
                print(f"[STOP] task failed: {task_key}")
                break

    print("")
    print(f"Executed actions: {executed}")
    print(f"Failures: {failures}")
    return 0 if failures == 0 else 1


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Send all tasks from Available_Tasks.json as single actions using random payload "
            "values sampled from world/world_config.json."
        )
    )
    parser.add_argument("--world-config", type=Path, default=DEFAULT_WORLD_CONFIG)
    parser.add_argument("--occupancy-trace", type=Path, default=DEFAULT_OCCUPANCY_TRACE)
    parser.add_argument("--task-catalog", type=Path, default=DEFAULT_TASK_CATALOG)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--timeout-s", type=float, default=120.0)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--continue-on-failure",
        action="store_true",
        help="Continue sweep even when a prerequisite or task fails.",
    )
    args = parser.parse_args()

    return run_random_task_sweep(
        world_config_path=args.world_config,
        occupancy_trace_path=args.occupancy_trace,
        task_catalog_path=args.task_catalog,
        seed=args.seed,
        timeout_s=float(args.timeout_s),
        dry_run=bool(args.dry_run),
        stop_on_failure=not bool(args.continue_on_failure),
    )


if __name__ == "__main__":
    raise SystemExit(main())
