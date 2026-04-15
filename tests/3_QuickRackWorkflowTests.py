from __future__ import annotations

import argparse
import json
import sys
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LIBRARY_ROOT = PROJECT_ROOT / "Library"
for _p in (PROJECT_ROOT, LIBRARY_ROOT):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from engine.command_layer import CommandSender, TaskCatalog
from engine.sender import DEFAULT_TASK_CATALOG, build_sender
from Device.registry import build_device_registry_from_world
from world.lab_world import WorldModel, load_world_from_file

DEFAULT_WORLD_CONFIG = PROJECT_ROOT / "world" / "world_config.json"
ULM_PLATE_STATION_ID = "uLMPlateStation"
CHARGE_STATION_ID = "CHARGE"
CENTRIFUGE_STATION_ID = "CentrifugeStation"
CENTRIFUGE_SLOT_IDS: Tuple[str, ...] = (
    "CentrifugeRacksSlot1",
    "CentrifugeRacksSlot2",
    "CentrifugeRacksSlot3",
    "CentrifugeRacksSlot4",
)

ACTION_PICK = 1
ACTION_PLACE = 2
ACTION_OPEN_HATCH = 1
ACTION_CLOSE_HATCH = 3
ACTION_SCAN_LANDMARK = 30
ACTION_MOVE_ROTOR = 4


@dataclass(frozen=True)
class RackWorkflowCase:
    name: str
    direction: str  # "PICKUP" or "RETURN"
    station_id: str
    station_slot_id: str
    plate_jig_id: int


def _rack_type_values(slot_cfg: Any) -> List[str]:
    values: List[str] = []
    for item in getattr(slot_cfg, "accepted_rack_types", []) or []:
        values.append(str(getattr(item, "value", item)).strip())
    return sorted({v for v in values if v})


def _resolve_plate_slot_id(world: WorldModel, plate_jig_id: int, rack_type: str) -> str:
    station = world.get_station(ULM_PLATE_STATION_ID)
    matches: List[str] = []
    for slot_id, cfg in station.slot_configs.items():
        if int(cfg.jig_id) != int(plate_jig_id):
            continue
        accepted = _rack_type_values(cfg)
        if rack_type in accepted:
            matches.append(str(slot_id))
    if not matches:
        raise ValueError(
            f"No uLMPlate slot found for JIG_ID={int(plate_jig_id)} and rack_type='{rack_type}'"
        )
    return sorted(matches)[0]


def _single_task_overrides(
    *,
    itm_id: int,
    jig_id: int,
    obj_nbr: int,
    action: int,
    obj_type: int,
) -> Dict[str, Any]:
    # Keep both ACTION and ACT to stay robust against payload-template variants.
    return {
        "ITM_ID": int(itm_id),
        "JIG_ID": int(jig_id),
        "OBJ_Nbr": int(obj_nbr),
        "ACTION": int(action),
        "ACT": int(action),
        "OBJ_Type": int(obj_type),
    }


def _print_step(step: str, payload: Dict[str, Any]) -> None:
    print(f"- {step}: {json.dumps(payload)}")


def _run_task(
    *,
    catalog: TaskCatalog,
    sender: Optional[CommandSender],
    dry_run: bool,
    timeout_s: float,
    task_key: str,
    overrides: Dict[str, Any],
    task_name: str,
    step_label: str,
) -> Tuple[bool, Dict[str, Any], Dict[str, Any]]:
    payload = catalog.build_payload(task_key, overrides=overrides)
    _print_step(step_label, payload)
    if dry_run:
        result = {
            "status": "succeeded",
            "task_id": "DRY_RUN",
            "message": "dry-run",
            "raw": {},
            "state_history": [],
        }
        return True, result, payload
    if sender is None:
        result = {
            "status": "failed",
            "task_id": None,
            "message": "sender is not available",
            "raw": {},
            "state_history": [],
        }
        return False, result, payload

    result = sender.run(task_key, overrides=overrides, timeout_s=float(timeout_s), task_name=task_name)
    ok = str(result.get("status", "")).strip().lower() == "succeeded"
    print(
        f"  result={result.get('status')} task_id={result.get('task_id')} "
        f"message={result.get('message', '')}"
    )
    return ok, result, payload


def _run_centrifuge_controller_action(
    *,
    centrifuge_runtime: Any,
    dry_run: bool,
    act: int,
    rotor_slot_index: int,
    step_label: str,
) -> bool:
    payload = {"ControllerAction": int(act), "RotorSlotIndex": int(rotor_slot_index)}
    _print_step(step_label, payload)
    if dry_run:
        print("  result=succeeded task_id=DRY_RUN message=dry-run")
        return True
    if centrifuge_runtime is None:
        print("  result=failed message=centrifuge runtime is not available")
        return False
    try:
        ok = bool(centrifuge_runtime.apply_single_device_action(int(act), rotor_slot_index=int(rotor_slot_index)))
    except Exception as exc:
        print(f"  result=failed message=centrifuge controller action raised: {exc}")
        return False
    if ok:
        print("  result=succeeded")
        return True
    diag: Dict[str, Any] = {}
    try:
        diag = dict(centrifuge_runtime.diagnose())
    except Exception as exc:
        diag = {"diagnose_error": str(exc)}
    print(
        "  result=failed message="
        f"fault_code={diag.get('fault_code', '')} "
        f"fault_message={diag.get('fault_message', '')} "
        f"state={diag.get('state', '')}"
    )
    return False


def _confirm_rotor_position(
    *,
    centrifuge_runtime: Any,
    dry_run: bool,
    rotor_slot_index: int,
    step_label: str,
) -> bool:
    _print_step(step_label, {"RotorSlotIndex": int(rotor_slot_index)})
    if dry_run:
        print("  result=succeeded task_id=DRY_RUN message=dry-run")
        return True
    controller = getattr(centrifuge_runtime, "controller", None)
    if controller is None or not hasattr(controller, "inspect_position"):
        print("  result=succeeded message=no controller inspect_position available")
        return True
    try:
        ok = bool(controller.inspect_position(int(rotor_slot_index)))
    except Exception as exc:
        print(f"  result=failed message=rotor position inspect raised: {exc}")
        return False
    if not ok:
        print(f"  result=failed message=rotor not at slot {int(rotor_slot_index)}")
        return False
    print("  result=succeeded")
    return True


def _start_rotor_move_async(
    *,
    centrifuge_runtime: Any,
    dry_run: bool,
    rotor_slot_index: int,
    step_label: str,
) -> Tuple[threading.Thread, Dict[str, Any]]:
    result: Dict[str, Any] = {"ok": False, "error": ""}

    def _worker() -> None:
        try:
            ok = _run_centrifuge_controller_action(
                centrifuge_runtime=centrifuge_runtime,
                dry_run=dry_run,
                act=ACTION_MOVE_ROTOR,
                rotor_slot_index=int(rotor_slot_index),
                step_label=step_label,
            )
            result["ok"] = bool(ok)
        except Exception as exc:
            result["ok"] = False
            result["error"] = str(exc)

    thread = threading.Thread(
        target=_worker,
        name=f"RotorMoveSlot{int(rotor_slot_index)}",
        daemon=True,
    )
    thread.start()
    return thread, result


def _validate_catalog_prerequisites(catalog: TaskCatalog) -> List[str]:
    errors: List[str] = []
    tasks_raw = catalog.raw.get("Available_Tasks", {})
    if not isinstance(tasks_raw, dict):
        return ["Available_Tasks.json missing top-level 'Available_Tasks' dictionary"]

    for key in ("Navigate", "SingleDeviceAction", "SingleTask", "Charge"):
        if key not in tasks_raw:
            errors.append(f"Missing task in catalog: '{key}'")

    single_device = tasks_raw.get("SingleDeviceAction", {})
    act_meta = (
        single_device.get("parameters", {}).get("ACT", {})
        if isinstance(single_device, dict)
        else {}
    )
    enum_vals = act_meta.get("enum", []) if isinstance(act_meta, dict) else []
    enum_ints = [int(x) for x in enum_vals if str(x).strip()]
    if ACTION_SCAN_LANDMARK not in enum_ints:
        errors.append(
            "SingleDeviceAction ACT enum does not include landmark scan action 30"
        )

    return errors


def _validate_world_prerequisites(world: WorldModel, cases: Sequence[RackWorkflowCase]) -> List[str]:
    errors: List[str] = []
    for case in cases:
        try:
            station = world.get_station(case.station_id)
        except Exception as exc:
            errors.append(f"[{case.name}] Missing station '{case.station_id}': {exc}")
            continue

        if not station.amr_pos_target:
            errors.append(f"[{case.name}] Station '{case.station_id}' has no amr_pos_target")

        try:
            station_slot_cfg = world.get_slot_config(case.station_id, case.station_slot_id)
        except Exception as exc:
            errors.append(
                f"[{case.name}] Missing slot '{case.station_id}.{case.station_slot_id}': {exc}"
            )
            continue

        rack_types = _rack_type_values(station_slot_cfg)
        if len(rack_types) != 1:
            errors.append(
                f"[{case.name}] Expected exactly one accepted rack type at "
                f"'{case.station_id}.{case.station_slot_id}', found: {rack_types}"
            )
            continue

        rack_type = rack_types[0]
        try:
            _resolve_plate_slot_id(world, case.plate_jig_id, rack_type)
        except Exception as exc:
            errors.append(f"[{case.name}] {exc}")
            continue

    return errors


def _validate_centrifuge_cycle_prerequisites(world: WorldModel) -> List[str]:
    errors: List[str] = []
    try:
        world.get_station(CENTRIFUGE_STATION_ID)
        world.get_station(ULM_PLATE_STATION_ID)
    except Exception as exc:
        return [f"[LoadUnloadCentrifuge] Missing station prerequisite: {exc}"]

    has_source_rack = False
    for slot_id in CENTRIFUGE_SLOT_IDS:
        try:
            plate_cfg = world.get_slot_config(ULM_PLATE_STATION_ID, slot_id)
            centrifuge_cfg = world.get_slot_config(CENTRIFUGE_STATION_ID, slot_id)
        except Exception as exc:
            errors.append(f"[LoadUnloadCentrifuge] Missing slot '{slot_id}': {exc}")
            continue

        plate_types = _rack_type_values(plate_cfg)
        centrifuge_types = _rack_type_values(centrifuge_cfg)
        if len(plate_types) != 1:
            errors.append(
                f"[LoadUnloadCentrifuge] Expected one rack type for "
                f"{ULM_PLATE_STATION_ID}.{slot_id}, found {plate_types}"
            )
        if len(centrifuge_types) != 1:
            errors.append(
                f"[LoadUnloadCentrifuge] Expected one rack type for "
                f"{CENTRIFUGE_STATION_ID}.{slot_id}, found {centrifuge_types}"
            )
        if plate_types and centrifuge_types and plate_types[0] != centrifuge_types[0]:
            errors.append(
                f"[LoadUnloadCentrifuge] Rack type mismatch for slot '{slot_id}': "
                f"plate={plate_types[0]} centrifuge={centrifuge_types[0]}"
            )

        if world.rack_placements.get((ULM_PLATE_STATION_ID, slot_id)):
            has_source_rack = True
        if world.rack_placements.get((CENTRIFUGE_STATION_ID, slot_id)):
            errors.append(
                f"[LoadUnloadCentrifuge] Target slot not empty before load: "
                f"{CENTRIFUGE_STATION_ID}.{slot_id}"
            )

    if not has_source_rack:
        errors.append(
            f"[LoadUnloadCentrifuge] No centrifuge rack present on "
            f"{ULM_PLATE_STATION_ID}.{','.join(CENTRIFUGE_SLOT_IDS)}"
        )
    return errors


def _charge(
    *,
    world: WorldModel,
    catalog: TaskCatalog,
    sender: Optional[CommandSender],
    dry_run: bool,
    timeout_s: float,
    task_name: str,
    step_label: str,
) -> bool:
    ok, _, _ = _run_task(
        catalog=catalog,
        sender=sender,
        dry_run=dry_run,
        timeout_s=timeout_s,
        task_key="Charge",
        overrides={},
        task_name=task_name,
        step_label=step_label,
    )
    if not ok:
        print("Stopped: Charge failed.")
        return False
    if CHARGE_STATION_ID in world.stations:
        world.set_robot_station(CHARGE_STATION_ID)
    return True


def _navigate_and_scan(
    *,
    world: WorldModel,
    station_id: str,
    catalog: TaskCatalog,
    sender: Optional[CommandSender],
    dry_run: bool,
    timeout_s: float,
    task_name_prefix: str,
) -> bool:
    station = world.get_station(station_id)
    navigate_overrides = {
        "AMR_PosTarget": str(station.amr_pos_target),
        "AMR_Footprint": "1",
        "AMR_DOCK": "1",
    }
    ok, _, _ = _run_task(
        catalog=catalog,
        sender=sender,
        dry_run=dry_run,
        timeout_s=timeout_s,
        task_key="Navigate",
        overrides=navigate_overrides,
        task_name=f"{task_name_prefix}.Navigate",
        step_label=f"Navigate to {station_id}",
    )
    if not ok:
        print("Stopped: Navigate failed.")
        return False
    world.set_robot_station(station_id)

    scan_overrides = {"ITM_ID": int(station.itm_id), "ACT": ACTION_SCAN_LANDMARK}
    ok, _, _ = _run_task(
        catalog=catalog,
        sender=sender,
        dry_run=dry_run,
        timeout_s=timeout_s,
        task_key="SingleDeviceAction",
        overrides=scan_overrides,
        task_name=f"{task_name_prefix}.ScanLandmark",
        step_label=f"SingleDeviceAction ScanLandMark at {station_id}",
    )
    if not ok:
        print("Stopped: Landmark scan failed.")
        return False
    return True


def _execute_transfer_step(
    *,
    world: WorldModel,
    catalog: TaskCatalog,
    sender: Optional[CommandSender],
    dry_run: bool,
    timeout_s: float,
    source_station_id: str,
    source_slot_id: str,
    target_station_id: str,
    target_slot_id: str,
    task_name_prefix: str,
    centrifuge_runtime: Optional[Any] = None,
    rotor_slot_index: Optional[int] = None,
) -> bool:
    source_cfg = world.get_slot_config(source_station_id, source_slot_id)
    target_cfg = world.get_slot_config(target_station_id, target_slot_id)

    source_is_centrifuge = source_station_id == CENTRIFUGE_STATION_ID
    target_is_centrifuge = target_station_id == CENTRIFUGE_STATION_ID

    rack_id = world.rack_placements.get((source_station_id, source_slot_id))
    if not rack_id:
        print(
            f"Failed prerequisite: no rack present at {source_station_id}.{source_slot_id} "
            f"for '{task_name_prefix}'."
        )
        return False
    if world.rack_placements.get((target_station_id, target_slot_id)):
        print(
            f"Failed prerequisite: target not empty at {target_station_id}.{target_slot_id} "
            f"for '{task_name_prefix}'."
        )
        return False

    rack = world.racks.get(str(rack_id))
    if rack is None:
        print(f"Failed prerequisite: unknown rack id '{rack_id}'.")
        return False
    rack_type = str(getattr(rack.rack_type, "value", rack.rack_type))
    if rack_type not in _rack_type_values(source_cfg):
        print(
            f"Failed prerequisite: rack type '{rack_type}' not accepted by "
            f"{source_station_id}.{source_slot_id}."
        )
        return False
    if rack_type not in _rack_type_values(target_cfg):
        print(
            f"Failed prerequisite: rack type '{rack_type}' not accepted by "
            f"{target_station_id}.{target_slot_id}."
        )
        return False

    needs_rotor_move = centrifuge_runtime is not None and (source_is_centrifuge or target_is_centrifuge)
    if needs_rotor_move and rotor_slot_index is None:
        print(
            f"Failed prerequisite: rotor slot index missing for '{task_name_prefix}' "
            "while rotor move is enabled."
        )
        return False

    rotor_thread: Optional[threading.Thread] = None
    rotor_result: Optional[Dict[str, Any]] = None
    if centrifuge_runtime is not None and target_is_centrifuge and not source_is_centrifuge:
        # Start rotor move in parallel with the pick from the non-centrifuge source.
        rotor_thread, rotor_result = _start_rotor_move_async(
            centrifuge_runtime=centrifuge_runtime,
            dry_run=dry_run,
            rotor_slot_index=int(rotor_slot_index),
            step_label=(
                f"CentrifugeController MoveRotor (async) for Place "
                f"(slot {int(rotor_slot_index)})"
            ),
        )

    if centrifuge_runtime is not None and source_is_centrifuge:
        ok = _run_centrifuge_controller_action(
            centrifuge_runtime=centrifuge_runtime,
            dry_run=dry_run,
            act=ACTION_MOVE_ROTOR,
            rotor_slot_index=int(rotor_slot_index),
            step_label=(
                f"CentrifugeController MoveRotor before Pick "
                f"(slot {int(rotor_slot_index)})"
            ),
        )
        if not ok:
            print("Stopped: MoveRotor before Pick failed.")
            return False
        ok = _confirm_rotor_position(
            centrifuge_runtime=centrifuge_runtime,
            dry_run=dry_run,
            rotor_slot_index=int(rotor_slot_index),
            step_label=(
                f"CentrifugeController ConfirmRotor before Pick "
                f"(slot {int(rotor_slot_index)})"
            ),
        )
        if not ok:
            print("Stopped: Rotor position check before Pick failed.")
            return False

    pick_overrides = _single_task_overrides(
        itm_id=int(source_cfg.itm_id),
        jig_id=int(source_cfg.jig_id),
        obj_nbr=int(source_cfg.rack_index),
        action=ACTION_PICK,
        obj_type=int(rack.pin_obj_type),
    )
    ok, _, _ = _run_task(
        catalog=catalog,
        sender=sender,
        dry_run=dry_run,
        timeout_s=timeout_s,
        task_key="SingleTask",
        overrides=pick_overrides,
        task_name=f"{task_name_prefix}.Pick",
        step_label=f"SingleTask Pick rack '{rack_id}' from {source_station_id}.{source_slot_id}",
    )
    if not ok:
        print("Stopped: Pick failed.")
        return False

    if rotor_thread is not None and rotor_result is not None:
        rotor_thread.join()
        if not bool(rotor_result.get("ok")):
            print(
                "Stopped: MoveRotor before Place failed."
                f"{' ' + rotor_result.get('error') if rotor_result.get('error') else ''}"
            )
            return False
        ok = _confirm_rotor_position(
            centrifuge_runtime=centrifuge_runtime,
            dry_run=dry_run,
            rotor_slot_index=int(rotor_slot_index),
            step_label=(
                f"CentrifugeController ConfirmRotor before Place "
                f"(slot {int(rotor_slot_index)})"
            ),
        )
        if not ok:
            print("Stopped: Rotor position check before Place failed.")
            return False

    if centrifuge_runtime is not None and target_is_centrifuge and source_is_centrifuge:
        ok = _run_centrifuge_controller_action(
            centrifuge_runtime=centrifuge_runtime,
            dry_run=dry_run,
            act=ACTION_MOVE_ROTOR,
            rotor_slot_index=int(rotor_slot_index),
            step_label=(
                f"CentrifugeController MoveRotor before Place "
                f"(slot {int(rotor_slot_index)})"
            ),
        )
        if not ok:
            print("Stopped: MoveRotor before Place failed.")
            return False
        ok = _confirm_rotor_position(
            centrifuge_runtime=centrifuge_runtime,
            dry_run=dry_run,
            rotor_slot_index=int(rotor_slot_index),
            step_label=(
                f"CentrifugeController ConfirmRotor before Place "
                f"(slot {int(rotor_slot_index)})"
            ),
        )
        if not ok:
            print("Stopped: Rotor position check before Place failed.")
            return False

    place_overrides = _single_task_overrides(
        itm_id=int(target_cfg.itm_id),
        jig_id=int(target_cfg.jig_id),
        obj_nbr=int(target_cfg.rack_index),
        action=ACTION_PLACE,
        obj_type=int(rack.pin_obj_type),
    )
    ok, _, _ = _run_task(
        catalog=catalog,
        sender=sender,
        dry_run=dry_run,
        timeout_s=timeout_s,
        task_key="SingleTask",
        overrides=place_overrides,
        task_name=f"{task_name_prefix}.Place",
        step_label=f"SingleTask Place rack '{rack_id}' to {target_station_id}.{target_slot_id}",
    )
    if not ok:
        print("Stopped: Place failed.")
        return False

    try:
        world.move_rack(
            source_station_id=source_station_id,
            source_station_slot_id=source_slot_id,
            target_station_id=target_station_id,
            target_station_slot_id=target_slot_id,
        )
    except Exception as exc:
        print(f"Stopped: world-state sync failed after Place ({exc}).")
        return False
    return True


def _execute_centrifuge_load_unload(
    *,
    world: WorldModel,
    catalog: TaskCatalog,
    sender: Optional[CommandSender],
    centrifuge_runtime: Optional[Any],
    dry_run: bool,
    timeout_s: float,
) -> bool:
    print("")
    print("=== LoadUnloadCentrifuge ===")
    slots_to_move: List[str] = [
        slot_id
        for slot_id in CENTRIFUGE_SLOT_IDS
        if world.rack_placements.get((ULM_PLATE_STATION_ID, slot_id))
    ]
    if not slots_to_move:
        print(
            f"Failed prerequisite: no centrifuge rack present on "
            f"{ULM_PLATE_STATION_ID}.{','.join(CENTRIFUGE_SLOT_IDS)}."
        )
        return False
    print(
        "Load slots from uLM plate to centrifuge and unload them back "
        f"for slots: {', '.join(slots_to_move)}"
    )
    if centrifuge_runtime is None:
        print(
            f"Failed prerequisite: no centrifuge runtime/controller found at {CENTRIFUGE_STATION_ID}."
        )
        return False

    if not _navigate_and_scan(
        world=world,
        station_id=CENTRIFUGE_STATION_ID,
        catalog=catalog,
        sender=sender,
        dry_run=dry_run,
        timeout_s=timeout_s,
        task_name_prefix="LoadUnloadCentrifuge.LoadPhase",
    ):
        return False

    ok = _run_centrifuge_controller_action(
        centrifuge_runtime=centrifuge_runtime,
        dry_run=dry_run,
        act=ACTION_OPEN_HATCH,
        rotor_slot_index=0,
        step_label="CentrifugeController OpenHatch at CentrifugeStation",
    )
    if not ok:
        print("Stopped: OpenHatch failed.")
        return False

    for slot_id in slots_to_move:
        rotor_slot_index = int(world.get_slot_config(CENTRIFUGE_STATION_ID, slot_id).rack_index)
        ok = _execute_transfer_step(
            world=world,
            catalog=catalog,
            sender=sender,
            dry_run=dry_run,
            timeout_s=timeout_s,
            source_station_id=ULM_PLATE_STATION_ID,
            source_slot_id=slot_id,
            target_station_id=CENTRIFUGE_STATION_ID,
            target_slot_id=slot_id,
            task_name_prefix=f"LoadUnloadCentrifuge.Load.{slot_id}",
            centrifuge_runtime=centrifuge_runtime,
            rotor_slot_index=rotor_slot_index,
        )
        if not ok:
            return False

    if not _charge(
        world=world,
        catalog=catalog,
        sender=sender,
        dry_run=dry_run,
        timeout_s=timeout_s,
        task_name="LoadUnloadCentrifuge.ChargeBetween",
        step_label="Charge between loading and unloading",
    ):
        return False

    if not _navigate_and_scan(
        world=world,
        station_id=CENTRIFUGE_STATION_ID,
        catalog=catalog,
        sender=sender,
        dry_run=dry_run,
        timeout_s=timeout_s,
        task_name_prefix="LoadUnloadCentrifuge.UnloadPhase",
    ):
        return False

    for slot_id in slots_to_move:
        rotor_slot_index = int(world.get_slot_config(CENTRIFUGE_STATION_ID, slot_id).rack_index)
        ok = _execute_transfer_step(
            world=world,
            catalog=catalog,
            sender=sender,
            dry_run=dry_run,
            timeout_s=timeout_s,
            source_station_id=CENTRIFUGE_STATION_ID,
            source_slot_id=slot_id,
            target_station_id=ULM_PLATE_STATION_ID,
            target_slot_id=slot_id,
            task_name_prefix=f"LoadUnloadCentrifuge.Unload.{slot_id}",
            centrifuge_runtime=centrifuge_runtime,
            rotor_slot_index=rotor_slot_index,
        )
        if not ok:
            return False

    ok = _run_centrifuge_controller_action(
        centrifuge_runtime=centrifuge_runtime,
        dry_run=dry_run,
        act=ACTION_CLOSE_HATCH,
        rotor_slot_index=0,
        step_label="CentrifugeController CloseHatch at CentrifugeStation",
    )
    if not ok:
        print("Stopped: CloseHatch failed.")
        return False

    if not _charge(
        world=world,
        catalog=catalog,
        sender=sender,
        dry_run=dry_run,
        timeout_s=timeout_s,
        task_name="LoadUnloadCentrifuge.ChargeEnd",
        step_label="Charge",
    ):
        return False

    print("Result:")
    print(f"- {CENTRIFUGE_STATION_ID} slots returned: {', '.join(slots_to_move)}")
    print(f"- Robot station state is {world.robot_current_station_id}")
    return True


def _execute_case(
    *,
    case: RackWorkflowCase,
    world: WorldModel,
    catalog: TaskCatalog,
    sender: Optional[CommandSender],
    dry_run: bool,
    timeout_s: float,
) -> bool:
    station = world.get_station(case.station_id)
    station_slot_cfg = world.get_slot_config(case.station_id, case.station_slot_id)
    station_rack_type = _rack_type_values(station_slot_cfg)[0]
    plate_slot_id = _resolve_plate_slot_id(world, case.plate_jig_id, station_rack_type)
    plate_slot_cfg = world.get_slot_config(ULM_PLATE_STATION_ID, plate_slot_id)

    if case.direction.upper() == "PICKUP":
        source_station_id, source_slot_id, source_cfg = case.station_id, case.station_slot_id, station_slot_cfg
        target_station_id, target_slot_id, target_cfg = ULM_PLATE_STATION_ID, plate_slot_id, plate_slot_cfg
        narrative = (
            f"Pick {station_rack_type} from {case.station_id}.{case.station_slot_id} "
            f"to {ULM_PLATE_STATION_ID}.{plate_slot_id}"
        )
    else:
        source_station_id, source_slot_id, source_cfg = ULM_PLATE_STATION_ID, plate_slot_id, plate_slot_cfg
        target_station_id, target_slot_id, target_cfg = case.station_id, case.station_slot_id, station_slot_cfg
        narrative = (
            f"Return {station_rack_type} from {ULM_PLATE_STATION_ID}.{plate_slot_id} "
            f"to {case.station_id}.{case.station_slot_id}"
        )

    rack_id = world.rack_placements.get((source_station_id, source_slot_id))
    if not rack_id:
        print(
            f"Failed prerequisite: no rack present at "
            f"{source_station_id}.{source_slot_id} for workflow '{case.name}'."
        )
        return False

    rack = world.racks.get(str(rack_id))
    if rack is None:
        print(f"Failed prerequisite: unknown rack id '{rack_id}' in world model.")
        return False

    source_rack_type = str(getattr(rack.rack_type, "value", rack.rack_type))
    if source_rack_type != station_rack_type:
        print(
            f"Failed prerequisite: rack type mismatch at source {source_station_id}.{source_slot_id} "
            f"(expected={station_rack_type}, found={source_rack_type})."
        )
        return False

    obj_type = int(rack.pin_obj_type)
    print("")
    print(f"=== {case.name} ===")
    print(narrative)

    # 1) Navigate
    navigate_overrides = {
        "AMR_PosTarget": str(station.amr_pos_target),
        "AMR_Footprint": "1",
        "AMR_DOCK": "1",
    }
    ok, _, _ = _run_task(
        catalog=catalog,
        sender=sender,
        dry_run=dry_run,
        timeout_s=timeout_s,
        task_key="Navigate",
        overrides=navigate_overrides,
        task_name=f"{case.name}.Navigate",
        step_label=f"Navigate to {case.station_id}",
    )
    if not ok:
        print("Stopped: Navigate failed.")
        return False
    world.set_robot_station(case.station_id)

    # 2) Scan landmark
    scan_overrides = {"ITM_ID": int(station.itm_id), "ACT": ACTION_SCAN_LANDMARK}
    ok, _, _ = _run_task(
        catalog=catalog,
        sender=sender,
        dry_run=dry_run,
        timeout_s=timeout_s,
        task_key="SingleDeviceAction",
        overrides=scan_overrides,
        task_name=f"{case.name}.ScanLandmark",
        step_label=f"SingleDeviceAction ScanLandMark at {case.station_id}",
    )
    if not ok:
        print("Stopped: Landmark scan failed.")
        return False

    # 3) Pick rack
    pick_overrides = _single_task_overrides(
        itm_id=int(source_cfg.itm_id),
        jig_id=int(source_cfg.jig_id),
        obj_nbr=int(source_cfg.rack_index),
        action=ACTION_PICK,
        obj_type=int(obj_type),
    )
    ok, _, _ = _run_task(
        catalog=catalog,
        sender=sender,
        dry_run=dry_run,
        timeout_s=timeout_s,
        task_key="SingleTask",
        overrides=pick_overrides,
        task_name=f"{case.name}.Pick",
        step_label=(
            f"SingleTask Pick rack '{rack_id}' from "
            f"{source_station_id}.{source_slot_id}"
        ),
    )
    if not ok:
        print("Stopped: Pick failed.")
        return False

    # 4) Place rack
    place_overrides = _single_task_overrides(
        itm_id=int(target_cfg.itm_id),
        jig_id=int(target_cfg.jig_id),
        obj_nbr=int(target_cfg.rack_index),
        action=ACTION_PLACE,
        obj_type=int(obj_type),
    )
    ok, _, _ = _run_task(
        catalog=catalog,
        sender=sender,
        dry_run=dry_run,
        timeout_s=timeout_s,
        task_key="SingleTask",
        overrides=place_overrides,
        task_name=f"{case.name}.Place",
        step_label=(
            f"SingleTask Place rack '{rack_id}' to "
            f"{target_station_id}.{target_slot_id}"
        ),
    )
    if not ok:
        print("Stopped: Place failed.")
        return False

    # Keep local world model in sync for next test in same run.
    try:
        world.move_rack(
            source_station_id=source_station_id,
            source_station_slot_id=source_slot_id,
            target_station_id=target_station_id,
            target_station_slot_id=target_slot_id,
        )
    except Exception as exc:
        print(f"Stopped: world-state sync failed after Place ({exc}).")
        return False

    # 5) Charge
    ok, _, _ = _run_task(
        catalog=catalog,
        sender=sender,
        dry_run=dry_run,
        timeout_s=timeout_s,
        task_key="Charge",
        overrides={},
        task_name=f"{case.name}.Charge",
        step_label="Charge",
    )
    if not ok:
        print("Stopped: Charge failed.")
        return False
    if CHARGE_STATION_ID in world.stations:
        world.set_robot_station(CHARGE_STATION_ID)

    print("Result:")
    print(f"- {source_station_id}.{source_slot_id} is now empty")
    print(f"- {target_station_id}.{target_slot_id} now has {rack_id}")
    print(f"- Robot station state is {world.robot_current_station_id}")
    return True


def _build_cases(input_slot: int) -> List[RackWorkflowCase]:
    input_slot_id = f"URGRackSlot{int(input_slot)}"
    return [
        RackWorkflowCase(
            name="PickUpFridgeRack",
            direction="PICKUP",
            station_id="FridgeStation",
            station_slot_id="URGFridgeRackSlot1",
            plate_jig_id=13,
        ),
        RackWorkflowCase(
            name="ReturnFridgeRack",
            direction="RETURN",
            station_id="FridgeStation",
            station_slot_id="URGFridgeRackSlot1",
            plate_jig_id=13,
        ),
        RackWorkflowCase(
            name="PickUpInputRack",
            direction="PICKUP",
            station_id="InputStation",
            station_slot_id=input_slot_id,
            plate_jig_id=1,
        ),
        RackWorkflowCase(
            name="ReturnInputRack",
            direction="RETURN",
            station_id="InputStation",
            station_slot_id=input_slot_id,
            plate_jig_id=1,
        ),
        RackWorkflowCase(
            name="PickUpArchiveRack",
            direction="PICKUP",
            station_id="ArchiveStation",
            station_slot_id="URGFridgeRackSlot",
            plate_jig_id=13,
        ),
        RackWorkflowCase(
            name="ReturnArchiveRack",
            direction="RETURN",
            station_id="ArchiveStation",
            station_slot_id="URGFridgeRackSlot",
            plate_jig_id=13,
        ),
    ]


def _resolve_input_slot(world: WorldModel, requested: str) -> int:
    raw = str(requested).strip().lower()
    if raw in {"1", "2"}:
        return int(raw)
    for idx in (1, 2):
        slot_id = f"URGRackSlot{int(idx)}"
        if world.rack_placements.get(("InputStation", slot_id)):
            return int(idx)
    return 1


def _select_cases(
    all_cases: Sequence[RackWorkflowCase],
    workflow: str,
) -> List[RackWorkflowCase]:
    key = str(workflow).strip().lower()
    if key == "all":
        return list(all_cases)
    name_map = {c.name.lower(): c for c in all_cases}
    aliases = {
        "pickup_fridge": "pickupfridgerack",
        "return_fridge": "returnfridgerack",
        "pickup_input": "pickupinputrack",
        "return_input": "returninputrack",
        "pickup_archive": "pickuparchiverack",
        "return_archive": "returnarchiverack",
    }
    resolved = aliases.get(key, key)
    if resolved not in name_map:
        allowed = ["all"] + sorted(aliases.keys()) + sorted(name_map.keys())
        raise ValueError(f"Unknown workflow '{workflow}'. Allowed: {allowed}")
    return [name_map[resolved]]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Quick rack move workflow tests.")
    parser.add_argument(
        "--workflow",
        default="all",
        help=(
            "Workflow to run: all | pickup_fridge | return_fridge | "
            "pickup_input | return_input | pickup_archive | return_archive | "
            "load_unload_centrifuge"
        ),
    )
    parser.add_argument(
        "--input-slot",
        default="auto",
        choices=["auto", "1", "2"],
        help="InputStation slot index for InputRack workflows (auto, 1, 2). Default: auto",
    )
    parser.add_argument(
        "--world-config",
        default=str(DEFAULT_WORLD_CONFIG),
        help=f"Path to world config (default: {DEFAULT_WORLD_CONFIG})",
    )
    parser.add_argument(
        "--task-catalog",
        default=str(DEFAULT_TASK_CATALOG),
        help=f"Path to task catalog (default: {DEFAULT_TASK_CATALOG})",
    )
    parser.add_argument(
        "--timeout-s",
        type=float,
        default=120.0,
        help="Task timeout in seconds (default: 120.0)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print payloads; do not send commands to backend.",
    )
    parser.add_argument(
        "--simulate",
        action="store_true",
        help="Use simulated sender backend (same effect as UGO_SIMULATE_DEVICES=1).",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    world = load_world_from_file(Path(args.world_config))
    catalog = TaskCatalog.from_file(str(args.task_catalog))
    workflow_key = str(args.workflow).strip().lower()

    if workflow_key in {"load_unload_centrifuge", "centrifuge_cycle", "centrifuge_load_unload"}:
        prereq_errors = (
            _validate_catalog_prerequisites(catalog)
            + _validate_centrifuge_cycle_prerequisites(world)
        )
        if prereq_errors:
            print("Cannot execute: prerequisite validation failed.")
            for item in prereq_errors:
                print(f"- {item}")
            return 2

        sender: Optional[CommandSender] = None
        if not bool(args.dry_run):
            sender = build_sender(
                task_catalog_path=Path(args.task_catalog),
                max_attempts=1,
                simulate=bool(args.simulate),
            )
            sender.post_error_on_fail = False
            sender.clear_error_immediately = False

        centrifuge_runtime: Optional[Any] = None
        try:
            registry = build_device_registry_from_world(world)
            centrifuge_runtime = registry.get_first_centrifuge_at_station(CENTRIFUGE_STATION_ID)
            if centrifuge_runtime is None:
                print(
                    f"Cannot execute: no centrifuge runtime/controller found at {CENTRIFUGE_STATION_ID}."
                )
                return 2
        except Exception as exc:
            print(f"Cannot execute: failed to initialize centrifuge controller ({exc}).")
            return 2

        print("Running quick rack workflow tests:")
        print("- LoadUnloadCentrifuge")
        ok = _execute_centrifuge_load_unload(
            world=world,
            catalog=catalog,
            sender=sender,
            centrifuge_runtime=centrifuge_runtime,
            dry_run=bool(args.dry_run),
            timeout_s=float(args.timeout_s),
        )
        if not ok:
            print("Workflow failed: LoadUnloadCentrifuge")
            return 1
        print("")
        print("All requested workflows executed successfully.")
        return 0

    input_slot = _resolve_input_slot(world, requested=str(args.input_slot))
    all_cases = _build_cases(input_slot=int(input_slot))
    selected_cases = _select_cases(all_cases, workflow=str(args.workflow))

    catalog_errors = _validate_catalog_prerequisites(catalog)
    world_errors = _validate_world_prerequisites(world, selected_cases)
    prereq_errors = list(catalog_errors) + list(world_errors)
    if prereq_errors:
        print("Cannot execute: prerequisite validation failed.")
        for item in prereq_errors:
            print(f"- {item}")
        return 2

    sender: Optional[CommandSender] = None
    if not bool(args.dry_run):
        sender = build_sender(
            task_catalog_path=Path(args.task_catalog),
            max_attempts=1,
            simulate=bool(args.simulate),
        )
        sender.post_error_on_fail = False
        sender.clear_error_immediately = False

    print("Running quick rack workflow tests:")
    for case in selected_cases:
        print(f"- {case.name}")

    for case in selected_cases:
        ok = _execute_case(
            case=case,
            world=world,
            catalog=catalog,
            sender=sender,
            dry_run=bool(args.dry_run),
            timeout_s=float(args.timeout_s),
        )
        if not ok:
            print(f"Workflow failed: {case.name}")
            return 1

    print("")
    print("All requested workflows executed successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
