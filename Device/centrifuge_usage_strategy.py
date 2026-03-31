from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

from world.jig_rack_strategy import (
    TARA_PROBE_PREFIXES_DEFAULT,
    PlannedSampleMove,
    is_tara_probe_sample_id,
    plan_tara_balance_moves,
)
from world.lab_world import CapState


DEVICE_ACTION_OPEN_HATCH = 1
DEVICE_ACTION_START_CENTRIFUGE = 2
DEVICE_ACTION_CLOSE_HATCH = 3
DEVICE_ACTION_MOVE_ROTOR = 4


@dataclass(frozen=True)
class ValidationStep:
    name: str


@dataclass(frozen=True)
class DeviceActionStep:
    name: str
    task_key: str
    overrides: Dict[str, Any]
    rotor_slot_index: int = 0


@dataclass(frozen=True)
class SampleTransferStep:
    name: str
    sample_id: str
    source_station_id: str
    source_station_slot_id: str
    source_slot_index: int
    source_itm_id: int
    source_jig_id: int
    source_obj_nbr: int
    target_station_id: str
    target_station_slot_id: str
    target_slot_index: int
    target_itm_id: int
    target_jig_id: int
    target_obj_nbr: int
    obj_type: int
    reason: str = ""


@dataclass(frozen=True)
class RackTransferStep:
    name: str
    transfer_index: int
    source_station_id: str
    source_station_slot_id: str
    source_itm_id: int
    source_jig_id: int
    source_obj_nbr: int
    target_station_id: str
    target_station_slot_id: str
    target_itm_id: int
    target_jig_id: int
    target_obj_nbr: int
    obj_type: int


@dataclass(frozen=True)
class RunningValidationStep:
    name: str


@dataclass(frozen=True)
class CentrifugeUsagePlan:
    mode: str
    source_station_id: str
    centrifuge_station_id: str
    source_slot_ids: Tuple[str, ...]
    centrifuge_slot_ids: Tuple[str, ...]
    operations: Tuple[Any, ...]


class CentrifugeUsageProfile:
    def to_config_dict(self) -> Dict[str, Any]:
        raise NotImplementedError


@dataclass(frozen=True)
class Rotina380UsageProfile(CentrifugeUsageProfile):
    source_station_id: str = "uLMPlateStation"
    centrifuge_station_id: str = "CentrifugeStation"
    fixed_receiver_obj_nbr: int = 1
    target_loading_jig_id: int = 2
    tara_probe_jig_id: int = 3
    tara_probe_prefixes: Tuple[str, ...] = TARA_PROBE_PREFIXES_DEFAULT
    enable_tara_balancing: bool = True
    return_tara_probes_on_unload: bool = True

    def to_config_dict(self) -> Dict[str, Any]:
        return {
            "type": "Rotina380UsageProfile",
            "source_station_id": self.source_station_id,
            "centrifuge_station_id": self.centrifuge_station_id,
            "fixed_receiver_obj_nbr": int(self.fixed_receiver_obj_nbr),
            "target_loading_jig_id": int(self.target_loading_jig_id),
            "tara_probe_jig_id": int(self.tara_probe_jig_id),
            "tara_probe_prefixes": list(self.tara_probe_prefixes),
            "enable_tara_balancing": bool(self.enable_tara_balancing),
            "return_tara_probes_on_unload": bool(self.return_tara_probes_on_unload),
        }


def usage_profile_from_config(raw: Optional[Dict[str, Any]]) -> CentrifugeUsageProfile:
    cfg = raw if isinstance(raw, dict) else {}
    profile_type = str(cfg.get("type", "Rotina380UsageProfile")).strip().lower()
    if profile_type in {
        "rotina380usageprofile",
        "rotina380rackusagestrategy",
        "rotina380",
        "rotina380r",
        "",
    }:
        source_station_id = str(cfg.get("source_station_id", "uLMPlateStation")).strip() or "uLMPlateStation"
        centrifuge_station_id = str(cfg.get("centrifuge_station_id", "CentrifugeStation")).strip() or "CentrifugeStation"
        try:
            fixed_receiver_obj_nbr = int(cfg.get("fixed_receiver_obj_nbr", 1))
        except Exception:
            fixed_receiver_obj_nbr = 1
        try:
            target_loading_jig_id = int(cfg.get("target_loading_jig_id", 2))
        except Exception:
            target_loading_jig_id = 2
        try:
            tara_probe_jig_id = int(cfg.get("tara_probe_jig_id", 3))
        except Exception:
            tara_probe_jig_id = 3
        raw_prefixes = cfg.get("tara_probe_prefixes", list(TARA_PROBE_PREFIXES_DEFAULT))
        parsed_prefixes: Tuple[str, ...]
        if isinstance(raw_prefixes, list):
            parsed_prefixes = tuple(str(x).strip() for x in raw_prefixes if str(x).strip())
        else:
            parsed_prefixes = TARA_PROBE_PREFIXES_DEFAULT
        if not parsed_prefixes:
            parsed_prefixes = TARA_PROBE_PREFIXES_DEFAULT
        enable_tara_balancing = bool(cfg.get("enable_tara_balancing", True))
        return_tara_probes_on_unload = bool(cfg.get("return_tara_probes_on_unload", True))
        return Rotina380UsageProfile(
            source_station_id=source_station_id,
            centrifuge_station_id=centrifuge_station_id,
            fixed_receiver_obj_nbr=fixed_receiver_obj_nbr,
            target_loading_jig_id=target_loading_jig_id,
            tara_probe_jig_id=tara_probe_jig_id,
            tara_probe_prefixes=parsed_prefixes,
            enable_tara_balancing=enable_tara_balancing,
            return_tara_probes_on_unload=return_tara_probes_on_unload,
        )
    raise ValueError(f"Unsupported centrifuge usage profile type '{cfg.get('type')}'")


def _slot_kind_text(slot_cfg: Any) -> str:
    kind = getattr(slot_cfg, "kind", "")
    if hasattr(kind, "value"):
        return str(getattr(kind, "value"))
    return str(kind)


def _ordered_centrifuge_slot_ids(world: Any, station_id: str) -> Sequence[str]:
    station = world.get_station(station_id)
    slots = []
    for slot_id, cfg in station.slot_configs.items():
        if _slot_kind_text(cfg) != "CENTRIFUGE_RACK_SLOT":
            continue
        slots.append((int(getattr(cfg, "rack_index", 1)), str(slot_id)))
    slots.sort(key=lambda x: x[0])
    return [slot_id for _, slot_id in slots]


def _rack_id_at(world: Any, station_id: str, slot_id: str) -> Optional[str]:
    rack_id = world.rack_placements.get((station_id, slot_id))
    if not rack_id:
        return None
    return str(rack_id)


def _obj_nbr_for_slot_index(world: Any, station_id: str, slot_id: str, slot_index: int) -> int:
    cfg = world.get_slot_config(station_id, slot_id)
    return int(getattr(cfg, "obj_nbr_offset", 0)) + int(slot_index)


def _sample_obj_type(world: Any, sample_id: str, fallback: int) -> int:
    sample = world.samples.get(sample_id)
    if sample is None:
        return int(fallback)
    obj_type = int(getattr(sample, "obj_type", fallback))
    if getattr(sample, "cap_state", None) == CapState.DECAPPED:
        obj_type += 1000
    return obj_type


def _sample_transfer_from_move(world: Any, move: PlannedSampleMove, *, name: str) -> SampleTransferStep:
    source_cfg = world.get_slot_config(move.source_station_id, move.source_station_slot_id)
    target_cfg = world.get_slot_config(move.target_station_id, move.target_station_slot_id)
    fallback_obj_type = 0
    target_rack_id = _rack_id_at(world, move.target_station_id, move.target_station_slot_id)
    if target_rack_id and target_rack_id in world.racks:
        fallback_obj_type = int(world.racks[target_rack_id].pin_obj_type)
    obj_type = _sample_obj_type(
        world,
        sample_id=str(move.sample_id),
        fallback=fallback_obj_type,
    )
    return SampleTransferStep(
        name=name,
        sample_id=str(move.sample_id),
        source_station_id=str(move.source_station_id),
        source_station_slot_id=str(move.source_station_slot_id),
        source_slot_index=int(move.source_slot_index),
        source_itm_id=int(source_cfg.itm_id),
        source_jig_id=int(source_cfg.jig_id),
        source_obj_nbr=_obj_nbr_for_slot_index(
            world,
            str(move.source_station_id),
            str(move.source_station_slot_id),
            int(move.source_slot_index),
        ),
        target_station_id=str(move.target_station_id),
        target_station_slot_id=str(move.target_station_slot_id),
        target_slot_index=int(move.target_slot_index),
        target_itm_id=int(target_cfg.itm_id),
        target_jig_id=int(target_cfg.jig_id),
        target_obj_nbr=_obj_nbr_for_slot_index(
            world,
            str(move.target_station_id),
            str(move.target_station_slot_id),
            int(move.target_slot_index),
        ),
        obj_type=int(obj_type),
        reason=str(move.reason or ""),
    )


def _tara_return_moves_for_unload(
    *,
    world: Any,
    profile: Rotina380UsageProfile,
    source_slot_ids: Sequence[str],
    centrifuge_slot_ids: Sequence[str],
) -> Sequence[PlannedSampleMove]:
    # At UNLOAD compile time, probes are still in centrifuge-station racks.
    # Map them to their post-unload source slots, then return to Tara jig.
    if not profile.return_tara_probes_on_unload:
        return []

    predicted_probe_positions: List[Tuple[str, int, str]] = []
    for source_slot_id, centrifuge_slot_id in zip(source_slot_ids, centrifuge_slot_ids):
        rack_id = _rack_id_at(world, profile.centrifuge_station_id, centrifuge_slot_id)
        if not rack_id or rack_id not in world.racks:
            continue
        rack = world.racks[rack_id]
        for slot_index in sorted(rack.occupied_slots.keys()):
            sample_id = str(rack.occupied_slots[slot_index])
            if not is_tara_probe_sample_id(sample_id, probe_prefixes=profile.tara_probe_prefixes):
                continue
            predicted_probe_positions.append((str(source_slot_id), int(slot_index), sample_id))

    if not predicted_probe_positions:
        return []

    tara_cfgs = world.slots_for_jig(profile.source_station_id, int(profile.tara_probe_jig_id))
    if not tara_cfgs:
        raise ValueError(
            "Rotina380 unload prerequisite failed: no Tara rack slot available for probe return "
            f"(station='{profile.source_station_id}', JIG_ID={int(profile.tara_probe_jig_id)})"
        )

    free_targets: List[Tuple[str, int]] = []
    planned_occupied_by_slot: Dict[str, set[int]] = {}
    for cfg in tara_cfgs:
        slot_id = str(cfg.slot_id)
        rack_id = _rack_id_at(world, profile.source_station_id, slot_id)
        if not rack_id or rack_id not in world.racks:
            continue
        rack = world.racks[rack_id]
        occupied = set(int(idx) for idx in rack.occupied_slots.keys())
        free_slots = [idx for idx in rack.available_slots() if idx not in occupied]
        for idx in free_slots:
            if idx in planned_occupied_by_slot.setdefault(slot_id, set()):
                continue
            planned_occupied_by_slot[slot_id].add(int(idx))
            free_targets.append((slot_id, int(idx)))

    if len(free_targets) < len(predicted_probe_positions):
        raise ValueError(
            "Rotina380 unload prerequisite failed: insufficient Tara free slots for probe return. "
            f"Need={len(predicted_probe_positions)}, Free={len(free_targets)}"
        )

    moves: List[PlannedSampleMove] = []
    for idx, (source_slot_id, source_slot_index, sample_id) in enumerate(predicted_probe_positions):
        target_slot_id, target_slot_index = free_targets[idx]
        moves.append(
            PlannedSampleMove(
                sample_id=str(sample_id),
                source_station_id=profile.source_station_id,
                source_station_slot_id=str(source_slot_id),
                source_slot_index=int(source_slot_index),
                target_station_id=profile.source_station_id,
                target_station_slot_id=str(target_slot_id),
                target_slot_index=int(target_slot_index),
                reason="tara_return",
            )
        )
    return moves


def _resolve_profile(profile_raw: Any) -> CentrifugeUsageProfile:
    if isinstance(profile_raw, CentrifugeUsageProfile):
        return profile_raw
    if isinstance(profile_raw, dict):
        return usage_profile_from_config(profile_raw)
    if profile_raw is None:
        return usage_profile_from_config(None)
    if hasattr(profile_raw, "to_config_dict"):
        try:
            cfg = profile_raw.to_config_dict()
        except Exception:
            cfg = None
        if isinstance(cfg, dict):
            return usage_profile_from_config(cfg)
    raise ValueError(f"Unsupported usage profile payload type '{type(profile_raw).__name__}'")


def _compile_rotina380_plan(
    *,
    world: Any,
    profile: Rotina380UsageProfile,
    mode: str = "AUTO",
) -> CentrifugeUsagePlan:
    source_slot_ids = tuple(_ordered_centrifuge_slot_ids(world, profile.source_station_id))
    centrifuge_slot_ids = tuple(_ordered_centrifuge_slot_ids(world, profile.centrifuge_station_id))
    if not source_slot_ids or not centrifuge_slot_ids:
        raise ValueError("Rotina380 strategy prerequisite failed: missing centrifuge rack slots")
    if len(source_slot_ids) != len(centrifuge_slot_ids):
        raise ValueError(
            "Rotina380 strategy prerequisite failed: source and centrifuge slot counts mismatch "
            f"({len(source_slot_ids)} != {len(centrifuge_slot_ids)})"
        )

    source_racks = [_rack_id_at(world, profile.source_station_id, slot_id) for slot_id in source_slot_ids]
    centrifuge_racks = [_rack_id_at(world, profile.centrifuge_station_id, slot_id) for slot_id in centrifuge_slot_ids]
    source_count = sum(1 for rid in source_racks if rid)
    centrifuge_count = sum(1 for rid in centrifuge_racks if rid)

    requested_mode = str(mode or "AUTO").strip().upper()
    if requested_mode not in {"AUTO", "LOAD", "UNLOAD"}:
        raise ValueError(f"Unsupported centrifuge strategy mode '{requested_mode}'")

    resolved_mode: str
    if requested_mode == "AUTO":
        if source_count == len(source_slot_ids) and centrifuge_count == 0:
            resolved_mode = "LOAD"
        elif centrifuge_count == len(centrifuge_slot_ids) and source_count == 0:
            resolved_mode = "UNLOAD"
        else:
            raise ValueError(
                "Rotina380 AUTO mode is ambiguous. Expected either source full+centrifuge empty "
                "or source empty+centrifuge full. "
                f"Observed source={source_count}/{len(source_slot_ids)}, "
                f"centrifuge={centrifuge_count}/{len(centrifuge_slot_ids)}"
            )
    else:
        resolved_mode = requested_mode

    if resolved_mode == "LOAD":
        if not (source_count == len(source_slot_ids) and centrifuge_count == 0):
            raise ValueError(
                "Rotina380 load prerequisites failed: expected source full and centrifuge empty. "
                f"Observed source={source_count}/{len(source_slot_ids)}, "
                f"centrifuge={centrifuge_count}/{len(centrifuge_slot_ids)}"
            )
    else:
        if not (source_count == 0 and centrifuge_count == len(centrifuge_slot_ids)):
            raise ValueError(
                "Rotina380 unload prerequisites failed: expected source empty and centrifuge full. "
                f"Observed source={source_count}/{len(source_slot_ids)}, "
                f"centrifuge={centrifuge_count}/{len(centrifuge_slot_ids)}"
            )

    centrifuge_itm_id = int(world.get_station(profile.centrifuge_station_id).itm_id)

    operations = [ValidationStep(name=f"Validate{resolved_mode}Prerequisites")]
    if resolved_mode == "LOAD" and profile.enable_tara_balancing:
        balance_moves = plan_tara_balance_moves(
            world,
            station_id=profile.source_station_id,
            target_jig_id=int(profile.target_loading_jig_id),
            tara_jig_id=int(profile.tara_probe_jig_id),
            probe_prefixes=profile.tara_probe_prefixes,
        )
        for idx, move in enumerate(balance_moves, start=1):
            operations.append(
                _sample_transfer_from_move(
                    world,
                    move,
                    name=f"BalanceWithTaraProbe{idx}",
                )
            )

    operations.append(
        DeviceActionStep(
            name="OpenLid",
            task_key="SingleDeviceAction",
            overrides={"ITM_ID": centrifuge_itm_id, "ACT": DEVICE_ACTION_OPEN_HATCH},
        )
    )

    for idx, (source_slot_id, centrifuge_slot_id) in enumerate(zip(source_slot_ids, centrifuge_slot_ids), start=1):
        if resolved_mode == "LOAD":
            operations.append(
                DeviceActionStep(
                    name=f"MoveRotorToPos{idx}",
                    task_key="SingleDeviceAction",
                    overrides={
                        "ITM_ID": centrifuge_itm_id,
                        "ACT": DEVICE_ACTION_MOVE_ROTOR,
                        "OBJ_Nbr": int(idx),
                    },
                    rotor_slot_index=int(idx),
                )
            )
            rack_id = _rack_id_at(world, profile.source_station_id, source_slot_id)
            if not rack_id or rack_id not in world.racks:
                raise ValueError(
                    f"Rotina380 load prerequisite failed: missing rack at "
                    f"{profile.source_station_id}.{source_slot_id}"
                )
            source_cfg = world.get_slot_config(profile.source_station_id, source_slot_id)
            target_cfg = world.get_slot_config(profile.centrifuge_station_id, centrifuge_slot_id)
            obj_type = int(world.racks[rack_id].pin_obj_type)
            operations.append(
                RackTransferStep(
                    name=f"LoadRack{idx}",
                    transfer_index=idx,
                    source_station_id=profile.source_station_id,
                    source_station_slot_id=source_slot_id,
                    source_itm_id=int(source_cfg.itm_id),
                    source_jig_id=int(source_cfg.jig_id),
                    source_obj_nbr=int(source_cfg.rack_index),
                    target_station_id=profile.centrifuge_station_id,
                    target_station_slot_id=centrifuge_slot_id,
                    target_itm_id=int(target_cfg.itm_id),
                    target_jig_id=int(target_cfg.jig_id),
                    target_obj_nbr=int(profile.fixed_receiver_obj_nbr),
                    obj_type=obj_type,
                )
            )
            continue

        operations.append(
            DeviceActionStep(
                name=f"MoveRotorToPos{idx}",
                task_key="SingleDeviceAction",
                overrides={
                    "ITM_ID": centrifuge_itm_id,
                    "ACT": DEVICE_ACTION_MOVE_ROTOR,
                    "OBJ_Nbr": int(idx),
                },
                rotor_slot_index=int(idx),
            )
        )
        rack_id = _rack_id_at(world, profile.centrifuge_station_id, centrifuge_slot_id)
        if not rack_id or rack_id not in world.racks:
            raise ValueError(
                f"Rotina380 unload prerequisite failed: missing rack at "
                f"{profile.centrifuge_station_id}.{centrifuge_slot_id}"
            )
        source_cfg = world.get_slot_config(profile.centrifuge_station_id, centrifuge_slot_id)
        target_cfg = world.get_slot_config(profile.source_station_id, source_slot_id)
        obj_type = int(world.racks[rack_id].pin_obj_type)
        operations.append(
            RackTransferStep(
                name=f"UnloadRack{idx}",
                transfer_index=idx,
                source_station_id=profile.centrifuge_station_id,
                source_station_slot_id=centrifuge_slot_id,
                source_itm_id=int(source_cfg.itm_id),
                source_jig_id=int(source_cfg.jig_id),
                source_obj_nbr=int(profile.fixed_receiver_obj_nbr),
                target_station_id=profile.source_station_id,
                target_station_slot_id=source_slot_id,
                target_itm_id=int(target_cfg.itm_id),
                target_jig_id=int(target_cfg.jig_id),
                target_obj_nbr=int(target_cfg.rack_index),
                obj_type=obj_type,
            )
        )

    operations.append(
        DeviceActionStep(
            name="CloseHatch",
            task_key="SingleDeviceAction",
            overrides={"ITM_ID": centrifuge_itm_id, "ACT": DEVICE_ACTION_CLOSE_HATCH},
        )
    )
    if resolved_mode == "UNLOAD" and profile.return_tara_probes_on_unload:
        return_moves = _tara_return_moves_for_unload(
            world=world,
            profile=profile,
            source_slot_ids=source_slot_ids,
            centrifuge_slot_ids=centrifuge_slot_ids,
        )
        for idx, move in enumerate(return_moves, start=1):
            operations.append(
                _sample_transfer_from_move(
                    world,
                    move,
                    name=f"ReturnTaraProbe{idx}",
                )
            )
    if resolved_mode == "LOAD":
        operations.append(
            DeviceActionStep(
                name="StartCentrifuge",
                task_key="SingleDeviceAction",
                overrides={"ITM_ID": centrifuge_itm_id, "ACT": DEVICE_ACTION_START_CENTRIFUGE},
            )
        )
        operations.append(RunningValidationStep(name="ValidateRunning"))

    return CentrifugeUsagePlan(
        mode=resolved_mode,
        source_station_id=profile.source_station_id,
        centrifuge_station_id=profile.centrifuge_station_id,
        source_slot_ids=source_slot_ids,
        centrifuge_slot_ids=centrifuge_slot_ids,
        operations=tuple(operations),
    )


def compile_centrifuge_usage_plan(*, world: Any, device: Any, mode: str = "AUTO") -> CentrifugeUsagePlan:
    profile_raw = getattr(device, "usage_profile", None)
    if profile_raw is None:
        raise ValueError(
            f"Centrifuge device '{getattr(getattr(device, 'identity', None), 'device_id', '?')}' "
            "is missing usage_profile"
        )
    profile = _resolve_profile(profile_raw)
    if isinstance(profile, Rotina380UsageProfile):
        return _compile_rotina380_plan(world=world, profile=profile, mode=mode)
    raise ValueError(f"No plan compiler available for profile '{type(profile).__name__}'")
