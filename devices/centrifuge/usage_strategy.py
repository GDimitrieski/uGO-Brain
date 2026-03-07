from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Sequence, Tuple


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

    def to_config_dict(self) -> Dict[str, Any]:
        return {
            "type": "Rotina380UsageProfile",
            "source_station_id": self.source_station_id,
            "centrifuge_station_id": self.centrifuge_station_id,
            "fixed_receiver_obj_nbr": int(self.fixed_receiver_obj_nbr),
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
        return Rotina380UsageProfile(
            source_station_id=source_station_id,
            centrifuge_station_id=centrifuge_station_id,
            fixed_receiver_obj_nbr=fixed_receiver_obj_nbr,
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
    operations.append(
        DeviceActionStep(
            name="OpenLid",
            task_key="SingleDeviceAction",
            overrides={"ITM_ID": centrifuge_itm_id, "ACT": DEVICE_ACTION_OPEN_HATCH},
        )
    )

    for idx, (source_slot_id, centrifuge_slot_id) in enumerate(zip(source_slot_ids, centrifuge_slot_ids), start=1):
        operations.append(
            DeviceActionStep(
                name=f"MoveRotorToPos{idx}",
                task_key="SingleDeviceAction",
                overrides={"ITM_ID": centrifuge_itm_id, "ACT": DEVICE_ACTION_MOVE_ROTOR},
            )
        )

        if resolved_mode == "LOAD":
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

