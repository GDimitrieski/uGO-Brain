from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Optional, Sequence, Tuple


TARA_PROBE_PREFIXES_DEFAULT: Tuple[str, ...] = ("DUMMY_", "TARA_")


@dataclass(frozen=True)
class PlannedSampleMove:
    sample_id: str
    source_station_id: str
    source_station_slot_id: str
    source_slot_index: int
    target_station_id: str
    target_station_slot_id: str
    target_slot_index: int
    reason: str


def _normalized_prefixes(prefixes: Optional[Sequence[str]]) -> Tuple[str, ...]:
    values = tuple(str(x).strip() for x in (prefixes or ()) if str(x).strip())
    return values or TARA_PROBE_PREFIXES_DEFAULT


def is_tara_probe_sample_id(
    sample_id: str,
    *,
    probe_prefixes: Optional[Sequence[str]] = None,
) -> bool:
    txt = str(sample_id or "").strip()
    if not txt:
        return False
    prefixes = _normalized_prefixes(probe_prefixes)
    return any(txt.upper().startswith(prefix.upper()) for prefix in prefixes)


def _free_slot_indices(
    world: Any,
    *,
    station_id: str,
    station_slot_id: str,
    planned_occupied: Optional[set[int]] = None,
) -> List[int]:
    rack_id = world.rack_placements.get((station_id, station_slot_id))
    if not rack_id:
        return []
    rack = world.racks.get(rack_id)
    if rack is None:
        return []
    occupied = set(int(idx) for idx in rack.occupied_slots.keys())
    if planned_occupied:
        occupied.update(int(idx) for idx in planned_occupied)
    return [idx for idx in rack.available_slots() if int(idx) not in occupied]


def _probe_positions_for_jig(
    world: Any,
    *,
    station_id: str,
    jig_id: int,
    probe_prefixes: Optional[Sequence[str]],
) -> List[Tuple[str, int, str]]:
    out: List[Tuple[str, int, str]] = []
    for cfg in world.slots_for_jig(station_id, jig_id):
        rack_id = world.rack_placements.get((station_id, cfg.slot_id))
        if not rack_id:
            continue
        rack = world.racks.get(rack_id)
        if rack is None:
            continue
        for slot_index in sorted(rack.occupied_slots.keys()):
            sample_id = str(rack.occupied_slots[slot_index])
            if not is_tara_probe_sample_id(sample_id, probe_prefixes=probe_prefixes):
                continue
            out.append((str(cfg.slot_id), int(slot_index), sample_id))
    return out


def select_next_target_slot_for_jig(
    world: Any,
    *,
    station_id: str,
    jig_id: int,
    strategy: Optional[str] = None,
) -> Tuple[str, int]:
    return world.select_next_target_slot_for_jig(
        station_id=station_id,
        jig_id=int(jig_id),
        strategy=strategy,
    )


def plan_tara_balance_moves(
    world: Any,
    *,
    station_id: str,
    target_jig_id: int,
    tara_jig_id: int = 3,
    probe_prefixes: Optional[Sequence[str]] = None,
) -> List[PlannedSampleMove]:
    target_cfgs = world.slots_for_jig(station_id, int(target_jig_id))
    if not target_cfgs:
        raise ValueError(
            f"Tara balance prerequisite failed: no target slots for station '{station_id}' JIG_ID={int(target_jig_id)}"
        )

    occupied_count_by_slot: dict[str, int] = {}
    planned_occupied_by_slot: dict[str, set[int]] = {}
    for cfg in target_cfgs:
        rack_id = world.rack_placements.get((station_id, cfg.slot_id))
        if not rack_id:
            continue
        rack = world.racks.get(rack_id)
        if rack is None:
            continue
        occupied_count_by_slot[str(cfg.slot_id)] = len(rack.occupied_slots)
        planned_occupied_by_slot[str(cfg.slot_id)] = set(int(idx) for idx in rack.occupied_slots.keys())

    if not occupied_count_by_slot:
        return []

    target_fill_depth = max(occupied_count_by_slot.values())
    if target_fill_depth <= 0:
        return []

    source_positions = _probe_positions_for_jig(
        world,
        station_id=station_id,
        jig_id=int(tara_jig_id),
        probe_prefixes=probe_prefixes,
    )
    source_index = 0

    moves: List[PlannedSampleMove] = []
    for cfg in target_cfgs:
        slot_id = str(cfg.slot_id)
        current_count = int(occupied_count_by_slot.get(slot_id, 0))
        missing = max(0, target_fill_depth - current_count)
        if missing <= 0:
            continue

        for _ in range(missing):
            if source_index >= len(source_positions):
                raise ValueError(
                    "Tara balance prerequisite failed: insufficient Tara probes "
                    f"in station '{station_id}' JIG_ID={int(tara_jig_id)}"
                )
            free_slots = _free_slot_indices(
                world,
                station_id=station_id,
                station_slot_id=slot_id,
                planned_occupied=planned_occupied_by_slot.get(slot_id, set()),
            )
            if not free_slots:
                raise ValueError(
                    f"Tara balance prerequisite failed: no free target slot in "
                    f"'{station_id}.{slot_id}' for JIG_ID={int(target_jig_id)}"
                )

            src_slot_id, src_slot_index, sample_id = source_positions[source_index]
            source_index += 1
            target_slot_index = int(free_slots[0])
            planned_occupied_by_slot.setdefault(slot_id, set()).add(target_slot_index)
            moves.append(
                PlannedSampleMove(
                    sample_id=str(sample_id),
                    source_station_id=station_id,
                    source_station_slot_id=str(src_slot_id),
                    source_slot_index=int(src_slot_index),
                    target_station_id=station_id,
                    target_station_slot_id=slot_id,
                    target_slot_index=target_slot_index,
                    reason="tara_balance",
                )
            )
    return moves


def plan_tara_return_moves(
    world: Any,
    *,
    station_id: str,
    source_jig_id: int,
    tara_jig_id: int = 3,
    probe_prefixes: Optional[Sequence[str]] = None,
) -> List[PlannedSampleMove]:
    probe_positions = _probe_positions_for_jig(
        world,
        station_id=station_id,
        jig_id=int(source_jig_id),
        probe_prefixes=probe_prefixes,
    )
    if not probe_positions:
        return []

    tara_cfgs = world.slots_for_jig(station_id, int(tara_jig_id))
    if not tara_cfgs:
        raise ValueError(
            f"Tara return prerequisite failed: no Tara slots for station '{station_id}' JIG_ID={int(tara_jig_id)}"
        )

    free_targets: List[Tuple[str, int]] = []
    planned_occupied_by_slot: dict[str, set[int]] = {}
    for cfg in tara_cfgs:
        slot_id = str(cfg.slot_id)
        free_slots = _free_slot_indices(
            world,
            station_id=station_id,
            station_slot_id=slot_id,
            planned_occupied=planned_occupied_by_slot.get(slot_id, set()),
        )
        for slot_index in free_slots:
            free_targets.append((slot_id, int(slot_index)))

    if len(free_targets) < len(probe_positions):
        raise ValueError(
            "Tara return prerequisite failed: insufficient free Tara slots for probe return. "
            f"Need={len(probe_positions)}, Free={len(free_targets)}"
        )

    moves: List[PlannedSampleMove] = []
    for idx, (src_slot_id, src_slot_index, sample_id) in enumerate(probe_positions):
        target_slot_id, target_slot_index = free_targets[idx]
        moves.append(
            PlannedSampleMove(
                sample_id=str(sample_id),
                source_station_id=station_id,
                source_station_slot_id=str(src_slot_id),
                source_slot_index=int(src_slot_index),
                target_station_id=station_id,
                target_station_slot_id=str(target_slot_id),
                target_slot_index=int(target_slot_index),
                reason="tara_return",
            )
        )
    return moves


__all__ = [
    "PlannedSampleMove",
    "TARA_PROBE_PREFIXES_DEFAULT",
    "is_tara_probe_sample_id",
    "select_next_target_slot_for_jig",
    "plan_tara_balance_moves",
    "plan_tara_return_moves",
]

