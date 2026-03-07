from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import List

from world.lab_world import ProcessType, RackType, SlotKind, StationKind, WorldConfigManager

DEFAULT_CONFIG_PATH = Path(__file__).resolve().with_name("world_config.json")


def _split_csv(value: str) -> List[str]:
    if not value.strip():
        return []
    return [part.strip() for part in value.split(",") if part.strip()]


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Edit world_config JSON/YAML files.")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH), help="Path to world config file")

    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("show", help="Print current config")
    sub.add_parser("validate", help="Validate config by loading it into WorldModel")

    p_station = sub.add_parser("upsert-station", help="Add or update a station")
    p_station.add_argument("--id", required=True, dest="station_id")
    p_station.add_argument("--name")
    p_station.add_argument("--itm-id", type=int)
    p_station.add_argument("--kind", choices=[e.value for e in StationKind])
    p_station.add_argument("--amr-pos-target")
    p_station.add_argument("--clear-amr-pos-target", action="store_true")
    p_station.add_argument("--landmark-id")
    p_station.add_argument("--clear-landmark-id", action="store_true")

    p_slot = sub.add_parser("upsert-slot", help="Add or update a station slot")
    p_slot.add_argument("--station-id", required=True)
    p_slot.add_argument("--slot-id", required=True)
    p_slot.add_argument("--kind", required=True, choices=[e.value for e in SlotKind])
    p_slot.add_argument("--jig-id", type=int, required=True)
    p_slot.add_argument("--itm-id", type=int, default=1)
    p_slot.add_argument("--rack-capacity", type=int, default=1)
    p_slot.add_argument("--rack-pattern")
    p_slot.add_argument("--rack-rows", type=int)
    p_slot.add_argument("--rack-cols", type=int)
    p_slot.add_argument("--rack-index", type=int, default=1, help="Position index within jig rack receiver")
    p_slot.add_argument("--obj-nbr-offset", type=int, default=0, help="Offset for OBJ_Nbr mapping inside jig")
    p_slot.add_argument("--accepted-rack-types", default="")

    p_landmark = sub.add_parser("upsert-landmark", help="Add or update landmark")
    p_landmark.add_argument("--id", required=True, dest="landmark_id")
    p_landmark.add_argument("--code", required=True)
    p_landmark.add_argument("--station-id", required=True)

    p_rack = sub.add_parser("upsert-rack", help="Add or update rack")
    p_rack.add_argument("--id", required=True, dest="rack_id")
    p_rack.add_argument("--rack-type", required=True, choices=[e.value for e in RackType])
    p_rack.add_argument("--capacity", type=int, required=True)
    p_rack.add_argument("--pattern", required=True)
    p_rack.add_argument("--pin-obj-type", type=int, required=True)
    p_rack.add_argument("--rows", type=int)
    p_rack.add_argument("--cols", type=int)
    p_rack.add_argument("--blocked-slots", default="", help="Comma-separated blocked slot indexes (e.g. 15,18)")

    p_device = sub.add_parser("upsert-device", help="Add or update device")
    p_device.add_argument("--id", required=True, dest="device_id")
    p_device.add_argument("--name", required=True)
    p_device.add_argument("--station-id", required=True)
    p_device.add_argument("--capabilities", required=True, help="Comma-separated ProcessType list")

    p_place = sub.add_parser("set-placement", help="Set rack placement on a station slot")
    p_place.add_argument("--station-id", required=True)
    p_place.add_argument("--slot-id", required=True)
    p_place.add_argument("--rack-id", required=True)

    p_clear_place = sub.add_parser("clear-placement", help="Clear rack placement from a station slot")
    p_clear_place.add_argument("--station-id", required=True)
    p_clear_place.add_argument("--slot-id", required=True)

    p_robot = sub.add_parser("set-robot-station", help="Set robot current station")
    p_robot.add_argument("--station-id", required=True)

    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    config_path = Path(args.config)
    manager = WorldConfigManager(config_path, create_if_missing=True)

    if args.cmd == "show":
        print(json.dumps(manager.data, indent=2))
        return

    if args.cmd == "validate":
        world = manager.to_world()
        summary = manager.summary()
        print(f"Valid: stations={summary['stations']} racks={summary['racks']} devices={summary['devices']}")
        print(f"Robot current station: {world.robot_current_station_id}")
        return

    if args.cmd == "upsert-station":
        existing_station = manager.get_station_config(args.station_id)
        amr_pos_target = args.amr_pos_target
        if args.clear_amr_pos_target:
            amr_pos_target = None
        elif args.amr_pos_target is None and existing_station is not None:
            amr_pos_target = existing_station.get("amr_pos_target")

        landmark_id = args.landmark_id
        if args.clear_landmark_id:
            landmark_id = None
        elif args.landmark_id is None and existing_station is not None:
            landmark_id = existing_station.get("landmark_id")

        manager.upsert_station(
            station_id=args.station_id,
            name=args.name,
            itm_id=args.itm_id,
            kind=args.kind,
            amr_pos_target=amr_pos_target,
            landmark_id=landmark_id,
        )

    elif args.cmd == "upsert-slot":
        accepted = _split_csv(args.accepted_rack_types)
        manager.upsert_station_slot(
            station_id=args.station_id,
            slot_id=args.slot_id,
            kind=args.kind,
            jig_id=args.jig_id,
            itm_id=args.itm_id,
            rack_capacity=args.rack_capacity,
            rack_pattern=args.rack_pattern,
            rack_rows=args.rack_rows,
            rack_cols=args.rack_cols,
            rack_index=args.rack_index,
            obj_nbr_offset=args.obj_nbr_offset,
            accepted_rack_types=accepted,
        )

    elif args.cmd == "upsert-landmark":
        manager.upsert_landmark(
            landmark_id=args.landmark_id,
            code=args.code,
            station_id=args.station_id,
        )

    elif args.cmd == "upsert-rack":
        blocked_slots = [int(x) for x in _split_csv(args.blocked_slots)] if args.blocked_slots else None
        manager.upsert_rack(
            rack_id=args.rack_id,
            rack_type=args.rack_type,
            capacity=args.capacity,
            pattern=args.pattern,
            pin_obj_type=args.pin_obj_type,
            rows=args.rows,
            cols=args.cols,
            blocked_slots=blocked_slots,
        )

    elif args.cmd == "upsert-device":
        caps = _split_csv(args.capabilities)
        manager.upsert_device(
            device_id=args.device_id,
            name=args.name,
            station_id=args.station_id,
            capabilities=[ProcessType(c.upper()) for c in caps],
        )

    elif args.cmd == "set-placement":
        manager.set_rack_placement(
            station_id=args.station_id,
            station_slot_id=args.slot_id,
            rack_id=args.rack_id,
        )

    elif args.cmd == "clear-placement":
        manager.clear_rack_placement(
            station_id=args.station_id,
            station_slot_id=args.slot_id,
        )

    elif args.cmd == "set-robot-station":
        manager.set_robot_station(args.station_id)

    # Validate before save to prevent writing broken world files.
    manager.to_world()
    manager.save()
    print(f"Updated config: {config_path.resolve()}")


if __name__ == "__main__":
    main()
