"""Service layer wrapping WorldConfigManager with delete ops, validation, and enum introspection."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from world.lab_world import (
    CapState,
    ProcessType,
    RackType,
    SlotKind,
    StationKind,
    WorldConfigManager,
)


def _resolve_policies_path() -> Path:
    env = os.environ.get("UGO_PROJECT_ROOT")
    if env:
        return Path(env).resolve() / "planning" / "process_policies.json"
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent.parent / "planning" / "process_policies.json"
    return Path(__file__).resolve().parents[2] / "planning" / "process_policies.json"


PROCESS_POLICIES_PATH = _resolve_policies_path()


class WorldConfigService:
    """Wraps WorldConfigManager with GUI-specific operations."""

    def __init__(self, config_path: Path) -> None:
        self.config_path = config_path
        self.manager = WorldConfigManager(config_path, create_if_missing=True)

    # ------------------------------------------------------------------
    # Delegated read operations
    # ------------------------------------------------------------------

    def get_config(self) -> Dict[str, Any]:
        return self.manager.data

    def get_summary(self) -> Dict[str, int]:
        return self.manager.summary()

    def get_stations(self) -> List[Dict[str, Any]]:
        return list(self.manager.data.get("stations", []))

    def get_racks(self) -> List[Dict[str, Any]]:
        return list(self.manager.data.get("racks", []))

    def get_devices(self) -> List[Dict[str, Any]]:
        return list(self.manager.data.get("devices", []))

    def get_landmarks(self) -> List[Dict[str, Any]]:
        return list(self.manager.data.get("landmarks", []))

    def get_placements(self) -> List[Dict[str, Any]]:
        return list(self.manager.data.get("rack_placements", []))

    def get_samples(self) -> List[Dict[str, Any]]:
        return list(self.manager.data.get("samples", []))

    def get_robot_station(self) -> Optional[str]:
        return self.manager.data.get("robot_current_station_id")

    # ------------------------------------------------------------------
    # Delegated write operations (upserts from WorldConfigManager)
    # ------------------------------------------------------------------

    def upsert_station(self, data: Dict[str, Any]) -> None:
        self.manager.upsert_station(
            station_id=data["id"],
            name=data.get("name"),
            itm_id=data.get("itm_id"),
            kind=data.get("kind"),
            amr_pos_target=data.get("amr_pos_target"),
            landmark_id=data.get("landmark_id"),
        )

    def upsert_station_slot(self, station_id: str, data: Dict[str, Any]) -> None:
        self.manager.upsert_station_slot(
            station_id=station_id,
            slot_id=data["slot_id"],
            kind=data["kind"],
            jig_id=int(data["jig_id"]),
            itm_id=int(data.get("itm_id", 1)),
            rack_capacity=int(data.get("rack_capacity", 1)),
            rack_pattern=data.get("rack_pattern"),
            rack_rows=data.get("rack_rows"),
            rack_cols=data.get("rack_cols"),
            rack_index=int(data.get("rack_index", 1)),
            obj_nbr_offset=int(data.get("obj_nbr_offset", 0)),
            loading_strategy=data.get("loading_strategy", ""),
            accepted_rack_types=data.get("accepted_rack_types", []),
        )

    def upsert_rack(self, data: Dict[str, Any]) -> None:
        self.manager.upsert_rack(
            rack_id=data["id"],
            rack_type=data["rack_type"],
            capacity=int(data["capacity"]),
            pattern=data["pattern"],
            pin_obj_type=int(data["pin_obj_type"]),
            rows=data.get("rows"),
            cols=data.get("cols"),
            blocked_slots=data.get("blocked_slots"),
        )

    def upsert_device(self, data: Dict[str, Any]) -> None:
        # Preserve extra keys (wise, device_capabilities, rotor_configuration, etc.)
        # that WorldConfigManager.upsert_device would otherwise discard.
        existing = self._find_device(data["id"])
        extra: Dict[str, Any] = {}
        if existing:
            standard_keys = {"id", "name", "station_id", "capabilities", "planner_role", "exclude_station_racks_from_idle_return"}
            extra = {k: v for k, v in existing.items() if k not in standard_keys}
        self.manager.upsert_device(
            device_id=data["id"],
            name=data["name"],
            station_id=data["station_id"],
            capabilities=data.get("capabilities", []),
            planner_role=data.get("planner_role", "PROCESSOR"),
            exclude_station_racks_from_idle_return=data.get("exclude_station_racks_from_idle_return", True),
            metadata=extra or None,
        )

    def _find_device(self, device_id: str) -> Optional[Dict[str, Any]]:
        for d in self.manager.data.get("devices", []):
            if str(d.get("id")) == device_id:
                return d
        return None

    def update_device_wise(self, device_id: str, wise_config: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Update or remove the WISE configuration block on a device."""
        device = self._find_device(device_id)
        if device is None:
            return {"updated": False, "reason": f"Device '{device_id}' not found"}
        if wise_config is None:
            device.pop("wise", None)
        else:
            device["wise"] = wise_config
        return {"updated": True}

    def upsert_landmark(self, data: Dict[str, Any]) -> None:
        self.manager.upsert_landmark(
            landmark_id=data["id"],
            code=data["code"],
            station_id=data["station_id"],
        )

    def upsert_sample(self, data: Dict[str, Any]) -> None:
        samples = self.manager.data.setdefault("samples", [])
        sample_id = data["id"]
        item = {
            "id": sample_id,
            "barcode": data.get("barcode", sample_id),
            "obj_type": int(data.get("obj_type", 811)),
            "length_mm": float(data.get("length_mm", 75.0)),
            "diameter_mm": float(data.get("diameter_mm", 13.0)),
            "cap_state": data.get("cap_state", "CAPPED"),
            "required_processes": data.get("required_processes", []),
        }
        for i, s in enumerate(samples):
            if str(s.get("id")) == sample_id:
                samples[i] = item
                return
        samples.append(item)

    def set_placement(self, data: Dict[str, Any]) -> None:
        self.manager.set_rack_placement(
            station_id=data["station_id"],
            station_slot_id=data["station_slot_id"],
            rack_id=data["rack_id"],
        )

    def clear_placement(self, station_id: str, slot_id: str) -> None:
        self.manager.clear_rack_placement(station_id, slot_id)

    def set_robot_station(self, station_id: Optional[str]) -> None:
        self.manager.set_robot_station(station_id)

    # ------------------------------------------------------------------
    # Delete operations (not available in WorldConfigManager)
    # ------------------------------------------------------------------

    def delete_station(self, station_id: str) -> Dict[str, Any]:
        """Delete station and cascade-remove placements and landmarks referencing it."""
        stations = self.manager.data.get("stations", [])
        original_count = len(stations)
        self.manager.data["stations"] = [s for s in stations if str(s.get("id")) != station_id]
        removed = original_count - len(self.manager.data["stations"])
        if removed == 0:
            return {"deleted": False, "reason": f"Station '{station_id}' not found"}

        # Cascade: remove placements for this station
        placements = self.manager.data.get("rack_placements", [])
        self.manager.data["rack_placements"] = [
            p for p in placements if str(p.get("station_id")) != station_id
        ]
        cascade_placements = len(placements) - len(self.manager.data["rack_placements"])

        # Cascade: remove landmarks referencing this station
        landmarks = self.manager.data.get("landmarks", [])
        self.manager.data["landmarks"] = [
            lm for lm in landmarks if str(lm.get("station_id")) != station_id
        ]
        cascade_landmarks = len(landmarks) - len(self.manager.data["landmarks"])

        # Cascade: remove devices at this station
        devices = self.manager.data.get("devices", [])
        self.manager.data["devices"] = [
            d for d in devices if str(d.get("station_id")) != station_id
        ]
        cascade_devices = len(devices) - len(self.manager.data["devices"])

        return {
            "deleted": True,
            "cascade": {
                "placements_removed": cascade_placements,
                "landmarks_removed": cascade_landmarks,
                "devices_removed": cascade_devices,
            },
        }

    def delete_rack(self, rack_id: str) -> Dict[str, Any]:
        """Delete rack and cascade-remove its placements."""
        racks = self.manager.data.get("racks", [])
        original_count = len(racks)
        self.manager.data["racks"] = [r for r in racks if str(r.get("id")) != rack_id]
        removed = original_count - len(self.manager.data["racks"])
        if removed == 0:
            return {"deleted": False, "reason": f"Rack '{rack_id}' not found"}

        placements = self.manager.data.get("rack_placements", [])
        self.manager.data["rack_placements"] = [
            p for p in placements if str(p.get("rack_id")) != rack_id
        ]
        cascade_placements = len(placements) - len(self.manager.data["rack_placements"])

        return {"deleted": True, "cascade": {"placements_removed": cascade_placements}}

    def delete_device(self, device_id: str) -> Dict[str, Any]:
        devices = self.manager.data.get("devices", [])
        original_count = len(devices)
        self.manager.data["devices"] = [d for d in devices if str(d.get("id")) != device_id]
        removed = original_count - len(self.manager.data["devices"])
        if removed == 0:
            return {"deleted": False, "reason": f"Device '{device_id}' not found"}
        return {"deleted": True}

    def delete_landmark(self, landmark_id: str) -> Dict[str, Any]:
        landmarks = self.manager.data.get("landmarks", [])
        original_count = len(landmarks)
        self.manager.data["landmarks"] = [
            lm for lm in landmarks if str(lm.get("id")) != landmark_id
        ]
        removed = original_count - len(self.manager.data["landmarks"])
        if removed == 0:
            return {"deleted": False, "reason": f"Landmark '{landmark_id}' not found"}
        return {"deleted": True}

    def delete_station_slot(self, station_id: str, slot_id: str) -> Dict[str, Any]:
        for station in self.manager.data.get("stations", []):
            if str(station.get("id")) != station_id:
                continue
            slots = station.get("slot_configs", [])
            original_count = len(slots)
            station["slot_configs"] = [s for s in slots if str(s.get("slot_id")) != slot_id]
            removed = original_count - len(station["slot_configs"])
            if removed == 0:
                return {"deleted": False, "reason": f"Slot '{slot_id}' not found on station '{station_id}'"}

            # Cascade: remove placement for this slot
            placements = self.manager.data.get("rack_placements", [])
            self.manager.data["rack_placements"] = [
                p for p in placements
                if not (str(p.get("station_id")) == station_id and str(p.get("station_slot_id")) == slot_id)
            ]
            cascade_placements = len(placements) - len(self.manager.data["rack_placements"])
            return {"deleted": True, "cascade": {"placements_removed": cascade_placements}}

        return {"deleted": False, "reason": f"Station '{station_id}' not found"}

    def delete_sample(self, sample_id: str) -> Dict[str, Any]:
        samples = self.manager.data.get("samples", [])
        original_count = len(samples)
        self.manager.data["samples"] = [s for s in samples if str(s.get("id")) != sample_id]
        removed = original_count - len(self.manager.data["samples"])
        if removed == 0:
            return {"deleted": False, "reason": f"Sample '{sample_id}' not found"}
        return {"deleted": True}

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def validate(self) -> Dict[str, Any]:
        """Validate current config by attempting to build WorldModel."""
        try:
            self.manager.to_world()
            return {"valid": True, "errors": []}
        except Exception as exc:
            return {"valid": False, "errors": [str(exc)]}

    # ------------------------------------------------------------------
    # File operations
    # ------------------------------------------------------------------

    def save(self) -> Dict[str, Any]:
        """Validate then save to disk."""
        result = self.validate()
        if not result["valid"]:
            return {"saved": False, "errors": result["errors"]}
        self.manager.save()
        return {"saved": True, "errors": []}

    def reload(self) -> None:
        """Reload config from disk."""
        self.manager.reload()

    # ------------------------------------------------------------------
    # Enums for UI dropdowns
    # ------------------------------------------------------------------

    @staticmethod
    def get_enums() -> Dict[str, List[str]]:
        return {
            "StationKind": [e.value for e in StationKind],
            "SlotKind": [e.value for e in SlotKind],
            "RackType": [e.value for e in RackType],
            "ProcessType": [e.value for e in ProcessType],
            "CapState": [e.value for e in CapState],
        }

    # ------------------------------------------------------------------
    # Process policies (read-only)
    # ------------------------------------------------------------------

    @staticmethod
    def get_policies() -> Dict[str, Any]:
        if PROCESS_POLICIES_PATH.exists():
            with open(PROCESS_POLICIES_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        return {}
