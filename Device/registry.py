from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence

from .analyzer_device import AnalyzerDeviceCapabilities, AnalyzerDeviceIdentity
from .centrifuge_device import CentrifugeAnalyzerDevice


def _to_upper_values(values: Iterable[Any]) -> List[str]:
    out: List[str] = []
    for value in values:
        txt = str(value).strip().upper()
        if txt:
            out.append(txt)
    return out


def _infer_supported_rack_types(metadata: Dict[str, Any]) -> Sequence[str]:
    raw = metadata.get("supported_rack_types")
    if isinstance(raw, (list, tuple, set)):
        values = _to_upper_values(raw)
        if values:
            return tuple(values)
    return ("CENTRIFUGE_RACK",)


def _infer_max_racks(world: Any, station_id: str, metadata: Dict[str, Any]) -> int:
    raw_value = metadata.get("max_racks")
    if raw_value in {None, ""}:
        raw_value = metadata.get("max_carriers")
    if raw_value not in {None, ""}:
        try:
            parsed = int(raw_value)
            if parsed > 0:
                return parsed
        except Exception:
            pass

    station = world.stations.get(station_id)
    if station is not None:
        count = 0
        for slot_cfg in station.slot_configs.values():
            kind = getattr(slot_cfg, "kind", None)
            kind_txt = str(getattr(kind, "value", kind)).strip().upper()
            if kind_txt == "CENTRIFUGE_RACK_SLOT":
                count += 1
        if count > 0:
            return count
    return 1


@dataclass
class DeviceRegistry:
    centrifuges: Dict[str, CentrifugeAnalyzerDevice] = field(default_factory=dict)

    def register_centrifuge(self, device: CentrifugeAnalyzerDevice) -> None:
        self.centrifuges[device.identity.device_id] = device

    def get_centrifuge(self, device_id: str) -> CentrifugeAnalyzerDevice:
        key = str(device_id).strip()
        if key not in self.centrifuges:
            raise KeyError(f"Unknown centrifuge device '{device_id}'")
        return self.centrifuges[key]

    def get_centrifuges_at_station(self, station_id: str) -> List[CentrifugeAnalyzerDevice]:
        sid = str(station_id).strip()
        out: List[CentrifugeAnalyzerDevice] = []
        for dev_id in sorted(self.centrifuges.keys()):
            dev = self.centrifuges[dev_id]
            if dev.identity.station_id == sid:
                out.append(dev)
        return out

    def get_first_centrifuge_at_station(self, station_id: str) -> Optional[CentrifugeAnalyzerDevice]:
        devices = self.get_centrifuges_at_station(station_id)
        if not devices:
            return None
        return devices[0]


def build_device_registry_from_world(world: Any) -> DeviceRegistry:
    registry = DeviceRegistry()
    for world_device in world.devices.values():
        process_names = [
            str(getattr(process, "value", process)).strip().upper()
            for process in world_device.capabilities
        ]
        if "CENTRIFUGATION" not in process_names:
            continue

        metadata = dict(world_device.metadata) if isinstance(world_device.metadata, dict) else {}
        identity = AnalyzerDeviceIdentity(
            device_id=str(world_device.id),
            name=str(world_device.name),
            station_id=str(world_device.station_id),
            model=str(metadata.get("model", "")),
        )
        capabilities = AnalyzerDeviceCapabilities(
            supported_processes=tuple(sorted(set(process_names))),
            supported_rack_types=tuple(_infer_supported_rack_types(metadata)),
            max_racks=_infer_max_racks(world, str(world_device.station_id), metadata),
            metadata=metadata,
        )
        registry.register_centrifuge(
            CentrifugeAnalyzerDevice(
                identity=identity,
                capabilities=capabilities,
                usage_profile=metadata.get("usage_profile", metadata.get("usage_strategy")),
            )
        )
    return registry
