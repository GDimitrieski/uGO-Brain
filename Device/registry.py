from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from .analyzer_device import AnalyzerDeviceCapabilities, AnalyzerDeviceIdentity
from .centrifuge_device import CentrifugeAnalyzerDevice
from .centrifuge_xmlrpc_adapter import (
    DEFAULT_CENTRIFUGE_RPC_URL,
    DEFAULT_CENTRIFUGE_RPC_TIMEOUT_S,
    CentrifugeXmlRpcAdapter,
)
from .wise_adapter import (
    DEFAULT_WISE_DI_ENDPOINT_TEMPLATE,
    DEFAULT_WISE_POLL_INTERVAL_S,
    DEFAULT_WISE_STALE_AFTER_S,
    DEFAULT_WISE_TIMEOUT_S,
    WiseModuleAdapter,
)


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


def _infer_centrifuge_rpc_url(metadata: Dict[str, Any]) -> str:
    candidates = [
        metadata.get("centrifuge_rpc_url"),
        metadata.get("xmlrpc_url"),
        metadata.get("rpc_url"),
    ]
    for candidate in candidates:
        if candidate is None:
            continue
        txt = str(candidate).strip()
        if txt:
            return txt
    return DEFAULT_CENTRIFUGE_RPC_URL


def _infer_float(metadata: Dict[str, Any], keys: Sequence[str], default: float) -> float:
    for key in keys:
        value = metadata.get(key)
        if value in {None, ""}:
            continue
        try:
            return float(value)
        except Exception:
            continue
    return float(default)


def _infer_int(metadata: Dict[str, Any], keys: Sequence[str], default: int) -> int:
    for key in keys:
        value = metadata.get(key)
        if value in {None, ""}:
            continue
        try:
            return int(value)
        except Exception:
            continue
    return int(default)


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


def _as_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return {str(k): v for k, v in value.items()}
    return {}


def _as_str(value: Any) -> str:
    return str(value or "").strip()


def _build_wise_adapter_from_metadata(device_id: str, metadata: Dict[str, Any]) -> Optional[WiseModuleAdapter]:
    raw_cfg = _as_dict(metadata.get("wise"))
    if not raw_cfg:
        return None

    enabled = _to_bool(raw_cfg.get("enabled"), False)
    if not enabled:
        return None

    host = _as_str(raw_cfg.get("host")) or _as_str(raw_cfg.get("ip")) or _as_str(raw_cfg.get("hostname"))
    if not host:
        raise ValueError("wise.enabled=true but 'wise.host'/'wise.ip' is missing")

    auth_cfg = _as_dict(raw_cfg.get("auth"))
    username = _as_str(raw_cfg.get("username")) or _as_str(auth_cfg.get("username"))
    password = _as_str(raw_cfg.get("password")) or _as_str(auth_cfg.get("password"))
    endpoint_template = (
        _as_str(raw_cfg.get("di_endpoint_template"))
        or _as_str(raw_cfg.get("di_endpoint"))
        or DEFAULT_WISE_DI_ENDPOINT_TEMPLATE
    )
    try:
        return WiseModuleAdapter(
            host=host,
            port=int(raw_cfg.get("port", 80)),
            scheme=_as_str(raw_cfg.get("scheme")) or "http",
            username=username,
            password=password,
            di_slot=int(raw_cfg.get("di_slot", raw_cfg.get("slot", 0))),
            di_endpoint_template=endpoint_template,
            timeout_s=float(raw_cfg.get("timeout_s", DEFAULT_WISE_TIMEOUT_S)),
            verify_tls=_to_bool(raw_cfg.get("verify_tls"), True),
            poll_interval_s=float(raw_cfg.get("poll_interval_s", DEFAULT_WISE_POLL_INTERVAL_S)),
            stale_after_s=float(raw_cfg.get("stale_after_s", DEFAULT_WISE_STALE_AFTER_S)),
        )
    except Exception as exc:
        raise ValueError(f"invalid Wise settings: {exc}") from exc


@dataclass
class DeviceRegistry:
    centrifuges: Dict[str, CentrifugeAnalyzerDevice] = field(default_factory=dict)
    wise_modules: Dict[str, WiseModuleAdapter] = field(default_factory=dict)
    device_station_ids: Dict[str, str] = field(default_factory=dict)

    def register_centrifuge(self, device: CentrifugeAnalyzerDevice) -> None:
        self.centrifuges[device.identity.device_id] = device

    def register_wise_module(self, device_id: str, module: WiseModuleAdapter) -> None:
        self.wise_modules[str(device_id).strip()] = module

    def register_device_station(self, device_id: str, station_id: str) -> None:
        self.device_station_ids[str(device_id).strip()] = str(station_id).strip()

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

    def get_wise_module(self, device_id: str) -> WiseModuleAdapter:
        key = str(device_id).strip()
        if key not in self.wise_modules:
            raise KeyError(f"Unknown Wise module for device '{device_id}'")
        return self.wise_modules[key]

    def get_wise_modules(self) -> Dict[str, WiseModuleAdapter]:
        return dict(self.wise_modules)

    def get_wise_modules_at_station(self, station_id: str) -> List[Tuple[str, WiseModuleAdapter]]:
        sid = str(station_id).strip()
        out: List[Tuple[str, WiseModuleAdapter]] = []
        for device_id in sorted(self.wise_modules.keys()):
            if self.device_station_ids.get(device_id) == sid:
                out.append((device_id, self.wise_modules[device_id]))
        return out


def build_device_registry_from_world(world: Any) -> DeviceRegistry:
    registry = DeviceRegistry()
    for world_device in world.devices.values():
        device_id = str(world_device.id)
        station_id = str(world_device.station_id)
        registry.register_device_station(device_id, station_id)

        metadata = dict(world_device.metadata) if isinstance(world_device.metadata, dict) else {}
        wise_adapter = _build_wise_adapter_from_metadata(device_id, metadata)
        if wise_adapter is not None:
            registry.register_wise_module(device_id, wise_adapter)

        process_names = [
            str(getattr(process, "value", process)).strip().upper()
            for process in world_device.capabilities
        ]
        if "CENTRIFUGATION" not in process_names:
            continue

        identity = AnalyzerDeviceIdentity(
            device_id=device_id,
            name=str(world_device.name),
            station_id=station_id,
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
                controller=CentrifugeXmlRpcAdapter(
                    rpc_url=_infer_centrifuge_rpc_url(metadata),
                    rpc_timeout_s=_infer_float(
                        metadata,
                        ("rpc_timeout_s", "xmlrpc_timeout_s"),
                        DEFAULT_CENTRIFUGE_RPC_TIMEOUT_S,
                    ),
                    state_wait_timeout_s=_infer_float(metadata, ("state_wait_timeout_s",), 60.0),
                    start_wait_timeout_s=_infer_float(metadata, ("start_wait_timeout_s",), 60.0),
                    inspect_attempts=_infer_int(metadata, ("inspect_attempts",), 5),
                    inspect_poll_s=_infer_float(metadata, ("inspect_poll_s",), 1.0),
                    state_poll_s=_infer_float(metadata, ("state_poll_s",), 0.5),
                    rotor_settle_s=_infer_float(metadata, ("rotor_settle_s",), 1.5),
                ),
            )
        )
    return registry
