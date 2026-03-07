from __future__ import annotations

from typing import Any, Mapping, Optional, Sequence, Tuple

from .analyzers import BioradIh1000Device, BioradIh500Device
from ..models import DeviceCapabilities, DeviceIdentity, LoadInterfaceConfig
from ..sample_processing_device import SampleProcessingDevice
from ..strategies import (
    start_strategy_from_config,
    status_strategy_from_config,
)


def _as_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    txt = str(value).strip().lower()
    if txt in {"1", "true", "yes", "on"}:
        return True
    if txt in {"0", "false", "no", "off"}:
        return False
    return default


def _as_optional_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except Exception:
        return None


def _as_str_tuple(raw: Any) -> Tuple[str, ...]:
    if isinstance(raw, (list, tuple)):
        out = []
        for item in raw:
            txt = str(item).strip()
            if txt:
                out.append(txt)
        return tuple(out)
    if raw is None:
        return ()
    txt = str(raw).strip()
    return (txt,) if txt else ()


def _build_identity(raw: Mapping[str, Any]) -> DeviceIdentity:
    metadata = raw.get("identity_metadata")
    return DeviceIdentity(
        device_id=str(raw.get("id", "")),
        name=str(raw.get("name", raw.get("id", ""))),
        model=str(raw.get("model", "")),
        station_id=str(raw.get("station_id", "")),
        landmark_id=(
            str(raw.get("landmark_id"))
            if raw.get("landmark_id") not in {None, ""}
            else None
        ),
        metadata=dict(metadata) if isinstance(metadata, dict) else {},
    )


def _build_capabilities(
    raw: Mapping[str, Any],
    *,
    default_supported_processes: Sequence[str],
    default_continuous_loading: bool,
    default_auto_start: bool,
    default_nominal_sample_capacity: Optional[int],
) -> DeviceCapabilities:
    cfg_raw = raw.get("device_capabilities")
    cfg = cfg_raw if isinstance(cfg_raw, dict) else {}
    supported = _as_str_tuple(cfg.get("supported_processes", raw.get("capabilities", default_supported_processes)))
    metadata = cfg.get("metadata")
    return DeviceCapabilities(
        supported_processes=supported or tuple(default_supported_processes),
        continuous_loading=_as_bool(cfg.get("continuous_loading"), default_continuous_loading),
        auto_start=_as_bool(cfg.get("auto_start"), default_auto_start),
        nominal_sample_capacity=_as_optional_int(
            cfg.get("nominal_sample_capacity", default_nominal_sample_capacity)
        ),
        max_carriers=_as_optional_int(cfg.get("max_carriers")),
        metadata=dict(metadata) if isinstance(metadata, dict) else {},
    )


def _build_load_interface(raw: Mapping[str, Any], default_loading_area: str) -> LoadInterfaceConfig:
    cfg_raw = raw.get("load_interface")
    cfg = cfg_raw if isinstance(cfg_raw, dict) else {}
    metadata = cfg.get("metadata")
    rack_geometry = cfg.get("rack_geometry")
    slot_layout = cfg.get("slot_layout")
    return LoadInterfaceConfig(
        carrier_type=str(cfg.get("carrier_type", "RACK")),
        loading_area=str(cfg.get("loading_area", default_loading_area)),
        rack_geometry=dict(rack_geometry) if isinstance(rack_geometry, dict) else {},
        slot_layout=dict(slot_layout) if isinstance(slot_layout, dict) else {},
        max_carriers=_as_optional_int(cfg.get("max_carriers")),
        metadata=dict(metadata) if isinstance(metadata, dict) else {},
    )


def _matches_ih500(raw: Mapping[str, Any]) -> bool:
    cls = str(raw.get("device_class", "")).strip().lower()
    model = str(raw.get("model", "")).strip().lower()
    name = str(raw.get("name", "")).strip().lower()
    return cls == "bioradih500device" or "ih500" in model or "ih-500" in name


def _matches_ih1000(raw: Mapping[str, Any]) -> bool:
    cls = str(raw.get("device_class", "")).strip().lower()
    model = str(raw.get("model", "")).strip().lower()
    name = str(raw.get("name", "")).strip().lower()
    return cls == "bioradih1000device" or "ih1000" in model or "ih-1000" in name


def create_processing_device(raw: Mapping[str, Any]) -> Optional[SampleProcessingDevice]:
    if not isinstance(raw, Mapping):
        return None
    if "station_id" not in raw or "id" not in raw:
        return None

    identity = _build_identity(raw)
    start_strategy = start_strategy_from_config(
        raw.get("start_strategy") if isinstance(raw.get("start_strategy"), dict) else None
    )
    status_strategy = status_strategy_from_config(
        raw.get("status_strategy") if isinstance(raw.get("status_strategy"), dict) else None
    )

    if _matches_ih500(raw):
        return BioradIh500Device(
            identity=identity,
            capabilities=_build_capabilities(
                raw,
                default_supported_processes=("IMMUNOANALYSIS",),
                default_continuous_loading=True,
                default_auto_start=False,
                default_nominal_sample_capacity=50,
            ),
            load_interface=_build_load_interface(raw, default_loading_area="SEPARATE_LOADING_AREA"),
            start_strategy=start_strategy,
            status_strategy=status_strategy,
        )

    if _matches_ih1000(raw):
        return BioradIh1000Device(
            identity=identity,
            capabilities=_build_capabilities(
                raw,
                default_supported_processes=("IMMUNOANALYSIS",),
                default_continuous_loading=True,
                default_auto_start=True,
                default_nominal_sample_capacity=180,
            ),
            load_interface=_build_load_interface(raw, default_loading_area="MAIN_LOADING_AREA"),
            start_strategy=start_strategy,
            status_strategy=status_strategy,
        )

    return None
