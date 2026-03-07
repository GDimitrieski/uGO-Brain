from __future__ import annotations

from typing import Any, Dict, Mapping, Optional, Tuple

from .centrifuge.hettich_rotina_380r_device import HettichRotina380RDevice
from .centrifuge.models import (
    AdapterConfiguration,
    BalanceModel,
    BucketConfiguration,
    DeviceCapabilities,
    DeviceIdentity,
    RotorConfiguration,
    RotorPosition,
)
from .centrifuge.strategies import (
    lid_control_strategy_from_config,
    start_strategy_from_config,
    status_strategy_from_config,
)
from .centrifuge.usage_strategy import CentrifugeUsageProfile, usage_profile_from_config


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


def _as_optional_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
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
    txt = str(raw).strip() if raw is not None else ""
    return (txt,) if txt else ()


def _build_identity(raw: Mapping[str, Any]) -> DeviceIdentity:
    meta = raw.get("identity_metadata")
    return DeviceIdentity(
        device_id=str(raw.get("id", "")),
        name=str(raw.get("name", raw.get("id", ""))),
        model=str(raw.get("model", "")),
        station_id=str(raw.get("station_id", "")),
        landmark_id=str(raw.get("landmark_id")) if raw.get("landmark_id") not in {None, ""} else None,
        metadata=dict(meta) if isinstance(meta, dict) else {},
    )


def _build_capabilities(raw: Mapping[str, Any]) -> DeviceCapabilities:
    cfg_raw = raw.get("device_capabilities")
    cfg = cfg_raw if isinstance(cfg_raw, dict) else {}
    meta = cfg.get("metadata")
    supported = _as_str_tuple(cfg.get("supported_processes", raw.get("capabilities", ("CENTRIFUGATION",))))
    return DeviceCapabilities(
        supported_processes=supported or ("CENTRIFUGATION",),
        refrigerated=_as_bool(cfg.get("refrigerated"), True),
        automatic_rotor_recognition=_as_bool(cfg.get("automatic_rotor_recognition"), True),
        powered_lid_lock=_as_bool(cfg.get("powered_lid_lock"), True),
        imbalance_detection=_as_bool(cfg.get("imbalance_detection"), True),
        interfaces=_as_str_tuple(cfg.get("interfaces", ("RS232", "LOCAL_UI"))),
        metadata=dict(meta) if isinstance(meta, dict) else {},
    )


def _build_rotor_configuration(raw: Mapping[str, Any]) -> RotorConfiguration:
    cfg_raw = raw.get("rotor_configuration")
    cfg = cfg_raw if isinstance(cfg_raw, dict) else {}

    positions = []
    for item in cfg.get("positions", []):
        if not isinstance(item, dict):
            continue
        try:
            idx = int(item.get("index"))
        except Exception:
            continue
        positions.append(
            RotorPosition(
                index=idx,
                angle_deg=float(item.get("angle_deg", 0.0)),
                opposite_index=(
                    int(item.get("opposite_index"))
                    if item.get("opposite_index") is not None
                    else None
                ),
                bucket_id=(
                    str(item.get("bucket_id"))
                    if item.get("bucket_id") not in {None, ""}
                    else None
                ),
                metadata=dict(item.get("metadata", {})) if isinstance(item.get("metadata"), dict) else {},
            )
        )

    buckets = []
    for item in cfg.get("buckets", []):
        if not isinstance(item, dict):
            continue
        bucket_id = str(item.get("bucket_id", "")).strip()
        if not bucket_id:
            continue
        buckets.append(
            BucketConfiguration(
                bucket_id=bucket_id,
                adapter_ids=_as_str_tuple(item.get("adapter_ids", ())),
                max_tube_loads=(
                    int(item.get("max_tube_loads"))
                    if item.get("max_tube_loads") is not None
                    else None
                ),
                metadata=dict(item.get("metadata", {})) if isinstance(item.get("metadata"), dict) else {},
            )
        )

    adapters = []
    for item in cfg.get("adapters", []):
        if not isinstance(item, dict):
            continue
        adapter_id = str(item.get("adapter_id", "")).strip()
        if not adapter_id:
            continue
        adapters.append(
            AdapterConfiguration(
                adapter_id=adapter_id,
                tube_types=_as_str_tuple(item.get("tube_types", ())),
                positions_per_bucket=(
                    int(item.get("positions_per_bucket"))
                    if item.get("positions_per_bucket") is not None
                    else None
                ),
                metadata=dict(item.get("metadata", {})) if isinstance(item.get("metadata"), dict) else {},
            )
        )

    meta = cfg.get("metadata")
    return RotorConfiguration(
        rotor_id=str(cfg.get("rotor_id", "GENERIC")),
        rotor_type=str(cfg.get("rotor_type", "CONFIGURABLE")),
        positions=tuple(sorted(positions, key=lambda x: int(x.index))),
        buckets=tuple(buckets),
        adapters=tuple(adapters),
        metadata=dict(meta) if isinstance(meta, dict) else {},
    )


def _build_balance_model(raw: Mapping[str, Any]) -> BalanceModel:
    cfg_raw = raw.get("balance_model")
    cfg = cfg_raw if isinstance(cfg_raw, dict) else {}
    return BalanceModel(
        rule_type=str(cfg.get("rule_type", "OPPOSITE_POSITION")),
        require_symmetry=_as_bool(cfg.get("require_symmetry"), True),
        tolerance_g=_as_optional_float(cfg.get("tolerance_g")),
        max_imbalance_g=_as_optional_float(cfg.get("max_imbalance_g")),
        metadata=dict(cfg.get("metadata", {})) if isinstance(cfg.get("metadata"), dict) else {},
    )


def _is_hettich_rotina_380r(raw: Mapping[str, Any]) -> bool:
    cls = str(raw.get("device_class", "")).strip().lower()
    model = str(raw.get("model", "")).strip().lower()
    name = str(raw.get("name", "")).strip().lower()
    if cls == "hettichrotina380rdevice":
        return True
    if "rotina380r" in model or "rotina 380 r" in name:
        return True
    if "centrifuge" in name and "centri" in model:
        return True
    return False


def _build_usage_profile(raw: Mapping[str, Any]) -> CentrifugeUsageProfile:
    cfg_raw = raw.get("usage_profile")
    if not isinstance(cfg_raw, dict):
        legacy_cfg_raw = raw.get("usage_strategy")
        cfg_raw = legacy_cfg_raw if isinstance(legacy_cfg_raw, dict) else None
    return usage_profile_from_config(cfg_raw)


def create_centrifuge_device(raw: Mapping[str, Any]) -> Optional[HettichRotina380RDevice]:
    if not isinstance(raw, Mapping):
        return None
    if "station_id" not in raw or "id" not in raw:
        return None
    if not _is_hettich_rotina_380r(raw):
        return None

    identity = _build_identity(raw)
    return HettichRotina380RDevice(
        identity=identity,
        capabilities=_build_capabilities(raw),
        rotor_configuration=_build_rotor_configuration(raw),
        balance_model=_build_balance_model(raw),
        start_strategy=start_strategy_from_config(
            raw.get("start_strategy") if isinstance(raw.get("start_strategy"), dict) else None
        ),
        status_strategy=status_strategy_from_config(
            raw.get("status_strategy") if isinstance(raw.get("status_strategy"), dict) else None
        ),
        lid_control_strategy=lid_control_strategy_from_config(
            raw.get("lid_control_strategy") if isinstance(raw.get("lid_control_strategy"), dict) else None
        ),
        usage_profile=_build_usage_profile(raw),
    )
