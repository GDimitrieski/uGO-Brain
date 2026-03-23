from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, Mapping, MutableMapping, Optional, Tuple

import requests


DEFAULT_WISE_TIMEOUT_S = 1.5
DEFAULT_WISE_STALE_AFTER_S = 5.0
DEFAULT_WISE_POLL_INTERVAL_S = 1.0
DEFAULT_WISE_DI_ENDPOINT_TEMPLATE = "/iocard/{slot}/di"


def _now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="milliseconds")


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(int(value))
    txt = str(value).strip().lower()
    if txt in {"1", "true", "yes", "y", "on", "high"}:
        return True
    if txt in {"0", "false", "no", "n", "off", "low"}:
        return False
    return bool(txt)


def _normalize_channel_key(value: Any) -> Optional[int]:
    if isinstance(value, int):
        return int(value)
    txt = str(value).strip()
    if not txt:
        return None
    if txt.isdigit():
        return int(txt)
    lowered = txt.lower()
    if lowered.startswith("di"):
        suffix = lowered[2:]
        if suffix.isdigit():
            return int(suffix)
    if lowered.startswith("ch"):
        suffix = lowered[2:]
        if suffix.isdigit():
            return int(suffix)
    return None


@dataclass(frozen=True)
class WisePollSnapshot:
    timestamp: str
    online: bool
    channels: Dict[int, bool]
    error: str
    latency_ms: float
    stale: bool
    age_s: Optional[float]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": str(self.timestamp),
            "online": bool(self.online),
            "channels": {str(int(k)): bool(v) for k, v in sorted(self.channels.items())},
            "error": str(self.error or ""),
            "latency_ms": float(self.latency_ms),
            "stale": bool(self.stale),
            "age_s": None if self.age_s is None else float(self.age_s),
        }


class WiseModuleAdapter:
    """Minimal REST adapter for Wise DI polling.

    Endpoint and payload formats vary by deployment, so the URL template is
    configurable and input parsing is intentionally tolerant.
    """

    def __init__(
        self,
        *,
        host: str,
        port: int = 80,
        scheme: str = "http",
        username: str = "",
        password: str = "",
        di_slot: int = 0,
        di_endpoint_template: str = DEFAULT_WISE_DI_ENDPOINT_TEMPLATE,
        timeout_s: float = DEFAULT_WISE_TIMEOUT_S,
        verify_tls: bool = True,
        poll_interval_s: float = DEFAULT_WISE_POLL_INTERVAL_S,
        stale_after_s: float = DEFAULT_WISE_STALE_AFTER_S,
        http_get: Optional[Callable[..., Any]] = None,
    ) -> None:
        host_txt = str(host or "").strip()
        if not host_txt:
            raise ValueError("Wise host is required")
        self.host = host_txt
        self.port = int(port)
        self.scheme = str(scheme or "http").strip().lower() or "http"
        self.username = str(username or "")
        self.password = str(password or "")
        self.di_slot = int(di_slot)
        self.di_endpoint_template = str(di_endpoint_template or "").strip() or DEFAULT_WISE_DI_ENDPOINT_TEMPLATE
        self.timeout_s = max(0.1, float(timeout_s))
        self.verify_tls = bool(verify_tls)
        self.poll_interval_s = max(0.0, float(poll_interval_s))
        self.stale_after_s = max(0.1, float(stale_after_s))
        self._http_get: Callable[..., Any] = http_get or requests.get
        self._last_poll_monotonic: float = 0.0
        self._last_success_monotonic: float = 0.0
        self._last_snapshot: Optional[WisePollSnapshot] = None

    @property
    def base_url(self) -> str:
        return f"{self.scheme}://{self.host}:{int(self.port)}"

    @property
    def di_url(self) -> str:
        template = self.di_endpoint_template.format(slot=int(self.di_slot))
        txt = str(template).strip()
        if txt.startswith("http://") or txt.startswith("https://"):
            return txt
        return f"{self.base_url}/{txt.lstrip('/')}"

    def poll_inputs(self, *, force: bool = False) -> WisePollSnapshot:
        now = time.monotonic()
        if (
            not force
            and self._last_snapshot is not None
            and (now - self._last_poll_monotonic) < self.poll_interval_s
        ):
            return self._snapshot_with_fresh_staleness(self._last_snapshot, now=now)

        started = time.monotonic()
        try:
            kwargs: Dict[str, Any] = {
                "timeout": float(self.timeout_s),
                "verify": bool(self.verify_tls),
            }
            if self.username:
                kwargs["auth"] = (self.username, self.password)
            response = self._http_get(self.di_url, **kwargs)
            response.raise_for_status()
            payload = response.json()
            channels = self._extract_channels(payload)
            if not channels:
                raise ValueError("Wise DI payload has no channels")
            for channel in range(4):
                channels.setdefault(channel, False)
            finished = time.monotonic()
            self._last_success_monotonic = finished
            snapshot = WisePollSnapshot(
                timestamp=_now_iso(),
                online=True,
                channels={int(k): bool(v) for k, v in channels.items()},
                error="",
                latency_ms=max(0.0, (finished - started) * 1000.0),
                stale=False,
                age_s=0.0,
            )
        except Exception as exc:
            finished = time.monotonic()
            fallback_channels = (
                dict(self._last_snapshot.channels)
                if self._last_snapshot is not None
                else {0: False, 1: False, 2: False, 3: False}
            )
            snapshot = WisePollSnapshot(
                timestamp=_now_iso(),
                online=False,
                channels={int(k): bool(v) for k, v in fallback_channels.items()},
                error=str(exc),
                latency_ms=max(0.0, (finished - started) * 1000.0),
                stale=True,
                age_s=None,
            )

        self._last_poll_monotonic = finished
        self._last_snapshot = snapshot
        return self._snapshot_with_fresh_staleness(snapshot, now=finished)

    def snapshot(self) -> WisePollSnapshot:
        if self._last_snapshot is None:
            return WisePollSnapshot(
                timestamp=_now_iso(),
                online=False,
                channels={0: False, 1: False, 2: False, 3: False},
                error="No Wise snapshot collected yet",
                latency_ms=0.0,
                stale=True,
                age_s=None,
            )
        return self._snapshot_with_fresh_staleness(self._last_snapshot)

    def diagnose(self) -> Dict[str, Any]:
        snap = self.snapshot()
        return {
            "base_url": self.base_url,
            "di_url": self.di_url,
            "di_slot": int(self.di_slot),
            "poll_interval_s": float(self.poll_interval_s),
            "stale_after_s": float(self.stale_after_s),
            "timeout_s": float(self.timeout_s),
            "verify_tls": bool(self.verify_tls),
            "online": bool(snap.online),
            "stale": bool(snap.stale),
            "error": str(snap.error),
            "channels": {str(int(k)): bool(v) for k, v in sorted(snap.channels.items())},
            "timestamp": str(snap.timestamp),
            "age_s": snap.age_s,
            "latency_ms": float(snap.latency_ms),
        }

    def _snapshot_with_fresh_staleness(
        self,
        snapshot: WisePollSnapshot,
        *,
        now: Optional[float] = None,
    ) -> WisePollSnapshot:
        mono_now = time.monotonic() if now is None else float(now)
        if self._last_success_monotonic <= 0.0:
            return WisePollSnapshot(
                timestamp=snapshot.timestamp,
                online=False,
                channels=dict(snapshot.channels),
                error=snapshot.error or "No successful Wise poll yet",
                latency_ms=float(snapshot.latency_ms),
                stale=True,
                age_s=None,
            )

        age_s = max(0.0, mono_now - self._last_success_monotonic)
        stale = age_s > float(self.stale_after_s)
        return WisePollSnapshot(
            timestamp=snapshot.timestamp,
            online=bool(snapshot.online),
            channels=dict(snapshot.channels),
            error=str(snapshot.error or ""),
            latency_ms=float(snapshot.latency_ms),
            stale=bool(stale or (not snapshot.online)),
            age_s=float(age_s),
        )

    @classmethod
    def _extract_channels(cls, payload: Any) -> Dict[int, bool]:
        out: Dict[int, bool] = {}

        def _merge(mapping: Mapping[Any, Any]) -> None:
            for key, value in mapping.items():
                idx = _normalize_channel_key(key)
                if idx is None:
                    continue
                out[int(idx)] = _to_bool(value)

        if isinstance(payload, Mapping):
            candidates = (
                payload.get("DIVal"),
                payload.get("di_val"),
                payload.get("di"),
                payload.get("inputs"),
                payload.get("channels"),
            )
            for candidate in candidates:
                if candidate is None:
                    continue
                nested = cls._extract_channels(candidate)
                if nested:
                    out.update(nested)
            if not out:
                _merge(payload)
            return out

        if isinstance(payload, (list, tuple)):
            for idx, item in enumerate(payload):
                if isinstance(item, Mapping):
                    channel = item.get("Ch")
                    if channel is None:
                        channel = item.get("channel")
                    if channel is None:
                        channel = item.get("idx")
                    value = item.get("Val")
                    if value is None:
                        value = item.get("value")
                    if value is None:
                        value = item.get("Stat")
                    if channel is None:
                        channel = idx
                    parsed_idx = _normalize_channel_key(channel)
                    if parsed_idx is None or value is None:
                        continue
                    out[int(parsed_idx)] = _to_bool(value)
                    continue

                out[int(idx)] = _to_bool(item)
            return out

        return out


def wise_snapshot_to_metadata(snapshot: WisePollSnapshot) -> Dict[str, Any]:
    return snapshot.to_dict()

