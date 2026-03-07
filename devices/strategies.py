from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from .models import DeviceIdentity, DeviceSession, DeviceStatusSnapshot


class StartStrategy(ABC):
    @abstractmethod
    def start(self, *, identity: DeviceIdentity, session: DeviceSession) -> Dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def to_config_dict(self) -> Dict[str, Any]:
        raise NotImplementedError


class StatusStrategy(ABC):
    @abstractmethod
    def read_status(self, *, snapshot: DeviceStatusSnapshot) -> DeviceStatusSnapshot:
        raise NotImplementedError

    @abstractmethod
    def to_config_dict(self) -> Dict[str, Any]:
        raise NotImplementedError


@dataclass
class ConfigurableStartStrategy(StartStrategy):
    strategy_type: str = "manual"
    method: str = "manual_trigger"
    parameters: Dict[str, Any] = field(default_factory=dict)

    def start(self, *, identity: DeviceIdentity, session: DeviceSession) -> Dict[str, Any]:
        return {
            "strategy_type": self.strategy_type,
            "method": self.method,
            "parameters": dict(self.parameters),
            "device_id": identity.device_id,
            "session_id": session.session_id,
        }

    def to_config_dict(self) -> Dict[str, Any]:
        return {
            "type": self.strategy_type,
            "method": self.method,
            "parameters": dict(self.parameters),
        }


@dataclass
class ConfigurableStatusStrategy(StatusStrategy):
    strategy_type: str = "in_memory"
    source: str = "in_memory"
    state_map: Dict[str, str] = field(default_factory=dict)
    parameters: Dict[str, Any] = field(default_factory=dict)

    def read_status(self, *, snapshot: DeviceStatusSnapshot) -> DeviceStatusSnapshot:
        mapped_state = self.state_map.get(snapshot.process_state.value)
        raw = dict(snapshot.raw)
        if mapped_state:
            raw["mapped_state"] = mapped_state
        if self.parameters:
            raw["status_parameters"] = dict(self.parameters)
        return DeviceStatusSnapshot(
            timestamp=snapshot.timestamp,
            mode=snapshot.mode,
            process_state=snapshot.process_state,
            is_faulted=snapshot.is_faulted,
            fault_code=snapshot.fault_code,
            message=snapshot.message,
            owned_carrier_ids=snapshot.owned_carrier_ids,
            active_session_id=snapshot.active_session_id,
            source=self.source,
            raw=raw,
        )

    def to_config_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "type": self.strategy_type,
            "source": self.source,
        }
        if self.state_map:
            payload["state_map"] = dict(self.state_map)
        if self.parameters:
            payload["parameters"] = dict(self.parameters)
        return payload


def start_strategy_from_config(raw: Optional[Dict[str, Any]]) -> ConfigurableStartStrategy:
    data = raw if isinstance(raw, dict) else {}
    strategy_type = str(data.get("type", "manual")).strip() or "manual"
    method = str(data.get("method", "manual_trigger")).strip() or "manual_trigger"
    params = data.get("parameters", {})
    return ConfigurableStartStrategy(
        strategy_type=strategy_type,
        method=method,
        parameters=dict(params) if isinstance(params, dict) else {},
    )


def status_strategy_from_config(raw: Optional[Dict[str, Any]]) -> ConfigurableStatusStrategy:
    data = raw if isinstance(raw, dict) else {}
    strategy_type = str(data.get("type", "in_memory")).strip() or "in_memory"
    source = str(data.get("source", "in_memory")).strip() or "in_memory"
    state_map = data.get("state_map", {})
    params = data.get("parameters", {})
    return ConfigurableStatusStrategy(
        strategy_type=strategy_type,
        source=source,
        state_map=dict(state_map) if isinstance(state_map, dict) else {},
        parameters=dict(params) if isinstance(params, dict) else {},
    )

