from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from .models import DeviceIdentity, DeviceStatusSnapshot, RunSession


class StartStrategy(ABC):
    @abstractmethod
    def start(self, *, identity: DeviceIdentity, session: RunSession) -> Dict[str, Any]:
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


class LidControlStrategy(ABC):
    @abstractmethod
    def open_lid(self, *, identity: DeviceIdentity) -> Dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def close_lid(self, *, identity: DeviceIdentity) -> Dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def to_config_dict(self) -> Dict[str, Any]:
        raise NotImplementedError


@dataclass
class ConfigurableStartStrategy(StartStrategy):
    strategy_type: str = "manual"
    method: str = "local_ui"
    parameters: Dict[str, Any] = field(default_factory=dict)

    def start(self, *, identity: DeviceIdentity, session: RunSession) -> Dict[str, Any]:
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
        raw = dict(snapshot.raw)
        mapped = self.state_map.get(snapshot.process_state.value)
        if mapped:
            raw["mapped_state"] = mapped
        if self.parameters:
            raw["status_parameters"] = dict(self.parameters)
        return DeviceStatusSnapshot(
            timestamp=snapshot.timestamp,
            mode=snapshot.mode,
            lid_state=snapshot.lid_state,
            rotor_state=snapshot.rotor_state,
            process_state=snapshot.process_state,
            is_faulted=snapshot.is_faulted,
            fault_code=snapshot.fault_code,
            message=snapshot.message,
            loaded_tube_ids=snapshot.loaded_tube_ids,
            active_session_id=snapshot.active_session_id,
            source=self.source,
            raw=raw,
        )

    def to_config_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"type": self.strategy_type, "source": self.source}
        if self.state_map:
            payload["state_map"] = dict(self.state_map)
        if self.parameters:
            payload["parameters"] = dict(self.parameters)
        return payload


@dataclass
class ConfigurableLidControlStrategy(LidControlStrategy):
    strategy_type: str = "manual"
    method: str = "local_ui"
    parameters: Dict[str, Any] = field(default_factory=dict)

    def open_lid(self, *, identity: DeviceIdentity) -> Dict[str, Any]:
        return {
            "strategy_type": self.strategy_type,
            "method": self.method,
            "parameters": dict(self.parameters),
            "device_id": identity.device_id,
            "action": "open",
        }

    def close_lid(self, *, identity: DeviceIdentity) -> Dict[str, Any]:
        return {
            "strategy_type": self.strategy_type,
            "method": self.method,
            "parameters": dict(self.parameters),
            "device_id": identity.device_id,
            "action": "close",
        }

    def to_config_dict(self) -> Dict[str, Any]:
        return {
            "type": self.strategy_type,
            "method": self.method,
            "parameters": dict(self.parameters),
        }


def start_strategy_from_config(raw: Optional[Dict[str, Any]]) -> ConfigurableStartStrategy:
    data = raw if isinstance(raw, dict) else {}
    return ConfigurableStartStrategy(
        strategy_type=str(data.get("type", "manual")),
        method=str(data.get("method", "local_ui")),
        parameters=dict(data.get("parameters", {})) if isinstance(data.get("parameters"), dict) else {},
    )


def status_strategy_from_config(raw: Optional[Dict[str, Any]]) -> ConfigurableStatusStrategy:
    data = raw if isinstance(raw, dict) else {}
    return ConfigurableStatusStrategy(
        strategy_type=str(data.get("type", "in_memory")),
        source=str(data.get("source", "in_memory")),
        state_map=dict(data.get("state_map", {})) if isinstance(data.get("state_map"), dict) else {},
        parameters=dict(data.get("parameters", {})) if isinstance(data.get("parameters"), dict) else {},
    )


def lid_control_strategy_from_config(raw: Optional[Dict[str, Any]]) -> ConfigurableLidControlStrategy:
    data = raw if isinstance(raw, dict) else {}
    return ConfigurableLidControlStrategy(
        strategy_type=str(data.get("type", "manual")),
        method=str(data.get("method", "local_ui")),
        parameters=dict(data.get("parameters", {})) if isinstance(data.get("parameters"), dict) else {},
    )

