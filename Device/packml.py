from __future__ import annotations

from enum import Enum
from typing import Dict, Optional


class PackMLMode(str, Enum):
    AUTOMATIC = "AUTOMATIC"
    MANUAL = "MANUAL"
    MAINTENANCE = "MAINTENANCE"


class PackMLState(str, Enum):
    IDLE = "IDLE"
    STARTING = "STARTING"
    EXECUTE = "EXECUTE"
    COMPLETE = "COMPLETE"
    STOPPED = "STOPPED"
    HELD = "HELD"
    SUSPENDED = "SUSPENDED"
    ABORTED = "ABORTED"
    FAULTED = "FAULTED"


class PackMLCommand(str, Enum):
    START = "START"
    COMPLETE = "COMPLETE"
    STOP = "STOP"
    HOLD = "HOLD"
    UNHOLD = "UNHOLD"
    SUSPEND = "SUSPEND"
    UNSUSPEND = "UNSUSPEND"
    ABORT = "ABORT"
    RESET = "RESET"
    FAULT = "FAULT"


STATE_TRANSITIONS: Dict[PackMLState, Dict[PackMLCommand, PackMLState]] = {
    PackMLState.IDLE: {
        PackMLCommand.START: PackMLState.STARTING,
        PackMLCommand.STOP: PackMLState.STOPPED,
        PackMLCommand.ABORT: PackMLState.ABORTED,
        PackMLCommand.FAULT: PackMLState.FAULTED,
    },
    PackMLState.STARTING: {
        PackMLCommand.COMPLETE: PackMLState.EXECUTE,
        PackMLCommand.STOP: PackMLState.STOPPED,
        PackMLCommand.ABORT: PackMLState.ABORTED,
        PackMLCommand.FAULT: PackMLState.FAULTED,
    },
    PackMLState.EXECUTE: {
        PackMLCommand.COMPLETE: PackMLState.COMPLETE,
        PackMLCommand.HOLD: PackMLState.HELD,
        PackMLCommand.SUSPEND: PackMLState.SUSPENDED,
        PackMLCommand.STOP: PackMLState.STOPPED,
        PackMLCommand.ABORT: PackMLState.ABORTED,
        PackMLCommand.FAULT: PackMLState.FAULTED,
    },
    PackMLState.COMPLETE: {
        PackMLCommand.RESET: PackMLState.IDLE,
        PackMLCommand.START: PackMLState.STARTING,
        PackMLCommand.ABORT: PackMLState.ABORTED,
    },
    PackMLState.STOPPED: {
        PackMLCommand.RESET: PackMLState.IDLE,
        PackMLCommand.ABORT: PackMLState.ABORTED,
        PackMLCommand.FAULT: PackMLState.FAULTED,
    },
    PackMLState.HELD: {
        PackMLCommand.UNHOLD: PackMLState.EXECUTE,
        PackMLCommand.STOP: PackMLState.STOPPED,
        PackMLCommand.ABORT: PackMLState.ABORTED,
        PackMLCommand.FAULT: PackMLState.FAULTED,
    },
    PackMLState.SUSPENDED: {
        PackMLCommand.UNSUSPEND: PackMLState.EXECUTE,
        PackMLCommand.STOP: PackMLState.STOPPED,
        PackMLCommand.ABORT: PackMLState.ABORTED,
        PackMLCommand.FAULT: PackMLState.FAULTED,
    },
    PackMLState.ABORTED: {
        PackMLCommand.RESET: PackMLState.IDLE,
    },
    PackMLState.FAULTED: {
        PackMLCommand.RESET: PackMLState.IDLE,
        PackMLCommand.ABORT: PackMLState.ABORTED,
    },
}


def next_state(state: PackMLState, command: PackMLCommand) -> Optional[PackMLState]:
    transitions = STATE_TRANSITIONS.get(state, {})
    return transitions.get(command)


def parse_mode(value: PackMLMode | str) -> PackMLMode:
    if isinstance(value, PackMLMode):
        return value
    return PackMLMode[str(value).strip().upper()]


def parse_command(value: PackMLCommand | str) -> PackMLCommand:
    if isinstance(value, PackMLCommand):
        return value
    return PackMLCommand[str(value).strip().upper()]
