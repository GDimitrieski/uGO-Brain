from __future__ import annotations

from enum import Enum


class Mode(str, Enum):
    OFFLINE = "OFFLINE"
    MANUAL = "MANUAL"
    AUTOMATIC = "AUTOMATIC"
    MAINTENANCE = "MAINTENANCE"


class LidState(str, Enum):
    OPEN = "OPEN"
    CLOSED = "CLOSED"
    LOCKED = "LOCKED"


class RotorState(str, Enum):
    STOPPED = "STOPPED"
    SPINNING = "SPINNING"
    STANDSTILL = "STANDSTILL"


class ProcessState(str, Enum):
    IDLE = "IDLE"
    LID_OPEN = "LID_OPEN"
    LOADING = "LOADING"
    LOADED = "LOADED"
    BALANCE_VALIDATED = "BALANCE_VALIDATED"
    READY_TO_START = "READY_TO_START"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    UNLOADING = "UNLOADING"
    FAULTED = "FAULTED"

