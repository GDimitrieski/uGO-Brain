from __future__ import annotations

from enum import Enum


class Mode(str, Enum):
    OFFLINE = "OFFLINE"
    MANUAL = "MANUAL"
    AUTOMATIC = "AUTOMATIC"
    MAINTENANCE = "MAINTENANCE"


class ProcessState(str, Enum):
    IDLE = "IDLE"
    PREPARING_FOR_LOAD = "PREPARING_FOR_LOAD"
    LOADED = "LOADED"
    STARTING = "STARTING"
    EXECUTING = "EXECUTING"
    COMPLETED = "COMPLETED"
    PREPARING_FOR_UNLOAD = "PREPARING_FOR_UNLOAD"
    RELEASED = "RELEASED"
    FAULTED = "FAULTED"

