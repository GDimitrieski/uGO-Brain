"""DKVM (Delimited Key-Value Message) protocol for UR5e <-> Planner TCP communication.

Message format:  <VER>|<TYPE>|<ID>|key1=val1|key2=val2|...\n

  VER   - protocol version (integer, currently 1)
  TYPE  - CMD, ACK, STS, HB
  ID    - 8-char hex message id (short enough for URScript 1024-char limit)
  pairs - order-independent key=value payload
  \\n   - message terminator (URScript socket_read_string suffix)

Reserved chars in values are escaped:
  |  -> \\p
  =  -> \\e
  \\n -> \\N
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

PROTOCOL_VERSION = 1
FIELD_SEP = "|"
KV_SEP = "="
MSG_TERM = "\n"
MAX_MSG_LEN = 1024  # URScript socket_read_string limit

# Escape sequences for reserved characters inside values
_ESCAPE_MAP = {
    "\\": "\\\\",
    "|": "\\p",
    "=": "\\e",
    "\n": "\\N",
}
_UNESCAPE_MAP = {v: k for k, v in _ESCAPE_MAP.items()}


def _escape_value(val: str) -> str:
    out = val
    out = out.replace("\\", "\\\\")  # must be first
    out = out.replace("|", "\\p")
    out = out.replace("=", "\\e")
    out = out.replace("\n", "\\N")
    return out


def _unescape_value(val: str) -> str:
    out = []
    i = 0
    while i < len(val):
        if val[i] == "\\" and i + 1 < len(val):
            pair = val[i:i + 2]
            if pair in _UNESCAPE_MAP:
                out.append(_UNESCAPE_MAP[pair])
                i += 2
                continue
        out.append(val[i])
        i += 1
    return "".join(out)


def generate_msg_id() -> str:
    """8-char hex ID — unique enough, fits URScript string limits."""
    return uuid.uuid4().hex[:8]


class MessageType:
    CMD = "CMD"  # Planner -> UR5e: execute a task
    ACK = "ACK"  # UR5e -> Planner: command received
    STS = "STS"  # UR5e -> Planner: state/completion update
    HB = "HB"    # Heartbeat (bidirectional)


@dataclass
class DKVMMessage:
    version: int = PROTOCOL_VERSION
    msg_type: str = ""
    msg_id: str = ""
    payload: Dict[str, str] = field(default_factory=dict)

    def encode(self) -> str:
        """Serialize to wire format (including trailing \\n)."""
        parts: List[str] = [
            str(self.version),
            self.msg_type,
            self.msg_id,
        ]
        for key, val in self.payload.items():
            parts.append(f"{_escape_value(str(key))}{KV_SEP}{_escape_value(str(val))}")
        raw = FIELD_SEP.join(parts) + MSG_TERM
        if len(raw) > MAX_MSG_LEN:
            raise ValueError(
                f"Message exceeds URScript limit ({len(raw)}/{MAX_MSG_LEN} chars): "
                f"type={self.msg_type} id={self.msg_id}"
            )
        return raw

    @classmethod
    def decode(cls, raw: str) -> "DKVMMessage":
        """Parse a wire-format message string."""
        line = raw.rstrip("\n").rstrip("\r")
        parts = line.split(FIELD_SEP)
        if len(parts) < 3:
            raise ValueError(f"Malformed DKVM message (need at least VER|TYPE|ID): {raw!r}")
        version = int(parts[0])
        msg_type = parts[1]
        msg_id = parts[2]
        payload: Dict[str, str] = {}
        for part in parts[3:]:
            if KV_SEP not in part:
                continue
            key, val = part.split(KV_SEP, 1)
            payload[_unescape_value(key)] = _unescape_value(val)
        return cls(version=version, msg_type=msg_type, msg_id=msg_id, payload=payload)


def build_cmd(task_payload: Dict[str, Any], msg_id: Optional[str] = None) -> DKVMMessage:
    """Build a CMD message from a task payload dict (same structure as Available_Tasks)."""
    mid = msg_id or generate_msg_id()
    kv: Dict[str, str] = {}
    for key, val in task_payload.items():
        kv[str(key)] = str(val)
    return DKVMMessage(
        version=PROTOCOL_VERSION,
        msg_type=MessageType.CMD,
        msg_id=mid,
        payload=kv,
    )


def build_ack(msg_id: str, state: str = "RECEIVED") -> DKVMMessage:
    return DKVMMessage(
        version=PROTOCOL_VERSION,
        msg_type=MessageType.ACK,
        msg_id=msg_id,
        payload={"state": state},
    )


def build_sts(msg_id: str, state: str, **extra: str) -> DKVMMessage:
    payload = {"state": state}
    payload.update(extra)
    return DKVMMessage(
        version=PROTOCOL_VERSION,
        msg_type=MessageType.STS,
        msg_id=msg_id,
        payload=payload,
    )


def build_hb(**extra: str) -> DKVMMessage:
    return DKVMMessage(
        version=PROTOCOL_VERSION,
        msg_type=MessageType.HB,
        msg_id="0",
        payload=extra,
    )
