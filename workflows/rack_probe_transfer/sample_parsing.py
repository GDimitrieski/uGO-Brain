import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _normalize_barcode_key(value: Any) -> str:
    return str(value or "").strip().upper()


def _load_immuno_kreuzprobe_map(path: Path) -> Dict[str, str]:
    """Load immuno barcode -> Kreuzprobe sample-id mapping from JSON."""
    if not path.exists():
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except Exception:
        return {}

    mapping: Dict[str, str] = {}

    if isinstance(raw, dict):
        forward = raw.get("immuno_to_kreuzprobe")
        reverse = raw.get("kreuzprobe_to_immuno")

        if isinstance(forward, dict):
            for barcode, kreuz_id in forward.items():
                key = _normalize_barcode_key(barcode)
                value = str(kreuz_id or "").strip()
                if key and value:
                    mapping[key] = value

        if isinstance(reverse, dict):
            for kreuz_id, barcode in reverse.items():
                key = _normalize_barcode_key(barcode)
                value = str(kreuz_id or "").strip()
                if key and value:
                    mapping[key] = value

        # Backward-compatible flat object: {"<barcode>": "KREUZPROBE_0001"}
        if not mapping:
            for barcode, kreuz_id in raw.items():
                if not isinstance(barcode, str):
                    continue
                if isinstance(kreuz_id, (str, int, float)):
                    key = _normalize_barcode_key(barcode)
                    value = str(kreuz_id).strip()
                    if key and value:
                        mapping[key] = value

    return mapping


def _try_parse_string_list(value: str) -> Optional[List[int]]:
    txt = value.strip()
    if not txt:
        return []

    # JSON-style list string, e.g. "[1,2,3]"
    if txt.startswith("[") and txt.endswith("]"):
        try:
            parsed = json.loads(txt)
            if isinstance(parsed, list):
                out: List[int] = []
                for item in parsed:
                    out.append(int(item))
                return out
        except Exception:
            pass

    # Comma-separated string, e.g. "1,2,3"
    try:
        return [int(part.strip()) for part in txt.split(",") if part.strip()]
    except Exception:
        return None


def _try_parse_presence_mask_positions(value: Any) -> Optional[List[int]]:
    """Parse camera mask formats like "[P,F,P,...]" and return 1-based occupied positions.

    Convention:
    - F => sample present
    - P => no sample
    """
    tokens: List[str] = []

    if isinstance(value, list):
        for item in value:
            txt = str(item).strip().upper()
            if txt:
                tokens.append(txt)
    elif isinstance(value, str):
        txt = value.strip()
        if not txt:
            return []
        parsed = None
        if txt.startswith("[") and txt.endswith("]"):
            try:
                parsed = json.loads(txt)
            except Exception:
                parsed = None
        if isinstance(parsed, list):
            for item in parsed:
                item_txt = str(item).strip().upper()
                if item_txt:
                    tokens.append(item_txt)
        else:
            core = txt[1:-1] if txt.startswith("[") and txt.endswith("]") else txt
            parts = [part.strip().strip("'\"").upper() for part in core.split(",")]
            tokens = [part for part in parts if part]
    else:
        return None

    if not tokens:
        return []

    valid = {"F", "P"}
    if any(token not in valid for token in tokens):
        return None

    # Camera mask is 1-based when mapped to rack slot positions.
    return [idx for idx, token in enumerate(tokens, start=1) if token == "F"]


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


def extract_positions(result: Dict[str, Any]) -> List[int]:
    raw = result.get("raw", {})
    data = raw.get("data", {}) if isinstance(raw, dict) else {}
    outputs = data.get("outputs", {}) if isinstance(data, dict) else {}

    candidates = [
        data.get("positions"),
        data.get("detectedPositions"),
        data.get("samplePositions"),
        data.get("samples"),
        outputs.get("Results") if isinstance(outputs, dict) else None,
        outputs.get("results") if isinstance(outputs, dict) else None,
        outputs.get("Detected") if isinstance(outputs, dict) else None,
        outputs.get("detected") if isinstance(outputs, dict) else None,
        outputs.get("Positions") if isinstance(outputs, dict) else None,
        outputs.get("positions") if isinstance(outputs, dict) else None,
        raw.get("positions") if isinstance(raw, dict) else None,
        raw.get("detectedPositions") if isinstance(raw, dict) else None,
    ]

    for candidate in candidates:
        mask_positions = _try_parse_presence_mask_positions(candidate)
        if mask_positions is not None:
            return mask_positions

        if isinstance(candidate, list):
            out: List[int] = []
            for item in candidate:
                if isinstance(item, int):
                    out.append(item)
                elif isinstance(item, str):
                    out.append(int(item))
                elif isinstance(item, dict):
                    value = item.get("position", item.get("slot", item.get("index")))
                    if value is not None:
                        out.append(int(value))
            if out:
                return out
        elif isinstance(candidate, str):
            parsed = _try_parse_string_list(candidate)
            if parsed is not None:
                return parsed

    # Last resort: task may return list as plain message string
    msg = str(result.get("message", "")).strip()
    parsed_mask_msg = _try_parse_presence_mask_positions(msg)
    if parsed_mask_msg is not None:
        return parsed_mask_msg
    parsed_msg = _try_parse_string_list(msg)
    if parsed_msg is not None:
        return parsed_msg

    return []


def extract_sample_type(result: Dict[str, Any]) -> Optional[int]:
    raw = result.get("raw", {})
    data = raw.get("data", {}) if isinstance(raw, dict) else {}
    outputs = data.get("outputs", {}) if isinstance(data, dict) else {}

    candidates = [
        outputs.get("SampleType") if isinstance(outputs, dict) else None,
        outputs.get("sampleType") if isinstance(outputs, dict) else None,
        outputs.get("Type") if isinstance(outputs, dict) else None,
        outputs.get("type") if isinstance(outputs, dict) else None,
        outputs.get("Results") if isinstance(outputs, dict) else None,
        outputs.get("results") if isinstance(outputs, dict) else None,
        data.get("sampleType"),
        data.get("type"),
        result.get("message"),
    ]

    for candidate in candidates:
        if candidate is None:
            continue
        if isinstance(candidate, int):
            if 1 <= candidate <= 4:
                return candidate
            continue

        txt = str(candidate).strip()
        if not txt:
            continue

        if txt.isdigit():
            value = int(txt)
            if 1 <= value <= 4:
                return value
            continue

        match = re.search(r"\b([1-4])\b", txt)
        if match:
            return int(match.group(1))

    return None


def extract_sample_barcode(result: Dict[str, Any]) -> Optional[str]:
    raw = result.get("raw", {})
    data = raw.get("data", {}) if isinstance(raw, dict) else {}
    outputs = data.get("outputs", {}) if isinstance(data, dict) else {}

    candidates: List[Tuple[Any, str]] = []
    if isinstance(outputs, dict):
        candidates.extend(
            [
                (outputs.get("Barcode"), "barcode_field"),
                (outputs.get("barcode"), "barcode_field"),
                (outputs.get("SampleBarcode"), "barcode_field"),
                (outputs.get("sampleBarcode"), "barcode_field"),
                (outputs.get("SampleId"), "barcode_field"),
                (outputs.get("sampleId"), "barcode_field"),
                (outputs.get("Results"), "barcode_field"),
                (outputs.get("results"), "barcode_field"),
            ]
        )
    if isinstance(data, dict):
        candidates.extend(
            [
                (data.get("Barcode"), "barcode_field"),
                (data.get("barcode"), "barcode_field"),
                (data.get("SampleBarcode"), "barcode_field"),
                (data.get("sampleBarcode"), "barcode_field"),
            ]
        )
    candidates.append((result.get("message"), "message"))

    for candidate, source_kind in candidates:
        if candidate is None:
            continue
        txt = str(candidate).strip()
        if not txt:
            continue
        if txt.lower() in {"none", "null", "n/a"}:
            continue
        if source_kind == "message" and txt.isdigit() and len(txt) <= 2:
            continue
        # Keep the full barcode as returned by 3FG.
        return txt
    return None


def _classification_key_from_barcode(barcode: Optional[str]) -> Optional[str]:
    txt = str(barcode or "").strip()
    if not txt:
        return None
    if txt.lower() in {"none", "null", "n/a"}:
        return None
    if len(txt) < 2:
        return txt
    # Sample class routing is based on the last two characters.
    return txt[-2:]

