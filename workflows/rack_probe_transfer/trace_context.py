import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional


TRACE_RECORD_META_KEYS = {
    "timestamp_sent",
    "command_sent",
    "result",
    "task_id",
    "receiver",
    "dispatch_path",
    "message",
    "state_path",
    "state_timeline",
    "timestamp_returned",
    "task_outputs",
    "task_output_results",
    "task_output_position",
    "task_data",
}


def _parse_iso_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    txt = str(value).strip()
    if not txt:
        return None
    if txt.endswith("Z"):
        txt = txt[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(txt)
    except Exception:
        return None


def _parse_json_maybe(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list, int, float, bool)):
        return value
    txt = str(value).strip()
    if not txt:
        return None
    if txt.lower() in {"none", "null"}:
        return None
    if (txt.startswith("{") and txt.endswith("}")) or (txt.startswith("[") and txt.endswith("]")):
        try:
            return json.loads(txt)
        except Exception:
            return txt
    return txt


def _normalize_dispatch_path(value: Any) -> List[str]:
    if isinstance(value, list):
        return [str(part).strip() for part in value if str(part).strip()]
    txt = str(value or "").strip()
    if not txt:
        return []
    return [part.strip() for part in txt.split(">") if part.strip()]


def _task_parameters_from_trace_record(record: Dict[str, Any]) -> Dict[str, Any]:
    params: Dict[str, Any] = {}
    for key in sorted(record.keys()):
        if key in TRACE_RECORD_META_KEYS:
            continue
        parsed = _parse_json_maybe(record.get(key))
        if parsed is None:
            continue
        params[key] = parsed
    return params


def _task_outputs_from_trace_record(record: Dict[str, Any]) -> Dict[str, Any]:
    outputs: Dict[str, Any] = {}
    parsed_outputs = _parse_json_maybe(record.get("task_outputs"))
    if parsed_outputs is not None:
        outputs["outputs"] = parsed_outputs

    parsed_results = _parse_json_maybe(record.get("task_output_results"))
    if parsed_results is not None:
        outputs["results"] = parsed_results

    parsed_position = _parse_json_maybe(record.get("task_output_position"))
    if parsed_position is not None:
        outputs["position"] = parsed_position

    parsed_task_data = _parse_json_maybe(record.get("task_data"))
    if parsed_task_data is not None:
        outputs["task_data"] = parsed_task_data
    return outputs


def _task_context_from_trace_record(record: Dict[str, Any]) -> Dict[str, Any]:
    context: Dict[str, Any] = {
        "task_id": str(record.get("task_id") or ""),
        "task_key": str(record.get("command_sent") or ""),
        "status": str(record.get("result") or ""),
        "receiver": str(record.get("receiver") or ""),
        "dispatch_path": _normalize_dispatch_path(record.get("dispatch_path")),
        "timestamps": {
            "sent": str(record.get("timestamp_sent") or ""),
            "returned": str(record.get("timestamp_returned") or ""),
        },
        "parameters": _task_parameters_from_trace_record(record),
        "outputs": _task_outputs_from_trace_record(record),
    }
    message = str(record.get("message") or "").strip()
    if message:
        context["message"] = message

    state_path = str(record.get("state_path") or "").strip()
    if state_path:
        context["state_path"] = state_path

    state_timeline = str(record.get("state_timeline") or "").strip()
    if state_timeline:
        context["state_timeline"] = state_timeline
    return context


def _match_task_context_for_event(
    event_ts: datetime, trace_rows: List[Dict[str, Any]], max_previous_gap_s: float = 5.0
) -> Optional[Dict[str, Any]]:
    covering: Optional[Dict[str, Any]] = None
    covering_delta_s: Optional[float] = None
    for row in trace_rows:
        sent_ts = row.get("sent_ts")
        returned_ts = row.get("returned_ts")
        if sent_ts is None and returned_ts is None:
            continue
        if sent_ts is None:
            sent_ts = returned_ts
        if returned_ts is None:
            returned_ts = sent_ts
        if sent_ts is None or returned_ts is None:
            continue
        if sent_ts <= event_ts <= (returned_ts + timedelta(seconds=0.75)):
            delta_s = abs((event_ts - returned_ts).total_seconds())
            if covering is None or covering_delta_s is None or delta_s < covering_delta_s:
                covering = row
                covering_delta_s = delta_s
    if covering is not None:
        return dict(covering.get("context") or {})

    best_previous: Optional[Dict[str, Any]] = None
    best_gap_s: Optional[float] = None
    for row in trace_rows:
        anchor_ts = row.get("returned_ts") or row.get("sent_ts")
        if anchor_ts is None or anchor_ts > event_ts:
            continue
        gap_s = (event_ts - anchor_ts).total_seconds()
        if best_previous is None or best_gap_s is None or gap_s < best_gap_s:
            best_previous = row
            best_gap_s = gap_s

    if best_previous is None or best_gap_s is None or best_gap_s > max_previous_gap_s:
        return None
    return dict(best_previous.get("context") or {})


def enrich_occupancy_records_with_task_context(
    occupancy_records: List[Dict[str, Any]],
    trace_records: List[Dict[str, Any]],
) -> None:
    if not occupancy_records or not trace_records:
        return

    trace_rows: List[Dict[str, Any]] = []
    for trace_record in trace_records:
        if not isinstance(trace_record, dict):
            continue
        sent_ts = _parse_iso_datetime(trace_record.get("timestamp_sent"))
        returned_ts = _parse_iso_datetime(trace_record.get("timestamp_returned"))
        if sent_ts is None and returned_ts is None:
            continue
        trace_rows.append(
            {
                "sent_ts": sent_ts,
                "returned_ts": returned_ts or sent_ts,
                "context": _task_context_from_trace_record(trace_record),
            }
        )

    if not trace_rows:
        return

    for event in occupancy_records:
        if not isinstance(event, dict):
            continue
        if isinstance(event.get("task_context"), dict):
            continue
        if str(event.get("event_type") or "").upper() == "WORLD_SNAPSHOT":
            continue
        event_ts = _parse_iso_datetime(event.get("timestamp"))
        if event_ts is None:
            continue
        context = _match_task_context_for_event(event_ts, trace_rows)
        if context:
            event["task_context"] = context

