import csv
from pathlib import Path
from typing import Any, Dict, List, Optional


DEFAULT_STATE_CHANGE_FIELDNAMES = [
    "task_id",
    "command_sent",
    "change_index",
    "state",
    "timestamp",
    "task_outputs",
    "task_output_results",
    "task_output_position",
    "task_data",
]


def _read_csv_header(path: Path) -> List[str]:
    if not path.exists():
        return []
    try:
        with open(path, "r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            row = next(reader, [])
            if not isinstance(row, list):
                return []
            return [str(x) for x in row if str(x)]
    except Exception:
        return []


def export_trace(records: List[Dict[str, Any]], path: Path, *, append: bool = False) -> None:
    if not records:
        return

    param_keys = sorted(
        {
            k
            for rec in records
            for k in rec.keys()
            if k
            not in {
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
            }
        }
    )
    fieldnames = [
        "timestamp_sent",
        "command_sent",
        *param_keys,
        "result",
        "task_id",
        "receiver",
        "dispatch_path",
        "message",
        "state_path",
        "state_timeline",
        "timestamp_returned",
    ]

    path.parent.mkdir(parents=True, exist_ok=True)
    use_append = False
    writer_fieldnames = list(fieldnames)
    if append and path.exists() and path.stat().st_size > 0:
        existing_header = _read_csv_header(path)
        if existing_header:
            writer_fieldnames = existing_header
            use_append = True

    with open(path, "a" if use_append else "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=writer_fieldnames, extrasaction="ignore")
        if not use_append:
            writer.writeheader()
        for rec in records:
            writer.writerow(rec)


def export_state_changes(
    records: List[Dict[str, Any]],
    path: Path,
    *,
    append: bool = False,
    fieldnames: Optional[List[str]] = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    use_append = bool(append and path.exists() and path.stat().st_size > 0)
    state_change_fieldnames = list(fieldnames or DEFAULT_STATE_CHANGE_FIELDNAMES)
    with open(path, "a" if use_append else "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=state_change_fieldnames, extrasaction="ignore")
        if not use_append:
            writer.writeheader()
        for rec in records:
            writer.writerow(rec)

