"""FastAPI server for the World Configuration GUI.

Launch:
    python -m world.world_config_gui.server [--port 8088] [--config path/to/world_config.json]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import webbrowser
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import quote_plus


def _resolve_project_root() -> Path:
    env = os.environ.get("UGO_PROJECT_ROOT")
    if env:
        return Path(env).resolve()
    if getattr(sys, "frozen", False):
        # Frozen exe at <root>/world_config_gui/world_config_gui.exe
        return Path(sys.executable).resolve().parent.parent
    return Path(__file__).resolve().parents[2]


_PROJECT_ROOT_PATH = _resolve_project_root()
PROJECT_ROOT = str(_PROJECT_ROOT_PATH)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles

from world.world_config_gui.config_service import WorldConfigService
from world.world_config_gui.mir_api import MirApiClient, MirApiError

WORLD_DIR = _PROJECT_ROOT_PATH / "world"
DEFAULT_CONFIG_PATH = WORLD_DIR / "world_config.json"
if getattr(sys, "frozen", False):
    DEFAULT_MIR_CONFIG_PATH = WORLD_DIR / "world_config_gui" / "mir_config.json"
    STATIC_DIR = WORLD_DIR / "world_config_gui" / "static"
else:
    DEFAULT_MIR_CONFIG_PATH = Path(__file__).resolve().parent / "mir_config.json"
    STATIC_DIR = Path(__file__).resolve().parent / "static"
OCCUPANCY_WIP_FILE = WORLD_DIR / "world_occupancy_trace.wip.jsonl"
WORLD_SNAPSHOT_WIP_FILE = WORLD_DIR / "world_snapshot.wip.jsonl"
WORLD_SNAPSHOT_FILE = WORLD_DIR / "world_snapshot.jsonl"

app = FastAPI(title="uGO World Configuration GUI", version="1.0.0")

# Global service instance -- set by main()
_service: Optional[WorldConfigService] = None
_mir_client: Optional[MirApiClient] = None
_mir_config_path: Optional[Path] = None


def get_service() -> WorldConfigService:
    assert _service is not None, "Service not initialized"
    return _service


def get_mir_client() -> MirApiClient:
    global _mir_client
    if _mir_client is None:
        settings = load_mir_settings(_mir_config_path)
        _mir_client = MirApiClient.from_settings(settings)
    return _mir_client


def _raise_mir_http_error(exc: MirApiError) -> None:
    status_code = 502
    if exc.status_code in (401, 403):
        status_code = 401
    elif exc.status_code == 404:
        status_code = 404
    raise HTTPException(status_code=status_code, detail=str(exc))


def load_mir_settings(path: Optional[Path]) -> Dict[str, Any]:
    if path is None:
        return {}
    if not path.exists():
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except Exception as exc:
        raise RuntimeError(f"Failed to read MiR config file '{path}': {exc}") from exc
    if not isinstance(raw, dict):
        raise RuntimeError(f"MiR config file '{path}' must contain a JSON object")
    out = raw.get("mir", raw)
    if not isinstance(out, dict):
        raise RuntimeError(f"MiR config in '{path}' must be an object")
    return out


# ======================================================================
# HTML entry point
# ======================================================================

@app.get("/")
async def index():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/live")
async def live_view():
    return FileResponse(STATIC_DIR / "live.html")


# ======================================================================
# Live world state endpoints (for real-time viewer)
# ======================================================================

@app.get("/api/live/snapshot")
async def live_snapshot():
    """Return the latest world snapshot (from .wip file if running, else final)."""
    import json as _json
    for path in (WORLD_SNAPSHOT_WIP_FILE, WORLD_SNAPSHOT_FILE):
        if path.exists() and path.stat().st_size > 0:
            records = []
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        records.append(_json.loads(line))
            return {"source": path.name, "records": records}
    return {"source": None, "records": []}


@app.get("/api/live/events")
async def live_events(after: int = 0):
    """Return occupancy events from the WIP trace file, starting after line number `after`.

    The client polls this endpoint, passing the last-seen line count to get only new events.
    """
    import json as _json
    path = OCCUPANCY_WIP_FILE
    if not path.exists() or path.stat().st_size == 0:
        return {"events": [], "cursor": 0}
    events = []
    cursor = 0
    with open(path, "r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            cursor = i + 1
            if i < after:
                continue
            line = line.strip()
            if line:
                events.append(_json.loads(line))
    return {"events": events, "cursor": cursor}


# ======================================================================
# MiR API proxy endpoints
# ======================================================================

@app.get("/api/mir/health")
async def mir_health():
    client = get_mir_client()
    try:
        status = client.get_status()
    except MirApiError as exc:
        _raise_mir_http_error(exc)

    return {
        "ok": True,
        "base_url": client.base_url,
        "map_id": client.extract_map_id(status),
        "pose": client.extract_pose(status),
        "state_text": status.get("state_text"),
    }


@app.get("/api/mir/status")
async def mir_status():
    client = get_mir_client()
    try:
        status = client.get_status()
    except MirApiError as exc:
        _raise_mir_http_error(exc)

    return {
        "ok": True,
        "base_url": client.base_url,
        "status": status,
    }


@app.get("/api/mir/maps")
async def mir_maps():
    client = get_mir_client()
    try:
        maps = client.get_maps()
    except MirApiError as exc:
        _raise_mir_http_error(exc)
    return {"ok": True, "maps": maps}


@app.get("/api/mir/map-positions")
async def mir_map_positions(map_id: Optional[str] = None):
    client = get_mir_client()
    try:
        resolved_map_id = client.resolve_map_id(map_id)
        positions = client.get_map_positions(resolved_map_id, include_details=True)
    except MirApiError as exc:
        _raise_mir_http_error(exc)

    return {
        "ok": True,
        "map_id": resolved_map_id,
        "positions": positions,
    }


@app.get("/api/mir/positions/{position_guid}")
async def mir_get_position(position_guid: str):
    client = get_mir_client()
    try:
        position = client.get_position(position_guid)
    except MirApiError as exc:
        _raise_mir_http_error(exc)
    return {"ok": True, "position": position}


@app.put("/api/mir/positions/{position_guid}")
async def mir_update_position(position_guid: str, data: Dict[str, Any]):
    client = get_mir_client()
    allowed_fields = {
        "name",
        "pos_x",
        "pos_y",
        "orientation",
        "type_id",
        "parent_id",
        "map_id",
    }
    payload = {k: v for k, v in (data or {}).items() if k in allowed_fields}
    if not payload:
        raise HTTPException(status_code=400, detail="No editable position fields provided")

    try:
        if "pos_x" in payload:
            payload["pos_x"] = float(payload["pos_x"])
        if "pos_y" in payload:
            payload["pos_y"] = float(payload["pos_y"])
        if "orientation" in payload:
            payload["orientation"] = float(payload["orientation"])
        if "type_id" in payload and payload["type_id"] is not None:
            payload["type_id"] = int(payload["type_id"])
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="Invalid numeric value in position update payload")

    try:
        position = client.update_position(position_guid, payload)
    except MirApiError as exc:
        _raise_mir_http_error(exc)
    return {"ok": True, "position": position}


@app.get("/api/mir/map")
async def mir_map(map_id: Optional[str] = None):
    client = get_mir_client()
    try:
        resolved_map_id = client.resolve_map_id(map_id)
        map_payload = client.get_map(resolved_map_id)
    except MirApiError as exc:
        _raise_mir_http_error(exc)

    image_data_url = client.extract_map_image_data_url(map_payload)
    proxy_url = f"/api/mir/map-image?map_id={quote_plus(resolved_map_id)}"
    return {
        "ok": True,
        "map_id": resolved_map_id,
        "map": map_payload,
        "image_data_url": image_data_url,
        "proxy_image_url": proxy_url,
    }


@app.get("/api/mir/map-image")
async def mir_map_image(map_id: Optional[str] = None):
    client = get_mir_client()
    try:
        resolved_map_id = client.resolve_map_id(map_id)
        map_payload = client.get_map(resolved_map_id)
    except MirApiError as exc:
        _raise_mir_http_error(exc)

    # First: inlined base64 image from /maps/{map_id}
    data_url = client.extract_map_image_data_url(map_payload)
    if data_url:
        decoded = client.decode_data_url(data_url)
        if decoded:
            content, media_type = decoded
            return Response(content=content, media_type=media_type or "image/png")

    # Second: probe common MiR binary map endpoints.
    try:
        image_binary = client.get_map_image_binary(resolved_map_id, map_payload=map_payload)
    except MirApiError as exc:
        _raise_mir_http_error(exc)
    if image_binary is None:
        raise HTTPException(
            status_code=404,
            detail=f"No map image data found for map_id='{resolved_map_id}'",
        )
    content, media_type = image_binary
    return Response(content=content, media_type=media_type or "image/png")


@app.get("/api/mir/pose")
async def mir_pose(map_id: Optional[str] = None):
    client = get_mir_client()
    try:
        status = client.get_status()
    except MirApiError as exc:
        _raise_mir_http_error(exc)

    status_map_id = client.extract_map_id(status)
    requested_map_id = map_id.strip() if map_id and map_id.strip() else None
    pose = client.extract_pose(status)
    has_pose = any(v is not None for v in pose.values())

    return {
        "ok": True,
        "requested_map_id": requested_map_id,
        "status_map_id": status_map_id,
        "map_match": (requested_map_id is None) or (requested_map_id == status_map_id),
        "pose": pose,
        "pose_available": has_pose,
        "state_text": status.get("state_text"),
        "timestamp_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


# ======================================================================
# Config-level endpoints
# ======================================================================

@app.get("/api/config")
async def get_config():
    return get_service().get_config()


@app.get("/api/config/summary")
async def get_summary():
    return get_service().get_summary()


@app.post("/api/config/validate")
async def validate_config():
    return get_service().validate()


@app.post("/api/config/save")
async def save_config():
    result = get_service().save()
    if not result["saved"]:
        raise HTTPException(status_code=400, detail=result["errors"])
    return result


@app.post("/api/config/reload")
async def reload_config():
    get_service().reload()
    return {"reloaded": True}


# ======================================================================
# Enums
# ======================================================================

@app.get("/api/enums")
async def get_enums():
    return WorldConfigService.get_enums()


# ======================================================================
# Stations
# ======================================================================

@app.get("/api/stations")
async def list_stations():
    return get_service().get_stations()


@app.post("/api/stations")
async def upsert_station(data: Dict[str, Any]):
    if "id" not in data:
        raise HTTPException(status_code=400, detail="Station 'id' is required")
    try:
        get_service().upsert_station(data)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {"ok": True}


@app.delete("/api/stations/{station_id}")
async def delete_station(station_id: str):
    result = get_service().delete_station(station_id)
    if not result["deleted"]:
        raise HTTPException(status_code=404, detail=result["reason"])
    return result


# ======================================================================
# Station Slots
# ======================================================================

@app.post("/api/stations/{station_id}/slots")
async def upsert_slot(station_id: str, data: Dict[str, Any]):
    if "slot_id" not in data:
        raise HTTPException(status_code=400, detail="'slot_id' is required")
    if "kind" not in data:
        raise HTTPException(status_code=400, detail="'kind' is required")
    if "jig_id" not in data:
        raise HTTPException(status_code=400, detail="'jig_id' is required")
    try:
        get_service().upsert_station_slot(station_id, data)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {"ok": True}


@app.delete("/api/stations/{station_id}/slots/{slot_id}")
async def delete_slot(station_id: str, slot_id: str):
    result = get_service().delete_station_slot(station_id, slot_id)
    if not result["deleted"]:
        raise HTTPException(status_code=404, detail=result["reason"])
    return result


# ======================================================================
# Racks
# ======================================================================

@app.get("/api/racks")
async def list_racks():
    return get_service().get_racks()


@app.post("/api/racks")
async def upsert_rack(data: Dict[str, Any]):
    for field in ("id", "rack_type", "capacity", "pattern", "pin_obj_type"):
        if field not in data:
            raise HTTPException(status_code=400, detail=f"'{field}' is required")
    try:
        get_service().upsert_rack(data)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {"ok": True}


@app.delete("/api/racks/{rack_id}")
async def delete_rack(rack_id: str):
    result = get_service().delete_rack(rack_id)
    if not result["deleted"]:
        raise HTTPException(status_code=404, detail=result["reason"])
    return result


# ======================================================================
# Devices
# ======================================================================

@app.get("/api/devices")
async def list_devices():
    return get_service().get_devices()


@app.post("/api/devices")
async def upsert_device(data: Dict[str, Any]):
    for field in ("id", "name", "station_id"):
        if field not in data:
            raise HTTPException(status_code=400, detail=f"'{field}' is required")
    try:
        get_service().upsert_device(data)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {"ok": True}


@app.delete("/api/devices/{device_id}")
async def delete_device(device_id: str):
    result = get_service().delete_device(device_id)
    if not result["deleted"]:
        raise HTTPException(status_code=404, detail=result["reason"])
    return result


# ======================================================================
# Device WISE configuration
# ======================================================================

@app.put("/api/devices/{device_id}/wise")
async def update_device_wise(device_id: str, data: Dict[str, Any]):
    """Update or remove the WISE config block on a device. Send null/empty to remove."""
    wise_config = data.get("wise")
    result = get_service().update_device_wise(device_id, wise_config)
    if not result["updated"]:
        raise HTTPException(status_code=404, detail=result["reason"])
    return result


# ======================================================================
# Landmarks
# ======================================================================

@app.get("/api/landmarks")
async def list_landmarks():
    return get_service().get_landmarks()


@app.post("/api/landmarks")
async def upsert_landmark(data: Dict[str, Any]):
    for field in ("id", "code", "station_id"):
        if field not in data:
            raise HTTPException(status_code=400, detail=f"'{field}' is required")
    try:
        get_service().upsert_landmark(data)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {"ok": True}


@app.delete("/api/landmarks/{landmark_id}")
async def delete_landmark(landmark_id: str):
    result = get_service().delete_landmark(landmark_id)
    if not result["deleted"]:
        raise HTTPException(status_code=404, detail=result["reason"])
    return result


# ======================================================================
# Rack Placements
# ======================================================================

@app.get("/api/placements")
async def list_placements():
    return get_service().get_placements()


@app.post("/api/placements")
async def set_placement(data: Dict[str, Any]):
    for field in ("station_id", "station_slot_id", "rack_id"):
        if field not in data:
            raise HTTPException(status_code=400, detail=f"'{field}' is required")
    try:
        get_service().set_placement(data)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {"ok": True}


@app.delete("/api/placements/{station_id}/{slot_id}")
async def clear_placement(station_id: str, slot_id: str):
    get_service().clear_placement(station_id, slot_id)
    return {"ok": True}


# ======================================================================
# Samples
# ======================================================================

@app.get("/api/samples")
async def list_samples():
    return get_service().get_samples()


@app.post("/api/samples")
async def upsert_sample(data: Dict[str, Any]):
    if "id" not in data:
        raise HTTPException(status_code=400, detail="'id' is required")
    try:
        get_service().upsert_sample(data)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {"ok": True}


@app.delete("/api/samples/{sample_id}")
async def delete_sample(sample_id: str):
    result = get_service().delete_sample(sample_id)
    if not result["deleted"]:
        raise HTTPException(status_code=404, detail=result["reason"])
    return result


# ======================================================================
# Robot station
# ======================================================================

@app.get("/api/robot-station")
async def get_robot_station():
    return {"station_id": get_service().get_robot_station()}


@app.post("/api/robot-station")
async def set_robot_station(data: Dict[str, Any]):
    get_service().set_robot_station(data.get("station_id"))
    return {"ok": True}


# ======================================================================
# Process policies (read-only)
# ======================================================================

@app.get("/api/policies")
async def get_policies():
    return WorldConfigService.get_policies()


# ======================================================================
# Static files (CSS, JS, etc. if ever needed beyond index.html)
# ======================================================================

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# ======================================================================
# CLI entry point
# ======================================================================

def main() -> None:
    global _service, _mir_client, _mir_config_path

    parser = argparse.ArgumentParser(description="uGO World Configuration GUI")
    parser.add_argument("--port", type=int, default=8088, help="Port to serve on (default: 8088)")
    parser.add_argument("--config", type=str, default=str(DEFAULT_CONFIG_PATH), help="Path to world_config.json")
    parser.add_argument("--mir-config", type=str, default=str(DEFAULT_MIR_CONFIG_PATH), help="Path to MiR config JSON (default: world/world_config_gui/mir_config.json)")
    parser.add_argument("--no-browser", action="store_true", help="Don't auto-open browser")
    args = parser.parse_args()

    config_path = Path(args.config).resolve()
    _mir_config_path = Path(args.mir_config).resolve()
    _service = WorldConfigService(config_path)
    _mir_client = MirApiClient.from_settings(load_mir_settings(_mir_config_path))

    print(f"World Config GUI serving on http://localhost:{args.port}")
    print(f"Config file: {config_path}")
    print(f"MiR config file: {_mir_config_path} (exists={_mir_config_path.exists()})")

    if not args.no_browser:
        import threading
        threading.Timer(1.0, lambda: webbrowser.open(f"http://localhost:{args.port}")).start()

    uvicorn.run(app, host="0.0.0.0", port=args.port, log_level="info")


if __name__ == "__main__":
    main()
