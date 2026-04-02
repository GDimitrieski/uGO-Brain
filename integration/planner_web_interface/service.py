from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional
import threading


PROJECT_ROOT = Path(__file__).resolve().parents[2]
LIB_DIR = PROJECT_ROOT / "Library"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(LIB_DIR) not in sys.path:
    sys.path.insert(0, str(LIB_DIR))

from credentials import credentials
from login import login
from mode_get import get_mode
from planner_get_event import planner_get_event
from planner_get_state import get_planner_state
from planner_post_event import planner_post_event
from planner_post_state import planner_post_state
from post_planner_message import dismiss_planner_prompt


WORLD_DIR = PROJECT_ROOT / "world"
WORLD_CONFIG_FILE = WORLD_DIR / "world_config.json"
WORLD_BACKUP_DIR = WORLD_DIR / "versions"
OCCUPANCY_TRACE_FILE = WORLD_DIR / "world_occupancy_trace.csv"
OCCUPANCY_EVENTS_FILE = WORLD_DIR / "world_occupancy_trace.jsonl"
OCCUPANCY_TRACE_WIP_FILE = WORLD_DIR / "world_occupancy_trace.wip.csv"
OCCUPANCY_EVENTS_WIP_FILE = WORLD_DIR / "world_occupancy_trace.wip.jsonl"
WORLD_SNAPSHOT_FILE = WORLD_DIR / "world_snapshot.jsonl"
WORLD_SNAPSHOT_WIP_FILE = WORLD_DIR / "world_snapshot.wip.jsonl"
RUNTIME_DIR = PROJECT_ROOT / "runtime"
DEFAULT_PAUSE_REQUEST_FILE = RUNTIME_DIR / "planner_workflow_pause.request"
DEFAULT_PAUSE_ACK_FILE = RUNTIME_DIR / "planner_workflow_paused.ack"


def _safe_print(*args: Any, **kwargs: Any) -> None:
    try:
        print(*args, **kwargs)
    except OSError:
        pass
    except ValueError:
        pass


@dataclass
class PlannerInterfaceSnapshot:
    mode: str = "unknown"
    requested_event: Optional[int] = None
    requested_state: Optional[int] = None
    runtime_mode: Optional[str] = None
    runtime_state: Optional[int] = None
    raw_mode: Optional[Dict[str, Any]] = None
    raw_event: Optional[Dict[str, Any]] = None
    raw_state: Optional[Dict[str, Any]] = None
    errors: Dict[str, str] = field(default_factory=dict)


class PlannerWebInterfaceBridge:
    """
    Bridge between planner runtime and planner web interface endpoints.

    Request direction (from web):
    - get_mode()
    - planner_get_event()
    - get_planner_state()

    Indication direction (to web):
    - planner_post_event()    -> planner currently handled event
    - planner_post_state()    -> planner current state indication
    """

    def __init__(
        self,
        url: str,
        user: str,
        password: str,
        poll_interval_s: float = 1.0,
    ) -> None:
        self.url = str(url).strip()
        self.user = str(user).strip()
        self.password = str(password)
        self.poll_interval_s = max(0.1, float(poll_interval_s))
        self.token: Optional[str] = None

        self._last_posted_event: Optional[int] = None
        self._last_posted_state: Optional[int] = None
        self._last_requested_event: Optional[int] = None
        self._last_requested_state: Optional[int] = None

        # Planner event/state numeric conventions.
        self._event_reset = int(os.getenv("UGO_PLANNER_EVENT_RESET", "0"))
        self._event_start = int(os.getenv("UGO_PLANNER_EVENT_START", "1"))
        self._event_stop = int(os.getenv("UGO_PLANNER_EVENT_STOP", "2"))

        self._state_execute = int(os.getenv("UGO_PLANNER_STATE_EXECUTE", "1"))
        self._state_stopping = int(os.getenv("UGO_PLANNER_STATE_STOPPING", "2"))
        self._state_resetting = int(os.getenv("UGO_PLANNER_STATE_RESETTING", "3"))
        self._state_starting = int(os.getenv("UGO_PLANNER_STATE_STARTING", "4"))
        self._state_manual_ready = int(os.getenv("UGO_PLANNER_STATE_MANUAL_READY", "0"))
        self._state_stopped = int(os.getenv("UGO_PLANNER_STATE_STOPPED", "0"))
        try:
            self._transient_min_hold_s = max(0.0, float(os.getenv("UGO_PLANNER_TRANSIENT_MIN_HOLD_S", "1.5")))
        except Exception:
            self._transient_min_hold_s = 1.5
        try:
            self._transient_republish_s = max(0.1, float(os.getenv("UGO_PLANNER_TRANSIENT_REPUBLISH_S", "1.0")))
        except Exception:
            self._transient_republish_s = 1.0

        self._runtime_mode: str = str(os.getenv("UGO_PLANNER_INITIAL_MODE", "unknown")).strip().lower() or "unknown"
        self._runtime_state: int = int(os.getenv("UGO_PLANNER_INITIAL_STATE", str(self._state_stopped)))

        self._workflow_module = (
            str(os.getenv("UGO_PLANNER_WORKFLOW_MODULE", "workflows.rack_probe_transfer_workflow")).strip()
            or "workflows.rack_probe_transfer_workflow"
        )
        self._workflow_process: Optional[subprocess.Popen[Any]] = None
        self._start_grace_s = max(0.0, float(os.getenv("UGO_PLANNER_START_GRACE_S", "0.3")))
        self._stop_timeout_s = max(1.0, float(os.getenv("UGO_PLANNER_STOP_TIMEOUT_S", "10.0")))
        self._post_handled_event_to_api = str(os.getenv("UGO_PLANNER_POST_HANDLED_EVENT_TO_API", "0")).strip().lower() in {
            "1",
            "true",
            "yes",
        }
        self._pause_request_file = Path(
            os.getenv("UGO_PLANNER_PAUSE_REQUEST_FILE", str(DEFAULT_PAUSE_REQUEST_FILE))
        ).resolve()
        self._pause_ack_file = Path(
            os.getenv("UGO_PLANNER_PAUSE_ACK_FILE", str(DEFAULT_PAUSE_ACK_FILE))
        ).resolve()
        for path in (self._pause_request_file, self._pause_ack_file):
            path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self._workflow_pause_poll_s = max(0.1, float(os.getenv("UGO_PLANNER_PAUSE_POLL_S", "0.2")))
        except Exception:
            self._workflow_pause_poll_s = 0.2
        self._debug_enabled = str(os.getenv("UGO_PLANNER_BRIDGE_DEBUG", "0")).strip().lower() in {
            "1",
            "true",
            "yes",
        }
        self._sync_seq = 0
        self._dbg_lock = threading.Lock()

    def _dbg(self, message: str) -> None:
        if not self._debug_enabled:
            return
        with self._dbg_lock:
            _safe_print(f"[BRIDGE_DEBUG] {message}")

    def authenticate(self) -> bool:
        token = login(self.url, self.user, self.password)
        if not token:
            self.token = None
            return False
        self.token = token
        return True

    def _ensure_auth(self) -> bool:
        if self.token:
            return True
        return self.authenticate()

    @staticmethod
    def _extract_int(d: Optional[Dict[str, Any]], *path: str) -> Optional[int]:
        if not isinstance(d, dict):
            return None
        cur: Any = d
        for key in path:
            if not isinstance(cur, dict):
                return None
            cur = cur.get(key)
        if cur is None:
            return None
        try:
            return int(cur)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _extract_str(d: Optional[Dict[str, Any]], *path: str) -> Optional[str]:
        if not isinstance(d, dict):
            return None
        cur: Any = d
        for key in path:
            if not isinstance(cur, dict):
                return None
            cur = cur.get(key)
        if cur is None:
            return None
        out = str(cur).strip()
        return out or None

    def poll_requested(self) -> PlannerInterfaceSnapshot:
        snap = PlannerInterfaceSnapshot()

        raw_mode: Optional[Dict[str, Any]] = None
        try:
            raw_mode = get_mode(self.url)
        except Exception as exc:
            snap.errors["mode"] = str(exc)
        snap.raw_mode = raw_mode
        mode = self._extract_str(raw_mode, "data")
        if mode:
            snap.mode = mode.lower()

        if not self._ensure_auth():
            snap.errors["auth"] = "login_failed"
            return snap

        raw_event: Optional[Dict[str, Any]] = None
        raw_state: Optional[Dict[str, Any]] = None
        try:
            raw_event = planner_get_event(self.url, self.token)
        except Exception as exc:
            snap.errors["event"] = str(exc)
        try:
            raw_state = get_planner_state(self.url, self.token)
        except Exception as exc:
            snap.errors["state"] = str(exc)

        snap.raw_event = raw_event
        snap.raw_state = raw_state
        snap.requested_event = self._extract_int(raw_event, "data", "event")
        snap.requested_state = self._extract_int(raw_state, "data", "state")
        snap.runtime_mode = self._runtime_mode
        snap.runtime_state = self._runtime_state
        return snap

    def set_runtime_mode(self, mode: str) -> None:
        normalized = str(mode).strip().lower()
        if normalized:
            self._runtime_mode = normalized

    def set_runtime_state(self, state: int) -> None:
        self._runtime_state = int(state)

    def publish_current_event(self, event: int, force: bool = False) -> bool:
        if not self._ensure_auth():
            return False
        event_int = int(event)
        if not force and self._last_posted_event == event_int:
            return True
        response = planner_post_event(self.url, self.token, event_int)
        ok = isinstance(response, dict)
        if ok:
            self._last_posted_event = event_int
        return ok

    def publish_current_state(self, state: int, force: bool = False) -> bool:
        if not self._ensure_auth():
            return False
        state_int = int(state)
        if not force and self._last_posted_state == state_int:
            return True
        response = planner_post_state(self.url, self.token, state_int)
        ok = isinstance(response, dict)
        if ok:
            self._last_posted_state = state_int
        return ok

    def _publish_runtime_state(self, observed_state: Optional[int]) -> bool:
        """
        Publish current runtime state and force-write when endpoint state drifted
        away from runtime state.
        """
        force = False
        if observed_state is not None:
            try:
                force = int(observed_state) != int(self._runtime_state)
            except Exception:
                force = True
        return self.publish_current_state(self._runtime_state, force=force)

    def _publish_transient_state_with_min_hold(self, state: int) -> None:
        """
        Publish a transient planner state and keep it visible for at least the
        configured minimum hold duration so UI users can observe the transition.
        """
        started_at = time.time()
        self._runtime_state = int(state)
        state_name = (
            "STARTING" if int(state) == int(self._state_starting)
            else "RESETTING" if int(state) == int(self._state_resetting)
            else f"STATE_{int(state)}"
        )
        _safe_print(
            f"Transient state begin: {state_name} ({int(state)}), "
            f"min_hold_s={float(self._transient_min_hold_s):.3f}"
        )
        self.publish_current_state(self._runtime_state, force=True)
        hold_s = float(self._transient_min_hold_s)
        if hold_s > 0:
            deadline = started_at + hold_s
            while True:
                now = time.time()
                if now >= deadline:
                    break
                sleep_s = min(float(self._transient_republish_s), max(0.0, deadline - now))
                if sleep_s > 0:
                    time.sleep(sleep_s)
                # Heartbeat publish so UI polling has multiple chances to observe transient state.
                self.publish_current_state(self._runtime_state, force=True)
        elapsed_s = time.time() - started_at
        _safe_print(f"Transient state end: {state_name} held_for_s={elapsed_s:.3f}")

    def initialize_startup_state(self) -> bool:
        """
        Initialize planner runtime state to STOPPED when bridge starts.
        """
        self._clear_workflow_pause_control()
        self._runtime_state = self._state_stopped
        return self.publish_current_state(self._runtime_state, force=True)

    def _pause_requested(self) -> bool:
        return self._pause_request_file.exists()

    def _workflow_pause_acknowledged(self) -> bool:
        return self._pause_ack_file.exists()

    def _request_workflow_pause(self) -> None:
        self._pause_request_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self._pause_request_file, "w", encoding="utf-8") as f:
            f.write(self._local_now_iso() + "\n")

    def _clear_workflow_pause_request(self) -> None:
        try:
            self._pause_request_file.unlink(missing_ok=True)
        except Exception as exc:
            _safe_print(f"Failed clearing pause request file: {exc}")

    def _clear_workflow_pause_control(self) -> None:
        self._clear_workflow_pause_request()
        try:
            self._pause_ack_file.unlink(missing_ok=True)
        except Exception as exc:
            _safe_print(f"Failed clearing pause ack file: {exc}")

    def _dismiss_active_prompt(self) -> None:
        """Dismiss any active control-system prompt so the UI is clean after RESET."""
        if not self._ensure_auth():
            return
        try:
            dismiss_planner_prompt(self.url, self.token)
        except Exception as exc:
            _safe_print(f"Failed to dismiss active prompt on reset: {exc}")

    def _is_workflow_running(self) -> bool:
        return self._workflow_process is not None and self._workflow_process.poll() is None

    def _refresh_workflow_runtime_state(self) -> None:
        proc = self._workflow_process
        if proc is None:
            return
        rc = proc.poll()
        if rc is None:
            return
        _safe_print(f"Planner workflow process ended with rc={rc}")
        self._workflow_process = None
        self._clear_workflow_pause_control()
        if self._runtime_state in {self._state_starting, self._state_execute, self._state_stopping}:
            self._runtime_state = self._state_stopped

    def _consume_new_event(self, requested_event: Optional[int]) -> Optional[int]:
        if requested_event is None:
            self._dbg(
                f"consume_event raw=None last_requested_event={self._last_requested_event} -> new=None (reset edge cache)"
            )
            self._last_requested_event = None
            return None
        event_int = int(requested_event)
        if self._last_requested_event == event_int:
            self._dbg(
                f"consume_event raw={event_int} last_requested_event={self._last_requested_event} -> new=None"
            )
            return None
        prev = self._last_requested_event
        self._last_requested_event = event_int
        self._dbg(f"consume_event raw={event_int} prev={prev} -> new={event_int}")
        return event_int

    def _consume_new_requested_state(self, requested_state: Optional[int]) -> Optional[int]:
        if requested_state is None:
            self._dbg(
                f"consume_state raw=None last_requested_state={self._last_requested_state} -> new=None (reset edge cache)"
            )
            self._last_requested_state = None
            return None
        state_int = int(requested_state)
        if self._last_requested_state == state_int:
            self._dbg(
                f"consume_state raw={state_int} last_requested_state={self._last_requested_state} -> new=None"
            )
            return None
        prev = self._last_requested_state
        self._last_requested_state = state_int
        self._dbg(f"consume_state raw={state_int} prev={prev} -> new={state_int}")
        return state_int

    def _event_from_requested_state(self, requested_state: int) -> Optional[int]:
        state_int = int(requested_state)
        running = self._is_workflow_running()

        # START command via requested state.
        if state_int == self._state_starting:
            if running and self._pause_requested():
                return self._event_start
            if not running and int(self._runtime_state) == self._state_stopped:
                return self._event_start
            return None

        # STOP command via requested state.
        if state_int == self._state_stopping:
            if running or int(self._runtime_state) in {self._state_starting, self._state_execute}:
                return self._event_stop
            return None

        # RESET command via requested state.
        if state_int == self._state_resetting:
            if not running and int(self._runtime_state) == self._state_stopped:
                return self._event_reset
            # Allow RESET from paused runtime:
            # workflow process can still be alive while planner state is STOPPED.
            if (
                running
                and self._pause_requested()
                and self._workflow_pause_acknowledged()
                and int(self._runtime_state) == self._state_stopped
            ):
                return self._event_reset
            return None

        return None

    def _start_workflow(self) -> bool:
        if self._is_workflow_running():
            self._runtime_state = self._state_execute
            self.publish_current_state(self._runtime_state)
            return True

        # UI should already indicate STARTING from requested state/event.
        self._clear_workflow_pause_control()
        self._publish_transient_state_with_min_hold(self._state_starting)

        env = os.environ.copy()
        env["UGO_RESUME_FROM_LAST_WORLD_SNAPSHOT"] = "1"
        env["UGO_PLANNER_PAUSE_REQUEST_FILE"] = str(self._pause_request_file)
        env["UGO_PLANNER_PAUSE_ACK_FILE"] = str(self._pause_ack_file)
        env["UGO_PLANNER_PAUSE_POLL_S"] = str(self._workflow_pause_poll_s)
        py_paths = [str(PROJECT_ROOT), str(LIB_DIR)]
        existing_py_path = str(env.get("PYTHONPATH", "")).strip()
        if existing_py_path:
            py_paths.append(existing_py_path)
        env["PYTHONPATH"] = os.pathsep.join(py_paths)
        env["PYTHONUNBUFFERED"] = "1"
        cmd = [sys.executable, "-u", "-m", self._workflow_module]
        _safe_print(f"Starting planner workflow: {' '.join(cmd)}")

        try:
            self._workflow_process = subprocess.Popen(
                cmd,
                cwd=str(PROJECT_ROOT),
                env=env,
            )
        except Exception as exc:
            _safe_print(f"Planner start failed: {exc}")
            self._workflow_process = None
            self._runtime_state = self._state_stopped
            self.publish_current_state(self._runtime_state, force=True)
            return False

        if self._start_grace_s > 0:
            time.sleep(self._start_grace_s)
        self._refresh_workflow_runtime_state()

        if not self._is_workflow_running():
            self._runtime_state = self._state_stopped
            self.publish_current_state(self._runtime_state, force=True)
            return False

        self._runtime_state = self._state_execute
        self.publish_current_state(self._runtime_state, force=True)
        return True

    def _request_graceful_stop(self) -> bool:
        proc = self._workflow_process
        if proc is None or proc.poll() is not None:
            self._workflow_process = None
            self._clear_workflow_pause_control()
            self._runtime_state = self._state_stopped
            self.publish_current_state(self._runtime_state)
            return True

        self._request_workflow_pause()
        self._runtime_state = self._state_stopping
        self.publish_current_state(self._runtime_state, force=True)
        _safe_print(
            "Graceful STOP requested: planner will pause before sending the next workflow action."
        )
        return True

    def _stop_workflow(self) -> bool:
        proc = self._workflow_process
        if proc is None or proc.poll() is not None:
            self._workflow_process = None
            self._clear_workflow_pause_control()
            self._runtime_state = self._state_stopped
            self.publish_current_state(self._runtime_state)
            return True

        _safe_print("Stopping planner workflow process")
        try:
            proc.terminate()
            proc.wait(timeout=self._stop_timeout_s)
        except subprocess.TimeoutExpired:
            _safe_print("Planner workflow did not terminate in time, killing process")
            proc.kill()
            proc.wait(timeout=5.0)
        except Exception as exc:
            _safe_print(f"Planner stop failed: {exc}")
            return False

        self._workflow_process = None
        self._clear_workflow_pause_control()
        self._runtime_state = self._state_stopped
        self.publish_current_state(self._runtime_state, force=True)
        return True

    @staticmethod
    def _local_now_iso() -> str:
        return datetime.now().astimezone().isoformat(timespec="milliseconds")

    @staticmethod
    def _world_state_snapshot(world: Any) -> Dict[str, Any]:
        from world.lab_world import GripperLocation, RackLocation

        racks = []
        station_slots = []
        sample_ids_in_gripper = []

        for station_id in sorted(world.stations.keys()):
            station = world.stations[station_id]
            for station_slot_id in sorted(station.slot_configs.keys()):
                slot_cfg = station.slot_configs[station_slot_id]
                mounted_rack_id = world.rack_placements.get((station_id, station_slot_id))
                mounted_rack = world.racks.get(mounted_rack_id) if mounted_rack_id else None
                station_slots.append(
                    {
                        "station_id": station_id,
                        "station_slot_id": station_slot_id,
                        "slot_kind": slot_cfg.kind.value,
                        "jig_id": slot_cfg.jig_id,
                        "itm_id": slot_cfg.itm_id,
                        "accepted_rack_types": sorted(t.value for t in slot_cfg.accepted_rack_types),
                        "mounted_rack_id": mounted_rack_id,
                        "mounted_rack_type": mounted_rack.rack_type.value if mounted_rack else None,
                        "slot_state": "RACK_PRESENT" if mounted_rack_id else "EMPTY",
                    }
                )

        for sample_id, sample_state in sorted(world.sample_states.items()):
            if isinstance(sample_state.location, GripperLocation):
                sample_ids_in_gripper.append(sample_id)

        accepted_rack_types = sorted({rack.rack_type.value for rack in world.racks.values()})
        station_slots.append(
            {
                "station_id": "uLM_GRIPPER",
                "station_slot_id": "RackGrip",
                "slot_kind": "VIRTUAL_GRIPPER_RACK_SLOT",
                "jig_id": -1,
                "itm_id": -1,
                "accepted_rack_types": accepted_rack_types,
                "mounted_rack_id": world.rack_in_gripper_id,
                "mounted_rack_type": (
                    world.racks[world.rack_in_gripper_id].rack_type.value
                    if world.rack_in_gripper_id and world.rack_in_gripper_id in world.racks
                    else None
                ),
                "slot_state": "RACK_PRESENT" if world.rack_in_gripper_id else "EMPTY",
            }
        )
        station_slots.append(
            {
                "station_id": "uLM_GRIPPER",
                "station_slot_id": "SampleGrip",
                "slot_kind": "VIRTUAL_GRIPPER_SAMPLE_SLOT",
                "jig_id": -1,
                "itm_id": -1,
                "accepted_rack_types": [],
                "mounted_rack_id": None,
                "mounted_rack_type": None,
                "mounted_sample_ids": sample_ids_in_gripper,
                "slot_state": "SAMPLE_PRESENT" if sample_ids_in_gripper else "EMPTY",
            }
        )

        for (station_id, station_slot_id), rack_id in sorted(world.rack_placements.items()):
            rack = world.racks.get(rack_id)
            if rack is None:
                continue
            racks.append(
                {
                    "station_id": station_id,
                    "station_slot_id": station_slot_id,
                    "rack_id": rack_id,
                    "rack_type": rack.rack_type.value,
                    "pattern": rack.pattern,
                    "rows": rack.rows,
                    "cols": rack.cols,
                    "blocked_slots": sorted(rack.blocked_slots),
                    "occupied_slots": {str(k): v for k, v in sorted(rack.occupied_slots.items())},
                    "reserved_slots": {str(k): v for k, v in sorted(rack.reserved_slots.items())},
                }
            )

        if world.rack_in_gripper_id:
            rack = world.racks.get(world.rack_in_gripper_id)
            if rack is not None:
                racks.append(
                    {
                        "station_id": "uLM_GRIPPER",
                        "station_slot_id": "RackGrip",
                        "rack_id": rack.id,
                        "rack_type": rack.rack_type.value,
                        "pattern": rack.pattern,
                        "rows": rack.rows,
                        "cols": rack.cols,
                        "blocked_slots": sorted(rack.blocked_slots),
                        "occupied_slots": {str(k): v for k, v in sorted(rack.occupied_slots.items())},
                        "reserved_slots": {str(k): v for k, v in sorted(rack.reserved_slots.items())},
                    }
                )

        sample_locations = []
        for sample_id, sample_state in sorted(world.sample_states.items()):
            loc = sample_state.location
            if isinstance(loc, RackLocation):
                sample_locations.append(
                    {
                        "sample_id": sample_id,
                        "location_type": "RACK",
                        "station_id": loc.station_id,
                        "station_slot_id": loc.station_slot_id,
                        "rack_id": loc.rack_id,
                        "slot_index": loc.slot_index,
                    }
                )
            elif isinstance(loc, GripperLocation):
                sample_locations.append(
                    {
                        "sample_id": sample_id,
                        "location_type": "GRIPPER",
                        "gripper_id": loc.gripper_id,
                    }
                )

        return {
            "robot_current_station_id": world.robot_current_station_id,
            "rack_in_gripper_id": world.rack_in_gripper_id,
            "station_slots": station_slots,
            "racks": racks,
            "sample_locations": sample_locations,
        }

    @staticmethod
    def _reset_backup_dir() -> Path:
        stamp = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S")
        candidate = WORLD_BACKUP_DIR / f"reset_{stamp}"
        idx = 1
        while candidate.exists():
            candidate = WORLD_BACKUP_DIR / f"reset_{stamp}_{idx:02d}"
            idx += 1
        candidate.mkdir(parents=True, exist_ok=True)
        return candidate

    @staticmethod
    def _backup_file_to_dir(path: Path, backup_dir: Path) -> None:
        if not path.exists():
            return
        try:
            shutil.copy2(path, backup_dir / path.name)
        except Exception as exc:
            _safe_print(f"Backup failed for {path}: {exc}")

    def _reset_world_to_baseline(self) -> bool:
        from world.export_world_snapshot_jsonl import build_snapshot_records, write_jsonl
        from world.lab_world import ensure_world_config_file

        try:
            world = ensure_world_config_file(WORLD_CONFIG_FILE)
            event = {
                "timestamp": self._local_now_iso(),
                "event_type": "WORLD_SNAPSHOT",
                "entity_type": "WORLD",
                "entity_id": "WORLD",
                "source": {},
                "target": {},
                "details": {"reason": "manual_reset_to_baseline", "source": "planner_web_interface"},
                "state_after": self._world_state_snapshot(world),
            }

            reset_backup_dir = self._reset_backup_dir()
            for path in (
                OCCUPANCY_TRACE_FILE,
                OCCUPANCY_EVENTS_FILE,
                OCCUPANCY_TRACE_WIP_FILE,
                OCCUPANCY_EVENTS_WIP_FILE,
                WORLD_SNAPSHOT_FILE,
                WORLD_SNAPSHOT_WIP_FILE,
            ):
                self._backup_file_to_dir(path, reset_backup_dir)

            line = json.dumps(event, ensure_ascii=True) + "\n"
            for path in (
                OCCUPANCY_TRACE_FILE,
                OCCUPANCY_EVENTS_FILE,
                OCCUPANCY_TRACE_WIP_FILE,
                OCCUPANCY_EVENTS_WIP_FILE,
            ):
                path.parent.mkdir(parents=True, exist_ok=True)
                with open(path, "w", encoding="utf-8") as f:
                    f.write(line)

            snapshot_records = build_snapshot_records(world, config_path=WORLD_CONFIG_FILE)
            write_jsonl(WORLD_SNAPSHOT_FILE, snapshot_records)
            write_jsonl(WORLD_SNAPSHOT_WIP_FILE, snapshot_records)

            _safe_print(
                "World reset to baseline complete. "
                f"Backup folder: {reset_backup_dir.resolve()}"
            )
            return True
        except Exception as exc:
            _safe_print(f"World reset to baseline failed: {exc}")
            return False

    def _handle_event_automatic(self, event: int) -> None:
        if self._post_handled_event_to_api:
            self.publish_current_event(event, force=True)

        if event == self._event_start:
            if self._is_workflow_running():
                if self._pause_requested():
                    _safe_print("Resuming paused planner workflow")
                    self._clear_workflow_pause_request()
                self._runtime_state = self._state_execute
                self.publish_current_state(self._runtime_state, force=True)
                return
            if self._runtime_state != self._state_stopped:
                _safe_print(f"Ignoring START event while state={self._runtime_state}")
                return
            self._start_workflow()
            return

        if event == self._event_stop:
            self._request_graceful_stop()
            return

        if event == self._event_reset:
            if self._is_workflow_running():
                paused_running = (
                    self._pause_requested()
                    and self._workflow_pause_acknowledged()
                    and int(self._runtime_state) == self._state_stopped
                )
                if not paused_running:
                    _safe_print("Ignoring RESET event while planner is running; stop first.")
                    return
                _safe_print("RESET requested while planner is paused; stopping paused workflow first.")
                if not self._stop_workflow():
                    _safe_print("RESET aborted: failed to stop paused workflow process.")
                    return
            elif self._runtime_state == self._state_execute:
                _safe_print("Ignoring RESET event while planner is running; stop first.")
                return
            self._clear_workflow_pause_control()
            self._dismiss_active_prompt()
            self._publish_transient_state_with_min_hold(self._state_resetting)
            self._reset_world_to_baseline()
            self._runtime_state = self._state_stopped
            self.publish_current_state(self._runtime_state, force=True)
            return

        _safe_print(f"Ignoring unknown event={event}")

    def sync_once(self, runtime_mode: Optional[str] = None, runtime_state: Optional[int] = None) -> PlannerInterfaceSnapshot:
        """
        Sync behavior:
        - MANUAL mode:
          - planner execution disabled
          - if workflow process is running it is stopped
          - READY indication is posted
        - AUTOMATIC mode:
          - START event: STOPPED -> STARTING -> EXECUTE and launch workflow
          - RESET event while stopped: RESETTING -> baseline world reset -> STOPPED
          - STOP event: stop workflow and set STOPPED
          - runtime state is continuously posted
        """
        if runtime_mode is not None:
            self.set_runtime_mode(runtime_mode)
        if runtime_state is not None:
            self.set_runtime_state(runtime_state)

        self._sync_seq += 1
        snap = self.poll_requested()
        mode = str(snap.mode or "").strip().lower()
        self._dbg(
            f"sync#{self._sync_seq} polled mode={mode} requested_event={snap.requested_event} "
            f"requested_state={snap.requested_state} runtime_state={self._runtime_state} "
            f"workflow_running={self._is_workflow_running()} workflow_pid={getattr(self._workflow_process, 'pid', None)}"
        )

        if mode in {"manual", "automatic"}:
            self._runtime_mode = mode

        self._refresh_workflow_runtime_state()
        new_event = self._consume_new_event(snap.requested_event)
        new_requested_state = self._consume_new_requested_state(snap.requested_state)
        self._dbg(
            f"sync#{self._sync_seq} edge_detected new_event={new_event} new_requested_state={new_requested_state}"
        )

        if mode == "manual":
            if self._is_workflow_running():
                self._stop_workflow()
            self._runtime_state = self._state_manual_ready
            self._publish_runtime_state(snap.requested_state)
            snap.runtime_mode = self._runtime_mode
            snap.runtime_state = self._runtime_state
            return snap

        if mode == "automatic":
            command_event = new_event
            if command_event is None and new_requested_state is not None:
                command_event = self._event_from_requested_state(new_requested_state)
                self._dbg(
                    f"sync#{self._sync_seq} command_event derived from requested_state={new_requested_state} -> {command_event}"
                )
            else:
                self._dbg(f"sync#{self._sync_seq} command_event from event edge -> {command_event}")
            if command_event is not None:
                self._dbg(f"sync#{self._sync_seq} handling command_event={command_event}")
                self._handle_event_automatic(command_event)

            self._refresh_workflow_runtime_state()
            if self._pause_requested():
                if self._is_workflow_running():
                    if self._workflow_pause_acknowledged():
                        self._runtime_state = self._state_stopped
                    else:
                        self._runtime_state = self._state_stopping
                else:
                    self._clear_workflow_pause_control()
                    self._runtime_state = self._state_stopped
            else:
                if self._is_workflow_running():
                    if self._runtime_state != self._state_starting:
                        self._runtime_state = self._state_execute
                elif self._runtime_state in {self._state_execute, self._state_starting, self._state_stopping}:
                    self._runtime_state = self._state_stopped
                    self._clear_workflow_pause_control()

            self._publish_runtime_state(snap.requested_state)
            snap.runtime_mode = self._runtime_mode
            snap.runtime_state = self._runtime_state
            return snap

        # Unknown mode: keep conservative behavior (state indication only).
        self._publish_runtime_state(snap.requested_state)
        snap.runtime_mode = self._runtime_mode
        snap.runtime_state = self._runtime_state
        return snap

    def run_forever(self) -> None:
        while True:
            snap = self.sync_once()
            _safe_print(
                "Bridge sync: "
                f"mode={snap.mode} "
                f"requested_event={snap.requested_event} "
                f"requested_state={snap.requested_state} "
                f"runtime_state={snap.runtime_state} "
                f"workflow_running={self._is_workflow_running()}"
            )
            time.sleep(self.poll_interval_s)


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Planner <-> Web Interface bridge loop")
    parser.add_argument("--url", default=credentials.get("url", "http://localhost:8080"))
    parser.add_argument("--user", default=credentials.get("user", "planner"))
    parser.add_argument("--password", default=credentials.get("password", ""))
    parser.add_argument("--poll-s", type=float, default=1.0)
    return parser


def main() -> None:
    args = _build_arg_parser().parse_args()
    bridge = PlannerWebInterfaceBridge(
        url=args.url,
        user=args.user,
        password=args.password,
        poll_interval_s=args.poll_s,
    )
    if not bridge.authenticate():
        raise SystemExit("Planner web bridge login failed.")
    bridge.initialize_startup_state()
    bridge.run_forever()


if __name__ == "__main__":
    main()
