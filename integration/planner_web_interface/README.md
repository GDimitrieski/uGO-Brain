# Planner Web Interface Integration

This folder adds a bridge service for planner <-> web interface synchronization.

## Implemented Behavior

Request side (read from web interface):
- `get_mode(url)` from `Library/mode_get.py`
- `planner_get_event(url, token)` from `Library/planner_get_event.py`
- `get_planner_state(url, token)` from `Library/planner_get_state.py`

Indication side (write to web interface):
- `planner_post_event(url, token, event)` from `Library/planner_post_event.py`
- `planner_post_state(url, token, state)` from `Library/planner_post_state.py`

Sync policy:
- `manual` mode:
  - planner command execution is disabled
  - if a workflow process is running, it is stopped
  - bridge posts READY-only indication (`UGO_PLANNER_STATE_MANUAL_READY`, default `0`)
- `automatic` mode:
  - `START` (`event=1`):
    - UI requested state indicates `STARTING`
    - if workflow is paused in-process, clears pause and resumes from next action
    - otherwise launches `python -m workflows.rack_probe_transfer_workflow`
    - once process is alive (or resumed), bridge posts stable state `EXECUTE`
  - `RESET` (`event=0`) while stopped:
    - UI requested state indicates `RESETTING`
    - if workflow is paused (process alive, planner STOPPED), bridge stops that paused process first
    - resets world traces/snapshots to baseline (`world/world_config.json`)
    - then bridge posts stable state `STOPPED`
  - `STOP` (`event=2`):
    - UI requested state indicates `STOPPING`
    - bridge requests a graceful pause (does not kill workflow process)
    - current in-flight action is allowed to finish
    - workflow then pauses before sending the next action and bridge posts stable state `STOPPED`

Runtime ownership:
- The system manager is this project.
- Runtime state is now managed in the bridge itself from process lifecycle and UI events.

## Files

- `service.py`:
  - `PlannerWebInterfaceBridge` class
  - `poll_requested()` to collect requested mode/event/state
  - process lifecycle handling for planner workflow subprocess
  - baseline-reset routine for world traces/snapshots
  - `publish_current_event()` and `publish_current_state()` helpers
  - `set_runtime_mode()` and `set_runtime_state()` helpers (optional external override)
  - `sync_once()` default sync cycle
  - `run_forever()` polling loop

## Run

From project root:

```powershell
python -m integration.planner_web_interface.service
```

With custom params:

```powershell
python -m integration.planner_web_interface.service --url http://localhost:8080 --user planner --password cobiotx --poll-s 1.0
```

## Notes

- Numeric mappings are configurable with env vars:
  - `UGO_PLANNER_EVENT_RESET` (default `0`)
  - `UGO_PLANNER_EVENT_START` (default `1`)
  - `UGO_PLANNER_EVENT_STOP` (default `2`)
  - `UGO_PLANNER_STATE_EXECUTE` (default `1`)
  - `UGO_PLANNER_STATE_STOPPING` (default `2`)
  - `UGO_PLANNER_STATE_RESETTING` (default `3`)
  - `UGO_PLANNER_STATE_STARTING` (default `4`)
  - `UGO_PLANNER_STATE_MANUAL_READY` (default `0`)
  - `UGO_PLANNER_STATE_STOPPED` (default `0`)

- Workflow launch behavior can be tuned with:
  - `UGO_PLANNER_WORKFLOW_MODULE` (default `workflows.rack_probe_transfer_workflow`)
  - `UGO_PLANNER_START_GRACE_S` (default `0.3`)
  - `UGO_PLANNER_STOP_TIMEOUT_S` (default `10.0`)
  - `UGO_PLANNER_PAUSE_REQUEST_FILE` (default `runtime/planner_workflow_pause.request`)
  - `UGO_PLANNER_PAUSE_ACK_FILE` (default `runtime/planner_workflow_paused.ack`)
  - `UGO_PLANNER_PAUSE_POLL_S` (default `0.2`)
  - `UGO_PLANNER_TRANSIENT_MIN_HOLD_S` (default `3.0`)
    - minimum hold time for transient published states (`STARTING`, `RESETTING`) before switching to next stable state
  - `UGO_PLANNER_TRANSIENT_REPUBLISH_S` (default `1.0`)
    - heartbeat re-publish period while transient state is being held (improves UI visibility with polling/caching)
  - `UGO_PLANNER_POST_HANDLED_EVENT_TO_API` (default `0`)
    - keep `0` when `/api/planner/event` POST is forbidden for planner user (prevents false failures on UI reset/start/stop)
