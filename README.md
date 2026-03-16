# uGO-Brain

This project executes a behavior-tree workflow for rack and probe handling, tracks a digital world state, and writes execution/world traces for simulation and replay.

## Where Everything Is

### Workflow model (tasks and sequence)

- Behavior-tree workflow definition: `workflows/rack_probe_transfer_workflow.py`
  - Tree construction: `build_tree(...)`
  - Intake planning mode: `GETTING_NEW_SAMPLES` (fixed plan)
    1. Wait for input rack presence at `InputStation.URGRackSlot`
    2. Navigate to `InputStation`
    3. Scan station landmark (`SingleDeviceAction` with `ACT=30`)
    4. Transfer input rack to plate
    5. Charge at `CHARGE`
    6. Inspect URG rack and classify samples via 3FG routing
    7. Handoff to state-driven planning based on enriched world state
- Task templates (payload schema, defaults, required fields): `Available_Tasks.json`

### Main behavior for execution

- Main entrypoint and run loop: `workflows/rack_probe_transfer_workflow.py` (`main()`)
- Behavior-tree node primitives: `engine/bt_nodes.py`

### Workflow runner / executor

- Sender construction (login + robot client + task catalog): `engine/sender.py`
- Task dispatch and wait/retry/error behavior: `engine/command_layer.py` (`CommandSender.run`)
- Robot communication client: `engine/ugo_robot_client.py`

### World model and simulation state

- World model + config schema + load/save utilities: `world/lab_world.py`
- Resume/restore world from previous occupancy trace: `world/state_resume.py`

### World snapshots and traces

- World config used for simulation: `world/world_config.json`
- Example YAML template: `world/world_config.yaml.example`
- Occupancy/event trace output (JSONL replay source): `world/world_occupancy_trace.jsonl`
- Occupancy trace mirror: `world/world_occupancy_trace.csv`
- Execution trace output: `tracing/tree_execution_trace.csv`
- BT state transitions: `tracing/tree_state_changes.csv`
- Snapshot exporter: `world/export_world_snapshot_jsonl.py` (writes `world/world_snapshot.jsonl`)

## Run

Run the workflow:

```powershell
python -m workflows.rack_probe_transfer_workflow
```

Run the interactive BT runtime viewer (tree + trace):

```powershell
python -m http.server 8000
```

Then open:

```text
http://localhost:8000/docs/bt_tree_runtime_viewer.html
```

## Configure Runtime Behavior

Environment variables used by the main workflow:

- `UGO_RESUME_FROM_LAST_WORLD_SNAPSHOT` (default `1`)
  - `1/true/yes`: resume from last state in `world/world_occupancy_trace.jsonl`
  - otherwise: start from `world/world_config.json`
- `UGO_FORCE_INPUT_RACK_AT_INPUT` (default off)
  - if enabled, input rack is prepared/reset at input station on startup
- `UGO_WORKFLOW_MODE` (default `GETTING_NEW_SAMPLES`)
  - supported: `GETTING_NEW_SAMPLES`, `CENTRIFUGE`
  - `CENTRIFUGE` enables the `CentrifugeCycle` BT phase
- `UGO_SIMULATE_DEVICES` (default off)
  - `1/true/yes`: use in-process simulated device/task execution (no login/backend calls)
  - otherwise: use live uGO backend sender
  - optional simulation tuning:
    - `UGO_SIM_CAMERA_POSITIONS` (comma-separated, default `1,2,3,4`)
    - `UGO_SIM_3FG_SAMPLE_TYPES` (comma-separated 1..4, default `1,2,3,4`)
    - `UGO_SIM_BARCODE_PREFIX` (default `SIMBC`)

PowerShell example:

```powershell
$env:UGO_RESUME_FROM_LAST_WORLD_SNAPSHOT = "0"
$env:UGO_WORKFLOW_MODE = "GETTING_NEW_SAMPLES"
$env:UGO_SIMULATE_DEVICES = "1"
python -m workflows.rack_probe_transfer_workflow
```

## Edit World Configuration

Use the world config editor:

```powershell
python -m world.world_config_editor --config world/world_config.json show
python -m world.world_config_editor --config world/world_config.json validate
```

## Export Snapshot JSONL

```powershell
python -m world.export_world_snapshot_jsonl --config world/world_config.json --out world/world_snapshot.jsonl
```

## Quick Troubleshooting

- `ModuleNotFoundError` or import failures:
  - Run commands from the project root (the folder that contains `engine/`, `workflows/`, `world/`).
  - Use module-style commands, for example: `python -m workflows.rack_probe_transfer_workflow`.

- Workflow starts with wrong simulation state:
  - Disable resume and start from config baseline:
    ```powershell
    $env:UGO_RESUME_FROM_LAST_WORLD_SNAPSHOT = "0"
    python -m workflows.rack_probe_transfer_workflow
    ```
  - Or keep resume enabled and verify `world/world_occupancy_trace.jsonl` exists.

- World config or slot/rack validation errors:
  - Validate config:
    ```powershell
    python -m world.world_config_editor --config world/world_config.json validate
    ```
  - Inspect current config:
    ```powershell
    python -m world.world_config_editor --config world/world_config.json show
    ```

- Login/token failures (`Login failed: received empty token`):
  - Check connection and credentials in `Library/credentials.py`.
  - Confirm the robot/planner endpoint is reachable from this machine.

- No updated trace outputs after a run:
  - Execution traces should be written to `tracing/`.
  - World traces should be written to `world/`.
  - Ensure the process has write permission to both folders.

## Project Structure

- `engine/`: behavior-tree runtime, sender/dispatch layer, and robot client communication
- `planning/`: supervisory planning layer (`Goal`, `PlanStep`, `RulePlanner`)
- `workflows/`: executable workflow entrypoints and orchestration logic
- `world/`: world model, config editing, state resume, snapshots, occupancy traces, and versioned trace backups
- `routing/`: sample-routing providers, rule files, and training-catalog routing support
- `Device/`: runtime device abstractions and concrete implementations (currently centrifuge)
- `tracing/`: execution/state trace export helpers and CSV outputs
- `Library/`: low-level uGO API helper modules used by the engine and scripts
- `docs/`: project and world-configuration documentation
- `TrainingData/`: workflow training/reference data (for example routing catalog source files)
- `Obsolete/`: legacy helper code retained for reference
- `Available_Tasks.json`: task catalog and payload templates used by sender/planner validation
- `1_SendSingleSkill.py`: utility script for sending a single task manually
- `world_trace_viewer_barcodes_persistent_v2.html`: local HTML trace viewer
- `docs/bt_tree_runtime_viewer.html`: interactive BT node inspector (Mermaid topology + execution trace CSV)
