# CLAUDE.md -- uGO-Brain

## References First
- **Workflow rules, physical world definitions, naming conventions, planner constraints**: See `AGENTS.md`
- **How to run, environment variables, project structure, troubleshooting**: See `README.md`
- **Refactor boundaries for the main workflow file**: See `workflows/rack_probe_transfer/Agent.md`

## Interaction Mode
Always advise before making code changes. Present the proposed change, explain the impact, and wait for explicit confirmation. Never auto-repair errors from the backend or uGO -- report and ask.

## Architecture At A Glance

```
main() in rack_probe_transfer_workflow.py (line 5893)
  |
  build_sender()           -- engine/sender.py (picks SimulatedRobotClient or UgoRobotClient or Ur5eDirectClient)
  load_world_with_resume() -- world/state_resume.py replays world_occupancy_trace.jsonl onto WorldModel
  build_tree()             -- line 1095, constructs BT from RulePlanner steps + DynamicStatePlanner
  |
  while tree.tick(bb): ... -- 100ms tick loop until SUCCESS or FAILURE
```

Data flow per tick:
```
Blackboard (bb) <--reads/writes-- BT Nodes
WorldModel (world) <--mutations-- action callbacks in workflow file
Trace lists (trace_records, occupancy_records) <--appended-- action callbacks
WIP files <--flushed-- after each action completes
Final files <--renamed from WIP-- only on SUCCESS
```

## Module Dependency Map

```
workflows/rack_probe_transfer_workflow.py  (orchestration, ~5990 lines)
  imports from:
    engine/bt_nodes.py          BT primitives: SequenceNode, ActionNode, ConditionNode, RetryNode, ForEachNode, UserInteractionRetryNode
    engine/command_layer.py     CommandSender.run() dispatches tasks via TaskCatalog + robot client
    engine/sender.py            build_sender() factory
    planning/planner.py         RulePlanner (fixed intake plan), DynamicStatePlanner (per-sample real-time)
    routing/sample_routing.py   ChainedSampleRouter with HardRule/RuleBased/Lis/TrainingCatalog providers
    Device/registry.py          build_device_registry_from_world()
    Device/centrifuge_usage_strategy.py  compile_centrifuge_usage_plan()
    world/lab_world.py          WorldModel + all enums (ProcessType, RackType, SlotKind, CapState, etc.)
    world/state_resume.py       Resume world from JSONL occupancy trace
```

## Critical Patterns

### Blackboard keys are stringly-typed global state
- 60+ keys set throughout `rack_probe_transfer_workflow.py`
- No schema or registry -- grep for `bb["key_name"]` to find all usages before adding or renaming
- Keys are consumed by ConditionNode predicates and ActionNode override lambdas

### WorldModel is the single source of truth for physical state
- All rack/sample movements MUST go through WorldModel methods
- Every mutation must be followed by an occupancy event append to maintain the trace
- The world is mutable at runtime but built from frozen dataclasses (Station configs, RackSlotConfig, etc.)

### Tracing is dual-track: WIP during execution, final on completion
- WIP files (`.wip.csv`, `.wip.jsonl`) are written continuously during execution
- Final canonical files are only written/renamed when the workflow ends with SUCCESS
- On FAILURE, WIP files are the only record -- never delete them after a failed run

### Task dispatch goes through CommandSender.run()
- Tasks must exist in `Available_Tasks.json` with payload template
- `TaskCatalog.build_payload()` merges template defaults with overrides
- `TaskCatalog.get_receiver()` determines dispatch path (AMR, ARM, WRIST_CAMERA, etc.)
- Retry logic: `CommandSender` retries up to `max_attempts` (default 3); `RetryNode` exists for BT-level retry
- Error codes posted to planner: `STEP_TIMEOUT` (action=ABORT), `STEP_FAILED` (action=RETRY or ABORT)

### Pause/resume uses filesystem signals
- `runtime/planner_workflow_pause.request` triggers pause
- `runtime/planner_workflow_paused.ack` confirms paused state
- Polled at `PAUSE_POLL_S` intervals inside tick loop

### Device integration follows PackML state machine
- All analyzer devices use PackML states: IDLE -> STARTING -> EXECUTE -> COMPLETE -> IDLE
- Centrifuge: XML-RPC adapter (`Device/centrifuge_xmlrpc_adapter.py`)
- WISE devices: REST polling adapter (`Device/wise_adapter.py`)
- Device registry built from world config at startup (`Device/registry.py`)

### Sender factory decision (engine/sender.py)
- `UGO_SIMULATE_DEVICES=1` -> SimulatedRobotClient (in-process, no backend)
- `UGO_USE_DIRECT_UR5E=1` -> Ur5eDirectClient (TCP to UR5e, HTTP fallback for prompts/errors)
- Otherwise -> UgoRobotClient (HTTP REST to uGO backend)

## What NOT To Do

1. **Never rename IDs** (station, slot, rack, JIG) without updating ALL of: `world_config.json`, `process_policies.json`, `sample_routing_rules.json`, `Available_Tasks.json`, and every constant in Python code. Grep the entire project.
2. **Never add a new ProcessType** without adding a corresponding entry in `planning/process_policies.json` and routing rules.
3. **Never mutate WorldModel without appending an occupancy event** -- downstream trace replay and `state_resume.py` depend on the event stream being complete.
4. **Never add external/AI dependencies** -- this is a pure Python project with minimal deps (`requests`, `fastapi`, `uvicorn`). Stdlib is preferred.
5. **Never modify BT node tick semantics** -- RUNNING means "call me again", SUCCESS means "done, advance", FAILURE means "done, abort or retry".
6. **Never assume station accessibility** -- every station except uLMPlateStation (ITM_ID=1, on-robot) requires Navigate + LandmarkScan before arm operations.
7. **Never add async/await** -- the workflow runs a synchronous tick loop with `time.sleep`. The architecture is intentionally single-threaded.
8. **Never delete WIP trace files** from a failed run -- they may be the only diagnostic record.

## Verification

### Syntax check (always, after any change)
```
python -m py_compile workflows/rack_probe_transfer_workflow.py
python -m py_compile engine/bt_nodes.py
python -m py_compile world/lab_world.py
python -m py_compile planning/planner.py
```

### Unit tests
```
python -m pytest tests/ -v
```

### Simulated run (integration check)
```powershell
$env:UGO_SIMULATE_DEVICES = "1"
$env:UGO_RESUME_FROM_LAST_WORLD_SNAPSHOT = "0"
python -m workflows.rack_probe_transfer_workflow
```

### World config validation
```
python -m world.world_config_editor --config world/world_config.json validate
```

## File Size Awareness
- `rack_probe_transfer_workflow.py`: ~5990 lines. Read targeted sections. `build_tree()` at line 1095; `main()` at line 5893.
- `lab_world.py`: ~3000 lines. Enums at top (line 38+), `WorldModel` class at line 217.
- `planner.py`: ~1330 lines. `RulePlanner` at line 153, `DynamicStatePlanner` at line 277.

## Adding a New Station/Device Checklist
1. Add station definition in `world/world_config.json` (with ITM_ID, JIG slots, landmark, coordinates)
2. Add slot kind to `SlotKind` enum in `world/lab_world.py` if new JIG type
3. Add rack type to `RackType` enum if new rack type
4. Add process policy in `planning/process_policies.json`
5. Add routing rules in `routing/sample_routing_rules.json`
6. Add device entry in `world_config.json` if analyzer device
7. Update `Available_Tasks.json` if new task types needed
8. Add constants in `rack_probe_transfer_workflow.py` header section
9. Run `world_config_editor validate`, then `py_compile`, then simulated run

## Adding a New BT Node Type
1. Define in `engine/bt_nodes.py` following the `Node` base class contract
2. Must implement `tick(bb: Blackboard) -> Status` and optionally `reset()`
3. Must return exactly one of: `Status.RUNNING`, `Status.SUCCESS`, `Status.FAILURE`
4. Stateful nodes must reset cleanly -- the parent may call `reset()` at any time
5. Update imports in `rack_probe_transfer_workflow.py`
