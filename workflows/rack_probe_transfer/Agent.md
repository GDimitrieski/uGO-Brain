# Rack Probe Transfer - Refactor Guide

This folder documents the split of `workflows/rack_probe_transfer_workflow.py` to make future changes safer.

## Current Split

1. `workflows/rack_probe_transfer_workflow.py`
- Owns workflow orchestration and runtime execution.
- Contains:
  - `build_tree(...)`
  - state-driven execution nodes/conditions
  - world synchronization and task dispatch logic
  - world snapshot/event appenders tied to runtime state

2. `workflows/rack_probe_transfer/sample_parsing.py`
- Owns reusable parsing/classification helpers:
  - camera position extraction
  - sample type extraction
  - barcode extraction
  - immuno<->kreuzprobe map loading
  - bool normalization helper

3. `workflows/rack_probe_transfer/trace_context.py`
- Owns task-context enrichment logic for occupancy events:
  - trace timestamp parsing
  - payload/output normalization from trace rows
  - matching events to closest task context

4. `workflows/rack_probe_transfer/trace_csv.py`
- Owns CSV export helpers:
  - execution trace export
  - state-change export

## Change Rules

1. Workflow behavior changes
- Edit `rack_probe_transfer_workflow.py` when changing sequence, conditions, task order, retries, or execution semantics.

2. Parsing changes
- Edit `sample_parsing.py` when API/device output format changes (camera/3FG payload schema).

3. Trace/event context changes
- Edit `trace_context.py` for correlation logic between world events and sent tasks.

4. CSV format changes
- Edit `trace_csv.py` when adding/removing exported columns or append/write behavior.

## Recommended Safe Workflow For Edits

1. Make changes in the smallest affected module.
2. Run:
- `python -m py_compile workflows/rack_probe_transfer_workflow.py workflows/rack_probe_transfer/sample_parsing.py workflows/rack_probe_transfer/trace_context.py workflows/rack_probe_transfer/trace_csv.py`
3. If behavior changed, execute one known workflow run and inspect:
- `tracing/tree_execution_trace.wip.csv`
- `world/world_occupancy_trace.wip.jsonl`
- `world/world_snapshot.wip.jsonl`

## Why This Split

- Keeps execution decisions in one place.
- Isolates parsing and logging helpers to reduce accidental regressions.
- Makes payload/trace format updates independent from BT logic.

