# uGO Brain vNext Architecture Refactor Guideline

Date: 2026-04-13
Status: Draft for implementation guidance
Scope: Refactor path from current codebase to vNext architecture without losing operational behavior
References:
- `docs/New Version/uGO-Brain-2.0-Greenfield-Specification.md`
- `AGENTS.md`
- `world/world_config.json`
- `Available_Tasks.json`

## Mission (Updated)
Design and operate an autonomous laboratory agent (`uLAB Mobile`) that can move across laboratory stations, pick/place racks and samples, and continuously plan/replan from both internal and external events.

Mission event sources include:
- Device readiness and device state changes.
- Sample detection/classification updates from camera pipelines.
- Operator/UI interactions and recovery decisions.

End goal:
- Samples entering the laboratory are correctly routed, processed by the appropriate devices, and tracked with deterministic world-state truth from intake to final destination.

## Why This Refactor Exists
The mission is valid and necessary. The current project pressure comes from implementation shape, not mission quality.

Current structural pain points:
1. Planning, execution, world mutation, and device integration are heavily coupled in large modules.
2. Process/station behavior is partly hardcoded, reducing scalability when adding devices or flows.
3. Recovery behavior is embedded in flow code instead of being policy-driven and reusable.
4. Continuous replanning without a formal lease model risks contention/oscillation under higher load.
5. Coupled runtime switches and mixed responsibilities increase regression risk during change.

Refactoring objective:
- Keep the same mission and operational rules, but implement them with explicit architectural boundaries and stable contracts.

## 1. Goal
Build a maintainable architecture where planning, execution, world-state management, and device integration are separated, while preserving current laboratory behavior and hard safety constraints.

## 2. Non-Negotiable Rules
1. Resume from world state by default; reset only on explicit command.
2. No station manipulation on external stations without required landmark referencing.
3. No silent auto-repair of backend/uGO errors.
4. If a prerequisite fails, dependent sequence must stop and report.
5. Sample process constraints are hard constraints, not optimization targets.
6. IDs and naming stay stable across world config, logic, and routing.

## 3. Architectural Separation
## 3.0 Rule of Thumb (Design Invariant)
**BT controls flow, policy selects branch, adapters do IO.**

Interpretation:
1. Behavior Tree nodes orchestrate sequencing and fault transitions, not business decision logic.
2. Policy/profile layers decide which branch/strategy/provider is selected for a given intent.
3. Adapter layers execute side effects (uGO tasks, device protocols, camera/sorter APIs) and return normalized results.

## 3.1 Domain Core
Responsibility:
- Canonical entities, value objects, invariants.
- No IO or backend calls.

Own:
- `Station`, `Slot`, `Rack`, `Sample`, `Device`, `WorldState`.
- Invariants such as location uniqueness and rack-slot compatibility.

## 3.2 World Store and Projection
Responsibility:
- Event append, deterministic projection, snapshot/replay/resume.
- Single write path for world mutations.

Own:
- `WorldEvent`, projection logic, state checksum, resume loading.

## 3.3 Planner Core
Responsibility:
- Given current world state, emit one actionable intent.
- Include reservation/lease manager and policy-driven scoring.

Own:
- Intake planner (fixed macro phase).
- Dynamic planner (horizon=1).
- Per-process handlers (`stage`, `execute`, `finalize`).

Must not:
- Call backend APIs directly.
- Mutate world state directly.

## 3.4 Executor Core
Responsibility:
- Execute planner intents through atomic actions.
- Enforce navigation/landmark prerequisites and recovery middleware.

Own:
- Atomic execution graph:
  - `NavigateToStation`
  - `EnsureStationReference`
  - `PickRack`/`PlaceRack`/`PullRackOut`/`PushRackIn`
  - `PickSample`/`PlaceSample`
  - `ProcessAt3FG`
  - `DeviceCommand` wrappers
- Emits execution events and deterministic world patches.

Must not:
- Decide next business intent (planner only).

## 3.5 Device Adapters
Responsibility:
- Translate generic device commands to real protocols.
- Provide readiness/fault/status contract.

Own:
- uGO task adapter
- WISE adapter
- centrifuge adapter
- IH500/IH1000 adapter

Must not:
- Contain sample routing/business process logic.

## 3.6 Control and Integration Layer
Responsibility:
- Mode/state bridge (manual/automatic, start/stop/reset/pause/resume).
- Operator prompts and recovery decision intake.

Must not:
- Own planner or executor internals.

## 4. Stable Contracts
## 4.1 Planner Input
- `world_snapshot`
- `capabilities_snapshot`
- `process_policies`
- `routing_rules`

## 4.2 Planner Output
- Exactly one intent or explicit `IDLE`/`BLOCKED`.

Intent minimum schema:
```json
{
  "intent_id": "uuid",
  "intent_type": "STAGE_SAMPLE | PROCESS_SAMPLE | PROVISION_RACK | RETURN_RACK_HOME",
  "sample_id": "optional",
  "process": "optional",
  "source": {"station_id": "...", "slot_id": "...", "slot_index": 1},
  "target": {"station_id": "...", "slot_id": "...", "slot_index": 1},
  "requires_navigation": true,
  "requires_landmark_scan": true,
  "selected_device_id": "optional",
  "notes": ""
}
```

## 4.3 Executor Result
```json
{
  "intent_id": "uuid",
  "status": "SUCCEEDED | FAILED | WAITING_RECOVERY",
  "events": [],
  "world_patch": {},
  "failure": {"code": "", "message": "", "step": ""}
}
```

## 5. Refactor Principles
1. Use strangler pattern: move behavior in slices, not a big-bang rewrite.
2. Preserve behavior first, simplify second.
3. Freeze expected behavior with regression traces before moving code.
4. Every moved function gets one owner module and one contract test.
5. No hidden cross-module world mutations.

## 6. Recommended Refactor Phases
## Phase 0: Baseline Guard
- Capture golden scenarios and traces.
- Add invariant tests around world transitions.

Exit criteria:
- Current behavior reproducible on baseline scenarios.

## Phase 1: Extract World Mutation Boundary
- Centralize all state mutations behind world-store APIs.
- Keep current workflow orchestration but call world-store methods only.

Exit criteria:
- No direct scattered world mutation in workflow layer.

## Phase 2: Extract Planner Engine
- Move dynamic planning functions into planner core module.
- Replace direct workflow decision logic with planner intent calls.

Exit criteria:
- Workflow loop requests and receives planner intents through a stable interface.

## Phase 3: Extract Executor Engine
- Move atomic action execution and prerequisite enforcement into executor module.
- Keep existing bridge and task catalog integration.

Exit criteria:
- Planner emits intent; executor runs intent; workflow file only orchestrates loop.

## Phase 4: Process Handlerization
- Split process-specific logic into handler plugins (`DECAP`, `CENTRIFUGATION`, `IMMUNO...`, `ARCHIVATION`).

Exit criteria:
- No process-specific branching in generic planner core.

## Phase 5: Lease Manager and Scoring
- Add formal reservations for rack, slot, device, and critical station operations.
- Add action-cost scoring to reduce oscillation.

Exit criteria:
- Deterministic conflict handling under multi-sample concurrency.

## 7. High-Risk Areas and Design Decisions
## 7.1 Landmark and Navigation
- Keep station requirements data-driven (`navigation_required`, `landmark_required`).
- Never infer by station name in planner/executor code.

## 7.2 Recovery
- Recovery outcomes must map to deterministic world patches.
- `WAITING_RECOVERY` is first-class execution status.

## 7.3 Device Readiness
- PACKML and WISE readiness must be adapter outputs, not planner internals.

## 7.4 IH500 and Kreuzprobe
- Current constants like `IH500_SAMPLE_SLOT_INDEXES` and `IH500_KREUZPROBE_SLOT_INDEXES` should move to process/device policy configuration.
- Planner should consume slot policy, not hardcoded arrays.

## 7.5 Rack Home Return
- Idle rack return stays policy-driven and low priority.
- Exclusions must be explicit in policy, not ad-hoc conditions.

## 8. File/Module Migration Target
Current pressure points:
- `workflows/rack_probe_transfer_workflow.py` (orchestration + planning + execution + IO mixed)
- `planning/planner.py` (planner and some process-device coupling)

Target shape:
- `planner/core/*`
- `executor/core/*`
- `world_store/*`
- `device_adapters/*`
- `orchestration/run_loop.py`

## 9. Definition of Done for Any Refactor PR
1. Behavior parity is proven on agreed scenarios.
2. No AGENTS hard rule regression.
3. New/changed module has explicit contract tests.
4. World mutations happen only through approved boundary.
5. Recovery behavior is deterministic and documented.
6. Changed files and migration rationale are clearly reported.

## 10. What Not To Do
1. Do not embed new process logic in monolithic workflow file.
2. Do not add new hardcoded station/device constants when policy/config can hold them.
3. Do not couple planner directly to backend transport.
4. Do not bypass prerequisite checks "just for speed."

## 11. uLAB Fault Handling Lessons (Operational)
These runtime faults are first-class design inputs, not edge cases.

## 11.1 Fault Families That Must Be Supported
1. Sample handling faults:
- sample lost/dropped
- false camera detection
- missed grip / empty grip after pick
- wrong object picked
2. Device state faults:
- unload/load initiated while device state is not safe
- device busy/faulted/offline during requested action
3. Station reference faults:
- landmark not detected
- stale/invalid station reference frame
4. Mobility faults:
- navigation target unreachable
- docking/positioning failure
5. World consistency faults:
- physical reality differs from expected world state after action

## 11.2 Mandatory Fault State Machine
1. Detect fault and classify by `fault_code`.
2. Move action to `FAILED_TRANSIENT` or `FAILED_HARD`.
3. Apply bounded auto-retry only for explicitly retryable transient faults.
4. If not recovered, transition to `WAITING_RECOVERY`.
5. Operator chooses allowed recovery outcome.
6. System applies one deterministic world patch.
7. Resume with `retry_action`, `skip_action`, `skip_sample`, `replan`, or `abort`.

## 11.3 Per-Fault Policy Lessons
1. Sample lost/dropped:
- Never silently continue.
- Mark sample as `UNKNOWN_LOCATION` or explicit operator-confirmed location.
- Block dependent process steps until resolved.
2. Wrong device state with unload initiated:
- Enforce device-state guard before unload/load.
- Reject command if PACKML/readiness contract is not satisfied.
- Retry only after state revalidation, otherwise `WAITING_RECOVERY`.
3. False camera detection or miss grip:
- Require post-pick verification (expected object ID/type present in gripper/world check).
- If mismatch, do not advance process completion.
- Recovery must include explicit world correction.
4. Landmark not detected:
- No manipulation at that station without valid reference.
- Bounded rescan attempts allowed.
- On failure, stop station actions and escalate.
5. Navigation point unreachable:
- Bounded re-route/retry policy allowed.
- If still unreachable, suspend dependent remote actions and request operator decision.

## 11.4 Retry/Skip Policy Requirements
1. Retry budgets are action-specific and declarative (not hardcoded in flow logic).
2. Retry must be idempotent-safe (no double-place, no duplicate completion mark).
3. `skip_action` is allowed only with operator-confirmed world patch.
4. `skip_sample` must isolate one sample, not corrupt global run state.
5. Any skip outcome must be auditable with reason + actor + timestamp.

## 11.5 World Patch Requirements for Recovery
Every recovery decision must patch at least:
1. Entity location (`sample`/`rack`/`cap`/`robot` as applicable).
2. Process completion flags (no implicit completion).
3. Device interaction status (if action involved a device).
4. Fault record linkage (`fault_code`, recovery decision, operator identity).

## 11.6 Implementation Constraint
Fault handling policy must be data-driven (policy table keyed by process + action + station/device + fault code), not hardcoded per workflow branch.

## 12. Proposed BT Diagram
Proposed vNext BT topology (Mermaid source):
- `docs/New Version/vNext_bt_proposal.mmd`

Use this diagram as the reference target for BT restructuring in the refactor phases.

## 13. BT Leaves (Execution Units)
BT leaves must be atomic, verifiable execution units. They are the only place where single-step physical or IO actions are performed.

## 13.1 Leaf Design Rules
1. A leaf executes one atomic operation only.
2. A leaf must not contain planner decision logic.
3. A leaf must not perform policy branch selection.
4. A leaf must not hide multi-step workflow behavior.
5. A leaf must return:
- BT status: `SUCCESS | FAILURE | RUNNING`
- `fault_code` (when failing)
- evidence payload for verification/audit

## 13.2 Recommended Core Leaf Catalog
Conditions:
1. `HasInputRack`
2. `NeedsNavigation`
3. `HasValidStationReference`
4. `IsDeviceReady`

Actions:
1. `NavigateToStation`
2. `ScanStationLandmark`
3. `PickRack`
4. `PlaceRack`
5. `PullRackOut`
6. `PushRackIn`
7. `PickSample`
8. `PlaceSample`
9. `DetectSampleByCamera`
10. `DetectSampleBySorterApi`
11. `StartDeviceProcess`
12. `PollDeviceCompletion`
13. `VerifyPickResult`
14. `VerifyPlaceResult`
15. `EmitExecutionEvent`
16. `ApplyWorldPatch`
17. `RequestRecoveryDecision`

## 13.3 Boundary Reminder
1. BT controls flow.
2. Policy selects branch/provider.
3. Leaves call adapters for IO.
