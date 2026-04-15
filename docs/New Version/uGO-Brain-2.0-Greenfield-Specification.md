# uGO Brain 2.0 Greenfield Specification

Version: 1.0  
Date: 2026-03-23  
Authoring basis: current `uGO-Brain` repository analysis + notes in `New Version/`

## 1. Purpose
Define a complete, implementation-ready specification for a new uGO Brain 2.0 system built from scratch (no code migration), with:
- the same laboratory orchestration mission as the current system,
- stronger structure and modularity,
- deterministic recovery and resume behavior,
- measurable performance/throughput benchmarks,
- state-of-the-art software architecture for 2026.

## 2. Explicit Scope Statement
This specification is for a greenfield rebuild.

- Reuse: domain knowledge, world topology, task semantics, and operational rules.
- Do not reuse: current orchestration code, current monolithic workflow module, or legacy runtime coupling.
- Baseline inputs (minimum required): `world/world_config.json`, `Available_Tasks.json`, process maps/routing rules, and AGENTS operational constraints.

## 3. Vision and Product Goals

### 3.1 Product Vision
uGO Brain 2.0 is a resilient robotic laboratory orchestration platform that continuously plans and executes sample workflows with strict process safety, deterministic state tracking, and operator-assisted recovery.

### 3.2 Primary Goals
1. Process correctness first: a sample is never executed outside required process steps.
2. Deterministic world truth: all actions and interventions are event-sourced and replayable.
3. Recovery-by-design: every physical action supports policy-based recovery and human confirmation.
4. High utilization: horizon-1 reactive planning with reservation-aware opportunistic scheduling.
5. Device abstraction maturity: first-class analyzer and centrifuge runtime contracts.
6. Operational clarity: strong observability, auditability, and benchmarkable KPIs.

### 3.3 Non-Goals
1. Medical decision logic (test interpretation) inside orchestration core.
2. Hardcoded device-specific logic inside planner core.
3. Silent auto-repair of backend/uGO errors.
4. Monolithic workflow files with mixed concerns.

## 4. Required Operational Rules (Normative)
These are hard requirements for v2.0:

1. Resume policy:
   - Default runtime behavior is resume from latest valid world state.
   - Reset to baseline occurs only when explicitly requested.
2. Landmark rule:
   - Landmark requirement must be configured per station (not hardcoded by station name).
   - If a station is configured with `landmark_required=true`, coordinate reference scan must be executed before manipulation at that station.
3. Station navigation profile rule:
   - Every station must declare navigation profile fields: `station_scope` (`EXTERNAL` or `INTERNAL`), `amr_pos_target`, `navigation_required`, and `landmark_required`.
   - If `navigation_required=true`, navigation to station AMR target is a prerequisite for station actions.
   - `landmark_required` and `navigation_required` are independent flags and must both be honored when enabled.
4. Parking/home rule:
   - Baseline robot home position defaults to `CHARGE`.
   - If no remote-station action is needed in upcoming planner steps, robot must execute plate/internal actions while parked at preferred home station.
   - Plate/internal actions include operations on `uLMPlateStation` (`ITM_ID = 1`) and mounted internal jigs.
5. Error policy:
   - Backend/uGO errors are never silently auto-repaired.
   - Execution pauses and requests clarification/recovery decision.
6. Failure policy:
   - If a prerequisite step fails, stop the dependent sequence and report the failed prerequisite.
7. Device multiplicity:
   - Planner/executor must support multiple devices with overlapping capabilities.
8. Naming stability:
   - IDs remain stable; no implicit renaming.

## 5. Baseline Physical World (Minimum v2.0 World)
The current world is accepted as minimum baseline and must be representable as-is on day one:

- Stations: 9
- Slot configs: 29
- Rack instances: 18
- Devices: 4
- Core stations: `InputStation`, `CHARGE`, `uLMPlateStation`, `3-FingerGripperStation`, `CentrifugeStation`, `BioRadIH500Station`, `BioRadIH1000Station`, `FridgeStation`, `ArchiveStation`
- Core processes:
  - `FRIDGE_RACK_PROVISIONING`
  - `CENTRIFUGATION`
  - `DECAP`
  - `CAP`
  - `SAMPLE_TYPE_DETECTION`
  - `IMMUNOHEMATOLOGY_ANALYSIS`
  - `HEMATOLOGY_ANALYSIS`
  - `CLINICAL_CHEMISTRY_ANALYSIS`
  - `COAGULATION_ANALYSIS`
  - `ARCHIVATION`

## 6. Functional Requirements

### 6.1 World and State
1. Maintain full digital twin:
   - station/slot/rack/sample/cap/device/robot position states.
2. Maintain strict location uniqueness:
   - each rack/sample/cap has exactly one current location.
3. Track sample lifecycle:
   - barcode, classification status/source/details, required processes, completed processes, cap state/location.
4. Enforce rack/slot compatibility:
   - explicit accepted rack types per slot.
5. Support deterministic snapshot + replay:
   - world state reconstructable from event log.

### 6.2 Planning
1. Two planning layers:
   - intake plan (fixed macro entry flow),
   - dynamic state-driven planner (continuous horizon-1 decisions).
2. Dynamic planner output must be one action intent per cycle:
   - `STAGE_SAMPLE`, `PROCESS_SAMPLE`, `PROVISION_RACK`, `RETURN_RACK_HOME`, etc.
3. Add formal reservation/lease manager for resources:
   - rack, slot, device, robot arm, gripper, and time-window reservations.
4. Process handlers are first-class per process:
   - `stage`, `execute`, `finalize` phases.
5. Planner must re-evaluate world after each action and every manual recovery patch.
6. Planner must include a station-work context check per cycle:
   - If next executable work is internal/plate-only, preferred parking station becomes active execution context.
   - If next executable work requires remote station, navigation prerequisite is enforced for that station.

### 6.3 Execution
1. Hierarchical fine-grained behavior tree (BT):
   - macro phases + reusable atomic action subtrees.
2. Atomic action families:
   - `Navigate+Scan`,
   - `PickSample`,
   - `PlaceSample`,
   - `PickRack`,
   - `PlaceRack`,
   - `PushRackIn`,
   - `PullRackOut`,
   - `ProcessAt3FG`,
   - device-specific command leaves.
3. Every atomic physical action is wrapped by recovery policy middleware.
4. Executor runs planner intents, not embedded planner logic.
5. Executor must support `NavigateToPreferredStation` as a first-class reusable action:
   - callable during `IDLE` and `WAITING_EXTERNAL` when no immediate remote action exists,
   - non-failing if robot is already at preferred station.

### 6.3.1 Robot Parking and Remote-Work Policy
1. Preferred robot station is taken from world baseline `robot_current_station_id`; default is `CHARGE`.
2. During waits or interim phases, if no immediate remote-station action is required, robot shall navigate to preferred station.
3. Plate-only follow-up work must execute from preferred station context (no unnecessary remote parking).
4. This applies to any remote-station workflow path (current or future stations):
   - after any remote operation, if the next executable actions are only on `uLMPlateStation`/internal jigs (`ITM_ID = 1`), execution context shall move/remain at preferred station.
   - archive and centrifuge unload flows are examples, not special cases.
5. Planner/executor must avoid oscillation:
   - do not bounce between preferred station and remote station unless next committed action requires remote execution.

### 6.4 Device Runtime
1. Unified device runtime contract with PACKML-compatible state model.
2. Dedicated interfaces:
   - `AnalyzerDevice` base,
   - `CentrifugeDevice` base,
   - concrete classes for `HettichRotina380R`, `BioRadIH500`, `BioRadIH1000`.
3. Device command execution must support:
   - start/wait/complete/fault/reset semantics,
   - readiness gates from PACKML and optional WISE IO.
4. IH500/IH1000 flows must include:
   - explicit start analysis,
   - running monitoring,
   - completion confirmation,
   - result acquisition/validation hooks,
   - partial/mixed rack handling where physically valid.

### 6.5 Recovery and Human-in-the-Loop
1. Recoverable failures transition action state to `WAITING_RECOVERY`.
2. Allowed operator outcomes are policy-driven by context:
   - process + failed_step + station/device + action_type.
3. Operator decision produces exactly one deterministic world patch.
4. Resume strategy per decision:
   - retry action,
   - skip action,
   - skip sample,
   - replan,
   - abort run.
5. Decap example outcomes must be represented exactly (capped/decapped + location).

### 6.6 Integration and Control
1. API layer for command/query separation:
   - write commands (planner/executor/recovery),
   - read projections (world, run, device status).
2. Planner-web bridge supports:
   - manual/automatic mode,
   - start/stop/reset events,
   - graceful pause and deterministic resume.
3. Runtime context/status messages published for UI visibility.

## 7. Non-Functional Requirements

### 7.1 Reliability and Determinism
1. Exactly-once world mutation per committed action event.
2. Idempotent command handling with `command_id`.
3. Deterministic replay to same world checksum for same event stream.

### 7.2 Performance Targets
1. Dynamic planner cycle p95 <= 150 ms for 500 active samples.
2. Intent-to-dispatch p95 <= 300 ms.
3. Recovery decision-to-resume p95 <= 2.0 s (excluding human delay).
4. Event append latency p95 <= 20 ms.

### 7.3 Availability
1. Resume after process restart with no lost committed actions.
2. No global freeze from single-sample recoverable fault where isolation is possible.

### 7.4 Observability
1. Structured logs, traces, metrics for planner, executor, devices, and world projection.
2. Full audit trail:
   - who decided recovery,
   - what patch applied,
   - resulting world diff.

## 8. Target Architecture

### 8.1 High-Level Components
1. `world-service`:
   - event store + projection + invariants.
2. `planner-service`:
   - intake planner + dynamic planner + lease manager + scoring.
3. `executor-service`:
   - BT runtime + action orchestration + recovery middleware.
4. `device-service`:
   - adapters for centrifuge/analyzers/WISE/backend channels.
5. `control-api`:
   - external command/query API and run control.
6. `ui-backend` + `operator-ui`:
   - monitoring and recovery decisions.
7. `benchmark-harness`:
   - scenario runner + KPI reporter.

### 8.2 Architectural Style
1. Event-sourced core domain.
2. CQRS for command and read separation.
3. Policy-as-data (JSON/YAML + schema validation) for:
   - process policy,
   - recovery policy,
   - device strategy,
   - scoring weights.
4. Hexagonal ports/adapters around device and backend integrations.

## 9. Domain Model Specification

### 9.1 Core Aggregates
1. `WorldAggregate`
2. `RunAggregate`
3. `SampleAggregate`
4. `RackAggregate`
5. `DeviceAggregate`
6. `LeaseAggregate`
7. `RecoveryCaseAggregate`

### 9.2 Essential Invariants
1. A sample exists in exactly one location.
2. A rack exists in exactly one location (slot or gripper).
3. A cap exists on sample or stored location, never both.
4. `completed_processes` is subset of `required_processes`.
5. `required_processes` sequence order preserved unless explicit policy modifies with prerequisites (for example DECAP before IMMUNO).
6. Device capabilities must include requested process for `PROCESS_SAMPLE`.

### 9.3 Process State Model (per sample/process)
1. `NOT_REQUIRED`
2. `PENDING`
3. `STAGED`
4. `EXECUTING`
5. `WAITING_EXTERNAL`
6. `COMPLETED`
7. `FAILED_RECOVERABLE`
8. `FAILED_FATAL`
9. `SKIPPED`

## 10. Planning Specification

### 10.1 Planning Modes
1. `INTAKE_FIXED_PLAN`
2. `STATE_DRIVEN_DYNAMIC`

### 10.2 Dynamic Planning Algorithm
Reactive horizon-1 + reservation-aware opportunistic scheduling:

1. Build candidate actions from pending samples.
2. Filter by hard constraints and resource feasibility.
3. Reserve tentative resources.
4. Score candidates.
5. Select best candidate.
6. Emit single intent.

### 10.3 Hard Constraints
1. Never execute outside sample required process map.
2. Enforce station-configured navigation/landmark prerequisites before station manipulation.
3. Do not move empty racks into devices.
4. Respect rack/slot/device capacity and compatibility.
5. Respect active leases and lock ownership.

### 10.4 Scoring Model
Weighted score (lower is better):

`score = w_travel*travel_cost + w_rack*rack_move_cost + w_ready*device_ready_delay + w_batch*batch_gain + w_thrash*oscillation_penalty`

Policy-configurable weights per process and station family.

### 10.5 Planner Outputs
Intent schema:
- `intent_id`
- `run_id`
- `action_type`
- `sample_id` or `rack_id`
- `process`
- source/target coordinates
- selected_device_id
- lease_set
- rationale and score components

## 11. Execution and BT Specification

### 11.1 BT Layers
1. Macro flow:
   - intake phase,
   - dynamic loop phase.
2. Process subtrees:
   - centrifuge cycle subtree,
   - immuno analyzer subtree,
   - 3FG process subtree.
3. Atomic leaves:
   - command-level physical actions.

### 11.2 Action Lifecycle
1. `CREATED`
2. `DISPATCHED`
3. `RUNNING`
4. `SUCCEEDED` | `FAILED_RECOVERABLE` | `FAILED_FATAL`
5. `WAITING_RECOVERY` (if recoverable)
6. `COMPENSATED` (if needed)

### 11.3 Recovery Wrapper
Each physical leaf is executed inside:
1. precondition check,
2. dispatch,
3. status wait,
4. failure classification,
5. policy lookup,
6. recovery gate or fail-fast.

## 12. Recovery Policy Specification

### 12.1 Recovery Policy Key
`(process, action_name, station_id, device_id?, error_class)`

### 12.2 Policy Entry Structure
1. allowed operator options
2. required confirmation fields
3. deterministic world patch template
4. resume mode
5. escalation requirements

### 12.3 Resume Modes
1. `RETRY_ACTION`
2. `SKIP_ACTION`
3. `SKIP_SAMPLE`
4. `REPLAN`
5. `ABORT_RUN`

### 12.4 Mandatory Decap Policies
Must include:
1. returned capped to centrifuge rack
2. moved capped to IH rack
3. decapped and moved to IH rack

Each option sets cap state, location, and process completion flags explicitly.

## 13. Device Abstraction Specification

### 13.1 Common Contracts
Interface methods:
1. `get_status()`
2. `can_accept(...)`
3. `start(...)`
4. `wait_for_completion(...)`
5. `diagnose()`
6. `reset_fault()`

### 13.2 Centrifuge Contract
Required sequence:
1. open lid
2. load (balanced)
3. validate balance
4. close/lock lid
5. start
6. wait completion/standstill
7. unload

Configurable strategies:
- lid control
- start mechanism
- status source
- rotor/bucket/adapter layout
- balance model

### 13.3 IH500/IH1000 Contract
Required sequence:
1. ensure load-ready
2. load racks
3. start analysis command
4. monitor running state
5. wait completion or ready-to-unload signal
6. unload ready racks (partial allowed)
7. ingest result metadata
8. mark process completion only on confirmed completion signal

## 14. World Event Model

### 14.1 Event Categories
1. world snapshot events
2. rack movement events
3. sample movement events
4. sample classification events
5. process completion events
6. device status update events
7. recovery case events
8. lease acquisition/release events
9. planner intent events
10. execution action events

### 14.2 Event Metadata
All events include:
- `event_id`, `event_type`, `occurred_at`, `run_id`, `correlation_id`, `causation_id`, `actor`, `payload`, `world_version`

### 14.3 Projections
1. current world projection
2. run timeline projection
3. per-sample lifecycle projection
4. device timeline projection
5. KPI/benchmark projection

## 15. API Specification (External)

### 15.1 Command API
1. `POST /runs/start`
2. `POST /runs/{run_id}/stop`
3. `POST /runs/{run_id}/reset`
4. `POST /recovery/{case_id}/decision`
5. `POST /world/patch` (restricted/manual override)

### 15.2 Query API
1. `GET /world/current`
2. `GET /runs/{run_id}`
3. `GET /runs/{run_id}/timeline`
4. `GET /samples/{sample_id}`
5. `GET /devices`
6. `GET /planner/next-intent`

### 15.3 Streaming
1. `GET /events/stream` (SSE/WebSocket)
2. topic filters for run/sample/device/recovery

## 16. Data Storage and Technology Choices

### 16.1 Recommended Stack
1. Language/runtime: Python 3.13
2. API: FastAPI + Pydantic v2
3. DB: PostgreSQL 16+ (event store + projections)
4. Message bus: NATS JetStream (or Kafka for large deployments)
5. Observability: OpenTelemetry + Prometheus + Grafana + Loki
6. UI: React + TypeScript

### 16.2 Persistence Model
1. `events` table (append-only)
2. `snapshots` table
3. projection tables:
   - `world_current`
   - `sample_state_current`
   - `device_state_current`
   - `run_state_current`
4. `leases` table with TTL and owner
5. `recovery_cases` + `recovery_decisions`

## 17. Benchmark and KPI Specification

### 17.1 Primary KPIs
1. throughput: samples/hour by class
2. planner latency p50/p95/p99
3. action success rate
4. mean recovery resolution time
5. unplanned abort rate
6. rack thrash index (non-productive rack moves)
7. resume determinism rate

### 17.2 Benchmark Scenario Suite (Required)
1. Landmark enforcement scenario.
2. Wrong-process prevention scenario.
3. Rack thrash suppression scenario.
4. Empty-rack home return scenario.
5. IH500 partial-ready unload scenario.
6. Decap recoverable failure with operator patch scenarios A/B/C.
7. Multi-device capability contention scenario.
8. Restart/resume determinism scenario.

### 17.3 Success Gates
1. 100% pass on hard-constraint scenarios.
2. >= 99.9% deterministic replay checksum match.
3. >= 30% rack-thrash reduction vs baseline benchmark dataset.
4. >= 20% throughput improvement in mixed-workload simulation benchmark.

## 18. Testing Strategy

### 18.1 Test Layers
1. unit tests (domain/planner/device adapters)
2. contract tests (API + device adapters)
3. integration tests (planner + executor + world store)
4. scenario tests (end-to-end lab workflows)
5. fault-injection tests (timeouts, stale status, partial failures)

### 18.2 Property-Based Tests
1. world invariants under random action sequences
2. replay determinism under randomized event ordering constraints
3. lease conflict resolution correctness

## 19. Security, Safety, and Compliance
1. RBAC:
   - operator, engineer, admin roles.
2. Signed audit entries for manual overrides.
3. Immutable recovery decision log.
4. Input validation and schema versioning for all commands.
5. Hard fail on unknown task/action enum values.

## 20. Codebase Structure (Target)
```text
ugo-brain-2/
  apps/
    control_api/
    planner_service/
    executor_service/
    world_service/
    device_service/
    bridge_service/
    operator_ui_backend/
  libs/
    domain_model/
    policies/
    events/
    telemetry/
    test_fixtures/
  configs/
    world/
    tasks/
    process_policies/
    recovery_policies/
    device_profiles/
    benchmark_scenarios/
  tools/
    world_editor/
    replay_cli/
    benchmark_runner/
  tests/
    unit/
    integration/
    e2e/
    benchmark/
```

## 21. Phased Delivery Plan (Greenfield)

### Phase 1 - Foundation
1. domain model + event store + world projection
2. config loaders/validators for world/tasks/policies
3. baseline API skeleton

### Phase 2 - Planner and Executor Core
1. intake fixed planner
2. dynamic planner with lease manager and scoring
3. BT executor with atomic actions

### Phase 3 - Device Runtime Layer
1. centrifuge adapter + usage strategy engine
2. IH500/IH1000 adapters with start/wait/complete semantics
3. WISE readiness integration

### Phase 4 - Recovery and Operator Flow
1. recovery policy engine
2. waiting-recovery state machine
3. operator decision endpoints/UI

### Phase 5 - Observability and Benchmarking
1. full telemetry dashboards
2. benchmark harness and KPI reports
3. optimization iteration to hit KPI gates

## 22. Acceptance Criteria
Project is accepted when:
1. all mandatory operational rules are enforced automatically,
2. full baseline world (`world_config`) runs end-to-end,
3. planner/executor/device/recovery separation is implemented as specified,
4. benchmark suite and KPI targets are met,
5. restart/resume determinism is validated,
6. no monolithic workflow module remains.

## 23. Assumptions
1. Existing world topology and task catalog remain authoritative minimum requirements for v2.0 behavior parity.
2. Device hardware/protocol details can vary by installation and are handled by adapter strategies.
3. External LIS/result systems are integrated through adapter contracts and may be mocked during initial phases.

---

This specification intentionally defines a clean rebuild path with strict contracts, policy-driven behavior, and measurable performance gates, while preserving the physical/laboratory truth and rules of the current world model.
