# uGO-Brain System Behavior Diagrams

## 1. Main Workflow Behavior Tree (GETTING_NEW_SAMPLES mode)

```mermaid
graph TD
    ROOT["SequenceNode: RackAndProbeTransferFlow"]

    ROOT --> V["RetryNode: ValidateScaffoldPrerequisites"]
    V --> VC["ConditionNode: check stations, tasks,<br/>planner, policies exist"]

    ROOT --> P1["RetryNode: await_input_rack_present"]
    P1 --> P1C["PHASE: Wait for URG rack<br/>at InputStation.URGRackSlot1"]

    ROOT --> P2["RetryNode: nav_input"]
    P2 --> P2C["TASK: Navigate<br/>AMR drives to InputStation"]

    ROOT --> P3["RetryNode: scan_input_landmark"]
    P3 --> P3C["TASK: SingleDeviceAction<br/>ACT=30 Scan Landmark"]

    ROOT --> P4["RetryNode: transfer_input_rack"]
    P4 --> P4C["PHASE: Pick rack from InputStation<br/>Place rack on uLMPlate"]

    ROOT --> P5["RetryNode: charge"]
    P5 --> P5C["TASK: Charge<br/>at CHARGE station"]

    ROOT --> P6["RetryNode: camera_inspect_urg_for_new_samples"]
    P6 --> P6C["PHASE: InspectRackAtStation<br/>Camera detect samples, register in world"]

    ROOT --> P7["RetryNode: urg_sort_via_3fg_router"]
    P7 --> P7C["PHASE: RouteUrgVia3Finger<br/>Pick each sample -> scan barcode at 3FG<br/>-> classify -> place in destination rack"]

    ROOT --> P8["RetryNode: handoff_to_state_driven_planning"]
    P8 --> P8C["PHASE: DynamicStatePlanner loop<br/>(see Diagram 2)"]

    style ROOT fill:#2d3436,color:#fff,stroke:#636e72
    style V fill:#6c5ce7,color:#fff
    style P1 fill:#0984e3,color:#fff
    style P2 fill:#0984e3,color:#fff
    style P3 fill:#0984e3,color:#fff
    style P4 fill:#00b894,color:#fff
    style P5 fill:#0984e3,color:#fff
    style P6 fill:#00b894,color:#fff
    style P7 fill:#00b894,color:#fff
    style P8 fill:#d63031,color:#fff
    style VC fill:#a29bfe,color:#000
    style P1C fill:#74b9ff,color:#000
    style P2C fill:#74b9ff,color:#000
    style P3C fill:#74b9ff,color:#000
    style P4C fill:#55efc4,color:#000
    style P5C fill:#74b9ff,color:#000
    style P6C fill:#55efc4,color:#000
    style P7C fill:#55efc4,color:#000
    style P8C fill:#ff7675,color:#000
```

## 2. Dynamic State-Driven Planning Loop

```mermaid
flowchart TD
    START(["Enter State-Driven Planning"])
    INIT["loop_index = 0"]
    CHECK_MAX{"loop_index < MAX_ACTIONS<br/>(default: 200)?"}

    REFRESH["Refresh Device States<br/>1. UpdateWorldState_From_uLM<br/>2. Poll WISE modules"]

    WAITING{"Waiting for<br/>external device<br/>completion?"}
    CHECK_WAIT["Check external wait satisfied<br/>(PackML state or WISE readiness)"]
    WAIT_OK{"Wait<br/>satisfied?"}
    SLEEP_WAIT["Sleep WAIT_POLL_S<br/>(default: 1.0s)"]

    PLAN["DynamicStatePlanner.propose_next_action(world)"]
    RESULT{"Planner<br/>result?"}

    BLOCKED["Log blocked reasons<br/>All samples blocked or complete"]

    ACTION_TYPE{"action_type?"}

    TRANSFER["TRANSFER_SAMPLE:<br/>1. Navigate to source station<br/>2. Pick sample from source rack<br/>3. Navigate to target station<br/>4. Place sample in target rack<br/>5. Update world state"]

    PROVISION["PROVISION_RACK:<br/>1. Navigate to source station<br/>2. Pull rack out<br/>3. Navigate to target station<br/>4. Push rack in<br/>5. Update world placements"]

    RETURN_RACK["RETURN_RACK:<br/>1. Navigate to current station<br/>2. Pull rack out<br/>3. Navigate to home station<br/>4. Push rack in<br/>5. Update world placements"]

    WAIT_DEVICE["WAIT_FOR_DEVICE:<br/>Set external wait flag<br/>+ device_id + process<br/>+ wait source (PACKML/WISE)"]

    UPDATE["Update world state<br/>Append occupancy event<br/>Mark process complete if applicable"]

    INC["loop_index += 1"]

    ALL_DONE{"All samples<br/>completed?"}

    DONE_OK(["SUCCESS: All samples processed"])
    DONE_MAX(["STOP: Max actions reached"])
    DONE_BLOCKED(["STOP: All samples blocked"])

    START --> INIT --> CHECK_MAX
    CHECK_MAX -- No --> DONE_MAX
    CHECK_MAX -- Yes --> REFRESH
    REFRESH --> WAITING
    WAITING -- Yes --> CHECK_WAIT
    CHECK_WAIT --> WAIT_OK
    WAIT_OK -- No --> SLEEP_WAIT --> INC
    WAIT_OK -- Yes --> PLAN
    WAITING -- No --> PLAN
    PLAN --> RESULT
    RESULT -- "BLOCKED" --> BLOCKED --> ALL_DONE
    RESULT -- "ACTION" --> ACTION_TYPE
    ACTION_TYPE -- TRANSFER_SAMPLE --> TRANSFER --> UPDATE
    ACTION_TYPE -- PROVISION_RACK --> PROVISION --> UPDATE
    ACTION_TYPE -- RETURN_RACK --> RETURN_RACK --> UPDATE
    ACTION_TYPE -- WAIT_FOR_DEVICE --> WAIT_DEVICE --> INC
    UPDATE --> INC
    INC --> CHECK_MAX
    ALL_DONE -- Yes --> DONE_OK
    ALL_DONE -- No --> DONE_BLOCKED

    style START fill:#00b894,color:#fff
    style DONE_OK fill:#00b894,color:#fff
    style DONE_MAX fill:#fdcb6e,color:#000
    style DONE_BLOCKED fill:#d63031,color:#fff
    style PLAN fill:#6c5ce7,color:#fff
    style TRANSFER fill:#0984e3,color:#fff
    style PROVISION fill:#e17055,color:#fff
    style RETURN_RACK fill:#e17055,color:#fff
    style WAIT_DEVICE fill:#fdcb6e,color:#000
    style REFRESH fill:#00cec9,color:#000
```

## 3. Sample Lifecycle State Machine

```mermaid
stateDiagram-v2
    [*] --> INTAKE: Rack arrives at InputStation

    INTAKE --> DETECTED: Camera inspects URG rack<br/>(InspectRackAtStation)

    DETECTED --> CLASSIFIED: 3-Finger barcode scan<br/>+ Router classification

    CLASSIFIED --> ROUTED: Placed in destination rack<br/>(URG -> IH500/Centrifuge/Archive rack)

    state "Process Pipeline" as pipeline {
        ROUTED --> DECAP: If DECAP required<br/>(3-Finger station)
        DECAP --> CENTRIFUGATION: If CENTRIFUGATION required<br/>(CentrifugeStation)
        ROUTED --> CENTRIFUGATION: If no DECAP needed
        CENTRIFUGATION --> IMMUNOHEMATOLOGY: If IMMUNOHEMATOLOGY required<br/>(BioRadIH500Station)
        ROUTED --> IMMUNOHEMATOLOGY: If centrifuge not needed
        IMMUNOHEMATOLOGY --> CAP: If CAP required<br/>(3-Finger station)
        CENTRIFUGATION --> CAP: If no IH analysis
    }

    CAP --> ARCHIVATION: Move to Archive/Fridge rack
    IMMUNOHEMATOLOGY --> ARCHIVATION: If no re-capping
    CENTRIFUGATION --> ARCHIVATION: Direct archive path

    ARCHIVATION --> COMPLETE: Sample in final location

    COMPLETE --> [*]

    note right of CLASSIFIED
        Classification determines which
        processes are required:
        - required_processes set on SampleState
        - completed_processes tracks progress
    end note

    note right of IMMUNOHEMATOLOGY
        WISE module polls IH-500 DI channels
        to detect when analysis is ready.
        Sample/Kreuzprobe slot separation:
        - Samples: slots 1,2,4,5,6
        - Kreuzprobe: slots 8,9,11,12,13
    end note
```

## 4. Lab Topology & Physical Flow

```mermaid
graph LR
    subgraph EXTERNAL["External Stations (require AMR navigation)"]
        INPUT["InputStation<br/>URGRackSlot1 (input)<br/>URGRackSlot2 (return)"]
        CHARGE["CHARGE<br/>Battery charging dock"]
        CENTRIFUGE["CentrifugeStation<br/>Hettich Rotina 380<br/>(XML-RPC control)"]
        IH500["BioRadIH500Station<br/>IH-500/IH-1000<br/>(WISE DI polling only)"]
        FRIDGE["FridgeStation<br/>Sample cold storage"]
        ARCHIVE["ArchiveStation<br/>Processed sample storage"]
    end

    subgraph ON_ROBOT["On-Robot Plate (uLM gripper)"]
        PLATE["uLMPlateStation<br/>URGRackSlot<br/>CentrifugeRackSlot (x4)<br/>IH500RackSlot (x2)<br/>ArchiveRackSlot"]
        THREEFG["3-FingerGripperStation<br/>SampleSlot1<br/>RecapCapsSlot<br/>KreuzprobeRecapCapsSlot"]
    end

    INPUT -- "1. Pick URG rack" --> PLATE
    PLATE -- "2. Inspect + classify" --> THREEFG
    THREEFG -- "3. Route to dest rack" --> PLATE
    PLATE -- "4. Navigate + transfer" --> CENTRIFUGE
    CENTRIFUGE -- "5. Centrifuged rack back" --> PLATE
    PLATE -- "6. Navigate + push rack" --> IH500
    IH500 -- "7. Analysis done, pull rack" --> PLATE
    PLATE -- "8. Final archivation" --> ARCHIVE
    PLATE -- "8b. Cold storage" --> FRIDGE
    CHARGE -. "Between steps" .-> PLATE

    style INPUT fill:#e17055,color:#fff
    style CHARGE fill:#fdcb6e,color:#000
    style CENTRIFUGE fill:#6c5ce7,color:#fff
    style IH500 fill:#00b894,color:#fff
    style FRIDGE fill:#0984e3,color:#fff
    style ARCHIVE fill:#0984e3,color:#fff
    style PLATE fill:#2d3436,color:#fff
    style THREEFG fill:#d63031,color:#fff
```

## 5. Device Communication Architecture

```mermaid
sequenceDiagram
    participant WF as Workflow<br/>(BT tick loop)
    participant CMD as CommandSender<br/>(command_layer.py)
    participant ROBOT as uGO Robot<br/>(UR5e + AMR)
    participant CENT as Centrifuge<br/>(Hettich Rotina 380)
    participant WISE as WISE Module<br/>(IH-500 DI)
    participant ULM as uLM Backend

    Note over WF: === Static Phase ===

    WF->>CMD: Navigate(InputStation)
    CMD->>ROBOT: Task dispatch (AMR_PosTarget)
    ROBOT-->>CMD: Succeeded
    CMD-->>WF: result

    WF->>CMD: SingleDeviceAction(ACT=30, ScanLandmark)
    CMD->>ROBOT: Wrist camera scan
    ROBOT-->>CMD: Landmark referenced
    CMD-->>WF: result

    WF->>CMD: SingleTask(PICK rack)
    CMD->>ROBOT: Pick from InputStation
    ROBOT-->>CMD: Succeeded

    WF->>CMD: SingleTask(PLACE rack on Plate)
    CMD->>ROBOT: Place on uLMPlate
    ROBOT-->>CMD: Succeeded

    WF->>CMD: InspectRackAtStation
    CMD->>ROBOT: Camera inspection
    ROBOT-->>CMD: Detected samples list

    loop For each detected sample
        WF->>CMD: ProcessAt3FingerStation
        CMD->>ROBOT: Pick sample + 3FG scan
        ROBOT-->>CMD: Barcode + classification
        WF->>WF: Router classifies sample
        WF->>CMD: SingleTask(PLACE in dest rack)
        CMD->>ROBOT: Place sample
        ROBOT-->>CMD: Succeeded
    end

    Note over WF: === Dynamic Phase ===

    loop State-Driven Planning (max 200 iterations)
        WF->>CMD: UpdateWorldState_From_uLM
        CMD->>ULM: Get device states
        ULM-->>CMD: PackML states
        CMD-->>WF: Device status payload

        WF->>WISE: HTTP GET /iocard/{slot}/di
        WISE-->>WF: Channel states (4 DI channels)

        WF->>WF: DynamicStatePlanner.propose_next_action()

        alt TRANSFER_SAMPLE
            WF->>CMD: Navigate(source_station)
            CMD->>ROBOT: Navigate
            ROBOT-->>CMD: OK
            WF->>CMD: SingleTask(PICK sample)
            CMD->>ROBOT: Pick
            ROBOT-->>CMD: OK
            WF->>CMD: Navigate(target_station)
            CMD->>ROBOT: Navigate
            ROBOT-->>CMD: OK
            WF->>CMD: SingleTask(PLACE sample)
            CMD->>ROBOT: Place
            ROBOT-->>CMD: OK
        else PROVISION_RACK
            WF->>CMD: Navigate + Pull rack + Navigate + Push rack
            CMD->>ROBOT: Rack transfer sequence
            ROBOT-->>CMD: OK
        else WAIT_FOR_DEVICE (Centrifuge)
            WF->>CENT: XML-RPC: start_centrifuge()
            CENT-->>WF: Running
            loop Poll until complete
                WF->>CMD: UpdateWorldState_From_uLM
                CMD->>ULM: PackML state?
                ULM-->>CMD: EXECUTE / COMPLETE
            end
        else WAIT_FOR_DEVICE (IH-500)
            loop Poll WISE until ready
                WF->>WISE: HTTP GET /iocard/{slot}/di
                WISE-->>WF: channels (ready/not ready)
            end
        end
    end
```

## 6. Planner Decision Logic

```mermaid
flowchart TD
    START["DynamicStatePlanner.propose_next_action(world)"]

    ENUM["Enumerate active samples<br/>(samples with remaining processes)"]

    FOREACH{"For each sample"}

    NEXT_PROC["Determine next unfinished process<br/>(ordered: DECAP -> CENTRIFUGATION<br/>-> IMMUNOHEMATOLOGY -> CAP -> ARCHIVATION)"]

    FIND_POLICY["Look up ProcessPolicy<br/>for this ProcessType"]

    CHECK_STAGED{"Sample already<br/>at target station<br/>+ correct jig?"}

    STAGED_DONE["Mark process complete<br/>(already staged = done)"]

    CHECK_DEVICE{"Policy requires<br/>device?"}

    SELECT_DEV["Select device:<br/>1. Check preferred_device_ids<br/>2. Check PackML state in READY_PACKML_STATES<br/>3. Check WISE readiness (if enabled)<br/>4. Fallback to any matching device"]

    DEV_FOUND{"Device<br/>available?"}

    BLOCK_DEV["BLOCKED: device not ready<br/>(PackML state / WISE stale)"]

    RESOLVE_TARGET["Resolve target slot:<br/>1. Check preferred_station_slot (pairing)<br/>2. Try each target_jig_id<br/>3. Check rack type match<br/>4. Find free slot index<br/>5. Kreuzprobe slot separation"]

    TARGET_OK{"Target slot<br/>found?"}

    NEED_PROVISION{"Need rack<br/>provisioning?"}

    PROVISION_ACTION["Build PROVISION_RACK action<br/>(move empty rack from source station)"]

    TRANSFER_ACTION["Build TRANSFER_SAMPLE action<br/>(pick from current -> place at target)"]

    BLOCK_SLOT["BLOCKED: no available slot"]

    RETURN_CHECK{"Rack return<br/>needed after process?"}
    RETURN_ACTION["Build RETURN_RACK action<br/>(move rack back to home station)"]

    EMIT["Return DynamicPlanResult<br/>(status=ACTION, action=...)"]

    START --> ENUM --> FOREACH
    FOREACH --> NEXT_PROC --> FIND_POLICY
    FIND_POLICY --> CHECK_STAGED
    CHECK_STAGED -- Yes --> STAGED_DONE --> FOREACH
    CHECK_STAGED -- No --> CHECK_DEVICE
    CHECK_DEVICE -- Yes --> SELECT_DEV --> DEV_FOUND
    CHECK_DEVICE -- No --> RESOLVE_TARGET
    DEV_FOUND -- Yes --> RESOLVE_TARGET
    DEV_FOUND -- No --> BLOCK_DEV --> FOREACH
    RESOLVE_TARGET --> TARGET_OK
    TARGET_OK -- Yes --> TRANSFER_ACTION --> RETURN_CHECK
    TARGET_OK -- No --> NEED_PROVISION
    NEED_PROVISION -- Yes --> PROVISION_ACTION --> EMIT
    NEED_PROVISION -- No --> BLOCK_SLOT --> FOREACH
    RETURN_CHECK -- Yes --> RETURN_ACTION --> EMIT
    RETURN_CHECK -- No --> EMIT

    style START fill:#6c5ce7,color:#fff
    style EMIT fill:#00b894,color:#fff
    style BLOCK_DEV fill:#d63031,color:#fff
    style BLOCK_SLOT fill:#d63031,color:#fff
    style TRANSFER_ACTION fill:#0984e3,color:#fff
    style PROVISION_ACTION fill:#e17055,color:#fff
    style RETURN_ACTION fill:#fdcb6e,color:#000
```

## 7. Component Architecture Overview

```mermaid
graph TB
    subgraph "Workflow Layer"
        MAIN["main()<br/>rack_probe_transfer_workflow.py"]
        BT["BehaviorTree<br/>SequenceNode / RetryNode / ConditionNode"]
    end

    subgraph "Planning Layer"
        RP["RulePlanner<br/>Static intake plan"]
        DSP["DynamicStatePlanner<br/>Horizon-1 sample-by-sample"]
        PP["ProcessPolicies<br/>(process_policies.json)"]
    end

    subgraph "Routing Layer"
        CHAIN["ChainedSampleRouter"]
        HARD["HardRuleRoutingProvider"]
        RULE["RuleBasedRoutingProvider<br/>(sample_routing_rules.json)"]
        TRAIN["TrainingCatalogRoutingProvider<br/>(XLSX catalogs)"]
        LIS["LisRoutingProvider<br/>(external HTTP)"]
    end

    subgraph "World Model Layer"
        WM["WorldModel<br/>lab_world.py"]
        CFG["world_config.json"]
        RESUME["state_resume.py<br/>Crash recovery"]
    end

    subgraph "Execution Layer"
        CMD["CommandSender<br/>command_layer.py"]
        SENDER["build_sender()<br/>Login + task catalog"]
    end

    subgraph "Device Layer"
        REG["DeviceRegistry"]
        CENT_DEV["CentrifugeAnalyzerDevice<br/>+ Rotina380UsageProfile"]
        CENT_RPC["CentrifugeXmlRpcAdapter<br/>XML-RPC @ 192.168.1.28:50002"]
        WISE_DEV["WiseModuleAdapter<br/>HTTP DI polling"]
    end

    subgraph "Tracing Layer"
        TRACE["Execution Traces<br/>(CSV)"]
        OCC["Occupancy Events<br/>(JSONL)"]
        SNAP["World Snapshots<br/>(JSONL)"]
    end

    MAIN --> BT
    BT --> RP
    BT --> DSP
    DSP --> PP
    BT --> CHAIN
    CHAIN --> HARD
    CHAIN --> RULE
    CHAIN --> TRAIN
    CHAIN --> LIS
    DSP --> WM
    RP --> WM
    WM --> CFG
    WM --> RESUME
    BT --> CMD
    CMD --> SENDER
    MAIN --> REG
    REG --> CENT_DEV --> CENT_RPC
    REG --> WISE_DEV
    MAIN --> TRACE
    MAIN --> OCC
    MAIN --> SNAP

    style MAIN fill:#2d3436,color:#fff
    style BT fill:#6c5ce7,color:#fff
    style DSP fill:#0984e3,color:#fff
    style WM fill:#00b894,color:#fff
    style CMD fill:#e17055,color:#fff
    style REG fill:#fdcb6e,color:#000
```

---

## How to View These Diagrams

1. **VS Code**: Install the "Markdown Preview Mermaid Support" extension, then open this file and press `Ctrl+Shift+V`
2. **GitHub**: Push this file -- GitHub renders Mermaid natively in markdown
3. **Mermaid Live Editor**: Copy any code block to [mermaid.live](https://mermaid.live)
