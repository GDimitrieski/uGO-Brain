from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from world.lab_world import CapState, ProcessType, RackLocation, RackType, WorldModel

INPUT_STATION_ID = "InputStation"
INPUT_SLOT_ID = "URGRackSlot"
PLATE_STATION_ID = "uLMPlateStation"
CHARGE_STATION_ID = "CHARGE"
SCAN_LANDMARK_ACT = 30
DEFAULT_PROCESS_POLICIES_PATH = Path(__file__).resolve().with_name("process_policies.json")
READY_PACKML_STATES = {"IDLE", "COMPLETE", "STOPPED"}


@dataclass(frozen=True)
class Goal:
    """High-level planning goal used by rule-based plan builders."""

    name: str
    final_plate_target: Optional[str] = None
    options: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "final_plate_target": self.final_plate_target,
            "options": dict(self.options),
        }


@dataclass(frozen=True)
class PlanStep:
    """Single planner output step.

    `step_type` is either:
    - TASK: directly mappable to a task key in Available_Tasks.json
    - PHASE: composite behavior executed by workflow logic
    """

    step_id: str
    label: str
    step_type: str
    task_key: Optional[str] = None
    station_id: Optional[str] = None
    overrides: Dict[str, Any] = field(default_factory=dict)
    required: bool = True
    notes: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "step_id": self.step_id,
            "label": self.label,
            "step_type": self.step_type,
            "task_key": self.task_key,
            "station_id": self.station_id,
            "overrides": dict(self.overrides),
            "required": bool(self.required),
            "notes": self.notes,
        }


@dataclass(frozen=True)
class ProcessPolicy:
    process: ProcessType
    target_station_id: str
    target_jig_ids: Tuple[int, ...]
    required_rack_types: Tuple[RackType, ...] = ()
    preferred_device_ids: Tuple[str, ...] = ()
    candidate_device_station_ids: Tuple[str, ...] = ()
    rack_source_station_ids: Tuple[str, ...] = ()
    requires_device: bool = False
    return_provisioned_rack_after_process: bool = False
    loading_strategy: str = ""

    def to_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "process": self.process.value,
            "target_station_id": self.target_station_id,
            "target_jig_ids": [int(x) for x in self.target_jig_ids],
            "required_rack_types": [x.value for x in self.required_rack_types],
            "preferred_device_ids": list(self.preferred_device_ids),
            "candidate_device_station_ids": list(self.candidate_device_station_ids),
            "rack_source_station_ids": list(self.rack_source_station_ids),
            "requires_device": bool(self.requires_device),
            "return_provisioned_rack_after_process": bool(self.return_provisioned_rack_after_process),
            "loading_strategy": str(self.loading_strategy or ""),
        }
        return payload


@dataclass(frozen=True)
class DynamicPlanAction:
    action_type: str
    sample_id: str
    process: ProcessType
    source_station_id: str
    source_station_slot_id: str
    source_slot_index: int
    target_station_id: str
    target_station_slot_id: str
    target_slot_index: int
    target_jig_id: int
    selected_device_id: Optional[str] = None
    notes: str = ""

    def to_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "action_type": self.action_type,
            "sample_id": self.sample_id,
            "process": self.process.value,
            "source_station_id": self.source_station_id,
            "source_station_slot_id": self.source_station_slot_id,
            "source_slot_index": int(self.source_slot_index),
            "target_station_id": self.target_station_id,
            "target_station_slot_id": self.target_station_slot_id,
            "target_slot_index": int(self.target_slot_index),
            "target_jig_id": int(self.target_jig_id),
            "selected_device_id": self.selected_device_id,
            "notes": self.notes,
        }
        return payload


@dataclass(frozen=True)
class DynamicPlanResult:
    status: str
    action: Optional[DynamicPlanAction] = None
    blocked: Tuple[Dict[str, Any], ...] = ()

    def to_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "status": str(self.status),
            "action": self.action.to_dict() if self.action else None,
            "blocked": [dict(x) for x in self.blocked],
        }
        return payload


class RulePlanner:
    """Simple rule-based planner.

    Purpose:
    - produce inspectable plan skeletons from world + goal
    - encode hard safety/ordering rules early
    """

    def build_plan(self, world: WorldModel, goal: Goal) -> List[PlanStep]:
        workflow_mode = str(goal.options.get("workflow_mode", "GETTING_NEW_SAMPLES")).strip().upper()
        if workflow_mode == "GETTING_NEW_SAMPLES":
            return self._build_getting_new_samples_plan(world)
        raise ValueError(
            f"Unsupported workflow_mode '{workflow_mode}'. "
            "Supported mode: GETTING_NEW_SAMPLES"
        )

    def _build_getting_new_samples_plan(self, world: WorldModel) -> List[PlanStep]:
        self._ensure_input_rack_present(world)
        input_station = world.get_station(INPUT_STATION_ID)
        world.get_station(CHARGE_STATION_ID)

        if not input_station.amr_pos_target:
            raise ValueError(f"Station '{INPUT_STATION_ID}' does not define AMR position target")

        scan_landmark_overrides = self._scan_landmark_overrides(INPUT_STATION_ID, input_station.itm_id)

        return [
            PlanStep(
                step_id="await_input_rack_present",
                label="Await Input Rack At InputStation",
                step_type="PHASE",
                station_id=INPUT_STATION_ID,
                notes=(
                    "Trigger/schedule this fixed intake plan only when InputStation.URGRackSlot "
                    "contains a rack."
                ),
            ),
            PlanStep(
                step_id="nav_input",
                label="Navigate InputStation",
                step_type="TASK",
                task_key="Navigate",
                station_id=INPUT_STATION_ID,
                overrides={
                    "AMR_PosTarget": input_station.amr_pos_target,
                    "AMR_Footprint": "1",
                    "AMR_DOCK": "1",
                },
                notes="Navigate to InputStation before rack handling.",
            ),
            PlanStep(
                step_id="scan_input_landmark",
                label="Scan Landmark InputStation",
                step_type="TASK",
                task_key="SingleDeviceAction",
                station_id=INPUT_STATION_ID,
                overrides=scan_landmark_overrides,
                notes="Mandatory station frame reference before non-plate handling.",
            ),
            PlanStep(
                step_id="transfer_input_rack",
                label="Transfer Input Rack To Plate",
                step_type="PHASE",
                notes="Pick rack from InputStation and place to uLMPlate URG slot.",
            ),
            PlanStep(
                step_id="charge",
                label="Charge At CHARGE",
                step_type="TASK",
                task_key="Charge",
                station_id=CHARGE_STATION_ID,
                overrides={},
                notes="Trigger charging via AMR Charge task.",
            ),
            PlanStep(
                step_id="camera_inspect_urg_for_new_samples",
                label="Scan URG Rack On Plate",
                step_type="PHASE",
                notes="Detect available incoming samples and register them in world state.",
            ),
            PlanStep(
                step_id="urg_sort_via_3fg_router",
                label="Route URG Samples Via 3-Finger By Router",
                step_type="PHASE",
                notes=(
                    "Pick each detected sample from URG rack, evaluate barcode at 3-Finger station, "
                    "and place into router-selected destination rack."
                ),
            ),
            PlanStep(
                step_id="handoff_to_state_driven_planning",
                label="Handoff To State-Driven Planning",
                step_type="PHASE",
                notes=(
                    "After intake/classification, perform more complex planning using current world state "
                    "(samples, racks, and device states)."
                ),
            ),
        ]

    @staticmethod
    def task_keys(plan: Sequence[PlanStep]) -> Set[str]:
        keys: Set[str] = set()
        for step in plan:
            if step.task_key:
                keys.add(step.task_key)
        return keys

    @staticmethod
    def missing_task_keys(plan: Sequence[PlanStep], available_task_keys: Set[str]) -> Set[str]:
        return {k for k in RulePlanner.task_keys(plan) if k not in available_task_keys}

    @staticmethod
    def _scan_landmark_overrides(station_id: str, itm_id_raw: Any) -> Dict[str, int]:
        try:
            itm_id = int(itm_id_raw)
        except Exception as exc:
            raise ValueError(
                f"Mandatory prerequisite missing: station '{station_id}' has invalid itm_id '{itm_id_raw}'"
            ) from exc
        return {"ITM_ID": itm_id, "ACT": SCAN_LANDMARK_ACT}

    @staticmethod
    def _ensure_input_rack_present(world: WorldModel) -> None:
        if (INPUT_STATION_ID, INPUT_SLOT_ID) not in world.rack_placements:
            raise ValueError(
                "Input rack not present at InputStation.URGRackSlot. "
                "Do not schedule GettingNewSamples plan until rack arrival."
            )


class DynamicStatePlanner:
    """Phase-1 dynamic planner:
    - evaluate active samples from world state
    - pick the next actionable process step
    - return a single action proposal or explicit blocked reasons
    """

    def __init__(
        self,
        policies: Dict[ProcessType, ProcessPolicy],
        *,
        default_target_station_id: str = PLATE_STATION_ID,
        policy_path: Optional[Path] = None,
    ) -> None:
        self.policies = dict(policies)
        self.default_target_station_id = str(default_target_station_id or PLATE_STATION_ID)
        self.policy_path = policy_path

    @classmethod
    def from_file(
        cls,
        path: Path | str = DEFAULT_PROCESS_POLICIES_PATH,
    ) -> "DynamicStatePlanner":
        resolved = Path(path).resolve()
        if not resolved.exists():
            raise FileNotFoundError(f"Process policy file not found: {resolved}")

        with open(resolved, "r", encoding="utf-8") as f:
            raw = json.load(f)
        if not isinstance(raw, dict):
            raise ValueError("Process policy file must be a JSON object")

        default_target_station_id = str(raw.get("default_target_station_id", PLATE_STATION_ID)).strip()
        if not default_target_station_id:
            default_target_station_id = PLATE_STATION_ID

        raw_policies = raw.get("process_policies", {})
        if not isinstance(raw_policies, dict):
            raise ValueError("'process_policies' must be a JSON object")

        parsed: Dict[ProcessType, ProcessPolicy] = {}
        for process_key_raw, payload in raw_policies.items():
            process = ProcessType(str(process_key_raw).strip().upper())
            if not isinstance(payload, dict):
                raise ValueError(f"Process policy for '{process.value}' must be an object")
            parsed[process] = cls._parse_policy(process, payload, default_target_station_id)

        return cls(
            parsed,
            default_target_station_id=default_target_station_id,
            policy_path=resolved,
        )

    @staticmethod
    def _normalize_loading_strategy(value: Any) -> str:
        txt = str(value or "").strip().upper()
        if txt in {"", "SEQUENTIAL", "ROUND_ROBIN"}:
            return txt
        raise ValueError(f"Unsupported loading strategy '{value}'")

    @staticmethod
    def _parse_station_ids_tuple(raw_value: Any, field_name: str, process: ProcessType) -> Tuple[str, ...]:
        if raw_value is None:
            return ()
        if not isinstance(raw_value, (list, tuple, set)):
            raise ValueError(f"Policy for process '{process.value}' has invalid {field_name}")
        values = [str(x).strip() for x in raw_value if str(x).strip()]
        return tuple(values)

    @classmethod
    def _parse_policy(
        cls,
        process: ProcessType,
        payload: Dict[str, Any],
        default_target_station_id: str,
    ) -> ProcessPolicy:
        station_id = str(payload.get("target_station_id", default_target_station_id)).strip()
        if not station_id:
            station_id = default_target_station_id

        raw_jig_ids: Any
        if "target_jig_ids" in payload:
            raw_jig_ids = payload.get("target_jig_ids")
        else:
            raw_jig_ids = payload.get("target_jig_id")
        if raw_jig_ids is None:
            raise ValueError(f"Policy for process '{process.value}' must define target_jig_ids")
        if isinstance(raw_jig_ids, (list, tuple, set)):
            target_jig_ids = tuple(int(x) for x in raw_jig_ids)
        else:
            target_jig_ids = (int(raw_jig_ids),)
        if not target_jig_ids:
            raise ValueError(f"Policy for process '{process.value}' has empty target_jig_ids")

        raw_rack_types = payload.get("required_rack_types", [])
        if raw_rack_types is None:
            raw_rack_types = []
        if not isinstance(raw_rack_types, (list, tuple, set)):
            raise ValueError(f"Policy for process '{process.value}' has invalid required_rack_types")
        required_rack_types = tuple(RackType(str(x).strip().upper()) for x in raw_rack_types)

        raw_preferred_devices = payload.get("preferred_device_ids", [])
        if raw_preferred_devices is None:
            raw_preferred_devices = []
        if not isinstance(raw_preferred_devices, (list, tuple, set)):
            raise ValueError(f"Policy for process '{process.value}' has invalid preferred_device_ids")
        preferred_device_ids = tuple(str(x).strip() for x in raw_preferred_devices if str(x).strip())

        raw_device_stations = payload.get("candidate_device_station_ids", [])
        if raw_device_stations is None:
            raw_device_stations = []
        candidate_station_ids = cls._parse_station_ids_tuple(
            raw_device_stations,
            "candidate_device_station_ids",
            process,
        )
        requires_device = bool(payload.get("requires_device", False))

        raw_rack_source_stations = payload.get("rack_source_station_ids", None)
        if raw_rack_source_stations is None and (not requires_device) and required_rack_types:
            # Backward-compatible fallback for existing policies where non-device
            # processes (e.g. ARCHIVATION) used candidate_device_station_ids to
            # indicate possible rack source stations.
            raw_rack_source_stations = raw_device_stations
        rack_source_station_ids = cls._parse_station_ids_tuple(
            raw_rack_source_stations,
            "rack_source_station_ids",
            process,
        )

        return ProcessPolicy(
            process=process,
            target_station_id=station_id,
            target_jig_ids=target_jig_ids,
            required_rack_types=required_rack_types,
            preferred_device_ids=preferred_device_ids,
            candidate_device_station_ids=candidate_station_ids,
            rack_source_station_ids=rack_source_station_ids,
            requires_device=requires_device,
            return_provisioned_rack_after_process=bool(
                payload.get("return_provisioned_rack_after_process", False)
            ),
            loading_strategy=cls._normalize_loading_strategy(payload.get("loading_strategy", "")),
        )

    def _select_device_id(
        self,
        world: WorldModel,
        process: ProcessType,
        policy: ProcessPolicy,
    ) -> Optional[str]:
        allowed_stations = set(policy.candidate_device_station_ids)

        def _matches(device_id: str) -> bool:
            dev = world.devices.get(device_id)
            if dev is None:
                return False
            if process not in dev.capabilities:
                return False
            if allowed_stations and str(dev.station_id) not in allowed_stations:
                return False
            metadata = dict(dev.metadata) if isinstance(dev.metadata, dict) else {}
            packml_state = str(metadata.get("packml_state", "")).strip().upper()
            if packml_state and packml_state not in READY_PACKML_STATES:
                return False
            return True

        for dev_id in policy.preferred_device_ids:
            if _matches(dev_id):
                return dev_id

        for dev_id in sorted(world.devices.keys()):
            if _matches(dev_id):
                return str(dev_id)
        return None

    def _is_sample_staged_for_policy(
        self,
        world: WorldModel,
        location: RackLocation,
        policy: ProcessPolicy,
    ) -> Tuple[bool, int]:
        if str(location.station_id) != str(policy.target_station_id):
            return False, -1
        cfg = world.get_slot_config(location.station_id, location.station_slot_id)
        jig_id = int(cfg.jig_id)
        if jig_id not in set(int(x) for x in policy.target_jig_ids):
            return False, jig_id

        if policy.required_rack_types:
            rack = world.get_rack_at(location.station_id, location.station_slot_id)
            if rack.rack_type not in set(policy.required_rack_types):
                return False, jig_id
        return True, jig_id

    def _resolve_target_slot(
        self,
        world: WorldModel,
        policy: ProcessPolicy,
    ) -> Tuple[str, int, int]:
        errors: List[str] = []
        for jig_id in policy.target_jig_ids:
            try:
                slot_id, slot_index = world.select_next_target_slot_for_jig(
                    station_id=policy.target_station_id,
                    jig_id=int(jig_id),
                    strategy=(policy.loading_strategy or None),
                )
            except Exception as exc:
                errors.append(f"JIG_ID={int(jig_id)}: {exc}")
                continue

            if policy.required_rack_types:
                try:
                    rack = world.get_rack_at(policy.target_station_id, str(slot_id))
                except Exception as exc:
                    errors.append(f"JIG_ID={int(jig_id)} slot='{slot_id}': {exc}")
                    continue
                if rack.rack_type not in set(policy.required_rack_types):
                    allowed = ", ".join(rt.value for rt in policy.required_rack_types)
                    errors.append(
                        f"JIG_ID={int(jig_id)} slot='{slot_id}': rack type '{rack.rack_type.value}' "
                        f"not in [{allowed}]"
                    )
                    continue
            return str(slot_id), int(slot_index), int(jig_id)

        detail = "; ".join(errors) if errors else "no candidate target JIG available"
        raise ValueError(
            f"No target slot available for process '{policy.process.value}' at station "
            f"'{policy.target_station_id}' ({detail})"
        )

    def _resolve_empty_target_rack_slot(
        self,
        world: WorldModel,
        policy: ProcessPolicy,
    ) -> Tuple[str, int, int]:
        required_types = set(policy.required_rack_types)
        errors: List[str] = []
        for jig_id in policy.target_jig_ids:
            slot_cfgs = world.slots_for_jig(policy.target_station_id, int(jig_id))
            if not slot_cfgs:
                errors.append(f"JIG_ID={int(jig_id)}: no slot configs")
                continue
            for cfg in slot_cfgs:
                slot_id = str(cfg.slot_id)
                if (policy.target_station_id, slot_id) in world.rack_placements:
                    continue
                accepted = set(cfg.accepted_rack_types)
                if required_types and accepted and not (required_types & accepted):
                    continue
                return slot_id, int(getattr(cfg, "rack_index", 1)), int(jig_id)
            errors.append(f"JIG_ID={int(jig_id)}: no empty rack receiver slot")
        detail = "; ".join(errors) if errors else "no candidate target JIG available"
        raise ValueError(
            f"No empty rack receiver slot available for process '{policy.process.value}' at station "
            f"'{policy.target_station_id}' ({detail})"
        )

    def _resolve_provision_source_slot(
        self,
        world: WorldModel,
        policy: ProcessPolicy,
        target_station_id: str,
        target_station_slot_id: str,
    ) -> Tuple[str, str, int]:
        if not policy.rack_source_station_ids:
            raise ValueError(
                f"Policy for process '{policy.process.value}' does not define rack source stations"
            )
        required_types = set(policy.required_rack_types)
        target_cfg = world.get_slot_config(target_station_id, target_station_slot_id)
        target_accepted = set(target_cfg.accepted_rack_types)

        errors: List[str] = []
        for source_station_id in policy.rack_source_station_ids:
            try:
                source_station = world.get_station(source_station_id)
            except Exception as exc:
                errors.append(f"station='{source_station_id}': {exc}")
                continue

            slot_cfgs = sorted(
                source_station.slot_configs.values(),
                key=lambda cfg: (int(getattr(cfg, "rack_index", 1)), str(cfg.slot_id)),
            )
            for cfg in slot_cfgs:
                source_slot_id = str(cfg.slot_id)
                rack_id = world.rack_placements.get((source_station_id, source_slot_id))
                if not rack_id:
                    continue
                rack = world.racks.get(str(rack_id))
                if rack is None:
                    errors.append(
                        f"station='{source_station_id}' slot='{source_slot_id}': unknown rack '{rack_id}'"
                    )
                    continue
                if required_types and rack.rack_type not in required_types:
                    continue
                if target_accepted and rack.rack_type not in target_accepted:
                    continue
                return source_station_id, source_slot_id, int(getattr(cfg, "rack_index", 1))

            allowed_txt = ", ".join(rt.value for rt in sorted(required_types, key=lambda x: x.value)) or "ANY"
            errors.append(
                f"station='{source_station_id}': no mounted rack matching required types [{allowed_txt}]"
            )

        detail = "; ".join(errors) if errors else "no candidate source station available"
        raise ValueError(
            f"No source rack available to provision process '{policy.process.value}' target "
            f"'{target_station_id}.{target_station_slot_id}' ({detail})"
        )

    def _build_provision_action(
        self,
        world: WorldModel,
        sample_id: str,
        process: ProcessType,
        policy: ProcessPolicy,
    ) -> Optional[DynamicPlanAction]:
        if not policy.required_rack_types:
            return None
        if not policy.rack_source_station_ids:
            return None

        target_slot_id, target_slot_index, target_jig_id = self._resolve_empty_target_rack_slot(world, policy)
        source_station_id, source_slot_id, source_slot_index = self._resolve_provision_source_slot(
            world,
            policy,
            policy.target_station_id,
            target_slot_id,
        )
        allowed = ", ".join(rt.value for rt in policy.required_rack_types)
        return DynamicPlanAction(
            action_type="PROVISION_RACK",
            sample_id=sample_id,
            process=process,
            source_station_id=source_station_id,
            source_station_slot_id=source_slot_id,
            source_slot_index=int(source_slot_index),
            target_station_id=policy.target_station_id,
            target_station_slot_id=target_slot_id,
            target_slot_index=int(target_slot_index),
            target_jig_id=int(target_jig_id),
            selected_device_id=None,
            notes=(
                f"Provision rack from source station for process target staging. "
                f"required_rack_types=[{allowed}]"
            ),
        )

    def _build_action_for_sample(
        self,
        world: WorldModel,
        sample_id: str,
        process: ProcessType,
    ) -> DynamicPlanAction:
        policy = self.policies.get(process)
        if policy is None:
            raise ValueError(
                f"No process policy configured for process '{process.value}'"
            )

        state = world.sample_states.get(sample_id)
        if state is None:
            raise ValueError(f"Sample state missing for '{sample_id}'")
        if not isinstance(state.location, RackLocation):
            raise ValueError(
                f"Sample '{sample_id}' is not rack-mounted; current location '{type(state.location).__name__}'"
            )

        location = state.location
        source_station_id = str(location.station_id)
        source_station_slot_id = str(location.station_slot_id)
        source_slot_index = int(location.slot_index)
        source_cfg = world.get_slot_config(source_station_id, source_station_slot_id)
        source_jig_id = int(source_cfg.jig_id)

        selected_device_id = self._select_device_id(world, process, policy)
        # Centrifugation can move racks between source and centrifuge stations.
        # Keep process execution active even when samples are not currently staged
        # at the source (plate) station, instead of attempting single-sample restaging.
        if process == ProcessType.CENTRIFUGATION and source_station_id != policy.target_station_id:
            return DynamicPlanAction(
                action_type="PROCESS_SAMPLE",
                sample_id=sample_id,
                process=process,
                source_station_id=source_station_id,
                source_station_slot_id=source_station_slot_id,
                source_slot_index=source_slot_index,
                target_station_id=source_station_id,
                target_station_slot_id=source_station_slot_id,
                target_slot_index=source_slot_index,
                target_jig_id=int(source_jig_id),
                selected_device_id=selected_device_id,
                notes="Centrifugation remains in progress across centrifuge transfer stations.",
            )

        staged, current_jig_id = self._is_sample_staged_for_policy(world, location, policy)
        if staged:
            if policy.requires_device and not selected_device_id:
                allowed = ", ".join(policy.candidate_device_station_ids) or "ANY"
                raise ValueError(
                    f"No device available for process '{process.value}' (allowed_stations={allowed})"
                )
            return DynamicPlanAction(
                action_type="PROCESS_SAMPLE",
                sample_id=sample_id,
                process=process,
                source_station_id=source_station_id,
                source_station_slot_id=source_station_slot_id,
                source_slot_index=source_slot_index,
                target_station_id=source_station_id,
                target_station_slot_id=source_station_slot_id,
                target_slot_index=source_slot_index,
                target_jig_id=int(current_jig_id),
                selected_device_id=selected_device_id,
                notes="Sample already staged for next process.",
            )

        try:
            target_slot_id, target_slot_index, target_jig_id = self._resolve_target_slot(world, policy)
        except Exception as stage_exc:
            try:
                provision_action = self._build_provision_action(world, sample_id, process, policy)
            except Exception as provision_exc:
                raise ValueError(
                    f"{stage_exc}; provisioning unavailable: {provision_exc}"
                ) from provision_exc
            if provision_action is not None:
                return provision_action
            raise

        return DynamicPlanAction(
            action_type="STAGE_SAMPLE",
            sample_id=sample_id,
            process=process,
            source_station_id=source_station_id,
            source_station_slot_id=source_station_slot_id,
            source_slot_index=source_slot_index,
            target_station_id=policy.target_station_id,
            target_station_slot_id=target_slot_id,
            target_slot_index=target_slot_index,
            target_jig_id=int(target_jig_id),
            selected_device_id=selected_device_id,
            notes="Move sample to process rack/jig target.",
        )

    def _effective_pending_processes(self, world: WorldModel, sample_id: str) -> Tuple[ProcessType, ...]:
        pending = list(world.pending_processes(sample_id))
        if ProcessType.IMMUNOHEMATOLOGY_ANALYSIS not in pending:
            return tuple(pending)

        state = world.sample_states.get(sample_id)
        if state is None:
            return tuple(pending)
        sample = world.samples.get(sample_id)
        if sample is None:
            return tuple(pending)

        decap_done = ProcessType.DECAP in state.completed_processes
        is_decapped = sample.cap_state == CapState.DECAPPED
        if decap_done or is_decapped:
            return tuple(pending)

        immuno_idx = pending.index(ProcessType.IMMUNOHEMATOLOGY_ANALYSIS)
        if ProcessType.DECAP in pending:
            decap_idx = pending.index(ProcessType.DECAP)
            if decap_idx > immuno_idx:
                pending.pop(decap_idx)
                pending.insert(immuno_idx, ProcessType.DECAP)
            return tuple(pending)

        # Hard pre-process guard: immuno requires a decapped sample.
        pending.insert(immuno_idx, ProcessType.DECAP)
        return tuple(pending)

    @staticmethod
    def _process_priority(process: ProcessType) -> int:
        # Lower value means higher scheduling priority.
        # Keep terminal ARCHIVATION as the latest priority so upstream
        # process chains can complete/batch first.
        order: Dict[ProcessType, int] = {
            ProcessType.SAMPLE_TYPE_DETECTION: 10,
            ProcessType.DECAP: 20,
            ProcessType.CENTRIFUGATION: 30,
            ProcessType.IMMUNOHEMATOLOGY_ANALYSIS: 40,
            ProcessType.HEMATOLOGY_ANALYSIS: 40,
            ProcessType.CLINICAL_CHEMISTRY_ANALYSIS: 40,
            ProcessType.COAGULATION_ANALYSIS: 40,
            ProcessType.CAP: 50,
            ProcessType.ARCHIVATION: 90,
        }
        return int(order.get(process, 60))

    @classmethod
    def _action_sort_key(cls, action: DynamicPlanAction) -> Tuple[int, int, str]:
        action_type = str(action.action_type).strip().upper()
        # For the same process priority, stage before process to maximize batching.
        action_rank = 0 if action_type == "STAGE_SAMPLE" else 1
        return (
            cls._process_priority(action.process),
            int(action_rank),
            str(action.sample_id),
        )

    def plan_next(
        self,
        world: WorldModel,
        *,
        excluded_sample_ids: Optional[Set[str] | Sequence[str]] = None,
    ) -> DynamicPlanResult:
        ready_process: List[DynamicPlanAction] = []
        ready_stage: List[DynamicPlanAction] = []
        blocked: List[Dict[str, Any]] = []
        active_samples = 0
        excluded: Set[str] = set()
        if excluded_sample_ids:
            excluded = {str(x).strip() for x in excluded_sample_ids if str(x).strip()}

        sample_ids = sorted(set(world.samples.keys()) & set(world.sample_states.keys()))
        for sample_id in sample_ids:
            if str(sample_id) in excluded:
                continue
            pending = self._effective_pending_processes(world, sample_id)
            if not pending:
                continue
            active_samples += 1
            process = pending[0]
            try:
                action = self._build_action_for_sample(world, sample_id, process)
            except Exception as exc:
                blocked.append(
                    {
                        "sample_id": str(sample_id),
                        "process": process.value,
                        "reason": str(exc),
                    }
                )
                continue

            if action.action_type == "PROCESS_SAMPLE":
                ready_process.append(action)
            else:
                ready_stage.append(action)

        # Batch staging first, but only within the same process:
        # if there are still stageable samples for a process, defer processing
        # for that process until staging is exhausted.
        stageable_processes = {a.process for a in ready_stage}
        filtered_ready_process = [a for a in ready_process if a.process not in stageable_processes]

        actionable = list(ready_stage) + list(filtered_ready_process)
        if actionable:
            actionable.sort(key=self._action_sort_key)
            return DynamicPlanResult(status="READY", action=actionable[0], blocked=tuple(blocked))
        if ready_process:
            ready_process.sort(key=self._action_sort_key)
            return DynamicPlanResult(status="READY", action=ready_process[0], blocked=tuple(blocked))
        if active_samples == 0:
            return DynamicPlanResult(status="IDLE", action=None, blocked=tuple())
        return DynamicPlanResult(status="BLOCKED", action=None, blocked=tuple(blocked))


__all__ = [
    "DEFAULT_PROCESS_POLICIES_PATH",
    "DynamicPlanAction",
    "DynamicPlanResult",
    "DynamicStatePlanner",
    "Goal",
    "PlanStep",
    "ProcessPolicy",
    "RulePlanner",
]
