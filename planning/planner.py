from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Set

from world.lab_world import WorldModel

INPUT_STATION_ID = "InputStation"
INPUT_SLOT_ID = "URGRackSlot"
PLATE_STATION_ID = "uLMPlateStation"
CHARGE_STATION_ID = "CHARGE"
SCAN_LANDMARK_ACT = 30


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


__all__ = ["Goal", "PlanStep", "RulePlanner"]
