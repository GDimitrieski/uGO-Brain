import unittest

from planning.planner import DynamicStatePlanner, ProcessPolicy
from world.lab_world import ProcessType, RackType, build_default_world


PLATE_STATION_ID = "uLMPlateStation"
INPUT_STATION_ID = "InputStation"
INPUT_URG_SLOT_ID = "URGRackSlot"
PLATE_ARCHIVE_SLOT_ID = "URGFridgeRackSlot"
ARCHIVE_STATION_ID = "ArchiveStation"
ARCHIVE_SLOT_ID = "URGFridgeRackSlot"
FRIDGE_STATION_ID = "FridgeStation"
FRIDGE_SLOT_1 = "URGFridgeRackSlot1"
FRIDGE_SLOT_2 = "URGFridgeRackSlot2"


def _archivation_policy() -> ProcessPolicy:
    return ProcessPolicy(
        process=ProcessType.ARCHIVATION,
        target_station_id=PLATE_STATION_ID,
        target_jig_ids=(13,),
        required_rack_types=(RackType.FRIDGE_URG_RACK,),
        rack_source_station_ids=(ARCHIVE_STATION_ID, "FridgeStation"),
        return_provisioned_rack_after_process=True,
        loading_strategy="SEQUENTIAL",
    )


def _decap_policy() -> ProcessPolicy:
    return ProcessPolicy(
        process=ProcessType.DECAP,
        target_station_id="3-FingerGripperStation",
        target_jig_ids=(10,),
        required_rack_types=(RackType.THREE_FINGER_GRIPPER_SAMPLE_HOLDER,),
        loading_strategy="SEQUENTIAL",
    )


class DynamicPlannerRackProvisioningTests(unittest.TestCase):
    def _prepare_world_for_archive_provisioning(self):
        world = build_default_world()

        if world.rack_placements.get((ARCHIVE_STATION_ID, ARCHIVE_SLOT_ID)) is None:
            if world.rack_placements.get((PLATE_STATION_ID, PLATE_ARCHIVE_SLOT_ID)) is not None:
                world.move_rack(
                    source_station_id=PLATE_STATION_ID,
                    source_station_slot_id=PLATE_ARCHIVE_SLOT_ID,
                    target_station_id=ARCHIVE_STATION_ID,
                    target_station_slot_id=ARCHIVE_SLOT_ID,
                )
            elif world.rack_placements.get((FRIDGE_STATION_ID, FRIDGE_SLOT_1)) is not None:
                world.move_rack(
                    source_station_id=FRIDGE_STATION_ID,
                    source_station_slot_id=FRIDGE_SLOT_1,
                    target_station_id=ARCHIVE_STATION_ID,
                    target_station_slot_id=ARCHIVE_SLOT_ID,
                )
            elif world.rack_placements.get((FRIDGE_STATION_ID, FRIDGE_SLOT_2)) is not None:
                world.move_rack(
                    source_station_id=FRIDGE_STATION_ID,
                    source_station_slot_id=FRIDGE_SLOT_2,
                    target_station_id=ARCHIVE_STATION_ID,
                    target_station_slot_id=ARCHIVE_SLOT_ID,
                )

        sample_id = world.ensure_placeholder_sample(
            station_id=INPUT_STATION_ID,
            station_slot_id=INPUT_URG_SLOT_ID,
            slot_index=1,
            obj_type=101,
        )
        world.classify_sample(
            sample_id,
            recognized=True,
            classification_source="unit-test",
            required_processes=(ProcessType.ARCHIVATION,),
        )
        return world, sample_id

    def test_returns_provision_action_when_archive_rack_missing_on_plate(self) -> None:
        world, _ = self._prepare_world_for_archive_provisioning()

        planner = DynamicStatePlanner({ProcessType.ARCHIVATION: _archivation_policy()})
        result = planner.plan_next(world)

        self.assertEqual(result.status, "READY")
        self.assertIsNotNone(result.action)
        assert result.action is not None
        self.assertEqual(result.action.action_type, "PROVISION_RACK")
        self.assertEqual(result.action.source_station_id, ARCHIVE_STATION_ID)
        self.assertEqual(result.action.source_station_slot_id, ARCHIVE_SLOT_ID)
        self.assertEqual(result.action.target_station_id, PLATE_STATION_ID)
        self.assertEqual(result.action.target_station_slot_id, PLATE_ARCHIVE_SLOT_ID)

    def test_stages_sample_after_archive_rack_is_provisioned(self) -> None:
        world, _ = self._prepare_world_for_archive_provisioning()
        planner = DynamicStatePlanner({ProcessType.ARCHIVATION: _archivation_policy()})
        first = planner.plan_next(world)
        self.assertEqual(first.status, "READY")
        self.assertIsNotNone(first.action)
        assert first.action is not None
        self.assertEqual(first.action.action_type, "PROVISION_RACK")
        world.move_rack(
            source_station_id=first.action.source_station_id,
            source_station_slot_id=first.action.source_station_slot_id,
            target_station_id=first.action.target_station_id,
            target_station_slot_id=first.action.target_station_slot_id,
        )
        result = planner.plan_next(world)

        self.assertEqual(result.status, "READY")
        self.assertIsNotNone(result.action)
        assert result.action is not None
        self.assertEqual(result.action.action_type, "STAGE_SAMPLE")
        self.assertEqual(result.action.target_station_id, PLATE_STATION_ID)
        self.assertEqual(result.action.target_station_slot_id, PLATE_ARCHIVE_SLOT_ID)
        self.assertEqual(result.action.process, ProcessType.ARCHIVATION)

    def test_prioritizes_upstream_stage_over_ready_archivation_process(self) -> None:
        world = build_default_world()

        # Make ARCHIVATION immediately processable for sample A.
        if world.rack_placements.get((PLATE_STATION_ID, PLATE_ARCHIVE_SLOT_ID)) is None:
            world.move_rack(
                source_station_id=ARCHIVE_STATION_ID,
                source_station_slot_id=ARCHIVE_SLOT_ID,
                target_station_id=PLATE_STATION_ID,
                target_station_slot_id=PLATE_ARCHIVE_SLOT_ID,
            )
        sample_a = world.ensure_placeholder_sample(
            station_id=INPUT_STATION_ID,
            station_slot_id=INPUT_URG_SLOT_ID,
            slot_index=1,
            obj_type=101,
        )
        world.classify_sample(
            sample_a,
            recognized=True,
            classification_source="unit-test",
            required_processes=(ProcessType.ARCHIVATION,),
        )
        world.move_sample(
            source_station_id=INPUT_STATION_ID,
            source_station_slot_id=INPUT_URG_SLOT_ID,
            source_slot_index=1,
            target_station_id=PLATE_STATION_ID,
            target_station_slot_id=PLATE_ARCHIVE_SLOT_ID,
            target_slot_index=1,
        )

        # Sample B still needs upstream DECAP staging.
        sample_b = world.ensure_placeholder_sample(
            station_id=INPUT_STATION_ID,
            station_slot_id=INPUT_URG_SLOT_ID,
            slot_index=2,
            obj_type=101,
        )
        world.classify_sample(
            sample_b,
            recognized=True,
            classification_source="unit-test",
            required_processes=(ProcessType.DECAP,),
        )

        planner = DynamicStatePlanner(
            {
                ProcessType.ARCHIVATION: _archivation_policy(),
                ProcessType.DECAP: _decap_policy(),
            }
        )
        result = planner.plan_next(world)

        self.assertEqual(result.status, "READY")
        self.assertIsNotNone(result.action)
        assert result.action is not None
        self.assertEqual(result.action.sample_id, sample_b)
        self.assertEqual(result.action.process, ProcessType.DECAP)
        self.assertEqual(result.action.action_type, "STAGE_SAMPLE")


if __name__ == "__main__":
    unittest.main()
