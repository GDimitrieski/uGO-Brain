import unittest

from Device.centrifuge_usage_strategy import (
    DeviceActionStep,
    RackTransferStep,
    Rotina380UsageProfile,
    SampleTransferStep,
    compile_centrifuge_usage_plan,
)
from world.lab_world import build_default_world


PLATE_STATION_ID = "uLMPlateStation"
CENTRIFUGE_STATION_ID = "CentrifugeStation"


class _DummyIdentity:
    device_id = "DUMMY_CENTRIFUGE"


class _DummyDevice:
    def __init__(self, profile: Rotina380UsageProfile) -> None:
        self.identity = _DummyIdentity()
        self.usage_profile = profile


class CentrifugeUsageStrategyTests(unittest.TestCase):
    def test_load_plan_includes_tara_balance_sample_transfers(self) -> None:
        world = build_default_world()
        world.get_rack_at(PLATE_STATION_ID, "CentrifugeRacksSlot1").occupied_slots[1] = "SMP_LOAD_01"
        device = _DummyDevice(Rotina380UsageProfile())

        plan = compile_centrifuge_usage_plan(world=world, device=device, mode="LOAD")
        transfer_ops = [op for op in plan.operations if isinstance(op, SampleTransferStep)]

        self.assertGreaterEqual(len(transfer_ops), 3)
        self.assertTrue(all(op.reason == "tara_balance" for op in transfer_ops))
        self.assertEqual(
            [op.target_station_slot_id for op in transfer_ops[:3]],
            ["CentrifugeRacksSlot2", "CentrifugeRacksSlot3", "CentrifugeRacksSlot4"],
        )
        first_device_action = next(
            (idx for idx, op in enumerate(plan.operations) if isinstance(op, DeviceActionStep)),
            -1,
        )
        self.assertGreaterEqual(first_device_action, len(transfer_ops))

    def test_unload_plan_returns_tara_probes_after_racks_return(self) -> None:
        world = build_default_world()
        tara_rack = world.get_rack_at(PLATE_STATION_ID, "TaraRacksSlot1")
        probe_sample_id = tara_rack.occupied_slots.pop(1)
        world.get_rack_at(PLATE_STATION_ID, "CentrifugeRacksSlot1").occupied_slots[1] = probe_sample_id

        source_slot_ids = [
            "CentrifugeRacksSlot1",
            "CentrifugeRacksSlot2",
            "CentrifugeRacksSlot3",
            "CentrifugeRacksSlot4",
        ]
        centrifuge_slot_ids = [
            "CentrifugeRacksSlot1",
            "CentrifugeRacksSlot2",
            "CentrifugeRacksSlot3",
            "CentrifugeRacksSlot4",
        ]
        for source_slot_id, centrifuge_slot_id in zip(source_slot_ids, centrifuge_slot_ids):
            world.move_rack(
                source_station_id=PLATE_STATION_ID,
                source_station_slot_id=source_slot_id,
                target_station_id=CENTRIFUGE_STATION_ID,
                target_station_slot_id=centrifuge_slot_id,
            )

        device = _DummyDevice(Rotina380UsageProfile())
        plan = compile_centrifuge_usage_plan(world=world, device=device, mode="UNLOAD")
        transfer_ops = [op for op in plan.operations if isinstance(op, SampleTransferStep)]
        self.assertTrue(any(op.reason == "tara_return" for op in transfer_ops))

        close_idx = next(
            idx
            for idx, op in enumerate(plan.operations)
            if isinstance(op, DeviceActionStep) and op.name == "CloseHatch"
        )
        first_return_idx = next(
            idx
            for idx, op in enumerate(plan.operations)
            if isinstance(op, SampleTransferStep) and op.reason == "tara_return"
        )
        self.assertGreater(first_return_idx, close_idx)

    def test_unload_plan_moves_rotor_before_each_rack_unload(self) -> None:
        world = build_default_world()
        slot_ids = [
            "CentrifugeRacksSlot1",
            "CentrifugeRacksSlot2",
            "CentrifugeRacksSlot3",
            "CentrifugeRacksSlot4",
        ]
        for slot_id in slot_ids:
            world.move_rack(
                source_station_id=PLATE_STATION_ID,
                source_station_slot_id=slot_id,
                target_station_id=CENTRIFUGE_STATION_ID,
                target_station_slot_id=slot_id,
            )

        device = _DummyDevice(Rotina380UsageProfile())
        plan = compile_centrifuge_usage_plan(world=world, device=device, mode="UNLOAD")

        unload_steps = [
            (idx, op)
            for idx, op in enumerate(plan.operations)
            if isinstance(op, RackTransferStep) and op.name.startswith("UnloadRack")
        ]
        self.assertEqual(len(unload_steps), len(slot_ids))
        for idx, op in unload_steps:
            self.assertGreater(idx, 0)
            prev_op = plan.operations[idx - 1]
            self.assertIsInstance(prev_op, DeviceActionStep)
            self.assertEqual(prev_op.name, f"MoveRotorToPos{int(op.transfer_index)}")
            self.assertEqual(int(prev_op.rotor_slot_index), int(op.transfer_index))
            self.assertEqual(int(prev_op.overrides.get("OBJ_Nbr", 0)), int(op.transfer_index))


if __name__ == "__main__":
    unittest.main()
