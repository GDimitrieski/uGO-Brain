import unittest

from world.jig_rack_strategy import (
    plan_tara_balance_moves,
    plan_tara_return_moves,
)
from world.lab_world import build_default_world


PLATE_STATION_ID = "uLMPlateStation"


class JigRackStrategyTests(unittest.TestCase):
    def test_round_robin_target_selection_for_jig2(self) -> None:
        world = build_default_world()
        picks = []
        for idx in range(5):
            slot_id, slot_index = world.select_next_target_slot_for_jig(
                PLATE_STATION_ID,
                2,
            )
            picks.append((slot_id, slot_index))
            rack = world.get_rack_at(PLATE_STATION_ID, slot_id)
            rack.occupied_slots[int(slot_index)] = f"SMP_{idx + 1:04d}"

        self.assertEqual(
            picks,
            [
                ("CentrifugeRacksSlot1", 1),
                ("CentrifugeRacksSlot2", 1),
                ("CentrifugeRacksSlot3", 1),
                ("CentrifugeRacksSlot4", 1),
                ("CentrifugeRacksSlot1", 2),
            ],
        )

    def test_tara_balance_moves_fill_underrepresented_racks(self) -> None:
        world = build_default_world()
        rack = world.get_rack_at(PLATE_STATION_ID, "CentrifugeRacksSlot1")
        rack.occupied_slots[1] = "SMP_9001"

        moves = plan_tara_balance_moves(
            world,
            station_id=PLATE_STATION_ID,
            target_jig_id=2,
            tara_jig_id=3,
        )

        self.assertEqual(len(moves), 3)
        self.assertEqual(
            [(m.target_station_slot_id, m.target_slot_index) for m in moves],
            [
                ("CentrifugeRacksSlot2", 1),
                ("CentrifugeRacksSlot3", 1),
                ("CentrifugeRacksSlot4", 1),
            ],
        )
        self.assertTrue(all(m.source_station_slot_id == "TaraRacksSlot1" for m in moves))
        self.assertTrue(all(m.reason == "tara_balance" for m in moves))

    def test_tara_return_moves_prioritize_probe_return(self) -> None:
        world = build_default_world()
        tara_rack = world.get_rack_at(PLATE_STATION_ID, "TaraRacksSlot1")
        probe_1 = tara_rack.occupied_slots.pop(1)
        probe_2 = tara_rack.occupied_slots.pop(2)
        world.get_rack_at(PLATE_STATION_ID, "CentrifugeRacksSlot1").occupied_slots[1] = probe_1
        world.get_rack_at(PLATE_STATION_ID, "CentrifugeRacksSlot2").occupied_slots[1] = probe_2

        moves = plan_tara_return_moves(
            world,
            station_id=PLATE_STATION_ID,
            source_jig_id=2,
            tara_jig_id=3,
        )

        self.assertEqual(len(moves), 2)
        self.assertEqual([m.sample_id for m in moves], [probe_1, probe_2])
        self.assertEqual(
            [(m.target_station_slot_id, m.target_slot_index) for m in moves],
            [("TaraRacksSlot1", 1), ("TaraRacksSlot1", 2)],
        )
        self.assertTrue(all(m.reason == "tara_return" for m in moves))


if __name__ == "__main__":
    unittest.main()
