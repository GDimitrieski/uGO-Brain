import unittest

from Device.registry import build_device_registry_from_world
from Device.wise_adapter import WiseModuleAdapter
from planning.planner import DynamicStatePlanner, ProcessPolicy
from world.lab_world import Device, ProcessType, RackType, build_default_world


INPUT_STATION_ID = "InputStation"
INPUT_SLOT_ID = "URGRackSlot2"
PLATE_STATION_ID = "uLMPlateStation"
PLATE_IH500_SLOT_ID = "BioRadIH500Slot1"
IH500_STATION_ID = "BioRadIH500Station"
IH500_DEVICE_SLOT_ID = "BioRadIH500Slot1"
IH500_DEVICE_ID = "BIORAD_IH500_DEVICE_01"


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


def _immuno_policy() -> ProcessPolicy:
    return ProcessPolicy(
        process=ProcessType.IMMUNOHEMATOLOGY_ANALYSIS,
        target_station_id=PLATE_STATION_ID,
        target_jig_ids=(12,),
        required_rack_types=(RackType.BIORAD_IH500_RACK,),
        preferred_device_ids=(IH500_DEVICE_ID,),
        candidate_device_station_ids=(IH500_STATION_ID,),
        requires_device=True,
        loading_strategy="SEQUENTIAL",
    )


def _set_wise_metadata(world, *, enabled: bool, host: str, required_for_selection: bool = False, stale: bool = False):
    dev = world.devices[IH500_DEVICE_ID]
    metadata = dict(dev.metadata)
    wise_cfg = dict(metadata.get("wise", {}))
    wise_cfg.update(
        {
            "enabled": bool(enabled),
            "host": str(host),
            "port": 80,
            "di_slot": 0,
            "rack_ready_channels": {"1": 0, "2": 1, "3": 2},
            "required_for_selection": bool(required_for_selection),
            "required_for_processes": ["IMMUNOHEMATOLOGY_ANALYSIS"],
        }
    )
    metadata["wise"] = wise_cfg
    metadata["wise_state"] = {
        "online": True,
        "stale": bool(stale),
        "error": "",
        "channels": {"0": True, "1": True, "2": True, "3": False},
    }
    world.devices[IH500_DEVICE_ID] = Device(
        id=dev.id,
        name=dev.name,
        station_id=dev.station_id,
        capabilities=dev.capabilities,
        metadata=metadata,
    )


class WiseAdapterTests(unittest.TestCase):
    def test_parses_di_payload(self) -> None:
        adapter = WiseModuleAdapter(
            host="127.0.0.1",
            di_endpoint_template="http://localhost/mock/di/{slot}",
            http_get=lambda *_args, **_kwargs: _FakeResponse(
                {"DIVal": [{"Ch": 0, "Val": 1}, {"Ch": 1, "Val": 0}, {"Ch": 2, "Val": 1}, {"Ch": 3, "Val": 0}]}
            ),
        )
        snapshot = adapter.poll_inputs(force=True)
        self.assertTrue(snapshot.online)
        self.assertFalse(snapshot.stale)
        self.assertTrue(snapshot.channels[0])
        self.assertFalse(snapshot.channels[1])
        self.assertTrue(snapshot.channels[2])
        self.assertFalse(snapshot.channels[3])

    def test_handles_poll_error_as_offline_snapshot(self) -> None:
        def _raise(*_args, **_kwargs):
            raise RuntimeError("network down")

        adapter = WiseModuleAdapter(
            host="127.0.0.1",
            di_endpoint_template="http://localhost/mock/di/{slot}",
            http_get=_raise,
        )
        snapshot = adapter.poll_inputs(force=True)
        self.assertFalse(snapshot.online)
        self.assertTrue(snapshot.stale)
        self.assertIn("network down", snapshot.error)


class WiseRegistryTests(unittest.TestCase):
    def test_registry_registers_enabled_wise_module(self) -> None:
        world = build_default_world()
        _set_wise_metadata(world, enabled=True, host="192.168.10.5")
        registry = build_device_registry_from_world(world)
        wise_modules = registry.get_wise_modules()
        self.assertIn(IH500_DEVICE_ID, wise_modules)

    def test_registry_rejects_enabled_wise_without_host(self) -> None:
        world = build_default_world()
        _set_wise_metadata(world, enabled=True, host="")
        with self.assertRaises(ValueError):
            build_device_registry_from_world(world)


class WisePlannerTests(unittest.TestCase):
    def _stage_sample_for_immuno(self):
        world = build_default_world()
        sample_id = world.ensure_placeholder_sample(
            station_id=INPUT_STATION_ID,
            station_slot_id=INPUT_SLOT_ID,
            slot_index=1,
            obj_type=101,
        )
        world.classify_sample(
            sample_id,
            recognized=True,
            classification_source="unit-test",
            required_processes=(ProcessType.IMMUNOHEMATOLOGY_ANALYSIS,),
        )
        world.mark_process_completed(sample_id, ProcessType.DECAP)
        world.move_sample(
            source_station_id=INPUT_STATION_ID,
            source_station_slot_id=INPUT_SLOT_ID,
            source_slot_index=1,
            target_station_id=PLATE_STATION_ID,
            target_station_slot_id=PLATE_IH500_SLOT_ID,
            target_slot_index=1,
        )
        return world, sample_id

    def test_wise_selection_filter_blocks_stale_device(self) -> None:
        world, _sample_id = self._stage_sample_for_immuno()
        _set_wise_metadata(world, enabled=True, host="192.168.10.5", required_for_selection=True, stale=True)
        planner = DynamicStatePlanner(
            {ProcessType.IMMUNOHEMATOLOGY_ANALYSIS: _immuno_policy()},
            use_wise_readiness=True,
        )
        result = planner.plan_next(world)
        self.assertEqual(result.status, "BLOCKED")

    def test_immuno_process_stays_process_action_when_rack_is_in_device_station(self) -> None:
        world, sample_id = self._stage_sample_for_immuno()
        _set_wise_metadata(world, enabled=False, host="")
        world.move_rack(
            source_station_id=PLATE_STATION_ID,
            source_station_slot_id=PLATE_IH500_SLOT_ID,
            target_station_id=IH500_STATION_ID,
            target_station_slot_id=IH500_DEVICE_SLOT_ID,
        )
        planner = DynamicStatePlanner({ProcessType.IMMUNOHEMATOLOGY_ANALYSIS: _immuno_policy()})
        result = planner.plan_next(world)
        self.assertEqual(result.status, "READY")
        self.assertIsNotNone(result.action)
        assert result.action is not None
        self.assertEqual(result.action.sample_id, sample_id)
        self.assertEqual(result.action.action_type, "PROCESS_SAMPLE")
        self.assertEqual(result.action.target_station_id, IH500_STATION_ID)
        self.assertEqual(result.action.target_station_slot_id, IH500_DEVICE_SLOT_ID)


if __name__ == "__main__":
    unittest.main()
