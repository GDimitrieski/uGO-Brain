import unittest

from Device.analyzer_device import AnalyzerDeviceCapabilities, AnalyzerDeviceIdentity
from Device.centrifuge_device import (
    DEVICE_ACTION_CLOSE_HATCH,
    DEVICE_ACTION_MOVE_ROTOR,
    DEVICE_ACTION_OPEN_HATCH,
    DEVICE_ACTION_START_CENTRIFUGE,
    CentrifugeAnalyzerDevice,
)
from Device.packml import PackMLState
from Device.registry import build_device_registry_from_world
from world.lab_world import build_default_world


class DeviceCentrifugeRuntimeTests(unittest.TestCase):
    def _build_centrifuge(self, max_racks: int = 4) -> CentrifugeAnalyzerDevice:
        return CentrifugeAnalyzerDevice(
            identity=AnalyzerDeviceIdentity(
                device_id="CENT_RUNTIME_01",
                name="Runtime Centrifuge",
                station_id="CentrifugeStation",
                model="Rotina380R",
            ),
            capabilities=AnalyzerDeviceCapabilities(
                supported_processes=("CENTRIFUGATION",),
                supported_rack_types=("CENTRIFUGE_RACK",),
                max_racks=max_racks,
            ),
        )

    def test_registry_builds_centrifuge_from_world(self) -> None:
        world = build_default_world()
        registry = build_device_registry_from_world(world)

        station_devices = registry.get_centrifuges_at_station("CentrifugeStation")
        self.assertGreaterEqual(len(station_devices), 1)
        device = station_devices[0]
        self.assertTrue(device.supports_process("CENTRIFUGATION"))
        self.assertTrue(device.supports_rack_type("CENTRIFUGE_RACK"))
        self.assertGreaterEqual(device.capabilities.max_racks, 1)

    def test_action_sequence_transitions_to_execute(self) -> None:
        device = self._build_centrifuge(max_racks=2)
        self.assertTrue(device.load_rack("RACK_01", "CENTRIFUGE_RACK"))

        self.assertTrue(device.apply_single_device_action(DEVICE_ACTION_OPEN_HATCH))
        self.assertTrue(device.apply_single_device_action(DEVICE_ACTION_CLOSE_HATCH))
        self.assertTrue(device.apply_single_device_action(DEVICE_ACTION_START_CENTRIFUGE))

        diag = device.diagnose()
        self.assertEqual(diag["packml_state"], PackMLState.EXECUTE.value)
        self.assertTrue(diag["rotor_spinning"])
        self.assertFalse(device.apply_single_device_action(DEVICE_ACTION_MOVE_ROTOR))

    def test_capacity_and_rack_type_constraints(self) -> None:
        device = self._build_centrifuge(max_racks=2)
        self.assertFalse(device.load_rack("RACK_XX", "URG_RACK"))
        self.assertTrue(device.load_rack("RACK_01", "CENTRIFUGE_RACK"))
        self.assertTrue(device.load_rack("RACK_02", "CENTRIFUGE_RACK"))
        self.assertFalse(device.load_rack("RACK_03", "CENTRIFUGE_RACK"))

    def test_move_rotor_requires_explicit_slot_index(self) -> None:
        device = self._build_centrifuge(max_racks=2)
        self.assertFalse(device.apply_single_device_action(DEVICE_ACTION_MOVE_ROTOR))
        self.assertTrue(device.apply_single_device_action(DEVICE_ACTION_MOVE_ROTOR, rotor_slot_index=2))
        diag = device.diagnose()
        self.assertEqual(diag["rotor_step_index"], 2)


if __name__ == "__main__":
    unittest.main()
