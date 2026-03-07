from __future__ import annotations

from typing import Optional

from .device import BioradDevice
from ..models import DeviceCapabilities, DeviceIdentity, LoadInterfaceConfig
from ..strategies import StartStrategy, StatusStrategy


class BioradIh500Device(BioradDevice):
    """BioRad IH-500 analyzer model (sample/rack handling only)."""

    DEFAULT_MODEL = "IH500"

    def __init__(
        self,
        *,
        identity: DeviceIdentity,
        capabilities: Optional[DeviceCapabilities] = None,
        load_interface: Optional[LoadInterfaceConfig] = None,
        start_strategy: Optional[StartStrategy] = None,
        status_strategy: Optional[StatusStrategy] = None,
    ) -> None:
        super().__init__(
            identity=identity,
            capabilities=capabilities
            or DeviceCapabilities(
                supported_processes=("IMMUNOANALYSIS",),
                continuous_loading=True,
                auto_start=False,
                nominal_sample_capacity=50,
            ),
            load_interface=load_interface
            or LoadInterfaceConfig(
                carrier_type="RACK",
                loading_area="SEPARATE_LOADING_AREA",
                metadata={"layout": "compact"},
            ),
            start_strategy=start_strategy,
            status_strategy=status_strategy,
        )

