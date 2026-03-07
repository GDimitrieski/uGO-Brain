from __future__ import annotations

from typing import Optional

from .device import BioradDevice
from ..models import DeviceCapabilities, DeviceIdentity, LoadInterfaceConfig
from ..strategies import StartStrategy, StatusStrategy


class BioradIh1000Device(BioradDevice):
    """BioRad IH-1000 analyzer model (sample/rack handling only)."""

    DEFAULT_MODEL = "IH1000"

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
                auto_start=True,
                nominal_sample_capacity=180,
            ),
            load_interface=load_interface
            or LoadInterfaceConfig(
                carrier_type="RACK",
                loading_area="MAIN_LOADING_AREA",
            ),
            start_strategy=start_strategy,
            status_strategy=status_strategy,
        )

