from __future__ import annotations

from typing import Optional

from .device import CentrifugeDevice
from .models import BalanceModel, DeviceCapabilities, DeviceIdentity, RotorConfiguration
from .strategies import LidControlStrategy, StartStrategy, StatusStrategy
from .usage_strategy import CentrifugeUsageProfile, Rotina380UsageProfile


class HettichRotina380RDevice(CentrifugeDevice):
    """Configurable Hettich Rotina 380R centrifuge model."""

    DEFAULT_MODEL = "Rotina380R"

    def __init__(
        self,
        *,
        identity: DeviceIdentity,
        capabilities: Optional[DeviceCapabilities] = None,
        rotor_configuration: Optional[RotorConfiguration] = None,
        balance_model: Optional[BalanceModel] = None,
        start_strategy: Optional[StartStrategy] = None,
        status_strategy: Optional[StatusStrategy] = None,
        lid_control_strategy: Optional[LidControlStrategy] = None,
        usage_profile: Optional[CentrifugeUsageProfile] = None,
    ) -> None:
        super().__init__(
            identity=identity,
            capabilities=capabilities
            or DeviceCapabilities(
                supported_processes=("CENTRIFUGATION",),
                refrigerated=True,
                automatic_rotor_recognition=True,
                powered_lid_lock=True,
                imbalance_detection=True,
                interfaces=("RS232", "LOCAL_UI"),
            ),
            rotor_configuration=rotor_configuration
            or RotorConfiguration(
                rotor_id="GENERIC",
                rotor_type="CONFIGURABLE",
                positions=(),
                buckets=(),
                adapters=(),
            ),
            balance_model=balance_model
            or BalanceModel(
                rule_type="OPPOSITE_POSITION",
                require_symmetry=True,
            ),
            start_strategy=start_strategy,
            status_strategy=status_strategy,
            lid_control_strategy=lid_control_strategy,
            usage_profile=usage_profile or Rotina380UsageProfile(),
        )
