from __future__ import annotations

from typing import Any

__all__ = ["PlannerWebInterfaceBridge", "PlannerInterfaceSnapshot"]


def __getattr__(name: str) -> Any:
    if name in __all__:
        from .service import PlannerInterfaceSnapshot, PlannerWebInterfaceBridge

        mapping = {
            "PlannerWebInterfaceBridge": PlannerWebInterfaceBridge,
            "PlannerInterfaceSnapshot": PlannerInterfaceSnapshot,
        }
        return mapping[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
