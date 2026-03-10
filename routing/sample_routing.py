from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Protocol, Sequence, Tuple

import requests

from world.lab_world import ProcessType
from routing.workflows_training_catalog import (
    SampleTypeWorkflowProfile,
    load_training_workflow_profiles,
    match_profile_for_barcode,
)


@dataclass(frozen=True)
class ProcessStep:
    process: ProcessType
    station_id: Optional[str] = None
    device_id: Optional[str] = None
    notes: str = ""


@dataclass(frozen=True)
class SampleRoutingRequest:
    sample_id: str
    barcode: Optional[str] = None
    sample_type: Optional[int] = None


@dataclass(frozen=True)
class SampleRoutingDecision:
    recognized: bool
    barcode: Optional[str]
    classification: str
    process_steps: Tuple[ProcessStep, ...]
    source: str
    sample_type: Optional[int] = None
    target_rack_index: Optional[int] = None
    target_station_slot_id: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)


class RoutingProvider(Protocol):
    name: str

    def resolve(self, request: SampleRoutingRequest) -> Optional[SampleRoutingDecision]:
        ...


PROCESS_HINTS: Dict[ProcessType, Dict[str, str]] = {
    ProcessType.CAP: {
        "station_id": "3-FingerGripperStation",
        "device_id": "THREE_FINGER_GRIPPER_DEVICE_01",
    },
    ProcessType.DECAP: {
        "station_id": "3-FingerGripperStation",
        "device_id": "THREE_FINGER_GRIPPER_DEVICE_01",
    },
    ProcessType.SAMPLE_TYPE_DETECTION: {
        "station_id": "3-FingerGripperStation",
        "device_id": "THREE_FINGER_GRIPPER_DEVICE_01",
    },
    ProcessType.CENTRIFUGATION: {
        "station_id": "uLMPlateStation",
        "device_id": "CENTRIFUGE_DEVICE_01",
    },
    ProcessType.IMMUNOANALYSIS: {
        "station_id": "uLMPlateStation",
        "device_id": "BIORAD_IH500_DEVICE_01",
    },
    ProcessType.HEMATOLOGY_ANALYSIS: {
        "station_id": "uLMPlateStation",
        "device_id": "XN1000_ANALYZER_DEVICE_01",
    },
    ProcessType.CLINICAL_CHEMISTRY_ANALYSIS: {
        "station_id": "uLMPlateStation",
        "device_id": "CLINICAL_CHEMISTRY_ANALYZER_DEVICE_01",
    },
    ProcessType.COAGULATION_ANALYSIS: {
        "station_id": "uLMPlateStation",
        "device_id": "COAGULATION_ANALYZER_DEVICE_01",
    },
    ProcessType.ARCHIVATION: {
        "station_id": "uLMPlateStation",
        "device_id": "ARCHIVE_DEVICE_01",
    },
}


def _build_process_steps(process_values: Sequence[str]) -> Tuple[ProcessStep, ...]:
    steps: List[ProcessStep] = []
    for raw in process_values:
        proc = ProcessType(str(raw).strip().upper())
        hint = PROCESS_HINTS.get(proc, {})
        steps.append(
            ProcessStep(
                process=proc,
                station_id=hint.get("station_id"),
                device_id=hint.get("device_id"),
            )
        )
    return tuple(steps)


def _rule_matches(match_cfg: Dict[str, Any], barcode: str) -> bool:
    exact = match_cfg.get("exact")
    if exact is not None and barcode == str(exact):
        return True

    prefix = match_cfg.get("prefix")
    if prefix is not None and barcode.startswith(str(prefix)):
        return True

    suffix = match_cfg.get("suffix")
    if suffix is not None and barcode.endswith(str(suffix)):
        return True

    contains = match_cfg.get("contains")
    if contains is not None and str(contains) in barcode:
        return True

    regex = match_cfg.get("regex")
    if regex is not None:
        try:
            return re.search(str(regex), barcode) is not None
        except re.error:
            return False

    return False


class RuleBasedRoutingProvider:
    name = "RULES"

    def __init__(
        self,
        rules: Sequence[Dict[str, Any]],
        default_rule: Optional[Dict[str, Any]] = None,
        *,
        apply_default: bool = True,
    ) -> None:
        self.rules = list(rules)
        self.default_rule = default_rule or {}
        self.apply_default = bool(apply_default)

    @classmethod
    def from_file(cls, path: Path, *, apply_default: bool = True) -> "RuleBasedRoutingProvider":
        if not path.exists():
            return cls(rules=[], default_rule={}, apply_default=apply_default)
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        rules = raw.get("rules", []) if isinstance(raw, dict) else []
        default_rule = raw.get("default", {}) if isinstance(raw, dict) else {}
        if not isinstance(rules, list):
            rules = []
        if not isinstance(default_rule, dict):
            default_rule = {}
        return cls(rules=rules, default_rule=default_rule, apply_default=apply_default)

    def _decision_from_rule(
        self,
        request: SampleRoutingRequest,
        rule: Dict[str, Any],
        *,
        source: str,
        matched_rule_id: Optional[str],
    ) -> SampleRoutingDecision:
        process_values = rule.get("process_steps", [])
        if not isinstance(process_values, list):
            process_values = []
        process_steps = _build_process_steps([str(x) for x in process_values])

        raw_target_index = rule.get("target_rack_index")
        target_rack_index: Optional[int] = None
        if raw_target_index is not None:
            try:
                target_rack_index = int(raw_target_index)
            except Exception:
                target_rack_index = None

        target_station_slot_id = rule.get("target_station_slot_id")
        if target_station_slot_id is not None:
            target_station_slot_id = str(target_station_slot_id)

        raw_sample_type = rule.get("sample_type", request.sample_type)
        sample_type: Optional[int] = None
        if raw_sample_type is not None:
            try:
                sample_type = int(raw_sample_type)
            except Exception:
                sample_type = request.sample_type

        consumed_keys = {
            "id",
            "match",
            "recognized",
            "classification",
            "process_steps",
            "target_rack_index",
            "target_station_slot_id",
            "sample_type",
        }
        details: Dict[str, Any] = {
            "matched_rule_id": matched_rule_id,
            "provider": self.name,
        }
        for passthrough_key in (
            "sample_class",
            "target_group",
            "workflow_steps",
            "notes",
            "priority",
            "route_template",
        ):
            if passthrough_key in rule:
                details[passthrough_key] = rule.get(passthrough_key)
        extra_metadata = {k: v for k, v in rule.items() if k not in consumed_keys and k not in details}
        if extra_metadata:
            details["rule_metadata"] = extra_metadata

        return SampleRoutingDecision(
            recognized=bool(rule.get("recognized", True)),
            barcode=request.barcode,
            classification=str(rule.get("classification", "Recognized")),
            process_steps=process_steps,
            source=source,
            sample_type=sample_type,
            target_rack_index=target_rack_index,
            target_station_slot_id=target_station_slot_id,
            details=details,
        )

    def resolve(self, request: SampleRoutingRequest) -> Optional[SampleRoutingDecision]:
        barcode = (request.barcode or "").strip()
        if barcode:
            for idx, raw_rule in enumerate(self.rules):
                if not isinstance(raw_rule, dict):
                    continue
                match_cfg = raw_rule.get("match", {})
                if not isinstance(match_cfg, dict):
                    continue
                if _rule_matches(match_cfg, barcode):
                    rule_id = str(raw_rule.get("id", f"rule_{idx + 1}"))
                    return self._decision_from_rule(
                        request,
                        raw_rule,
                        source=self.name,
                        matched_rule_id=rule_id,
                    )

        if self.apply_default and self.default_rule:
            return self._decision_from_rule(
                request,
                self.default_rule,
                source=f"{self.name}_DEFAULT",
                matched_rule_id=None,
            )
        return None


class TrainingCatalogRoutingProvider:
    name = "TRAINING_CATALOG"

    def __init__(self, profiles: Sequence[SampleTypeWorkflowProfile]) -> None:
        self.profiles = list(profiles)

    @classmethod
    def from_xlsx(
        cls,
        path: Path,
    ) -> "TrainingCatalogRoutingProvider":
        profiles = load_training_workflow_profiles(path)
        return cls(profiles=profiles)

    def resolve(self, request: SampleRoutingRequest) -> Optional[SampleRoutingDecision]:
        barcode = (request.barcode or "").strip()
        if not barcode:
            return None
        profile = match_profile_for_barcode(self.profiles, barcode)
        if profile is None:
            return None

        process_steps = _build_process_steps([proc.value for proc in profile.canonical_process_steps])
        return SampleRoutingDecision(
            recognized=True,
            barcode=barcode,
            classification="RecognizedTrainingCatalog",
            process_steps=process_steps,
            source=self.name,
            sample_type=request.sample_type,
            target_rack_index=None,
            target_station_slot_id=None,
            details={
                "provider": self.name,
                "sample_type_key": profile.sample_type_key,
                "sample_type_label": profile.display_name,
                "material_codes": list(profile.material_codes),
                "article_numbers": list(profile.article_numbers),
                "cap_colors": list(profile.cap_colors),
                "canonical_process_steps": [p.value for p in profile.canonical_process_steps],
                "process_step_variants": [
                    [p.value for p in variant]
                    for variant in profile.process_step_variants
                ],
                "variant_count": len(profile.process_step_variants),
                "evidence_count": profile.evidence_count,
            },
        )


class HardRuleRoutingProvider:
    name = "HARD_RULES"

    def __init__(
        self,
        *,
        invalid_target_station_slot_id: str = "IntermediateRackSlot1",
    ) -> None:
        self.invalid_target_station_slot_id = str(invalid_target_station_slot_id).strip()

    def resolve(self, request: SampleRoutingRequest) -> Optional[SampleRoutingDecision]:
        barcode = (request.barcode or "").strip()
        if barcode:
            return None

        return SampleRoutingDecision(
            recognized=False,
            barcode=None,
            classification="InvalidUnrecognizedNoBarcode",
            process_steps=(),
            source=self.name,
            sample_type=request.sample_type,
            target_rack_index=None,
            target_station_slot_id=self.invalid_target_station_slot_id or "IntermediateRackSlot1",
            details={
                "provider": self.name,
                "rule_id": "NO_BARCODE_INVALID",
                "reason": "No barcode returned from ProcessAt3FingerStation ACTION=DetermineSampleType",
                "invalid": True,
            },
        )


class LisRoutingProvider:
    name = "LIS"

    def __init__(
        self,
        endpoint: str,
        *,
        token: str = "",
        timeout_s: float = 2.0,
    ) -> None:
        self.endpoint = endpoint.strip()
        self.token = token.strip()
        self.timeout_s = max(0.2, float(timeout_s))

    def resolve(self, request: SampleRoutingRequest) -> Optional[SampleRoutingDecision]:
        if not self.endpoint:
            return None
        if not request.barcode:
            return None

        headers: Dict[str, str] = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"

        payload = {
            "sample_id": request.sample_id,
            "barcode": request.barcode,
            "sample_type": request.sample_type,
        }
        try:
            response = requests.post(
                self.endpoint,
                headers=headers,
                json=payload,
                timeout=self.timeout_s,
            )
            response.raise_for_status()
            body = response.json()
        except Exception:
            return None

        if not isinstance(body, dict):
            return None

        process_values = body.get("process_steps", [])
        if not isinstance(process_values, list):
            process_values = []

        target_rack_index = body.get("target_rack_index")
        try:
            target_rack_index = int(target_rack_index) if target_rack_index is not None else None
        except Exception:
            target_rack_index = None

        sample_type = body.get("sample_type", request.sample_type)
        try:
            sample_type = int(sample_type) if sample_type is not None else None
        except Exception:
            sample_type = request.sample_type

        target_station_slot_id = body.get("target_station_slot_id")
        if target_station_slot_id is not None:
            target_station_slot_id = str(target_station_slot_id)

        return SampleRoutingDecision(
            recognized=bool(body.get("recognized", True)),
            barcode=request.barcode,
            classification=str(body.get("classification", "Recognized")),
            process_steps=_build_process_steps([str(x) for x in process_values]),
            source=self.name,
            sample_type=sample_type,
            target_rack_index=target_rack_index,
            target_station_slot_id=target_station_slot_id,
            details={
                "provider": self.name,
                "raw": body,
            },
        )


class ChainedSampleRouter:
    def __init__(self, providers: Sequence[RoutingProvider]) -> None:
        self.providers = list(providers)

    def route(self, request: SampleRoutingRequest) -> SampleRoutingDecision:
        for provider in self.providers:
            decision = provider.resolve(request)
            if decision is not None:
                return decision
        return SampleRoutingDecision(
            recognized=False,
            barcode=request.barcode,
            classification="Unrecognized",
            process_steps=(),
            source="UNRESOLVED",
            sample_type=request.sample_type,
            details={"provider_count": len(self.providers)},
        )
