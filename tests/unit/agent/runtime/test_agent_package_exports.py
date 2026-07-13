from __future__ import annotations

import importlib
from pathlib import Path


def test_agent_package_lazy_exports_are_resolvable() -> None:
    for init_file in Path("shinbot/agent").rglob("__init__.py"):
        if "_archive" in init_file.parts:
            continue
        module_name = ".".join(init_file.parent.parts)
        module = importlib.import_module(module_name)
        for export_name in getattr(module, "__all__", []):
            assert getattr(module, export_name) is not None


def test_session_actor_package_exports_complete_recovery_contract() -> None:
    module = importlib.import_module("shinbot.agent.runtime.session_actor")
    expected = {
        "RECOVERY_CERTIFICATE_SCHEMA",
        "RECOVERY_CERTIFICATE_VERSION",
        "RECOVERY_DELIVERY_EVENT_KIND",
        "RECOVERY_DELIVERY_EVENT_SOURCE",
        "RECOVERY_DELIVERY_SCHEMA",
        "RECOVERY_DELIVERY_VERSION",
        "RecoveryAggregateFence",
        "RecoveryCaseIdentity",
        "RecoveryCertificate",
        "RecoveryContractDecodeError",
        "RecoveryDecision",
        "RecoveryDecisionKind",
        "RecoveryDeliveryEnvelopeIdentity",
        "RecoveryDeliveryPayload",
        "RecoveryGraphEdge",
        "RecoveryGraphNode",
        "RecoveryInvariant",
        "RecoveryInvariantSeverity",
        "RecoverySubject",
        "UnsupportedRecoveryCertificateVersion",
        "UnsupportedRecoveryDeliveryVersion",
        "build_recovery_certificate",
        "canonical_recovery_digest",
        "canonical_recovery_json",
        "decode_recovery_certificate",
        "decode_recovery_delivery_payload",
        "recovery_delivery_event_id",
    }

    assert expected <= set(module.__all__)
    for export_name in expected:
        assert getattr(module, export_name) is not None
