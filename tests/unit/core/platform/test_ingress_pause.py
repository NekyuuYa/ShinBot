"""Coverage for the unmounted adapter ingress pause capability contract."""

from __future__ import annotations

from typing import Any

import pytest

from shinbot.core.platform.adapter_manager import AdapterManager, BaseAdapter, MessageHandle
from shinbot.core.platform.ingress_pause import (
    AdapterIngressPauseDeliveryGuarantee,
    AdapterIngressPauseParticipant,
    AdapterIngressPauseReceipt,
    AdapterIngressPauseRequest,
    AdapterIngressPauseStatus,
    AdapterIngressPauseSupportStatus,
    AdapterIngressPauseTicket,
    new_adapter_ingress_pause_token,
)
from shinbot.schema.elements import MessageElement


class _Adapter(BaseAdapter):
    """Minimal adapter used to expose or omit a pause participant."""

    def __init__(
        self,
        instance_id: str,
        platform: str,
        *,
        participant: AdapterIngressPauseParticipant | object | None = None,
    ) -> None:
        super().__init__(instance_id, platform)
        self._participant = participant

    async def start(self) -> None:
        """Start the controlled test adapter."""

    async def shutdown(self) -> None:
        """Stop the controlled test adapter."""

    async def send(
        self,
        target_session: str,
        elements: list[MessageElement],
    ) -> MessageHandle:
        """Return a synthetic message handle."""

        return MessageHandle("message-a", adapter_ref=self)

    async def call_api(self, method: str, params: dict[str, Any]) -> Any:
        """Provide a no-op test API call."""

        return {"method": method, "params": params}

    async def get_capabilities(self) -> dict[str, Any]:
        """Return the minimal adapter capability manifest."""

        return {"elements": [], "actions": [], "limits": {}}

    def ingress_pause_participant(self) -> AdapterIngressPauseParticipant | None:
        """Expose a valid participant only when the test configured one."""

        if isinstance(self._participant, AdapterIngressPauseParticipant):
            return self._participant
        if self._participant is None:
            return None
        return self._participant  # type: ignore[return-value]


class _Participant:
    """Minimal lossless-contract stand-in used only for capability discovery."""

    def __init__(self, instance_id: str) -> None:
        self._instance_id = instance_id
        self.pause_calls = 0

    @property
    def adapter_instance_id(self) -> str:
        """Return the bound adapter id."""

        return self._instance_id

    @property
    def participant_id(self) -> str:
        """Return an opaque process-incarnation test identity."""

        return f"test-process:{self._instance_id}"

    @property
    def delivery_guarantee(self) -> AdapterIngressPauseDeliveryGuarantee:
        """Declare durable post-pause retention for contract inspection."""

        return AdapterIngressPauseDeliveryGuarantee.DURABLE_BUFFER

    def pause_ingress(self, request: AdapterIngressPauseRequest) -> AdapterIngressPauseTicket:
        """Create a synthetic local pause ticket."""

        self.pause_calls += 1
        return AdapterIngressPauseTicket(
            request=request,
            participant_id=self.participant_id,
            participant_epoch=1,
            token=new_adapter_ingress_pause_token(),
        )

    async def await_ingress_quiescent(
        self,
        ticket: AdapterIngressPauseTicket,
        *,
        timeout_seconds: float | None = None,
    ) -> AdapterIngressPauseReceipt:
        """Return a clean synthetic callback-drain receipt."""

        return AdapterIngressPauseReceipt(
            ticket=ticket,
            status=AdapterIngressPauseStatus.QUIESCENT,
        )

    def resume_ingress(self, ticket: AdapterIngressPauseTicket) -> bool:
        """Accept the exact synthetic ticket."""

        return ticket.participant_id == self.participant_id


def test_pause_receipt_rejects_a_quiescent_inflight_callback() -> None:
    """A local participant cannot report a contradictory drain receipt."""

    request = AdapterIngressPauseRequest(
        adapter_instance_id="adapter-a",
        legacy_session_id="adapter-a:group:room",
        cutover_id="cutover-a",
        cutover_epoch=1,
    )
    ticket = AdapterIngressPauseTicket(
        request=request,
        participant_id="process-a",
        participant_epoch=1,
        token="opaque-token",
    )

    assert "opaque-token" not in repr(ticket)

    with pytest.raises(ValueError, match="cannot retain in-flight"):
        AdapterIngressPauseReceipt(
            ticket=ticket,
            status=AdapterIngressPauseStatus.QUIESCENT,
            in_flight_callback_count=1,
        )


@pytest.mark.asyncio
async def test_manager_inventory_is_read_only_and_requires_running_participant() -> None:
    """Discovery cannot mistake a normal adapter for a cutover-safe participant."""

    manager = AdapterManager()
    standard = _Adapter("standard", "test")
    participant = _Participant("capable")
    capable = _Adapter("capable", "test", participant=participant)
    invalid = _Adapter("invalid", "test", participant=object())
    manager.register_adapter(
        "standard",
        lambda instance_id, platform: standard,
    )
    manager.register_adapter(
        "capable",
        lambda instance_id, platform: capable,
    )
    manager.register_adapter(
        "invalid",
        lambda instance_id, platform: invalid,
    )
    manager.create_instance("standard", "standard")
    manager.create_instance("capable", "capable")
    manager.create_instance("invalid", "invalid")

    before_start = manager.inspect_ingress_pause_support(["capable"])
    assert before_start.supports[0].status is AdapterIngressPauseSupportStatus.NOT_RUNNING

    await manager.start_all()
    inventory = manager.inspect_ingress_pause_support(
        ["missing", "standard", "capable", "invalid"]
    )
    statuses = {item.adapter_instance_id: item.status for item in inventory.supports}

    assert statuses == {
        "capable": AdapterIngressPauseSupportStatus.AVAILABLE,
        "invalid": AdapterIngressPauseSupportStatus.INVALID,
        "missing": AdapterIngressPauseSupportStatus.MISSING_INSTANCE,
        "standard": AdapterIngressPauseSupportStatus.UNSUPPORTED,
    }
    assert inventory.all_available is False
    assert inventory.unavailable_instance_ids == ("invalid", "missing", "standard")
    assert participant.pause_calls == 0


def test_manager_inventory_rejects_a_single_string_identifier() -> None:
    """A string is iterable but cannot name an adapter set safely."""

    manager = AdapterManager()

    with pytest.raises(TypeError, match="not a string"):
        manager.inspect_ingress_pause_support("adapter-a")
