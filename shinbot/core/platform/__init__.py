"""Platform adapter management."""

from shinbot.core.platform.adapter_manager import AdapterManager, BaseAdapter, MessageHandle
from shinbot.core.platform.ingress_pause import (
    AdapterIngressPauseDeliveryGuarantee,
    AdapterIngressPauseParticipant,
    AdapterIngressPauseReceipt,
    AdapterIngressPauseRequest,
    AdapterIngressPauseStatus,
    AdapterIngressPauseSupport,
    AdapterIngressPauseSupportInventory,
    AdapterIngressPauseSupportStatus,
    AdapterIngressPauseTicket,
)

__all__ = [
    "AdapterIngressPauseDeliveryGuarantee",
    "AdapterIngressPauseParticipant",
    "AdapterIngressPauseReceipt",
    "AdapterIngressPauseRequest",
    "AdapterIngressPauseStatus",
    "AdapterIngressPauseSupport",
    "AdapterIngressPauseSupportInventory",
    "AdapterIngressPauseSupportStatus",
    "AdapterIngressPauseTicket",
    "AdapterManager",
    "BaseAdapter",
    "MessageHandle",
]
