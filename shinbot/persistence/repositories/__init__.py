"""Repository implementations grouped by persistence domain."""

from .actor_v2_admission_fence import ActorV2AdmissionFenceRepository
from .actor_v2_canary_isolation_lease import ActorV2CanaryIsolationLeaseRepository
from .actor_v2_core_ingress_drain import ActorV2CoreIngressDrainRepository
from .actor_v2_cutover_journal import ActorV2CutoverJournalRepository
from .actor_v2_fenced_wake_target_lease import ActorV2FencedWakeTargetLeaseRepository
from .actor_v2_ingress_drain import ActorV2IngressDrainRepository
from .actor_v2_legacy_recovery_gate import ActorV2LegacyRecoveryGateRepository
from .actor_v2_legacy_state_handoff import ActorV2LegacyStateHandoffRepository
from .actor_v2_mailbox_handoff import ActorV2MailboxHandoffRepository
from .actor_v2_migration_barrier import ActorV2MigrationBarrierRepository
from .admin import InstanceConfigRepository
from .agent_runtime_diagnostics import AgentRuntimeDiagnosticsRepository
from .agent_runtime_ownership import AgentRuntimeOwnershipRepository
from .ai import AIInteractionRepository, PromptSnapshotRepository
from .base import ContextProvider, Repository
from .durable_routing import DurableMessageRoutingRepository
from .media import (
    MediaAssetRepository,
    MediaSemanticRepository,
    MessageMediaLinkRepository,
    SessionMediaOccurrenceRepository,
)
from .messages import MessageLogRepository
from .model import ModelExecutionRepository, ModelRegistryRepository
from .sessions import AuditRepository, SessionRepository

__all__ = [
    "AIInteractionRepository",
    "ActorV2AdmissionFenceRepository",
    "ActorV2CanaryIsolationLeaseRepository",
    "ActorV2CoreIngressDrainRepository",
    "ActorV2CutoverJournalRepository",
    "ActorV2FencedWakeTargetLeaseRepository",
    "ActorV2IngressDrainRepository",
    "ActorV2LegacyStateHandoffRepository",
    "ActorV2LegacyRecoveryGateRepository",
    "ActorV2MailboxHandoffRepository",
    "ActorV2MigrationBarrierRepository",
    "AgentRuntimeDiagnosticsRepository",
    "AgentRuntimeOwnershipRepository",
    "AuditRepository",
    "ContextProvider",
    "DurableMessageRoutingRepository",
    "InstanceConfigRepository",
    "MediaAssetRepository",
    "MediaSemanticRepository",
    "MessageLogRepository",
    "MessageMediaLinkRepository",
    "ModelExecutionRepository",
    "ModelRegistryRepository",
    "PromptSnapshotRepository",
    "Repository",
    "SessionMediaOccurrenceRepository",
    "SessionRepository",
]
