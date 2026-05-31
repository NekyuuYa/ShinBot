"""LiteLLM model runtime backend."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from shinbot.agent.services.model_runtime.backends.litellm.completion import (
    invoke_completion,
)
from shinbot.agent.services.model_runtime.backends.litellm.embedding import (
    invoke_embedding,
)
from shinbot.agent.services.model_runtime.backends.litellm.image import (
    invoke_image_generation,
)
from shinbot.agent.services.model_runtime.backends.litellm.rerank import invoke_rerank
from shinbot.agent.services.model_runtime.backends.litellm.speech import invoke_speech
from shinbot.agent.services.model_runtime.backends.litellm.transcription import (
    invoke_transcription,
)
from shinbot.agent.services.model_runtime.backends.litellm.video import (
    invoke_video_generation,
)
from shinbot.agent.services.model_runtime.backends.protocol import (
    BackendOperation,
    BackendRequestPlan,
)
from shinbot.agent.services.model_runtime.planning import (
    build_litellm_kwargs,
    sanitize_litellm_kwargs,
)
from shinbot.agent.services.model_runtime.types import ModelRuntimeCall

_EndpointInvoker = Callable[[BackendRequestPlan], Any]

_ENDPOINTS: dict[BackendOperation, _EndpointInvoker] = {
    "completion": invoke_completion,
    "embedding": invoke_embedding,
    "rerank": invoke_rerank,
    "speech": invoke_speech,
    "transcription": invoke_transcription,
    "image": invoke_image_generation,
    "video": invoke_video_generation,
}


class LiteLLMBackend:
    """Backend adapter that preserves the existing LiteLLM execution behavior."""

    name = "litellm"

    def plan_request(
        self,
        *,
        provider: dict[str, Any],
        model: dict[str, Any],
        call: ModelRuntimeCall,
        timeout_override: float | None,
        operation: BackendOperation,
    ) -> BackendRequestPlan:
        """Build a LiteLLM kwargs payload for one execution attempt."""

        payload = build_litellm_kwargs(
            provider=provider,
            model=model,
            call=call,
            timeout_override=timeout_override,
            mode=operation,
        )
        return BackendRequestPlan(
            operation=operation,
            payload=payload,
            safe_payload=sanitize_litellm_kwargs(payload),
            backend_name=self.name,
            backend_model=str(payload.get("model") or model.get("litellm_model") or ""),
        )

    def invoke(self, plan: BackendRequestPlan) -> Any:
        """Invoke the matching LiteLLM operation for a prepared request plan."""

        invoker = _ENDPOINTS.get(plan.operation)
        if invoker is not None:
            return invoker(plan)
        raise ValueError(f"Unsupported LiteLLM operation: {plan.operation}")
