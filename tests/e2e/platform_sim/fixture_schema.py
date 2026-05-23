"""Schema validation for platform simulation JSON fixtures."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from shinbot.schema.elements import MessageElement


class ScenarioValidationError(ValueError):
    """Raised when a platform simulation fixture uses an unsupported shape."""


def validate_scenario(scenario: Any, *, source: Path | str = "<scenario>") -> None:
    validator = _ScenarioValidator(str(source))
    validator.validate(scenario)


class _ScenarioValidator:
    ROOT_KEYS = {
        "actions",
        "adapter",
        "agentEntryProbe",
        "agentSchedulerProbe",
        "commands",
        "config",
        "eventBusHandlers",
        "expect",
        "modelRuntime",
        "name",
        "sessions",
        "steps",
    }
    ADAPTER_KEYS = {"instanceId", "platform", "selfId"}
    PROBE_KEYS = {"enabled", "now", "responseProfile"}
    SESSION_INIT_KEYS = {
        "channelId",
        "config",
        "displayName",
        "guildId",
        "id",
        "instanceId",
        "platform",
        "type",
    }
    SESSION_CONFIG_KEYS = {"isMuted", "prefixes"}
    COMMAND_KEYS = {
        "api",
        "call",
        "kind",
        "name",
        "prompt",
        "reply",
        "replyAfterInput",
        "timeout",
    }
    COMMAND_KINDS = {"reply", "prompt", "model", "api"}
    COMMAND_API_KEYS = {"method", "params"}
    COMMAND_CALL_KEYS = {
        "caller",
        "metadata",
        "modelId",
        "params",
        "prompt",
        "purpose",
        "routeId",
    }
    STEP_KEYS = {
        "content",
        "elements",
        "eventType",
        "expect",
        "expectSentCount",
        "id",
        "member",
        "noticeType",
        "operator",
        "sender",
        "session",
        "timestamp",
        "type",
    }
    STEP_TYPES = {"message", "notice"}
    STEP_SESSION_KEYS = {"channelId", "channelName", "guildId", "guildName", "type"}
    STEP_SESSION_TYPES = {"group", "private"}
    STEP_USER_KEYS = {"id", "name", "nick"}
    STEP_MEMBER_KEYS = {"nick", "user"}
    ACTION_KEYS = {
        "activeChatDecayHalfLifeSeconds",
        "activeChatInitialInterest",
        "enterActiveChat",
        "now",
        "sessionId",
        "type",
    }
    ACTION_TYPES = {"agentReviewDue", "agentCompleteReview", "agentActiveChatTick"}
    EVENT_BUS_HANDLER_KEYS = {"eventType"}
    MODEL_RUNTIME_KEYS = {"debugPlugin", "fakeCompletion", "models", "providers"}
    FAKE_COMPLETION_KEYS = {
        "cacheReadTokens",
        "cacheWriteTokens",
        "inputTokens",
        "outputTokens",
        "text",
    }
    MODEL_PROVIDER_KEYS = {
        "auth",
        "defaultParams",
        "displayName",
        "enabled",
        "id",
        "type",
    }
    MODEL_DEFINITION_KEYS = {
        "capabilities",
        "costMetadata",
        "defaultParams",
        "displayName",
        "enabled",
        "id",
        "litellmModel",
        "providerId",
    }
    CONFIG_KEYS = {"adapter_instances", "bots", "plugins"}
    CONFIG_ADAPTER_KEYS = {"id", "platform"}
    CONFIG_PLUGIN_KEYS = {"id"}
    CONFIG_BOT_KEYS = {
        "agent",
        "bindings",
        "commands",
        "display_name",
        "id",
        "plugins",
    }
    CONFIG_BOT_COMMANDS_KEYS = {"enabled", "prefixes"}
    CONFIG_BOT_PLUGINS_KEYS = {"enabled", "enabled_plugins"}
    CONFIG_BOT_AGENT_KEYS = {"config", "mode"}
    CONFIG_BOT_BINDING_KEYS = {
        "adapter_instance_id",
        "id",
        "priority",
        "session_patterns",
    }
    EXPECT_KEYS = {
        "agentEntrySignals",
        "agentScheduler",
        "apiCalls",
        "messageLogs",
        "messageLogsBySession",
        "modelRuntime",
        "noticeEvents",
        "sent",
        "sessions",
    }
    EXPECT_SENT_KEYS = {
        "elements",
        "messageId",
        "messageIdStartsWith",
        "sessionId",
        "text",
        "textContains",
    }
    EXPECT_SESSION_KEYS = {"displayName", "id", "type"}
    EXPECT_API_CALL_KEYS = {"method", "params", "paramsContains"}
    EXPECT_MESSAGE_LOG_KEYS = {
        "contentElements",
        "countAtLeast",
        "countExact",
        "ids",
        "incomingRoutingStatus",
        "isMentioned",
        "isRead",
        "limit",
        "platformMsgIds",
        "rawTextContains",
        "rawTexts",
        "roles",
        "routingSkipReasons",
        "routingStatuses",
        "senderIds",
        "senderNames",
        "sessionId",
    }
    EXPECT_AGENT_ENTRY_KEYS = {
        "alreadyHandled",
        "botBindingId",
        "botId",
        "botSessionId",
        "eventType",
        "instanceId",
        "isMentionToOther",
        "isMentioned",
        "isPokeToBot",
        "isPokeToOther",
        "isPrivate",
        "isReplyToBot",
        "isStopped",
        "messageLogId",
        "platform",
        "senderId",
        "sessionId",
    }
    EXPECT_AGENT_SCHEDULER_KEYS = {
        "activeChatState",
        "knownSessionIds",
        "reviewPlan",
        "sessionId",
        "state",
        "unreadCount",
        "unreadMessageLogIds",
    }
    EXPECT_REVIEW_PLAN_KEYS = {"nextReviewAt", "reason"}
    EXPECT_ACTIVE_CHAT_KEYS = {
        "activeEpoch",
        "decayHalfLifeSeconds",
        "enteredAt",
        "exists",
        "interestValue",
        "tickCount",
        "updatedAt",
    }
    EXPECT_MODEL_RUNTIME_KEYS = {
        "caller",
        "countAtLeast",
        "debugModelLog",
        "limit",
        "modelId",
        "promptSnapshotId",
        "providerId",
        "success",
    }
    EXPECT_DEBUG_LOG_KEYS = {
        "lineCountAtLeast",
        "requestContains",
        "requestEventType",
        "responseContains",
        "responseEventType",
        "timeout",
    }
    ELEMENT_KEYS = {"attrs", "children", "type"}

    def __init__(self, source: str) -> None:
        self.source = source

    def validate(self, scenario: Any) -> None:
        self._require_object(scenario, "$")
        self._check_keys(scenario, "$", self.ROOT_KEYS)
        self._require_string(scenario.get("name"), "$.name")
        if "adapter" in scenario:
            self._validate_adapter(scenario["adapter"], "$.adapter")
        if "agentEntryProbe" in scenario:
            self._validate_probe(scenario["agentEntryProbe"], "$.agentEntryProbe")
        if "agentSchedulerProbe" in scenario:
            self._validate_probe(scenario["agentSchedulerProbe"], "$.agentSchedulerProbe")
        if "config" in scenario:
            self._validate_config(scenario["config"], "$.config")
        if "sessions" in scenario:
            self._validate_session_inits(scenario["sessions"], "$.sessions")
        if "modelRuntime" in scenario:
            self._validate_model_runtime(scenario["modelRuntime"], "$.modelRuntime")
        if "commands" in scenario:
            self._validate_commands(scenario["commands"], "$.commands")
        if "eventBusHandlers" in scenario:
            self._validate_event_bus_handlers(
                scenario["eventBusHandlers"],
                "$.eventBusHandlers",
            )
        if "steps" in scenario:
            self._validate_steps(scenario["steps"], "$.steps")
        if "actions" in scenario:
            self._validate_actions(scenario["actions"], "$.actions")
        if "expect" in scenario:
            self._validate_expect(scenario["expect"], "$.expect")

    def _validate_adapter(self, value: Any, path: str) -> None:
        self._require_object(value, path)
        self._check_keys(value, path, self.ADAPTER_KEYS)
        self._require_optional_string_fields(value, path, self.ADAPTER_KEYS)

    def _validate_probe(self, value: Any, path: str) -> None:
        self._require_object(value, path)
        self._check_keys(value, path, self.PROBE_KEYS)
        self._require_optional_bool(value, path, "enabled")
        self._require_optional_number(value, path, "now")
        self._require_optional_string(
            value.get("responseProfile"),
            f"{path}.responseProfile",
            "responseProfile" in value,
        )

    def _validate_session_inits(self, value: Any, path: str) -> None:
        for index, item in enumerate(self._require_list(value, path)):
            item_path = f"{path}[{index}]"
            self._require_object(item, item_path)
            self._check_keys(item, item_path, self.SESSION_INIT_KEYS)
            self._require_string(item.get("id"), f"{item_path}.id")
            self._require_string(item.get("type"), f"{item_path}.type")
            self._require_optional_string_fields(
                item,
                item_path,
                self.SESSION_INIT_KEYS - {"config"},
            )
            if "config" in item:
                config_path = f"{item_path}.config"
                config = item["config"]
                self._require_object(config, config_path)
                self._check_keys(config, config_path, self.SESSION_CONFIG_KEYS)
                self._require_optional_bool(config, config_path, "isMuted")
                self._require_optional_string_list(config, config_path, "prefixes")

    def _validate_commands(self, value: Any, path: str) -> None:
        for index, item in enumerate(self._require_list(value, path)):
            item_path = f"{path}[{index}]"
            self._require_object(item, item_path)
            self._check_keys(item, item_path, self.COMMAND_KEYS)
            self._require_string(item.get("name"), f"{item_path}.name")
            if "kind" in item:
                self._require_enum(
                    item["kind"],
                    f"{item_path}.kind",
                    self.COMMAND_KINDS,
                )
            self._require_optional_string_fields(
                item,
                item_path,
                {"prompt", "reply", "replyAfterInput"},
            )
            self._require_optional_number(item, item_path, "timeout")
            if "api" in item:
                self._validate_command_api(item["api"], f"{item_path}.api")
            if "call" in item:
                self._validate_command_call(item["call"], f"{item_path}.call")

    def _validate_command_api(self, value: Any, path: str) -> None:
        self._require_object(value, path)
        self._check_keys(value, path, self.COMMAND_API_KEYS)
        self._require_optional_string(
            value.get("method"),
            f"{path}.method",
            "method" in value,
        )
        if "params" in value:
            self._require_object(value["params"], f"{path}.params")

    def _validate_command_call(self, value: Any, path: str) -> None:
        self._require_object(value, path)
        self._check_keys(value, path, self.COMMAND_CALL_KEYS)
        self._require_optional_string_fields(
            value,
            path,
            {"caller", "modelId", "prompt", "purpose", "routeId"},
        )
        if "metadata" in value:
            self._require_object(value["metadata"], f"{path}.metadata")
        if "params" in value:
            self._require_object(value["params"], f"{path}.params")

    def _validate_event_bus_handlers(self, value: Any, path: str) -> None:
        for index, item in enumerate(self._require_list(value, path)):
            item_path = f"{path}[{index}]"
            self._require_object(item, item_path)
            self._check_keys(item, item_path, self.EVENT_BUS_HANDLER_KEYS)
            self._require_string(item.get("eventType"), f"{item_path}.eventType")

    def _validate_steps(self, value: Any, path: str) -> None:
        for index, item in enumerate(self._require_list(value, path)):
            item_path = f"{path}[{index}]"
            self._require_object(item, item_path)
            self._check_keys(item, item_path, self.STEP_KEYS)
            if "type" in item:
                self._require_enum(item["type"], f"{item_path}.type", self.STEP_TYPES)
            self._require_optional_string_fields(
                item,
                item_path,
                {"content", "eventType", "id", "noticeType"},
            )
            self._require_optional_number(item, item_path, "timestamp")
            self._require_optional_int(item, item_path, "expectSentCount")
            if "session" in item:
                self._validate_step_session(item["session"], f"{item_path}.session")
            if "sender" in item:
                self._validate_user(item["sender"], f"{item_path}.sender")
            if "operator" in item:
                self._validate_user(item["operator"], f"{item_path}.operator")
            if "member" in item:
                self._validate_member(item["member"], f"{item_path}.member")
            if "elements" in item:
                self._validate_elements(item["elements"], f"{item_path}.elements")
            if "expect" in item:
                self._validate_expect(item["expect"], f"{item_path}.expect")

    def _validate_step_session(self, value: Any, path: str) -> None:
        self._require_object(value, path)
        self._check_keys(value, path, self.STEP_SESSION_KEYS)
        if "type" in value:
            self._require_enum(value["type"], f"{path}.type", self.STEP_SESSION_TYPES)
        self._require_optional_string_fields(
            value,
            path,
            self.STEP_SESSION_KEYS - {"type"},
        )

    def _validate_user(self, value: Any, path: str) -> None:
        self._require_object(value, path)
        self._check_keys(value, path, self.STEP_USER_KEYS)
        self._require_optional_string_fields(value, path, self.STEP_USER_KEYS)

    def _validate_member(self, value: Any, path: str) -> None:
        self._require_object(value, path)
        self._check_keys(value, path, self.STEP_MEMBER_KEYS)
        self._require_optional_string(value.get("nick"), f"{path}.nick", "nick" in value)
        if "user" in value:
            self._validate_user(value["user"], f"{path}.user")

    def _validate_actions(self, value: Any, path: str) -> None:
        for index, item in enumerate(self._require_list(value, path)):
            item_path = f"{path}[{index}]"
            self._require_object(item, item_path)
            self._check_keys(item, item_path, self.ACTION_KEYS)
            self._require_enum(item.get("type"), f"{item_path}.type", self.ACTION_TYPES)
            self._require_string(item.get("sessionId"), f"{item_path}.sessionId")
            self._require_optional_bool(item, item_path, "enterActiveChat")
            self._require_optional_number(item, item_path, "now")
            self._require_optional_number(item, item_path, "activeChatInitialInterest")
            self._require_optional_number(
                item,
                item_path,
                "activeChatDecayHalfLifeSeconds",
            )

    def _validate_model_runtime(self, value: Any, path: str) -> None:
        self._require_object(value, path)
        self._check_keys(value, path, self.MODEL_RUNTIME_KEYS)
        self._require_optional_bool(value, path, "debugPlugin")
        if "fakeCompletion" in value:
            fake_path = f"{path}.fakeCompletion"
            fake = value["fakeCompletion"]
            self._require_object(fake, fake_path)
            self._check_keys(fake, fake_path, self.FAKE_COMPLETION_KEYS)
            self._require_optional_string(
                fake.get("text"),
                f"{fake_path}.text",
                "text" in fake,
            )
            for field in self.FAKE_COMPLETION_KEYS - {"text"}:
                self._require_optional_int(fake, fake_path, field)
        if "providers" in value:
            for index, provider in enumerate(
                self._require_list(value["providers"], f"{path}.providers")
            ):
                provider_path = f"{path}.providers[{index}]"
                self._require_object(provider, provider_path)
                self._check_keys(provider, provider_path, self.MODEL_PROVIDER_KEYS)
                self._require_string(provider.get("id"), f"{provider_path}.id")
                self._require_optional_string_fields(
                    provider,
                    provider_path,
                    {"displayName", "type"},
                )
                self._require_optional_bool(provider, provider_path, "enabled")
                for field in ("auth", "defaultParams"):
                    if field in provider:
                        self._require_object(provider[field], f"{provider_path}.{field}")
        if "models" in value:
            for index, model in enumerate(
                self._require_list(value["models"], f"{path}.models")
            ):
                model_path = f"{path}.models[{index}]"
                self._require_object(model, model_path)
                self._check_keys(model, model_path, self.MODEL_DEFINITION_KEYS)
                self._require_string(model.get("id"), f"{model_path}.id")
                self._require_string(model.get("providerId"), f"{model_path}.providerId")
                self._require_optional_string_fields(
                    model,
                    model_path,
                    {"displayName", "litellmModel"},
                )
                self._require_optional_bool(model, model_path, "enabled")
                self._require_optional_string_list(model, model_path, "capabilities")
                for field in ("costMetadata", "defaultParams"):
                    if field in model:
                        self._require_object(model[field], f"{model_path}.{field}")

    def _validate_config(self, value: Any, path: str) -> None:
        self._require_object(value, path)
        self._check_keys(value, path, self.CONFIG_KEYS)
        if "adapter_instances" in value:
            for index, item in enumerate(
                self._require_list(value["adapter_instances"], f"{path}.adapter_instances")
            ):
                item_path = f"{path}.adapter_instances[{index}]"
                self._require_object(item, item_path)
                self._check_keys(item, item_path, self.CONFIG_ADAPTER_KEYS)
                self._require_string(item.get("id"), f"{item_path}.id")
                self._require_string(item.get("platform"), f"{item_path}.platform")
        if "plugins" in value:
            for index, item in enumerate(
                self._require_list(value["plugins"], f"{path}.plugins")
            ):
                item_path = f"{path}.plugins[{index}]"
                self._require_object(item, item_path)
                self._check_keys(item, item_path, self.CONFIG_PLUGIN_KEYS)
                self._require_string(item.get("id"), f"{item_path}.id")
        if "bots" in value:
            for index, item in enumerate(self._require_list(value["bots"], f"{path}.bots")):
                self._validate_config_bot(item, f"{path}.bots[{index}]")

    def _validate_config_bot(self, value: Any, path: str) -> None:
        self._require_object(value, path)
        self._check_keys(value, path, self.CONFIG_BOT_KEYS)
        self._require_string(value.get("id"), f"{path}.id")
        self._require_optional_string(
            value.get("display_name"),
            f"{path}.display_name",
            "display_name" in value,
        )
        if "commands" in value:
            commands_path = f"{path}.commands"
            commands = value["commands"]
            self._require_object(commands, commands_path)
            self._check_keys(commands, commands_path, self.CONFIG_BOT_COMMANDS_KEYS)
            self._require_optional_bool(commands, commands_path, "enabled")
            self._require_optional_string_list(commands, commands_path, "prefixes")
        if "plugins" in value:
            plugins_path = f"{path}.plugins"
            plugins = value["plugins"]
            self._require_object(plugins, plugins_path)
            self._check_keys(plugins, plugins_path, self.CONFIG_BOT_PLUGINS_KEYS)
            self._require_optional_bool(plugins, plugins_path, "enabled")
            self._require_optional_string_list(plugins, plugins_path, "enabled_plugins")
        if "agent" in value:
            agent_path = f"{path}.agent"
            agent = value["agent"]
            self._require_object(agent, agent_path)
            self._check_keys(agent, agent_path, self.CONFIG_BOT_AGENT_KEYS)
            self._require_optional_string_fields(agent, agent_path, self.CONFIG_BOT_AGENT_KEYS)
        if "bindings" in value:
            for index, binding in enumerate(
                self._require_list(value["bindings"], f"{path}.bindings")
            ):
                binding_path = f"{path}.bindings[{index}]"
                self._require_object(binding, binding_path)
                self._check_keys(binding, binding_path, self.CONFIG_BOT_BINDING_KEYS)
                self._require_string(binding.get("id"), f"{binding_path}.id")
                self._require_string(
                    binding.get("adapter_instance_id"),
                    f"{binding_path}.adapter_instance_id",
                )
                self._require_optional_int(binding, binding_path, "priority")
                self._require_optional_string_list(
                    binding,
                    binding_path,
                    "session_patterns",
                )

    def _validate_expect(self, value: Any, path: str) -> None:
        self._require_object(value, path)
        self._check_keys(value, path, self.EXPECT_KEYS)
        if "sent" in value:
            for index, item in enumerate(self._require_list(value["sent"], f"{path}.sent")):
                self._validate_expect_sent(item, f"{path}.sent[{index}]")
        if "sessions" in value:
            for index, item in enumerate(
                self._require_list(value["sessions"], f"{path}.sessions")
            ):
                item_path = f"{path}.sessions[{index}]"
                self._require_object(item, item_path)
                self._check_keys(item, item_path, self.EXPECT_SESSION_KEYS)
                self._require_string(item.get("id"), f"{item_path}.id")
                self._require_optional_string_fields(
                    item,
                    item_path,
                    self.EXPECT_SESSION_KEYS - {"id"},
                )
        if "messageLogs" in value:
            self._validate_message_logs(value["messageLogs"], f"{path}.messageLogs")
        if "messageLogsBySession" in value:
            for index, item in enumerate(
                self._require_list(value["messageLogsBySession"], f"{path}.messageLogsBySession")
            ):
                self._validate_message_logs(item, f"{path}.messageLogsBySession[{index}]")
        if "noticeEvents" in value:
            self._require_string_list(value["noticeEvents"], f"{path}.noticeEvents")
        if "apiCalls" in value:
            for index, item in enumerate(
                self._require_list(value["apiCalls"], f"{path}.apiCalls")
            ):
                item_path = f"{path}.apiCalls[{index}]"
                self._require_object(item, item_path)
                self._check_keys(item, item_path, self.EXPECT_API_CALL_KEYS)
                self._require_optional_string(
                    item.get("method"),
                    f"{item_path}.method",
                    "method" in item,
                )
                for field in ("params", "paramsContains"):
                    if field in item:
                        self._require_object(item[field], f"{item_path}.{field}")
        if "agentEntrySignals" in value:
            for index, item in enumerate(
                self._require_list(value["agentEntrySignals"], f"{path}.agentEntrySignals")
            ):
                self._validate_agent_entry_expect(item, f"{path}.agentEntrySignals[{index}]")
        if "agentScheduler" in value:
            self._validate_agent_scheduler_expect(value["agentScheduler"], f"{path}.agentScheduler")
        if "modelRuntime" in value:
            self._validate_model_runtime_expect(value["modelRuntime"], f"{path}.modelRuntime")

    def _validate_expect_sent(self, value: Any, path: str) -> None:
        self._require_object(value, path)
        self._check_keys(value, path, self.EXPECT_SENT_KEYS)
        self._require_string(value.get("sessionId"), f"{path}.sessionId")
        self._require_optional_string_fields(
            value,
            path,
            {"messageId", "messageIdStartsWith", "text", "textContains"},
        )
        if "elements" in value:
            self._validate_elements(value["elements"], f"{path}.elements")

    def _validate_message_logs(self, value: Any, path: str) -> None:
        self._require_object(value, path)
        self._check_keys(value, path, self.EXPECT_MESSAGE_LOG_KEYS)
        self._require_string(value.get("sessionId"), f"{path}.sessionId")
        for field in ("countAtLeast", "countExact", "limit"):
            self._require_optional_int(value, path, field)
        self._require_optional_string(
            value.get("incomingRoutingStatus"),
            f"{path}.incomingRoutingStatus",
            "incomingRoutingStatus" in value,
        )
        for field in (
            "ids",
            "platformMsgIds",
            "rawTextContains",
            "rawTexts",
            "roles",
            "routingSkipReasons",
            "routingStatuses",
            "senderIds",
            "senderNames",
        ):
            if field in value:
                self._require_nullable_string_list(value[field], f"{path}.{field}")
        for field in ("isMentioned", "isRead"):
            if field in value:
                self._require_bool_list(value[field], f"{path}.{field}")
        if "contentElements" in value:
            self._validate_content_element_rows(
                value["contentElements"],
                f"{path}.contentElements",
            )

    def _validate_agent_entry_expect(self, value: Any, path: str) -> None:
        self._require_object(value, path)
        self._check_keys(value, path, self.EXPECT_AGENT_ENTRY_KEYS)
        self._require_optional_string_fields(
            value,
            path,
            {
                "botBindingId",
                "botId",
                "botSessionId",
                "eventType",
                "instanceId",
                "platform",
                "senderId",
                "sessionId",
            },
        )
        for field in (
            "alreadyHandled",
            "isMentionToOther",
            "isMentioned",
            "isPokeToBot",
            "isPokeToOther",
            "isPrivate",
            "isReplyToBot",
            "isStopped",
            "messageLogId",
        ):
            self._require_optional_bool(value, path, field)

    def _validate_agent_scheduler_expect(self, value: Any, path: str) -> None:
        self._require_object(value, path)
        self._check_keys(value, path, self.EXPECT_AGENT_SCHEDULER_KEYS)
        self._require_string(value.get("sessionId"), f"{path}.sessionId")
        self._require_optional_string(value.get("state"), f"{path}.state", "state" in value)
        self._require_optional_int(value, path, "unreadCount")
        self._require_optional_int_list(value, path, "unreadMessageLogIds")
        self._require_optional_string_list(value, path, "knownSessionIds")
        if "reviewPlan" in value:
            review_path = f"{path}.reviewPlan"
            review_plan = value["reviewPlan"]
            self._require_object(review_plan, review_path)
            self._check_keys(review_plan, review_path, self.EXPECT_REVIEW_PLAN_KEYS)
            self._require_optional_string(
                review_plan.get("reason"),
                f"{review_path}.reason",
                "reason" in review_plan,
            )
            self._require_optional_number(review_plan, review_path, "nextReviewAt")
        if "activeChatState" in value:
            active_path = f"{path}.activeChatState"
            active = value["activeChatState"]
            self._require_object(active, active_path)
            self._check_keys(active, active_path, self.EXPECT_ACTIVE_CHAT_KEYS)
            self._require_optional_bool(active, active_path, "exists")
            self._require_optional_int(active, active_path, "activeEpoch")
            self._require_optional_int(active, active_path, "tickCount")
            for field in (
                "decayHalfLifeSeconds",
                "enteredAt",
                "interestValue",
                "updatedAt",
            ):
                self._require_optional_number(active, active_path, field)

    def _validate_model_runtime_expect(self, value: Any, path: str) -> None:
        self._require_object(value, path)
        self._check_keys(value, path, self.EXPECT_MODEL_RUNTIME_KEYS)
        self._require_optional_string_fields(
            value,
            path,
            {"caller", "modelId", "promptSnapshotId", "providerId"},
        )
        self._require_optional_bool(value, path, "success")
        for field in ("countAtLeast", "limit"):
            self._require_optional_int(value, path, field)
        if "debugModelLog" in value:
            debug_path = f"{path}.debugModelLog"
            debug_log = value["debugModelLog"]
            self._require_object(debug_log, debug_path)
            self._check_keys(debug_log, debug_path, self.EXPECT_DEBUG_LOG_KEYS)
            self._require_optional_int(debug_log, debug_path, "lineCountAtLeast")
            self._require_optional_number(debug_log, debug_path, "timeout")
            self._require_optional_string_fields(
                debug_log,
                debug_path,
                {"requestEventType", "responseEventType"},
            )
            for field in ("requestContains", "responseContains"):
                if field in debug_log:
                    self._require_object(debug_log[field], f"{debug_path}.{field}")

    def _validate_elements(self, value: Any, path: str) -> None:
        elements = self._require_list(value, path)
        for index, item in enumerate(elements):
            item_path = f"{path}[{index}]"
            self._require_object(item, item_path)
            self._check_keys(item, item_path, self.ELEMENT_KEYS)
            self._require_string(item.get("type"), f"{item_path}.type")
            MessageElement.model_validate(item)

    def _validate_content_element_rows(self, value: Any, path: str) -> None:
        rows = self._require_list(value, path)
        for index, row in enumerate(rows):
            self._validate_elements(row, f"{path}[{index}]")

    def _check_keys(self, value: dict[str, Any], path: str, allowed: set[str]) -> None:
        unknown = sorted(set(value) - allowed)
        if unknown:
            self._fail(f"{path} has unsupported key(s): {', '.join(unknown)}")

    def _require_object(self, value: Any, path: str) -> dict[str, Any]:
        if not isinstance(value, dict):
            self._fail(f"{path} must be an object")
        return value

    def _require_list(self, value: Any, path: str) -> list[Any]:
        if not isinstance(value, list):
            self._fail(f"{path} must be a list")
        return value

    def _require_string(self, value: Any, path: str) -> None:
        if not isinstance(value, str) or value == "":
            self._fail(f"{path} must be a non-empty string")

    def _require_optional_string(self, value: Any, path: str, present: bool) -> None:
        if present and not isinstance(value, str):
            self._fail(f"{path} must be a string")

    def _require_optional_string_fields(
        self,
        value: dict[str, Any],
        path: str,
        fields: set[str],
    ) -> None:
        for field in fields:
            self._require_optional_string(value.get(field), f"{path}.{field}", field in value)

    def _require_string_list(self, value: Any, path: str) -> None:
        for index, item in enumerate(self._require_list(value, path)):
            if not isinstance(item, str):
                self._fail(f"{path}[{index}] must be a string")

    def _require_nullable_string_list(self, value: Any, path: str) -> None:
        for index, item in enumerate(self._require_list(value, path)):
            if item is not None and not isinstance(item, str):
                self._fail(f"{path}[{index}] must be a string or null")

    def _require_optional_string_list(
        self,
        value: dict[str, Any],
        path: str,
        field: str,
    ) -> None:
        if field in value:
            self._require_string_list(value[field], f"{path}.{field}")

    def _require_bool_list(self, value: Any, path: str) -> None:
        for index, item in enumerate(self._require_list(value, path)):
            if not isinstance(item, bool):
                self._fail(f"{path}[{index}] must be a boolean")

    def _require_optional_bool(self, value: dict[str, Any], path: str, field: str) -> None:
        if field in value and not isinstance(value[field], bool):
            self._fail(f"{path}.{field} must be a boolean")

    def _require_optional_number(self, value: dict[str, Any], path: str, field: str) -> None:
        if field in value and not self._is_number(value[field]):
            self._fail(f"{path}.{field} must be a number")

    def _require_optional_int(self, value: dict[str, Any], path: str, field: str) -> None:
        if field in value and not self._is_int(value[field]):
            self._fail(f"{path}.{field} must be an integer")

    def _require_optional_int_list(
        self,
        value: dict[str, Any],
        path: str,
        field: str,
    ) -> None:
        if field not in value:
            return
        for index, item in enumerate(self._require_list(value[field], f"{path}.{field}")):
            if not self._is_int(item):
                self._fail(f"{path}.{field}[{index}] must be an integer")

    def _require_enum(self, value: Any, path: str, choices: set[str]) -> None:
        if not isinstance(value, str) or value not in choices:
            choices_text = ", ".join(sorted(choices))
            self._fail(f"{path} must be one of: {choices_text}")

    def _is_number(self, value: Any) -> bool:
        return isinstance(value, int | float) and not isinstance(value, bool)

    def _is_int(self, value: Any) -> bool:
        return isinstance(value, int) and not isinstance(value, bool)

    def _fail(self, message: str) -> None:
        raise ScenarioValidationError(f"{self.source}: {message}")
