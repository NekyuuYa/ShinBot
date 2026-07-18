from __future__ import annotations

from dataclasses import replace
from enum import IntEnum, StrEnum

import pytest

from shinbot.agent.runtime.session_actor.effect_contracts import (
    ACTOR_V2_CLEAN_SESSION_EFFECT_REFS,
    ACTOR_V2_HISTORICAL_UNBOUND_EFFECT_REFS,
    EffectContractAuthority,
    EffectContractAuthorityError,
    EffectDeclarationValidationError,
    EffectExecutionContract,
    EffectLane,
    builtin_clean_session_actor_v2_effect_contracts,
    builtin_effect_contract_authority,
    validate_effect_declaration,
)
from shinbot.agent.runtime.session_actor.events import SessionEffect
from shinbot.agent.runtime.session_actor.json_validation import (
    MAX_DURABLE_JSON_BYTES,
    MAX_DURABLE_JSON_DEPTH,
    MAX_DURABLE_JSON_NODES,
)

_LEGACY_CONTRACT = EffectExecutionContract(
    effect_kind="test_effect",
    version=1,
    lane=EffectLane.DEFAULT,
    completion_event_kind="TestEffectCompleted",
)
_CURRENT_CONTRACT = EffectExecutionContract(
    effect_kind="test_effect",
    version=2,
    lane=EffectLane.DEFAULT,
    completion_event_kind="TestEffectCompleted",
    outcome_fence_fields=("input_watermark", "plan_id"),
)
_AUTHORITY = EffectContractAuthority((_LEGACY_CONTRACT, _CURRENT_CONTRACT))


def test_clean_session_contract_classification_is_explicit_and_exhaustive() -> None:
    """New authority contracts cannot silently become canary-executable."""

    authority_refs = {contract.ref for contract in builtin_effect_contract_authority().contracts()}
    clean_refs = {contract.ref for contract in builtin_clean_session_actor_v2_effect_contracts()}

    assert clean_refs == ACTOR_V2_CLEAN_SESSION_EFFECT_REFS
    assert not (ACTOR_V2_CLEAN_SESSION_EFFECT_REFS & ACTOR_V2_HISTORICAL_UNBOUND_EFFECT_REFS)
    assert authority_refs == (
        ACTOR_V2_CLEAN_SESSION_EFFECT_REFS | ACTOR_V2_HISTORICAL_UNBOUND_EFFECT_REFS
    )


class _ContractText(StrEnum):
    EFFECT_KIND = " test_effect "
    EVENT_KIND = " TestEffectCompleted "
    SOURCE = " effect_executor "
    INPUT_WATERMARK = " input_watermark "
    PLAN_ID = " plan_id "


class _ContractInteger(IntEnum):
    VERSION = 2
    MAX_ATTEMPTS = 5
    PRIORITY = 100
    TIMEOUT_SECONDS = 30
    RETRY_BASE_SECONDS = 1
    RETRY_MAX_SECONDS = 60


class _OtherLane(StrEnum):
    DEFAULT = "default"


class _Stringable:
    def __str__(self) -> str:
        return "test_effect"


def _effect(
    contract: EffectExecutionContract = _CURRENT_CONTRACT,
) -> SessionEffect:
    return SessionEffect(
        effect_id="effect-a",
        kind=contract.effect_kind,
        contract_version=contract.version,
        contract_signature=contract.signature,
        payload={"input_watermark": 10, "plan_id": "plan-a"},
    )


def test_contract_canonicalizes_strenum_intenum_and_numeric_policy() -> None:
    contract = EffectExecutionContract(
        effect_kind=_ContractText.EFFECT_KIND,
        version=_ContractInteger.VERSION,
        lane=EffectLane.DEFAULT,
        completion_event_kind=_ContractText.EVENT_KIND,
        completion_source=_ContractText.SOURCE,
        timeout_seconds=_ContractInteger.TIMEOUT_SECONDS,
        max_attempts=_ContractInteger.MAX_ATTEMPTS,
        retry_base_seconds=_ContractInteger.RETRY_BASE_SECONDS,
        retry_max_seconds=_ContractInteger.RETRY_MAX_SECONDS,
        priority=_ContractInteger.PRIORITY,
        outcome_fence_fields=(
            _ContractText.PLAN_ID,
            _ContractText.INPUT_WATERMARK,
            "plan_id",
        ),
    )

    assert contract.effect_kind == "test_effect"
    assert type(contract.effect_kind) is str
    assert contract.completion_event_kind == "TestEffectCompleted"
    assert type(contract.completion_event_kind) is str
    assert contract.completion_source == "effect_executor"
    assert type(contract.completion_source) is str
    assert contract.version == 2
    assert type(contract.version) is int
    assert contract.max_attempts == 5
    assert type(contract.max_attempts) is int
    assert contract.priority == 100
    assert type(contract.priority) is int
    assert contract.timeout_seconds == 30.0
    assert type(contract.timeout_seconds) is float
    assert contract.retry_base_seconds == 1.0
    assert type(contract.retry_base_seconds) is float
    assert contract.retry_max_seconds == 60.0
    assert type(contract.retry_max_seconds) is float
    assert contract.outcome_fence_fields == ("input_watermark", "plan_id")
    assert all(type(field_name) is str for field_name in contract.outcome_fence_fields)
    assert type(contract.ref[0]) is str
    assert type(contract.ref[1]) is int


def test_canonicalized_contract_signature_matches_primitive_contract() -> None:
    enum_contract = EffectExecutionContract(
        effect_kind=_ContractText.EFFECT_KIND,
        version=_ContractInteger.VERSION,
        lane=EffectLane.DEFAULT,
        completion_event_kind=_ContractText.EVENT_KIND,
        completion_source=_ContractText.SOURCE,
        timeout_seconds=_ContractInteger.TIMEOUT_SECONDS,
        max_attempts=_ContractInteger.MAX_ATTEMPTS,
        retry_base_seconds=_ContractInteger.RETRY_BASE_SECONDS,
        retry_max_seconds=_ContractInteger.RETRY_MAX_SECONDS,
        priority=_ContractInteger.PRIORITY,
        outcome_fence_fields=(
            _ContractText.PLAN_ID,
            _ContractText.INPUT_WATERMARK,
        ),
    )
    primitive_contract = EffectExecutionContract(
        effect_kind="test_effect",
        version=2,
        lane=EffectLane.DEFAULT,
        completion_event_kind="TestEffectCompleted",
        completion_source="effect_executor",
        timeout_seconds=30.0,
        max_attempts=5,
        retry_base_seconds=1.0,
        retry_max_seconds=60.0,
        priority=100,
        outcome_fence_fields=("input_watermark", "plan_id"),
    )

    assert enum_contract == primitive_contract
    assert enum_contract.signature == primitive_contract.signature


@pytest.mark.parametrize(
    ("field_name", "invalid_value"),
    (
        ("effect_kind", 1),
        ("effect_kind", True),
        ("effect_kind", b"test_effect"),
        ("effect_kind", _Stringable()),
        ("completion_event_kind", 1),
        ("completion_event_kind", _Stringable()),
        ("completion_source", b"effect_executor"),
        ("completion_source", _Stringable()),
    ),
)
def test_contract_rejects_coercive_text_fields(
    field_name: str,
    invalid_value: object,
) -> None:
    values: dict[str, object] = {
        "effect_kind": "test_effect",
        "version": 1,
        "lane": EffectLane.DEFAULT,
        "completion_event_kind": "TestEffectCompleted",
        "completion_source": "effect_executor",
    }
    values[field_name] = invalid_value

    with pytest.raises(TypeError):
        EffectExecutionContract(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize("field_name", ("version", "max_attempts", "priority"))
@pytest.mark.parametrize("invalid_value", (True, 1.0, b"1", _Stringable()))
def test_contract_rejects_coercive_integer_policy(
    field_name: str,
    invalid_value: object,
) -> None:
    values: dict[str, object] = {
        "effect_kind": "test_effect",
        "version": 1,
        "lane": EffectLane.DEFAULT,
        "completion_event_kind": "TestEffectCompleted",
    }
    values[field_name] = invalid_value

    with pytest.raises(TypeError):
        EffectExecutionContract(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "lane",
    ("default", _OtherLane.DEFAULT, b"default", _Stringable()),
)
def test_contract_requires_effect_lane(lane: object) -> None:
    with pytest.raises(TypeError):
        EffectExecutionContract(
            effect_kind="test_effect",
            version=1,
            lane=lane,  # type: ignore[arg-type]
            completion_event_kind="TestEffectCompleted",
        )


@pytest.mark.parametrize(
    "invalid_fields",
    (
        ["plan_id"],
        "plan_id",
        (1,),
        (True,),
        (b"plan_id",),
        (_Stringable(),),
    ),
)
def test_contract_requires_exact_string_fence_tuple(invalid_fields: object) -> None:
    with pytest.raises(TypeError):
        EffectExecutionContract(
            effect_kind="test_effect",
            version=1,
            lane=EffectLane.DEFAULT,
            completion_event_kind="TestEffectCompleted",
            outcome_fence_fields=invalid_fields,  # type: ignore[arg-type]
        )


@pytest.mark.parametrize(
    ("field_name", "invalid_value"),
    (
        ("timeout_seconds", True),
        ("retry_base_seconds", True),
        ("retry_max_seconds", True),
        ("timeout_seconds", "30"),
        ("retry_base_seconds", _Stringable()),
        ("retry_max_seconds", b"60"),
    ),
)
def test_contract_rejects_coercive_numeric_policy(
    field_name: str,
    invalid_value: object,
) -> None:
    values: dict[str, object] = {
        "effect_kind": "test_effect",
        "version": 1,
        "lane": EffectLane.DEFAULT,
        "completion_event_kind": "TestEffectCompleted",
    }
    values[field_name] = invalid_value

    with pytest.raises(TypeError):
        EffectExecutionContract(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("field_name", "invalid_value"),
    (
        ("timeout_seconds", 0.0),
        ("timeout_seconds", float("nan")),
        ("timeout_seconds", float("inf")),
        ("retry_base_seconds", -0.1),
        ("retry_base_seconds", float("nan")),
        ("retry_max_seconds", float("inf")),
    ),
)
def test_contract_rejects_out_of_range_numeric_policy(
    field_name: str,
    invalid_value: float,
) -> None:
    values: dict[str, object] = {
        "effect_kind": "test_effect",
        "version": 1,
        "lane": EffectLane.DEFAULT,
        "completion_event_kind": "TestEffectCompleted",
    }
    values[field_name] = invalid_value

    with pytest.raises(ValueError):
        EffectExecutionContract(**values)  # type: ignore[arg-type]


def test_authority_uses_canonical_identity_without_numeric_key_aliases() -> None:
    contract = EffectExecutionContract(
        effect_kind=_ContractText.EFFECT_KIND,
        version=_ContractInteger.VERSION,
        lane=EffectLane.DEFAULT,
        completion_event_kind=_ContractText.EVENT_KIND,
    )
    authority = EffectContractAuthority((contract,))

    assert (
        authority.resolve(
            effect_kind=_ContractText.EFFECT_KIND,
            version=_ContractInteger.VERSION,
            signature=contract.signature,
        )
        is contract
    )
    for invalid_version in (True, 2.0):
        with pytest.raises(EffectContractAuthorityError):
            authority.resolve(
                effect_kind="test_effect",
                version=invalid_version,  # type: ignore[arg-type]
                signature=contract.signature,
            )
    with pytest.raises(EffectContractAuthorityError):
        authority.resolve(
            effect_kind=_Stringable(),  # type: ignore[arg-type]
            version=2,
            signature=contract.signature,
        )


def test_exact_v1_declaration_remains_valid_without_retroactive_fences() -> None:
    effect = replace(
        _effect(_LEGACY_CONTRACT),
        payload={},
    )

    assert validate_effect_declaration(effect, authority=_AUTHORITY) is _LEGACY_CONTRACT


def test_exact_v2_declaration_requires_and_validates_declared_fences() -> None:
    assert validate_effect_declaration(_effect(), authority=_AUTHORITY) is _CURRENT_CONTRACT


@pytest.mark.parametrize(
    "effect",
    (
        replace(_effect(), contract_version=99),
        replace(_effect(), contract_signature="0" * 64),
        replace(_effect(), payload={"input_watermark": 10}),
        replace(
            _effect(),
            payload={"input_watermark": 10.9, "plan_id": "plan-a"},
        ),
        replace(
            _effect(),
            payload={"input_watermark": True, "plan_id": "plan-a"},
        ),
        replace(
            _effect(),
            payload={"input_watermark": 10, "plan_id": ["plan-a"]},
        ),
        replace(
            _effect(),
            payload={"input_watermark": 10, "plan_id": "plan-a", 1: "bad-key"},
        ),
    ),
)
def test_invalid_effect_declaration_fails_closed(effect: SessionEffect) -> None:
    with pytest.raises(EffectDeclarationValidationError):
        validate_effect_declaration(effect, authority=_AUTHORITY)


@pytest.mark.parametrize("invalid_shape", ("depth", "nodes", "size", "utf8"))
def test_effect_declaration_json_validation_is_bounded(
    invalid_shape: str,
) -> None:
    payload = dict(_effect().payload)
    if invalid_shape == "depth":
        nested: object = None
        for _ in range(MAX_DURABLE_JSON_DEPTH + 1):
            nested = {"child": nested}
        payload["nested"] = nested
    elif invalid_shape == "nodes":
        payload["nodes"] = [None] * MAX_DURABLE_JSON_NODES
    elif invalid_shape == "size":
        payload["large"] = "x" * MAX_DURABLE_JSON_BYTES
    else:
        payload["text"] = "\ud800"

    with pytest.raises(EffectDeclarationValidationError):
        validate_effect_declaration(
            replace(_effect(), payload=payload),
            authority=_AUTHORITY,
        )
