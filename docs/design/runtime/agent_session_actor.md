# Agent Session Actor Architecture

Status: target architecture; Actor v2 foundation implemented but deliberately
inactive for production message ingress

## Current Rollout Boundary

This document is both the target contract and a record of the implemented
Actor v2 slice.  It must not be read as claiming that production traffic already
uses the actor.

Today, the legacy scheduler/coordinator runtime remains the only writer for
real Agent ingress.  The Actor v2 reducer, SQLite stores, message ledger,
durable effect executor, versioned contracts, workflow adapters, registry, and
activation harness exist and are covered by focused tests.  They are not yet
constructed and activated by `AgentRuntime` as the live ingress owner.  In
particular, a deployment must not flip a session to `actor_v2` ownership or
route live messages, timers, workflow completions, or management commands to
the v2 mailbox until the activation gate at the end of this document is met.

Core durable routing has conditional support for an active Actor v2 owner, but
that is plumbing for the future activation rather than a second live runtime.
The normal runtime does not currently publish an active actor registry/harness
as its ingress wake target.  This deliberate boundary prevents legacy and v2
from becoming concurrent writers while the remaining end-to-end adapters are
finished.

The Actor-native Active Chat v3 workflows described below are likewise
diagnostic-only assembly components. They do not replace or take ownership from
the legacy `ActiveChatCoordinator`/`ActiveChatFastRunner` path. Production
`actor_v2` ownership remains disabled because state-specific recovery
materializers for non-idle Active Chat state and the full `AgentRuntime`
production composition (registry, executor, ingress wake, and ownership
wiring) are not complete. The v3 handlers are testable in isolation, but are
not a production wake target.

The invariant and workflow sections below therefore use two meanings:

- **Target contract** describes the required behavior after activation.
- **Implemented slice** describes reducer/outbox behavior that is already
  durable and testable in isolation, but is not yet reachable from production
  ingress.

The current reducer dispatches only its implemented event kinds.
`ManualReviewRequested` is presently a reserved `AgentSessionEventKind` value
rather than a reducer branch, and pause/force-idle events described by the
target contract have not yet been added to that enum.  They are activation
work, not a production capability.

`SQLiteSessionActorStore` already creates legacy heuristic `RecoveryRequested`
mailbox events for orphaned non-idle aggregates. Those rows use source
`session_actor_recovery`; they do not contain a recovery certificate and cannot
authorize the target protocol. An inactive `SQLiteRecoveryGraphScanner` now
exists for bounded certificate discovery, typed case/delivery insertion, and
raw-corruption findings. Its `SQLiteRecoveryGraphReader` is a separate,
transaction-bound authority port used by `SQLiteRecoveryCommitCoordinator`.
`RecoveryRequested` is an `AgentSessionEventKind`: the legacy source produces
only an explicit ignored no-op, while the typed scanner source produces a
compact `RecoveryCommitIntent` that the coordinator re-proves inside the store
transaction. None of the scanner, coordinator, or state-specific materializer
registry is constructed by the production registry, scheduled at startup, or
exposed through `AgentRuntime`. Durable recovery is therefore still an explicit
activation blocker, not a live v2 feature.

## Problem

The Agent runtime currently distributes mutable session state across the
scheduler, workflow dispatcher, review coordinator, active-chat coordinator,
timers, and management API. Some paths are serialized by an in-memory lock,
while background workflow callbacks mutate the scheduler outside that lock.
Long-running model calls may also execute while the lock is held.

This produces several invalid states:

- a workflow completion can commit after the state that launched it is stale;
- the same `ACTIVE_CHAT -> IDLE` transition can use different review planning
  behavior depending on which caller initiated it;
- persisted scheduler state can disagree with in-memory coordinator state;
- a process crash can commit a state transition without its review plan;
- a completed model call cannot be traced to an accepted, defaulted,
  superseded, or rejected schedule decision.

The fix is a single-writer, durable per-session runtime. This document defines
the target contract. Compatibility adapters may exist during migration, but no
new Actor v2 state-mutation path may bypass this contract.  Until activation,
the legacy runtime intentionally continues to own its existing mutation paths.

## Invariants

After Actor v2 activation for a session:

1. A session has exactly one logical writer: its `AgentSessionActor`.
2. The actor key is `(profile_id, session_id)`, not `session_id` alone.
3. Every external signal, timer, workflow completion, and management command is
   persisted as a mailbox event before it can affect session state.
4. Duplicate `event_id` values are idempotent.
5. Event handling does not await model, network, adapter, or tool I/O.
6. Event handling produces a transition and zero or more durable effects.
7. The aggregate, inbox disposition, operation changes, transition journal,
   review schedule record, and outbox effects commit in one SQLite transaction.
8. Effects execute only after their creating transaction commits.
9. Effect completion is another mailbox event. It never calls scheduler
   mutation methods directly.
10. Long-running work is identified by `operation_id`; cancellation first
    supersedes the operation durably and only then requests task cancellation.
11. A late completion for a superseded operation is recorded as stale and has
    no state side effects.
12. Every `ACTIVE_CHAT -> IDLE` transition creates exactly one review schedule
    outcome, including explicit default, bypass, failure, and superseded cases.
13. A review delay starts at schedule commit time, not model-call start time.
14. State and review-plan revisions are monotonic and independently visible.
15. Runtime prompt content used by a model call is explicit in the prompt
   snapshot; metadata is not the only carrier of model-visible input.
16. Persisting a message and making its Agent delivery recoverable has no crash
    gap; delivery is driven by a routing outbox or a durable log watermark.
17. A long-running exit planner has a durable deadline. Deadline expiry settles
    the exit with an explicit fallback outcome rather than leaving the session
    active forever.

## Stable Session Identity

`profile_id` is a durable runtime ownership id, not the editable Agent
configuration's `agent_id`. Bot-backed profiles use the stable bot service
configuration id. The unbound/default runtime uses a reserved constant. Two
bots may intentionally share the same Agent configuration without sharing an
actor aggregate.

The session portion is always the bot-scoped session id. At ingress this is
`signal.bot_session_id` when present; compatibility inputs derive the same
scope from `(bot_id, signal.session_id)` exactly once at the runtime boundary.
Timers, management commands, effects, and workflow completions carry the
resolved `SessionKey` and never reconstruct it independently.

The actor key is not an adapter transport address. Each message ledger entry
also preserves its ingress `base_session_id`; an accepted external action
carries that value as `target_session_id` together with the adapter instance.
The target is part of the request digest and is never derived from the
bot-scoped actor key. This keeps multi-bot ownership isolation separate from
the platform session used by `adapter.send()`.

Changing a durable profile id is an explicit state migration. It must not
silently create a new aggregate and leave live schedules under the old id.

## Durable Session Aggregate

The materialized aggregate is stored per `(profile_id, session_id)`:

```text
AgentSessionAggregate
  profile_id
  session_id
  state
  state_revision
  event_sequence
  activity_generation
  active_epoch
  review_plan
  active_reply_resume
  active_chat_state
  review_operation_id
  active_reply_operation_id
  active_chat_round_operation_id
  idle_planning_operation_id
  updated_at
```

`ACTIVE_CHAT_SETTLING` is an authoritative state, not an in-memory coordinator
flag. It preserves the active-chat snapshot while idle planning is in flight,
prevents new rounds from starting, and makes the pending exit visible to
recovery and management APIs. A message received while settling follows one
explicit policy selected by the reducer: cancel the exit and start a new
activity generation, or remain settling and supersede/restart planning. It may
not silently mutate the captured planning input.

`event_sequence` increments for every handled mailbox event, including stale or
duplicate outcomes that are useful for diagnostics. `state_revision` increments
only when authoritative aggregate state changes. Long-running completions match
their operation slot rather than requiring an unchanged state revision, because
messages may legitimately arrive while work is running.

An idle-planning completion additionally matches `activity_generation`; any
message or active-chat mutation that invalidates the captured planning snapshot
increments that generation.

## Reducer Decision Matrix

The Actor v2 reducer preserves the useful policy of the legacy scheduler, not
its callback structure. Every branch below is a synchronous transition; starts,
stops, cancellations, model calls, timers, and platform actions are effects.

### MessageReceived

The transition first creates exactly one ledger fact or an explicit suppressed
fact for a non-actionable/self event. Suppressed input never appears in unread,
review, chat, or high-priority projections.

- `IDLE`: ordinary input remains idle. High-priority input creates one
  active-reply operation at a captured watermark and enters `ACTIVE_REPLY`.
- `REVIEW`: ordinary input remains available after the review watermark. A
  high-priority message durably supersedes the review operation, records a
  review-resume snapshot, creates a pending active-reply operation, emits only
  the review-cancellation control effect, and enters `ACTIVE_REPLY`.  It does
  **not** enqueue the active-reply workflow in that transition.
- `ACTIVE_REPLY`: input is appended but cannot start a concurrent one-shot;
  unconsumed input remains for the completion/resumed review decision.
- `ACTIVE_CHAT`: mentions and replies update the current active-chat epoch and
  attention; they do not create a separate active-reply operation. At most one
  round operation exists, and later input stays beyond its watermark.
- `ACTIVE_CHAT_SETTLING`: input at or below the exit snapshot watermark is
  delayed captured input. New actionable input supersedes idle planning,
  increments `activity_generation`, emits cancellation, and returns to the same
  active-chat epoch. Suppressed input does not cancel settling.

### Interrupted Review Ordering (Implemented Slice)

Review interruption has an intentionally strict two-stage protocol.  Its
purpose is to prevent the high-priority reply model call from overlapping the
review model call that it interrupted.

```text
high-priority MessageReceived while REVIEW
  -> persist superseded review + pending active-reply operation
  -> persist cancel_review_workflow control intent and effect
  -> wait for fenced ReviewCancellationCompleted
  -> validate the saved active-reply fence
  -> enqueue run_active_reply_workflow
```

The active-reply operation is visible while waiting, but its model effect is
absent from the outbox.  Only a completion that matches the cancellation
intent's effect identity, causation, ownership, contract version/signature,
and completion event id may release it.  A cancellation failure, stale
completion, or invalid saved reply fence fails closed: the reply is blocked,
its unread input is retained for a later review, and the reducer records a
review retry/defer outcome instead of starting another model call into an
unknown cancellation tail.

### ReviewDue And ManualReviewRequested

The event identifies the exact `plan_id`, `plan_revision`, and ownership
generation. A mismatched plan is journaled as stale.

- `IDLE` with no pending high-priority input enters `REVIEW`, creates one review
  operation, and captures the ledger watermark.
- `IDLE` with pending high-priority input enters `ACTIVE_REPLY` and records that
  the same due review must resume afterwards.
- `REVIEW` treats the same operation as already running; it never launches a
  second review.
- `ACTIVE_REPLY`, `ACTIVE_CHAT`, and `ACTIVE_CHAT_SETTLING` persist an explicit
  deferred/retry outcome. Observing a due schedule is not equivalent to
  consuming it.

`ReviewDue` is implemented in the current reducer slice.  The
`ManualReviewRequested` behavior remains target work until it gains a reducer
branch and a durable ingress adapter.

The first concrete Actor v2 review vertical is implemented and remains
diagnostic-only: `ReviewWorkflowEffectHandler` reads only the operation's
captured unread ledger snapshot, and `RunnerReviewWorkflow` projects exactly
those message-log rows in ledger order before one reply-decision call. The
Actor composition uses intent collection rather than tool execution, disables
configured extra tools and model repair/retry calls, carries model execution
provenance, and rejects unbound `send_poke` actions. It always returns to
`IDLE` with a defaulted next-review outcome. Active Chat bootstrap and round
work now have the separate diagnostic-only v3 slice below; summaries,
compression, and production runtime mounting remain later activation work.
The context projector rechecks ownership generation, transport session, ledger
fences, and message-log identity before model work. A projection or model
decision failure does not consume input. Registering these handlers improves
inactive assembly readiness only; it does not expose an actor wake target or
start an Actor v2 worker.

The first Actor v2 active-reply vertical is also implemented and remains
diagnostic-only. `RunnerActiveReplyWorkflow` never calls the legacy
`ActiveReplyDispatcher` or active-chat coordinator. Its context projector
accepts only the effect-captured selection, treats request IDs as an
authorization set rather than an ordering source, and rebuilds model-visible
input in durable ledger order. The shared Actor reply-decision builder disables
configured extension tools and repair calls, and collects rather than executes
visible actions. A valid completion carries model execution provenance into the
terminal operation record; a projection or decision error leaves the selected
input unread. The first slice accepts at most one reply plus bound reactions;
every action targets captured message-log IDs, while raw platform IDs and
unbound pokes are rejected. After the actor accepts the completion, its intent
becomes a receipt-fenced outbound effect; only that effect may call an adapter.
SQLite integration coverage proves
the full high-priority-message -> workflow -> accepted intent -> receipt ->
adapter-send -> assistant-log chain while the runtime remains inactive.

### Actor-Native Active Chat V3 (Diagnostic-Only)

The Actor-native Active Chat vertical now implements version 3 contracts for
`run_active_chat_bootstrap` and `run_active_chat_round`. The v3 contracts seal
the extra bootstrap handoff and round-selection fields into their outcome
fences, and the native handler registration deliberately serves only v3.
Historical v1/v2 records retain their compatibility/recovery semantics; this
slice does not install a new native workflow implementation for them.

Bootstrap projects only the frozen review handoff: ledger rows already consumed
by the named `handoff_operation_id` and exact
`handoff_message_log_ids`, restored in durable ledger order. It does not read
legacy coordinator state, tail history, summaries, or pending buffers. A round
re-reads its dual-fenced unread snapshot (`input_watermark` and
`input_ledger_sequence`) and exposes only the effect's explicit selected
message-log IDs in ledger order. Both projectors revalidate ownership, effect
fences, base transport session, and message-log identity before model work.

Each reducer-created v3 handoff also carries a versioned review certificate:
the source review operation, source active epoch/activity generation, captured
input boundary, exact selected IDs, and (for a non-empty handoff) the review
ledger-consumption identity. The SQLite store compares that certificate with
the completed review operation and proves the consumption against either the
same transition's mutation or the already-persisted consumption and ledger
rows after an outbound receipt wait. A v3 bootstrap with a missing, changed, or
unapplied certificate cannot enter the outbox.

The bootstrap workflow returns one structured disposition. The round workflow
collects action intents only and accepts at most one terminal model tool call.
Replies and reactions must target a selected `message_log_id`; unquoted
replies, raw platform identifiers, `send_poke`, selection widening, multiple
visible actions, and malformed action payloads fail closed. Neither workflow
executes the legacy tool loop or adapter I/O. Only an actor-accepted completion
may create the existing receipt-gated external action effect.

Bootstrap and round completions preserve `model_execution_id` and
`prompt_signature` in their terminal operation records. This makes the frozen
input, model invocation, and accepted result inspectable without granting the
v3 workflows live production ownership.

#### Version Pinning

`active_chat_state.actor_workflow_contract_version` is an epoch-level
compatibility marker, not a request to upgrade old work. Only an exact v3 marker
authorizes the Actor-native contracts; a missing, malformed, historical, or
unknown marker is pinned to the last compatible v2 contract. A newly created
Active Chat epoch writes the v3 marker together with its frozen review handoff.
A legacy `waiting_outbound` state cannot become v3 merely because its pending
external action receipt succeeds: without the already-persisted v3 handoff it
starts a v2 bootstrap/round path. This prevents receipt recovery from
reinterpreting an existing epoch with the newer input contract.

#### Bounded Round Retry Chain

Actor-native v3 rounds persist one retry chain keyed by the `active_epoch` and
the exact frozen selected `message_log_ids`. A logical `retry` result and a
terminal round-effect failure advance that same chain, so they cannot receive
independent retry budgets. This model-work retry chain is separate from the
round-due control effect's bounded reconciliation policy. The first failure,
and any later failure below the configured maximum, leaves the selection unread
and schedules the next round through the durable round-due control effect. Once
the budget is exhausted, the reducer records a durable blocker and requests the
normal Active Chat exit; idle review planning then owns the eventual review
schedule rather than another unbounded round retry.

A valid consuming completion clears the retry chain. New input that changes the
exact selection clears both the chain and any blocker before the next frozen
round begins, giving that new selection a fresh bounded budget. An invalid or
unprovable selection cannot extend the old chain; it must fail closed into the
normal exit path or be replaced by a newly frozen selection. This retry policy
is part of the diagnostic v3 reducer slice and does not relax the production
ownership gate.

### Workflow Completions

Every completion matches the active operation slot, operation id, effect id,
idempotency key, source/expected event ids, ownership generation, input
watermark, input ledger sequence, and the state-specific epoch/generation
fences. A stale completion
advances diagnostic event sequence only; it cannot consume messages, schedule
work, or alter state.

- `ActiveReplyCompleted` consumes only the captured high-priority/chat inputs.
  It either creates a new review operation from the durable resume decision or
  returns to `IDLE`; it never calls a scheduler completion method.
- `ReviewCompleted` applies explicit ledger consumption, finishes the current
  schedule/operation, and either enters `ACTIVE_CHAT` with a new `active_epoch`
  or commits `IDLE` plus a typed next-review schedule outcome.
- `ActiveChatBootstrapCompleted` applies once to its epoch. A correction that
  exits follows the normal `ACTIVE_CHAT_SETTLING` path.
- `ActiveChatRoundCompleted` consumes only its captured inputs, commits accepted
  action intents as external-action effects, and updates attention. An exit
  request follows the same settling path as a timer-driven exit.
- `IdleReviewPlanningCompleted` and its deadline event are the only paths from
  `ACTIVE_CHAT_SETTLING` to `IDLE`; both atomically create the next review
  schedule before emitting runtime-stop work.

### Control Events

`ActiveChatTick` applies durable decay to one epoch and either remains active or
requests the normal settling flow. Recovery/reconciliation is represented by
mailbox events in the implemented slice. `PauseRequested`, `PauseCleared`, and
`ForceIdleRequested` are target events, not current reducer branches. Once
implemented, a force or pause may supersede operations and emit cancellations,
but it may not use a shortcut transition that omits ledger, schedule, or
journal outcomes.

## Mailbox Events

After activation, public/core signals are converted to Agent-internal events at
the runtime boundary. Core does not need to know workflow completion types.
The currently implemented reducer dispatches the following mailbox events:

```text
ExitRequested
MessageReceived
ReviewDue
ActiveChatTick
ActiveChatBootstrapCompleted
ReviewCompleted
ActiveReplyCompleted
ReviewCancellationCompleted
ActiveChatRoundDue
ActiveChatRoundCompleted
IdleReviewPlanningCompleted
IdleReviewPlanningDeadlineReached
ExternalActionCompleted
EffectFailed
ActiveChatRuntimeStopped
IdleReviewPlanningCancellationCompleted
ActiveChatRuntimeReconciled
IdleReviewPlanningCancellationReconciled
```

`ManualReviewRequested`, `PauseRequested`, `PauseCleared`, and
`ForceIdleRequested` remain part of the target public-event surface, but are
not yet live reducer branches.

`RecoveryRequested` is different: the SQLite store already persists it during
recovery scans, but the reducer does not yet consume it.  It must gain a
fenced, deterministic recovery branch before this mailbox can be used for
production recovery.

Every event includes:

```text
event_id
profile_id
session_id
kind
source
occurred_at
payload
causation_id
correlation_id
trace_id
status
attempt_count
available_at
created_at
handled_at
```

The actor claims pending events for one session in sequence order. SQLite
leases allow recovery when a process dies after claiming but before completing
an event.

## Operations And Effects

An operation is the durable intent to run long-lived work. An effect is a
specific executable action created by a transition.

```text
AgentOperation
  operation_id
  profile_id
  session_id
  kind
  status
  launched_by_event_id
  state_revision
  active_epoch
  activity_generation
  input_watermark
  input_ledger_sequence
  started_at
  superseded_at
  finished_at
  failure
```

Operation statuses are `pending`, `running`, `completed`, `failed`,
`superseded`, and `cancelled`. Effects use an outbox-style table with retry and
lease metadata. Model calls, adapter sends, workflow runs, and timer scheduling
are effects.

Every effect claim receives a new `claim_id`, even when the same worker
reclaims it. Completion, renewal, retry, and failure compare both `claim_id`
and lease owner so an expired claim cannot complete after an ABA reclaim.
Handlers run outside actor transactions and must pass the durable
`idempotency_key` to external I/O. A successful handler settles only through
`complete_with_event`: effect completion and its deterministic mailbox event
commit in one transaction. Terminal retry exhaustion uses the same boundary to
insert `EffectFailed`. The actor registry is woken only after commit; a failed
wake triggers durable mailbox recovery and never reruns the handler.

Effects may declare an explicit operation-status precondition. The store
rechecks it inside the settlement transaction to close the claim/commit TOCTOU
window. A failed precondition completes the effect with a deterministic
`EffectSkipped` mailbox event containing the intended event identity. Effects
without that explicit payload, including stop and cancel effects, are never
implicitly skipped merely because their operation is terminal.

### Effect Execution Lanes And Contracts

Effect kinds are registered with a durable execution contract. The contract,
not a handler return value, owns the execution lane, allowed completion event
kind, completion source, handler timeout, retry limit, and bounded backoff.
Handlers may return only domain payload data. An outcome with a different kind
or source is rejected before settlement.

Planner/model work runs in the `planner` lane. Deadlines, cancellation, runtime
stop, and desired-state reconciliation run in a separately supervised
`control` lane with at least one dedicated worker and higher claim priority.
The control lane must remain runnable when every planner worker is blocked.
Planner handlers have a finite execution timeout; lease renewal never turns a
hung call into an immortal claim. A deadline is therefore independent of the
work it supervises rather than another item behind that work in the same
queue.

Every workflow completion carries and validates all of the following as
required provenance: expected event id, source event/causation id, effect id,
idempotency key, operation id, plan id, active epoch, and activity generation.
Workflow operations additionally require their input watermark and input ledger
sequence. Missing fields fail the fence just like mismatched fields. A
control-effect terminal failure must have an explicit bounded disposition:
retry through a new fence, a dedicated reconciliation effect, or a durable
blocker. Recording an intent without an executable recovery path or blocker is
insufficient.

### Control Intents And Contract Snapshots

A control effect is not represented only by an outbox row.  The reducer also
stores a durable control intent under
`aggregate.data.effect_control_intents[effect_kind]` in the same transition.
The intent records the desired state, effect/idempotency identity, expected
completion and failure event ids, causation id, ownership generation, relevant
state/plan/epoch/watermark fences, and retry cycle.  It is the authoritative
answer to "what must this timer/cancellation request still be allowed to do?"
after a restart.

Every actor-owned workflow operation fence and control intent snapshots both
`contract_version` and `contract_signature`.  Idle planning persists separate
planner and deadline snapshots because those effects share one operation but
have different contracts. Reconciliation persists its own snapshot instead of
reusing the failed control effect's identity, and its operation fence carries
the same authoritative input boundary as the effect. The outbox effect carries
the matching identity. A completion or `EffectFailed` validates against that
persisted snapshot, not against whichever contract happens to be current when
the event is handled.

Every actor-owned effect has a versioned contract with an explicit outcome-field
declaration signed into policy. General current effects use v2 declarations;
Actor-native Active Chat bootstrap and round use their stricter v3 declarations.
In the normal executor path, handlers provide domain output; they do not choose
the fence projection or reinterpret the contract. The lower-level store
interface retains an optional outcome-field argument only as a compatibility
assertion: it resolves the projection from sealed contract authority and rejects
a caller value that differs. Compatibility handlers can drain historical rows
without granting them newer execution semantics.

For v3 Active Chat bootstrap and round effects, the durable payload is also
bound to `aggregate.data.operation_fences[operation_id]` at commit. The store
rejects a missing or changed input boundary; bootstrap must retain its verified
review handoff, while a round must retain its ordered selection, schedule,
interest, and bootstrap-disposition fences. The payload therefore cannot widen
or replace the aggregate's frozen operation input.

`EffectQuarantined` is a store-owned terminal event, not a generic caller
failure. After the effect store has revalidated and terminalized the exact
durable effect identity, it emits deterministic quarantine evidence containing
only that identity and diagnostic reason. The reducer rebinds it to the
persisted operation or control fence and applies the same fail-closed terminal
recovery path as a verified effect failure. A wrong source, event identity,
contract, or effect identity is stale/ignored and cannot release a live
operation. In particular, quarantine never consumes pending ledger input by
itself.

Current (v2+) external-action completion evidence also includes the receipt
idempotency key returned by the receipt store. The reducer requires that key to
match the accepted action's durable idempotency identity before it releases a
pending outbound gate. Legacy v1 completion recovery remains compatible with
its historical envelope shape.

### Legacy V1 Recovery Policy

Legacy v1 effect records retain their historical signatures and outcome shape.
The three effects that already had stricter compatibility projections retain
those projections byte-for-byte; adding v2 declarations for the remaining
effects does not expand their v1 projections. This preserves recovery of work
written before v2 outcome-field declarations without changing persisted v1
behavior or signatures.

An aggregate or intent written before contract snapshots existed is always
validated as v1.  An inbound completion must never select a newer contract on
its behalf.  Unknown versions, missing/incomplete snapshots, and signature
drift are stale/fail-closed outcomes.

Some early v1 exit records do not contain enough durable intent identity to
prove that a terminal exit-request failure belongs to the live aggregate.  The
reducer must not invent a retry or a receipt for those records.  It leaves them
fail-closed for explicit reconciliation/operator handling; compatibility exists
for verifiable v1 completion projections, not for unsafe automatic recovery.

### Ownership Generation Fencing

Actor ownership generation is copied into the aggregate, mailbox event,
operation, schedule/effect work, and every claim. Enqueue, claim, aggregate
commit, and effect settlement each re-read the ownership row inside their
write transaction and require `actor_v2`, `active`, and the expected generation.
Beginning or completing migration invalidates every earlier claim. Relay-time
validation alone is not a fence: work already delivered before migration must
also fail closed when it later attempts to claim or commit.

### Model-Selected External Actions

A workflow effect may ask a model which visible action to take, but it must not
perform that action while the workflow effect is running. `send_reply`,
`send_poke`, `send_reaction`, and future externally visible tools are split into
two contracts:

1. the model-facing tool validates and records a normalized action intent in
   the workflow result;
2. the workflow completion enters the mailbox;
3. the actor validates the completion fences and atomically turns accepted
   intents into dedicated action effects;
4. only the action-effect handler calls the platform adapter.

This boundary prevents a workflow from producing a visible side effect before
the actor accepts the model result. Re-running a failed or stale workflow can
therefore produce proposals, but cannot send another message by itself. Action
effect identity is deterministic from the accepted operation, model tool-call
identity, action ordinal, and action kind. Contract version and normalized
payload are persisted as a separate request digest: reusing the logical action
slot with different content is a hard conflict, not a new action. The model
cannot choose or override its idempotency key.

Every externally visible action also has a durable receipt keyed by the action
effect's idempotency key. The receipt stores the exact action kind and contract
version, normalized request digest, profile/session ownership generation,
effect and operation provenance, claim/lease identity, attempt count, platform
result, assistant message-log id when applicable, and one of these outcomes:

```text
prepared
executing
succeeded
rejected_before_dispatch
abandoned_before_dispatch
unknown
```

The action claim and ownership check occur before adapter I/O. After a
successful reply, the success receipt and assistant message log commit in one
SQLite transaction; context projection happens only after that commit. A
reused idempotency key with a different action contract or request digest is a
hard conflict.

Action effects from the same operation are also serialized by their persisted
`action_ordinal`, not by a process-local lock or outbox insertion timing. An
action with ordinal `n > 0` cannot receive an effect claim or begin adapter
execution until the receipt for ordinal `n - 1` is durably `succeeded`.
Missing, `prepared`, `executing`, `rejected_before_dispatch`, `unknown`, and
`abandoned_before_dispatch` predecessors leave the follower pending with an
inspectable durable blocker; actions from different operations remain eligible
to run independently. The receipt-side check repeats the gate after a worker
claim so a caller cannot bypass it by invoking the action handler directly.

Receipt preparation and execution start accept only the live claimed durable
action effect. In the same write transaction they validate the outbox effect,
operation, contract, payload, ownership generation, claim id, worker, attempt,
and unexpired lease. The receipt attempt reuses that outer effect claim id and
worker, and its lease never extends beyond the effect lease. A crash after
`prepared` but before `executing` may therefore continue under a fresh effect
claim. Once `executing` has committed, an expired or newer effect claim may only
settle the old receipt as `unknown`; it cannot dispatch the action again.

Ownership generation is relational fence metadata, not part of the canonical
external request JSON or logical idempotency key. A terminal `succeeded`,
`abandoned_before_dispatch`, or `unknown` receipt with the same exact request
remains the global deduplication answer after ownership changes and keeps its
original generation. Recording a late success or unknown outcome is evidence
settlement rather than execution:
it requires the exact receipt/attempt ABA claim, but does not require that the
old ownership generation is still active. This exception prevents a migration
or recovery race from discarding the result of platform I/O that may already
have happened; it never authorizes new I/O.

Exactly-once delivery cannot be manufactured locally when a platform provides
no idempotency token or result lookup. Once adapter invocation may have begun,
a timeout, cancellation, lost acknowledgement, process crash, or expired
`executing` lease becomes `unknown`; it is not retried automatically. Operators
may reconcile it using platform evidence. When an adapter supports a durable
idempotency token or authoritative result lookup, its capability contract may
resolve `unknown` and permit a fenced retry with the same token. Exceptions
known to happen before dispatch may settle as `rejected_before_dispatch` and
remain retryable under the effect contract.

An exhausted parent action effect is not a receipt outcome. Its deterministic
`EffectFailed` event carries the exact effect id, action ordinal, canonical
request digest, contract, causation, and ownership fences. The actor records
that evidence as an `effect_failed` outbound-gate entry and an inspectable
blocker, while leaving the receipt itself unchanged. It must not synthesize an
`abandoned_before_dispatch` receipt: only receipt-side ownership reconciliation
can prove that no adapter invocation began. No bootstrap, review, active-chat
round, or queued priority reply may pass that gate automatically.

When an exact parent action effect reaches terminal `failed` after exhausting
its retry policy, a `prepared` or `rejected_before_dispatch` receipt may be
atomically reconciled to `abandoned_before_dispatch` during ownership
transition validation. The reconciliation matches the complete immutable
effect/receipt identity and never changes `executing` or `unknown`; it is the
durable proof that the old retry loop cannot block a later ownership migration
forever without pretending a platform action was safe to retry.

The existing in-memory action guard is only a legacy concurrency optimization.
It is neither a receipt nor part of the Actor v2 correctness boundary.

## Review Scheduling

`ReviewPlan | None` is not a valid planning result. Planning produces a typed
outcome:

```text
Planned
DefaultRequested
Failed
Bypassed
Superseded
```

The policy resolves every outcome into a non-null `ReviewScheduleEffect` with:

```text
effect_id
plan_id
profile_id
session_id
trigger
outcome
source
requested_delay_seconds
applied_delay_seconds
scheduled_from
next_review_at
reason
fallback_reason
model_execution_id
prompt_signature
expected_active_epoch
expected_activity_generation
committed_state_revision
created_at
```

All active-chat exit causes use the same event flow:

```text
bootstrap/round/tick requests exit
  -> persist enqueue_active_chat_exit_request control intent + effect
  -> fenced ExitRequested
  -> enter ACTIVE_CHAT_SETTLING
  -> snapshot planning input and create IdleReviewPlanning operation
  -> run planner as an effect outside the actor
  -> IdleReviewPlanningCompleted | IdleReviewPlanningDeadlineReached
  -> validate operation/epoch/activity generation
  -> atomically commit IDLE + ReviewPlan + schedule event
  -> emit StopActiveChatRuntime effect
```

Callers cannot inject an optional `next_review_plan`. Manual/recovery paths that
intentionally avoid a model produce an explicit `Bypassed` outcome and still use
the same commit path.

The `plan_id` carried by an exit-control intent/completion is the current-plan
provenance fence: it proves which active review schedule the exit was authorized
against, rather than naming its replacement. Idle planning creates a distinct
successor plan id and records the former current id as `previous_plan_id`; a
new review revision must never reuse the fenced current plan id.

Exactly one schedule is current per session. The aggregate stores
`current_plan_id` and `review_plan_revision`; committing revision `N + 1`
atomically supersedes revision `N`. Due-review delivery checks both values so a
late timer for an older plan is recorded as superseded and cannot start review.

### Active-Chat Control Intents (Implemented Slice)

The active-chat semantic wait and exit trigger are control effects, rather than
in-memory timer callbacks.  A buffered message creates an
`enqueue_active_chat_round_due` effect and a matching intent with its schedule
id/revision, due event id, input watermark, epoch, contract snapshot, and retry
cycle.  The `ActiveChatRoundDue` event may start a round only when all of those
fences still match; otherwise it is diagnostic stale work.

An active-chat bootstrap result, round result, or decay tick requests exit by
creating `enqueue_active_chat_exit_request` plus an intent.  Its trusted
`ExitRequested` completion revalidates the active epoch and message watermark
before entering `ACTIVE_CHAT_SETTLING` and starting idle-review planning.  New
actionable active-chat input supersedes a pending or failed exit intent rather
than reviving its old effect, clears the exit blocker, and schedules fresh
round work from the new watermark.

Terminal failure of either control effect follows a bounded policy governed by
`control_reconciliation_max_cycles` (currently defaulting to two total
cycles). A failed round-due request is retried through a fresh fenced intent
while buffered input remains. If its budget is exhausted, the reducer keeps the
pending ledger rows unread and automatically enters the normal exit path by
creating a fenced exit request; it does not wait for another message. If an
exit-request control effect exhausts its budget, the reducer enters
`ACTIVE_CHAT_SETTLING` and starts idle planning plus its deadline directly,
rather than leaving an `exit_blocker` that needs a future signal. The successor
review then owns any still-pending input through its normal durable review
workflow.

These quarantine and control-liveness semantics are part of the diagnostic-only
Actor slice. They do not activate `actor_v2` ownership: state-specific recovery
materializers and full production `AgentRuntime` composition remain explicit
production blockers.

## Ingress Delivery

The core message log and the Agent mailbox cannot be connected by an in-memory
task alone. The accepted ingress transaction records an Agent-routing outbox
entry keyed by `(profile_id, session_id, message_log_id)`. A supervised relay
converts that entry to the deterministic mailbox event
`message-received:{message_log_id}` and marks delivery only after the mailbox
insert is durable. Duplicate relay attempts are idempotent.

The Agent mailbox identity is scoped to the logical message, not to the route
rule which discovered it. Route outbox rows may remain distinct per rule for
audit purposes, but every delivery for the same
`(profile_id, session_id, message_log_id)` converges on one canonical
`MessageReceived` event and one message-ledger row. The actor payload excludes
rule-specific delivery ids. Conflicting projections of the same message fail
closed instead of causing a second state transition.

The first `eligible_for_work` `MessageReceived` handled by a virgin `IDLE`
aggregate creates review-plan revision 1 in that same actor transition. The
outcome is an explicit `Defaulted` policy decision: its relative delay and
reason come from the profile's actor reducer configuration, while the store
anchors `scheduled_from` and `next_review_at` to the transaction commit clock.
The message-ledger append, aggregate plan fence, current schedule, schedule
journal, state-transition journal, and mailbox completion therefore have no
crash gap. A high-priority first message binds its active-reply operation to
this plan before emitting the workflow effect. Suppressed input remains
auditable in the ledger but does not create a plan; a later first actionable
message still creates revision 1.

Any transition that advances `current_plan_id` and `review_plan_revision` must
carry exactly one matching `SessionReviewSchedule`. The actor validates this
before commit and the SQLite store repeats the check inside its write
transaction, so alternate handlers cannot persist an orphan aggregate plan.
The payload of an existing current plan is canonical and immutable: a
transition that keeps the same plan id and revision may not rewrite any plan
field. For a new plan, the aggregate plan payload and its sole schedule are
normalized and must agree on every semantic field, including plan identity,
revision, policy outcome, trigger/source, requested and applied delay, reason
and fallback, model/prompt evidence, epoch/activity fences, and committed state
revision. The scheduled journal carries one exact `metadata.schedule_outcome`
record; its field set is closed, its overlapping fields must match the journal,
and its full decision semantics must match the schedule. `scheduled_from` and
`next_review_at` are not caller authority; only the store may stamp them from
the transaction commit clock, and the aggregate, schedule row, and schedule
journal must all use that same committed timing.

A routing worker separates decision from dispatch. It reconstructs context,
runs the routing policy, and commits the route decision plus every Agent outbox
row before invoking a command, plugin, or observer target. Recovery may repeat
pure matching and explicitly idempotent hooks, but it never treats an external
target side effect as a prerequisite for committing the Agent delivery. Normal
targets remain best-effort until they receive their own durable outbox contract.

If the persistence adapter cannot share that transaction, the runtime keeps a
durable per-session message-log watermark and performs a recovery scan before
accepting live traffic. Merely logging an enqueue failure is not recovery.

## Actor-Owned Message Ledger

Actor-owned sessions do not reuse the legacy unread-message or unread-range
tables. They keep one durable message ledger keyed by
`(profile_id, session_id, message_log_id)`. Each row records the source mailbox
event, ownership generation, immutable message/priority facts, and review,
active-reply, and active-chat consumption provenance.

Unread ranges are projections over ordered ledger rows. They are not another
mutable table. Consuming a middle interval therefore cannot leave range state
out of sync with individual messages, and a restart requires no range rebuild.

Every consumption mutation carries an `operation_id` and the operation's
`input_watermark` plus `input_ledger_sequence`. The sequence is the last ledger
row visible in the transaction that creates the operation, including a
`MessageReceived` row created by that same transition. A workflow may consume
only the explicit message ids it captured at or below both fences. Messages
committed after the operation started remain unread even if their numeric ids
fall inside a coarse range.

An `all_through_watermark` mutation applies only to eligible rows already
present at its captured ledger-sequence boundary. A later append never inherits
an earlier consumption merely because its `message_log_id` is below the coarse
watermark. The store stamps the resolved sequence into the operation, aggregate
pending-operation fence at
`data.operation_fences[operation_id]`, and effect payload before the creating
transaction commits; completions validate the same sequence. Replaying the
same operation and mutation is idempotent; a different operation trying to
rewrite existing provenance fails closed.

`MessageReceived` inserts the ledger row in the same transaction that advances
the aggregate, completes the mailbox claim, appends the transition journal,
and creates any workflow effects. A duplicate event cannot advance attention,
activity generation, mention thresholds, or workflow state twice.

The active-chat exit snapshot stores its `input_watermark`. A message at or
below that value is old captured input even when its delivery is delayed; a
message above it is new activity and follows the reducer's explicit settling
cancellation policy.

## Durable Review-Due Delivery

The review scanner addresses a schedule by `(plan_id, plan_revision,
ownership_generation)`. In one write transaction it rechecks active Actor v2
ownership and the aggregate's current-plan fields, then either inserts the
deterministic `ReviewDue` mailbox event or records a superseded/retry outcome.
It never invokes a scheduler method directly.

A due event is not considered consumed merely because a timer observed it.
The actor transition either starts a fenced review operation, defers the
schedule with an explicit retry time, or supersedes it with an append-only
reason. Stable cursor scanning and per-row retry times prevent one unavailable
session from occupying every scan page.

## Prompt Assets And Projection

Built-in prompt files and editable runtime copies use three-way synchronization:

- source: the incoming package version;
- base: the last source version accepted into the runtime directory;
- local: the editable runtime version.

If `local == base`, source updates apply automatically. If both local and source
changed, non-overlapping edits may merge; conflicts preserve local runtime
content and stage incoming content separately. Source version and digest changes
are validated in CI. Legacy runtime files are auto-upgraded only when their hash
matches a known built-in revision.

Review model input is projected through one `ReviewPromptProjector`:

- context messages become an explicit `CONTEXT` injection;
- source records and instruction blocks become explicit `INSTRUCTIONS`
  injections;
- runtime facts are rendered by a dedicated component;
- prompt snapshots contain the exact messages sent to the model.

## Timer Supervision

Timers enqueue durable events; they do not invoke scheduler methods directly.
Polling loops catch failures per iteration, apply bounded backoff, and remain
alive until shutdown. Health state includes last scan, last success, consecutive
failures, last error, and current lease owner. Management APIs expose both live
tasks and completed task failures.

Due-review scanning must not allow unavailable sessions to occupy the first
page forever. Skipped plans receive an explicit retry time, and scanning uses a
stable cursor.

## Recovery

The following is the target recovery contract. The current store still enqueues
the legacy `session_actor_recovery` heuristic described above, while the
inactive certificate authority uses source
`durable_session_recovery_scanner`. Typed discovery, reducer intent creation,
commit-time proof, and generic state-keyed materializer dispatch are implemented
and integration-tested, but no production runtime constructs or activates them.
Do not activate Actor v2 ownership until legacy rows have a fenced terminal
policy and typed materializers deterministically rebuild or settle every
non-idle aggregate reachable from production ingress.

### Implemented Inactive Discovery Boundary

`SQLiteRecoveryGraphScanner` is a diagnostic and protocol-building component,
not a recovery executor. Each candidate uses one `BEGIN IMMEDIATE` transaction
and a bounded raw SQLite projection. Its graph currently includes the aggregate
and ownership fence, reachable state operations, live/referenced effects,
mailbox work, schedules and schedule journal, message consumption and linked
ledger rows, external-action receipts and attempts, route outbox debt, and the
aggregate transition-journal tail. Text keys are queried through raw BLOB
expressions so a SQLite TEXT/BLOB alias is surfaced as a finding instead of
being silently omitted by affinity matching.

Discovery uses a conservative state shape. Review, active reply, and
active-chat-settling require the matching live operation and aggregate fence;
active chat is `no_recovery` only after a completed bootstrap with no remaining
round/control work. Missing, terminal, wrong-kind, malformed, oversized, or
ambiguous authority records create a typed blocker or an operator-visible
finding. Executing or unknown external actions are blockers and are never
replayed. A delivery is preflighted by its raw `(profile_id, session_id,
event_id)` logical key both before and after insertion; its complete typed
envelope and payload are decoded again before a case cycle can advance.

`SQLiteRecoveryGraphReader` is deliberately a separate read-only module and
the only authority dependency a commit coordinator receives. Its
`rebuild_certificate(conn, ...)` method requires the caller's already-open
transaction, revalidates Actor v2 ownership in that transaction, and never
opens a connection or writes a case, finding, mailbox, aggregate, effect, or
transition. The reader also owns raw typed recovery-delivery decoding and
immutable-envelope validation, so a coordinator cannot fall back to the normal
mailbox decoder and lose TEXT/BLOB alias evidence. The scanner composes one
reader using the exact same database and policy instance; its compatibility
`rebuild_certificate()` method only delegates. An `idle` aggregate or moved
ownership produces `RecoveryGraphNotEligible`, which the coordinator settles as
`superseded`, while malformed authority remains a
`RecoveryGraphReadError` and must become a blocker/finding.

The coordinator receives only a compact immutable intent from the reducer. In
the store's existing `BEGIN IMMEDIATE` transaction it re-reads the claimed raw
mailbox row, raw case snapshot, ownership, graph certificate, physical mailbox
id, and aggregate fence before invoking a pure materializer. A changed lease or
claim is a `RecoveryDeliveryClaimLost` outcome: the actor makes no aggregate,
case, or mailbox write on that path. The scanner remains unwired until the
runtime supplies materializers and full route-to-mailbox-to-ledger convergence
proof; the presence of these components is not an authorization to activate
recovery in production.

The actor must never send a scanner-owned delivery through ordinary
`release()` or `fail()` after reducer decoding or commit-time proof rejects it.
Before the raw claim/case can be proven, it leaves that claim untouched so a
scanner pass can record a finding without advancing aggregate sequence. After
the coordinator has proven the raw delivery and case, an unsafe provisional
carrier or materializer result settles as a completed no-op plus
`scanner_blocked`; it does not become a generic mailbox-failed transition.

### Recovery Certificates And Cases

Recovery discovery produces a versioned, immutable certificate from one
transactionally consistent authority graph. The certificate contains the exact
profile/session/ownership subject, aggregate fence, normalized authority nodes
and edges, policy invariants, and one exhaustive policy decision. Canonical JSON
rejects floating-point values and uses sorted object keys and normalized
set-like collections. Persisted input is decoded through a version registry;
unknown versions, extra or missing fields, non-canonical ordering, and any
work-graph, case, certificate, or mailbox identity mismatch fail closed.

The v1 work-graph digest includes the semantic aggregate fence, graph, and
decision, but excludes transient aggregate `event_sequence` and
state-transition journal-tail nodes. The complete certificate digest retains
that raw audit evidence. A no-op journal or mailbox failure may therefore
produce a new certificate for the same semantic case, while a state revision,
epoch, activity generation, plan fence, graph fact, invariant, or decision
change produces a new case. The case id binds certificate version,
profile-scoped subject, policy version, and work-graph digest. Its v1 form is:

```text
recovery-case:v1:<64 lowercase SHA-256 hex characters>
```

`agent_session_recovery_cases` is the delivery ledger for that semantic case.
Identity columns are immutable. A new row may only be inserted when its
ownership generation exactly matches the aggregate in that transaction. A
later ownership generation does not delete the old case: historical rows must
remain available to be marked `superseded`, but they cannot authorize work for
the new owner.

Fresh rows start in `open`, or in `scanner_blocked` with a non-empty diagnostic.
They have zero delivery count/cycle, no last event, and identical creation and
update times. Terminal or progressed rows can only be reached through updates.
`INSERT OR REPLACE` is not an idempotency mechanism for this ledger: the insert
guard rejects an existing case id or semantic identity before SQLite can delete
the old row. Writers select and validate an existing row instead.

Case status has the following bounded meaning:

- `open`: current authority may still require a delivery or commit-time result;
- `applied`: the fenced recovery decision committed successfully;
- `superseded`: ownership or graph authority changed before application;
- `delivery_exhausted`: bounded delivery policy reached its terminal limit;
- `scanner_blocked`: discovery or a verified commit-time materializer recorded
  a durable operator-visible blocker without applying business state.

The status matrix is explicit. `open` may remain open or move to any other
status. `scanner_blocked` may remain blocked, return to `open` after a successful
scan of the same semantic graph, or move to a terminal status. `applied`,
`superseded`, and `delivery_exhausted` are terminal: their certificate digest,
status, delivery progress, last event/error, and update time are immutable.
Status changes never rewrite the case identity or creation time.

Delivery cycles start at zero. `next_delivery_cycle` and `delivery_count` are
the same monotonic value and can advance by at most one in a transaction: before
delivery both are zero; after committing cycle `N`, both are `N + 1`, and
`last_event_id` is exactly:

```text
recovery-requested:v1:<case digest>:<N>
```

`created_at` never changes. `updated_at` never moves backwards, and any change
to `latest_certificate_digest`, status, delivery progress, last event id, or
last error must strictly advance it. Thus a case update has a durable ordering
even when no delivery cycle is consumed. Repeated writes of identical authority
may retain the same update time; they do not create a new logical transition.

The ledger only accepts progress when every consumed cycle has a matching
`RecoveryRequested` mailbox row. The mailbox envelope, deterministic event id,
typed delivery schema/version, case/cycle, certificate identity, graph digest,
policy, and subject must agree. The latest certificate digest remains tied to
the last consumed delivery. A legacy event cannot satisfy this evidence check:
typed deliveries use source `durable_session_recovery_scanner`, while the
pre-certificate startup heuristic retains `session_actor_recovery` until it is
retired at the activation gate.

When a completed typed delivery finds the same semantic case but a different
complete certificate, the coordinator records a `refreshed` no-op journal and
leaves the case open. A later scan may emit exactly one next cycle only when the
last completed delivery still matches the case's previous certificate digest.
An unchanged completed delivery is a finding, not a retry. If that refreshed
path reaches the delivery limit, the case becomes `scanner_blocked` rather than
`delivery_exhausted`, because the final mailbox evidence is completed rather
than failed.

Once aggregate ownership advances, an older-generation case is history. It may
be marked `superseded`, `delivery_exhausted`, or retain a blocker for audit, but
it cannot consume another delivery, rewrite its certificate, reopen from
`scanner_blocked`, or become `applied`.

### V1 Decision Matrix

The first scanner maps durable rows into one bounded
`RecoveryWorkClassification` before producing a certificate. The pure v1 policy
is shared by discovery and commit-time revalidation and has fixed precedence:

1. Blocking findings win and produce `record_blocker`. Examples include an
   unknown external action, a malformed authority record, or an aggregate that
   references work that cannot be located.
2. Existing recoverable work produces `wait_for_progress`. Pending mailbox or
   route deliveries and live idempotent effects must settle through their normal
   paths instead of being replaced by recovery work.
3. Only a confirmed orphaned work node can produce
   `recover_orphaned_work`. That decision is not a state mutation by itself; a
   state-specific commit-time materializer must still prove the replacement
   transition before the case can become `applied`.
4. A clear graph produces `no_recovery` and does not manufacture mailbox work.

The graph contract is bounded before it is serialized or persisted: canonical
authority records are limited to 1 MiB, 128 nesting levels, 65,536 JSON values,
128 graph nodes, 256 edges, 128 invariants, and 4 KiB UTF-8 text fields. The
scanner must record malformed raw authority separately from a valid
certificate-backed policy blocker; it must never invent a certificate from an
unparseable row.

The scanner performs discovery and delivery under one `BEGIN IMMEDIATE`
transaction. It reads the aggregate and every operation, effect, control
intent, external-action receipt, and pending outbound authority needed by the
work graph; constructs and verifies the certificate; inserts or validates the
case; inserts the deterministic mailbox event; and compare-and-swaps the case
digest, cycle, count, last event id, status, and update time. It commits before
waking the registry. A failed mailbox insert or case CAS rolls back the entire
delivery and cannot consume a cycle.

The mailbox payload contains the complete certificate, case id, and delivery
cycle. Its typed decoder also receives the mailbox envelope projection and
verifies profile id, session id, ownership generation, event kind, event
source, and the deterministic event id. Before applying `RecoveryRequested`,
the pure reducer may only validate its typed envelope and emit a
`RecoveryCommitIntent`; it must not construct a target aggregate or read
persistence. A `RecoveryCommitCoordinator`, running inside the store's single
commit transaction, must re-read the claimed raw mailbox row, case, ownership,
and all graph authority through `SQLiteRecoveryGraphReader`; reconstruct the
current certificate under the persisted policy version; and verify the case is
still `open` with the delivered certificate and cycle. Only then may a
state-specific pure materializer produce a normal fenced transition. The same
transaction commits either that transition plus `applied`, or no business state
change plus a no-op transition journal, completed mailbox, and
`superseded`/durable-blocker case settlement. Materializer journal metadata and
every persistence-facing materializer field are bounded to values the raw
reader can reload; a complete certificate or delivery record is forbidden from
all of them, including aggregate, effect, operation, and journal data.
Comparing only aggregate state or trusting a previously decoded in-memory
certificate is not commit-time revalidation.

Schema initialization verifies the canonical recovery table and replaces all
same-name recovery triggers. A weak legacy table is rebuilt under a savepoint:
valid authority is copied without changing its SQLite storage classes or values;
empty incomplete tables can be replaced; non-empty incomplete, coercible, or
unjustified authority aborts initialization and leaves the original table and
trigger definitions intact.

At startup:

1. release expired inbox, operation, and effect leases;
2. start profile actors for sessions with pending events or non-idle aggregates;
3. supersede operations that cannot be resumed safely;
4. replay pending effects that are idempotent;
5. convert expired external-action executions without authoritative platform
   idempotency into `unknown` rather than blindly replaying them;
6. rebuild active-chat runtime state from the durable aggregate or settle it
   through the normal exit flow;
7. enqueue due review events using the persisted `plan_id` and plan revision.

Recovery never calls a separate transition shortcut.

## Migration Sequence

1. Add durable aggregate revision, mailbox, operations, transition journal,
   schedule events, and effect outbox without changing current behavior.
2. Add the actor registry and route public signals, timers, and management
   commands through it.
3. Convert review and active-reply completions to mailbox events.
4. Convert active-chat bootstrap, rounds, and exits to mailbox events.
5. Remove direct coordinator-to-scheduler mutation callbacks and the runtime
   session lock.
6. Enable durable recovery and remove legacy reconcile shortcuts.
7. Remove compatibility repository mutation methods once no caller remains.

Each migration step must preserve a runnable application and include restart,
duplicate, stale-completion, cancellation-tail, and fault-injection tests.

## Activation Gate

The v2 tables are not a shadow-write or observability mirror. A session is
owned by either the legacy scheduler or the actor runtime, never both. Runtime
selection is made before the first event for `(profile_id, session_id)` and is
persisted with the aggregate. Falling back to legacy mutation after an actor
event has been accepted is forbidden.

### Durable Ownership Contract

Runtime ownership is persisted independently from both legacy scheduler state
and the actor aggregate. Its key is the stable `SessionKey` pair
`(profile_id, session_id)`; editable Agent configuration ids are not ownership
identities. The ownership mode is one of `legacy` or `actor_v2`, and its status
is one of `active` or `migrating`.

Because the legacy scheduler is still keyed by the unscoped base session id,
the ownership row also records `legacy_session_id` as a migration alias. It is
not part of the actor key. Multiple actor-owned profiles may share that alias
without sharing state, but any active legacy owner for the alias conflicts with
actor ownership, and only one legacy owner may use an alias at a time.

The first ownership claim uses a SQLite write transaction. Concurrent claims
for the same key and mode are idempotent and return the same generation.
Concurrent claims for different modes fail closed. Before inserting the first
claim, the repository checks durable evidence:

- an actor aggregate or mailbox row forbids selecting `legacy`;
- a legacy scheduler, unread-message, or unread-range row forbids selecting
  `actor_v2`;
- evidence is checked inside the same `BEGIN IMMEDIATE` transaction as the
  ownership insert.

Ownership starts at generation 1. It cannot be changed with a direct mode
update. Migration is an explicit two-phase state transition:

```text
active(mode=A, generation=N)
  -> begin migration CAS(N, target=B, reason)
  -> migrating(mode=A, pending=B, generation=N+1)
  -> external state migration and source cleanup
  -> complete migration CAS(N+1, reason)
  -> active(mode=B, generation=N+2)
```

Both migration transitions require a non-empty audit reason and append an
ownership event. A stale generation, wrong status, or different pending target
fails closed. Completion repeats the target-mode evidence check: legacy state
must be gone before activating `actor_v2`, and actor aggregate/mailbox state
must be gone before activating `legacy`. An explicit abort returns to the
source mode through another generation-checked transition and audit event.

Changing the ownership generation also invalidates durable rows, not only live
leases. A migration which transfers Actor state therefore requires a durable
manifest describing the exact routing jobs and outbox deliveries, aggregate,
mailbox, operation, schedule, ledger, journal, effect, and prepared external
action revisions being moved. Completion re-fences those rows to the new active
generation in the same transaction as the ownership change. Aborting a
migration whose source is `actor_v2` likewise releases stale processing leases
and re-fences the unchanged source rows to the abort generation atomically.

Re-fencing must update the complete signed representation. If a canonical JSON
payload or digest embeds `ownership_generation`, changing only its relational
column corrupts the durable identity and is forbidden. Prefer keeping mutable
lease/fence metadata outside immutable ingress payloads; compatibility rows
which embed it require an atomic payload-and-digest rewrite.

Terminal external-action receipts (`succeeded`, `abandoned_before_dispatch`,
and `unknown`) are execution history, not live owner state. They retain the
generation under which the platform action may have happened. Migration must
reject or explicitly settle `prepared`/`executing` actions; it must not rewrite
terminal evidence to imply that a later owner performed it. Global idempotency
identity continues to deduplicate that historical action across generations.

Until that manifest and re-fencing transaction exist, stateful legacy-to-Actor
or Actor-to-legacy migration is unsupported. The repository may activate
`actor_v2` only for a new empty session; deleting source state and calling the
result a migration is not a lossless compatibility path.

Repositories which atomically relay an outbox record into the actor mailbox
must validate `active(actor_v2)` ownership using their existing SQLite
connection before inserting the mailbox event. This validation never creates
or changes ownership and may optionally fence on the expected generation.

Actor ownership may be enabled for production traffic only after all of the
following are true for that ownership mode:

- public signals, timer events, workflow completions, and management commands
  enter through the durable mailbox;
- scheduler state, unread/high-priority inbox mutations, operations, schedules,
  journals, and outbox effects share one commit boundary;
- the actor store, effect store/executor, and handler registry share the same
  immutable `EffectContractAuthority` instance; stores cannot replace that
  binding after construction, and its exact graph is revalidated across every
  activation await and effect execution boundary;
- actor and effect stores expose the same persistence-domain identity, and the
  executor's post-settlement wake target is the exact registry owned by the
  activation harness;
- mailbox and schedule-journal logical-key columns have passed a canonical
  storage-class audit, and the exact raw-key expressions used to reject SQLite
  TEXT/BLOB/numeric aliases have matching indexes rather than table scans;
- no coordinator retains a direct scheduler mutation callback;
- model/workflow handlers cannot perform externally visible actions before an
  accepted mailbox completion creates a dedicated action effect;
- every external effect has a lease token, an idempotency key, a bounded retry
  policy, and a deterministic completion event id;
- every send, poke, reaction, and future external action has a durable receipt;
  ambiguous platform outcomes remain `unknown` until reconciled and are never
  retried merely because a process restarted;
- recovery can rebuild or settle every non-idle aggregate without calling a
  legacy transition shortcut;
- every typed `RecoveryRequested` emitted by the scanner is consumed through a
  fenced reducer intent and commit coordinator that deterministically rebuilds
  or settles its non-idle aggregate, while legacy
  `session_actor_recovery` rows have an explicit terminal policy;
- management reads use the same profile-scoped actor state that runtime writes;
- migration tests prove that the same external session id can be owned by two
  profiles without sharing state.

Until the gate is satisfied, v2 code is an inactive persistence and reducer
foundation. Compatibility code may translate inputs and outputs at the actor
boundary, but it may not mutate legacy state as an actor effect.
