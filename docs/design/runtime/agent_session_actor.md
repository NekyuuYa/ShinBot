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
as its ingress wake target. Core also does not infer a durable actor consumer
from arbitrary runtime attributes during normal runtime mounting; only a future
explicit lifecycle controller may bind one after every activation gate is
proven. This deliberate boundary prevents legacy and v2 from becoming
concurrent writers while the remaining end-to-end adapters are finished.

The Actor-native Active Chat v3 workflows described below are likewise
diagnostic-only assembly components. They do not replace or take ownership from
the legacy `ActiveChatCoordinator`/`ActiveChatFastRunner` path. `AgentRuntime`
now constructs one private, inactive Actor v2 assembly per database domain. It
contains the registry, executor, typed recovery scanner and its supervisor,
review-due scanner supervisor, commit coordinator, no-replay materializers, and
a strict profile-aware handler graph, but it does not schedule scans, start
workers, change ownership, or publish an ingress wake target. Its public
diagnostics surface is a read-only readiness snapshot,
including the authenticated `GET /api/v1/agent-runtime/actor-v2/readiness`
endpoint: it does not expose the scanner, registry, executor, stores, or
handler registry. The endpoint also returns stable `activationBlockers`; this
is deliberately more specific than `activationPermitted=false`, so a complete
clean-session handler graph cannot be mistaken for a deployable cutover. Its
`closed` and `shutdownComplete` fields are intentionally distinct for the same
reason: a shutdown request is not evidence that actor workers have stopped.
The graph registers the 24 actor-native contracts that have real
implementations and rejects unknown durable profiles instead of falling back to
the default bot configuration. The 13 remaining historical Active Chat and
control/reconciliation contracts remain deliberately unbound, so production
`actor_v2` ownership stays disabled until actor-native control semantics,
historical isolation, ingress routing, scanner supervision, and the ownership
cutover protocol are complete.

The invariant and workflow sections below therefore use two meanings:

- **Target contract** describes the required behavior after activation.
- **Implemented slice** describes reducer/outbox behavior that is already
  durable and testable in isolation, but is not yet reachable from production
  ingress.

The current reducer dispatches only its implemented event kinds.
`ManualReviewRequested` now has a diagnostic-only reducer and schedule-claim
admission path; it is not mounted into the management API or live runtime.
Pause/force-idle events described by the target contract have not yet been
added to that enum. They remain activation work, not production capability.

### Implemented Inactive Handler Graph

There is one outer durable effect registry per database domain, not one per bot
profile. Each supported contract is wrapped by a strict lookup on
`SessionKey.profile_id`; profile removal or a malformed identity is an effect
failure before any model, tool, or adapter code runs. The default ingress
profile is therefore not a fallback for a persisted Actor effect.

The graph currently composes actor review/active reply, Active Chat v3
bootstrap/round, idle review planning, delayed control delivery, and
receipt-fenced external actions. It intentionally leaves the five v1/v2
control/reconciliation kinds and v1/v2 Active Chat workflow kinds unbound.
Those missing handlers are an activation gate, not a reason to install no-op
completions: a control completion would otherwise falsely assert that an old
runtime task had stopped. Historical Active Chat effects require a separate
shape-fenced maintenance terminalizer; current v2 controls require real
actor-native semantics before cutover. The terminalizer is not an executor
lane or an activation bypass: it isolates only proven never-claimed historical
rows and leaves all 13 contracts unbound.

The readiness snapshot reports both complete-history and clean-session handler
coverage. Complete-history coverage remains false while the 13 old shapes are
isolated. Clean-session coverage verifies the 24 contracts emitted by the
current actor-native reducer/handler graph and is currently complete. Neither
status is an activation permit: ingress, timer, recovery, ownership, and
management-command lifecycle gates remain independently closed.

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
transaction. The no-replay materializer registry, scanner, coordinator, and
unstarted recovery/review-due supervisors are constructed inside `AgentRuntime`'s
private inactive assembly. Their stopped health snapshots are visible through
readiness, but neither supervisor is scheduled at startup. Durable recovery is
therefore still an explicit activation blocker, not a live v2 feature.
Focused integration coverage does drive a scanner-produced typed delivery
through a test-owned registry, reducer, commit coordinator, and materializer;
that proof never uses `registry.recover()`, starts an executor, or mounts a
runtime wake target.

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

`ReviewDue` and diagnostic-only `ManualReviewRequested` are implemented in the
current reducer slice. A manual admission atomically verifies the active
Actor ownership generation, current aggregate plan, and scheduled plan row;
it then claims that schedule and writes one immutable mailbox event. The
request id is idempotent only for that exact admission proof, so it cannot
silently rebase across a plan or ownership generation. A later due scanner or
manual request observes the claimed schedule and cannot start a second review.
The scanner also redrives pending manual mailbox work after a post-commit wake
failure or restart. Generic mailbox enqueue rejects this event kind, and a
raw-key partial unique index prevents a second `(profile, session, request id)`
admission even through a SQLite TEXT/BLOB storage alias. The admission service
is intentionally not mounted into `AgentRuntime`, core ingress, or the
management API yet.

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
The current v3 active-reply contract keeps the v2 selection fence but extends
the bounded workflow budget to 180 seconds; v1/v2 records retain their original
one-minute policy for replay and recovery.
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
ManualReviewRequested
ActiveChatTick
ActiveChatBootstrapCompleted
ReviewCompleted
ActiveReplyCompleted
ReviewCancellationCompleted
ReviewExecutionUnknown
ModelExecutionUnknown
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

`ManualReviewRequested` is an implemented diagnostic-only mailbox branch.
`PauseRequested`, `PauseCleared`, and `ForceIdleRequested` remain part of the
target public-event surface, but are not yet live reducer branches.

`RecoveryRequested` is different: only a certificate-backed delivery from
`durable_session_recovery_scanner` reaches the typed reducer branch. It emits
a compact `RecoveryCommitIntent`, and the commit coordinator re-proves the
certificate before a state-specific no-replay materializer can settle work.
The legacy `session_actor_recovery` heuristic remains an explicit ignored
terminal path; it cannot be promoted into typed recovery evidence. Scanner
supervision, route-to-mailbox convergence, and production lifecycle mounting
are still required before any of this becomes live recovery behavior.

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

### Model Execution Liveness

Every model workflow has a second liveness boundary because an expired outbox
lease cannot prove whether the model provider received the request. Review
keeps its cancellation-aware `agent_review_execution_runs` protocol; active
reply, Active Chat bootstrap/round, and idle review planning use the parallel
`agent_model_execution_runs` protocol. Their execution witness is a one-way
state machine:

```text
no witness -> running -> finished / cancelled
                 |             |
                 +-- expired before effect settlement --+-> unknown
```

Every executor iteration performs expired-effect maintenance before claiming
new work. When an exact model effect lease has expired, the same SQLite
transaction changes its `running` or unsettled `finished` witness to `unknown`,
retains the outbox row as non-replayable, and writes one deterministic mailbox
event: `ReviewExecutionUnknown` for review or `ModelExecutionUnknown` for the
other model workflows. Each event carries the complete contract, operation,
claim, worker, attempt, timestamp, and reason. The actor validates it against
the current operation fence and records an `execution_unknown` blocker without
pretending the workflow operation finished.

An unknown witness blocks every model workflow for the same session in the
outbox claim query, including review, active reply, Active Chat bootstrap/round,
and idle review planning. It also blocks `ReviewDue`, `ManualReviewRequested`,
priority active reply, and late model completions in the reducer. Recovery-graph
discovery records a live generic witness as `waiting` and an unknown witness as
`blocking`; ownership migration rejects either `running` or `unknown` evidence.
A cancellation-control completion observes the same witness as a durable
blocker rather than retrying forever. Unknown work is never reclaimed,
replayed, refenced across ownership generations, or made terminal by a late
worker. Only explicit reconciliation backed by trustworthy provider or operator
evidence may clear it.

### Generic V3 Model Cancellation

`cancel_model_execution:v3` is the first Actor-native generic cancellation
protocol. It is opt-in by exact `(effect_kind, contract_version)` identity;
today only `run_idle_review_planning:v3` participates. Its target fence names
the operation, outbox effect, kind, contract version/signature, and ownership
generation. The reducer writes that fence, the superseded operation, the
control effect, and `agent_model_execution_cancellation_gates` in one actor
transaction.

The gate is durable authority rather than a signal to inspect a local task:

```text
requested  -> target is still processing; no witness or one running witness
cancelled  -> target outbox is cancelled; one remote witness is still running
terminal   -> target is terminal and no witness, or its one witness finished
blocked    -> target remains processing with one unknown witness
```

The effect claim query excludes every gated target. Before a claimed model
handler creates a task, the generic witness checks the gate in the same SQLite
transaction. That closes the claim-to-task-start race: an unstarted target is
cancelled without a task, while an already witnessed task may unwind but cannot
settle its model result after supersession. The control handler retries only
while a real witness is running; a missing or unknown witness is never treated
as successful cancellation. `blocked` is reported to the reducer as a global
model-work blocker.

Recovery projects unresolved generic gates as first-class graph nodes and
connects each to its target operation, target outbox effect, and control effect.
`requested` and `cancelled` are waiting authority; `blocked` is a recovery
blocker. Ownership migration accepts a gate only after its terminal target,
control effect, and optional finished witness still match the frozen fence.

The historical v1/v2 `stop_active_chat_runtime`,
`cancel_idle_review_planning`, and reconciliation effects are not a valid
foundation for new cancellation behavior: they do not carry this durable model
start witness or gate protocol. They remain historical maintenance inputs only.

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

### Historical Effect Isolation

`HistoricalEffectTerminalizer` is an explicit maintenance API, not a worker,
executor lane, recovery materializer, or background scanner. A caller names
one `(SessionKey, effect_id)` after inspection; the terminalizer obtains a
single SQLite write transaction and re-proves all of the following before it
changes anything:

- the outbox row is still `pending`, has `attempt_count == 0`, an empty claim
  and worker, no lease, no prior error, and no terminal timestamp;
- active `actor_v2` ownership still has the row's exact generation;
- the built-in contract identity and signature are exactly one of
  `run_active_chat_bootstrap`, `run_active_chat_round`,
  `active_chat_runtime_reconciliation`, `stop_active_chat_runtime`,
  `cancel_idle_review_planning`, or
  `idle_review_planning_cancellation_reconciliation`, at v1 or v2;
- the aggregate, operation fence or control intent, operation record, state,
  plan, epoch, activity generation, input boundary, and effect identity still
  form the expected historical shape; and
- the same ownership generation has no live mailbox event, route-outbox row,
  or external-action receipt.

On success it changes only that outbox row to `failed` with
`historical_effect_never_claimed_terminalized`, then writes a compact audit
row to `agent_historical_effect_terminalizations` in the same transaction. It
does not emit `EffectQuarantined` or any completion, wake an actor, claim the
effect, alter an aggregate or operation, invoke `registry.recover()`, or claim
that a legacy task stopped. A repeated request returns the existing audit
record without another write. Any mismatch, including a processing row or an
expired processing lease, is rejected and left unchanged.

`cancel_review_workflow:v1` is deliberately excluded. Its review execution
witness has separate liveness semantics; a missing handler, an expired lease,
or a cancellation request is a blocker, never evidence that it is safe to
terminalize.

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
Actor slice. They do not activate `actor_v2` ownership: the no-replay recovery
materializers are wired only into an unstarted private assembly, and full
production `AgentRuntime` composition is still an explicit production blocker.

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
commit-time proof, and concrete state-keyed no-replay materializers are
implemented and integration-tested. `AgentRuntime` constructs them in an
unstarted private assembly, but does not schedule the scanner or activate the
registry. Do not activate Actor v2 ownership until legacy rows have a fenced
terminal policy and runtime assembly proves
scanner-to-mailbox-to-registry convergence for every supported shape.

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

Discovery uses a conservative state shape. A recoverable root must have the
matching pending/running operation, complete operation fence, exact terminal
workflow effect set, no external-action receipt, no unrelated live operation,
and no pending mailbox/effect/route work. Review and generic model execution
witnesses are projected as first-class graph nodes: a running witness is a
wait condition and an unknown witness is a blocker. Review and active reply additionally
reject stale resume state; Active Chat distinguishes a pending bootstrap from a
completed-bootstrap round with its completed due-control history; settling
accepts only a fully fenced `idle_exit` plus completed exit-control history.
Completed bootstrap without a remaining round/control root is `no_recovery`.
Missing, terminal, wrong-kind, malformed, oversized, or ambiguous authority
records create a typed blocker or an operator-visible finding. Executing or
unknown external actions are blockers and are never replayed. A delivery is
preflighted by its raw `(profile_id, session_id, event_id)` logical key both
before and after insertion; its complete typed envelope and payload are decoded
again before a case cycle can advance.

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
case, or mailbox write on that path. The scanner and materializer registry are
wired into the private assembly, but scanner supervision and
route-to-mailbox-to-ledger convergence proof remain absent; the presence of
these components is not an authorization to activate recovery in production.

The implemented materializers never recreate uncertain model, tool, adapter, or
external-action I/O. For a certificate-approved `review`, `active_reply`,
`active_chat` bootstrap/round, or `active_chat_settling` planner root, they mark
the root operation `failed`, remove only its operation fence and the certified
completed control/exit metadata, preserve every unread ledger entry, and create
a new fixed-default review plan. The transition records the no-replay reason;
it does not persist the certificate itself. A shape outside this whitelist is a
durable blocker, not a broad forced transition to idle.

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

### Clean-Domain Canary Prerequisite

`ActorRuntimeActivationScope.CLEAN_SESSION` is a future canary prerequisite,
not a production authorization or a migration mechanism. It retains the full
immutable effect authority for decoding and audit, but its executable graph is
limited to the current Actor-native contracts. The historical v1/v2 Active Chat
and control contracts remain unbound and cannot be silently treated as safe
no-ops.

The clean graph is an explicit contract allowlist, not "all contracts except a
known historical blacklist." Any new complete-authority contract must be
classified as either canary-executable or historical/otherwise blocked; an
unclassified or overlapping reference fails composition before workers start.

Before that scope can start a registry or effect executor, a read-only SQLite
preflight must inspect the exact same persistence-domain object as the actor
store and effect store. It rejects a domain containing any of the following:

- current, pending, or historical `actor_v2` ownership evidence;
- aggregate, mailbox, operation, ledger, schedule, journal, effect, recovery,
  execution-gate, external-action, or route-outbox residue;
- an unavailable or malformed durable table/query.

Schema initialization's zero-valued `agent_effect_scrub_state` cursor is an
inert singleton and is accepted. A cursor that has advanced beyond zero is
durable effect-history evidence and blocks the canary.

The preflight is intentionally domain-wide rather than a per-session cleanup
check. It makes a future empty-domain canary unable to inherit ambiguous work
from a prior Actor v2 attempt. Legacy-only ownership history does not by itself
fail this check, but it does not grant Actor v2 ownership either.

Passing the preflight only proves that the harness may be considered by a
future lifecycle controller. It does not acquire ownership, serialize against
new ingress, start scanner supervisors, route a timer, publish a wake target,
or permit any management command. Those remain separate activation-gate
requirements, and the current production runtime does not construct a clean
scope or call its activation method.

A clean-scope harness also skips `registry.recover()` and expired-effect claim
recovery before and during worker polling. Empty-domain proof is not restart
recovery authority: a durable mailbox, operation, or effect discovered at that
point is a lifecycle violation for the future controller to block, not work the
clean canary may repair or execute.

`DurableRoutingService` deliberately has no broad-recovery callback, including
one that accepts a `LegacyRecoveryPermit`. A routing pass ends before it can
prove that a target has stopped every actor it created, so lending a permit to
that callback would allow a detached actor to observe a later fenced mailbox.
The service may only issue an exact key wake for a known unfenced route debt.
Historical broad recovery needs a separate controller that holds the permit
until its registry and every actor have proved shutdown.

The first actor-only controller is now the unmounted
`LegacyRecoveryActorLifecycleController`. It acquires the durable permit,
performs permit-aware discovery and actor startup, and stops the registry before
releasing the permit even when its caller is cancelled. It does not start effect
workers, publish a wake target, accept ingress, or constitute complete-history
runtime activation; those responsibilities remain with a later full lifecycle
controller.

`ActorRuntimeHistoryLifecycleController` is that next lifecycle boundary, but
it is also deliberately unmounted. It accepts only a
`COMPLETE_HISTORY` harness with a complete handler graph and a same-domain
legacy-recovery gate, retains the permit while permit-aware actor recovery and
the durable effect executor run, and uses the harness stop order (executor,
then registry) before releasing the permit. A partial diagnostic harness is
rejected before it can acquire the permit. A startup failure or caller
cancellation follows the same stop-before-release path. This closes the
lifetime gap where a recovered actor or effect worker could outlive a short
discovery callback and later run beside a fenced mailbox.

The durable gate is bidirectional: while this controller owns
`LEGACY_RECOVERY_ACTIVE`, a new fenced admission is rejected; an already
`FENCED_ONLY` domain likewise prevents the controller from starting any
historical actor or executor. There is no ordering-dependent path that lets the
two worker populations overlap.

This controller is not production Actor v2 activation. It publishes no ingress
or ownership cutover, no fenced wake target, no timer/recovery scanner
supervision, and no management admission surface. A future cutover controller
must still establish and continuously verify those boundaries before it can
mount a complete-history runtime. Until then, this controller is test and
diagnostic lifecycle infrastructure only.

`AgentSessionActorRegistry.recover()` now rejects the SQLite store outright.
It remains available only to in-memory test doubles that have no durable gate or
actor-fence history. This prevents a future caller from bypassing the controller
through a superficially convenient registry method.

The first controller layer is now an unmounted
`ActorV2CanaryLifecycleController`. It accepts only a clean-session harness and
an active same-domain `ActorV2CanaryIsolationLease`; it verifies that lease
and durable-domain identity before and after harness activation, stops the
harness before releasing the lease, and closes rather than proceeding if either
proof disappears at an explicit lifecycle boundary. Its
`verify_active_isolation()` guard is deliberately not a background lease
monitor: a future cutover controller must invoke it before every later
ownership or ingress transition and provide durable lease-revocation delivery.
`ActorRuntimeHarness.closed` means only that shutdown was requested; releasing
an isolation lease requires the stronger `shutdown_complete` proof that both
the executor and registry returned from their stop paths. If either stop or
lease release fails, the controller remains `FAILED`, retains the lease, and
allows only an explicit `shutdown()` retry. It never resumes activation from
that state. Lease release must therefore be idempotent and retry-safe.
It has no ownership repository, route service, registry, timer, scanner, or
management API publication path. The concrete durable lease now exists, but a
later ownership/ingress cutover controller remains required before this can
become a production feature.

`ActorV2CanaryIsolationLeaseRepository` now owns one domain-wide, durable
canary slot. Its opaque-holder-token grant has a monotonically increasing epoch
and is deliberately non-expiring: a live harness cannot be safely distinguished
from a slow or partitioned holder by elapsed time alone. The active epoch can
end only through its holder's stop-proof release or an explicit, exact-epoch
operator revocation after an external stop proof. Neither a stale token nor an
older epoch can release or alter a replacement holder. The
`SQLiteActorV2CanaryIsolationLease` adapter gives the unmounted controller this
durable proof without exposing the token in lifecycle snapshots.

While that slot is active, the database rejects new Actor v2 admission and
ownership claims, every Actor-related generic migration boundary, transactional
Actor-owner validation, and broad legacy recovery. Canary acquisition likewise
rejects active broad legacy recovery. This freezes competing durable Actor work;
it does not make a dirty domain clean. The clean-session preflight remains the
separate proof that no prior Actor v2 state exists before a canary harness may
start.

### Admission Fence and Remaining Isolation Lease Work

The repository now has a session-scoped durable admission fence, but it is not
yet a production isolation lease or a canary activation mechanism. The row
records the profile/session scope, opaque holder-token digest, fence id and
generation, lifecycle state (`reserved`, `committed`, or `revoked`), and an
expiry. Reservation history is intentionally not recycled: a lost, expired, or
revoked holder must use a new clean domain rather than silently taking over the
same session.

The implemented data-plane protocol is deliberately narrow and atomic:

- a first `legacy` claim checks the fence in its `BEGIN IMMEDIATE` transaction;
  while fence history exists it raises a token-free reservation result instead
  of selecting legacy;
- `MessageIngress` catches that result, persists a scoped routing job with
  ownership generation zero plus the fence id/generation, and returns before
  pre-route hooks, route targets, or legacy Agent callbacks run. The job insert
  re-reads the fence in its own transaction: if the owner committed after
  ingress observed `reserved`, it writes the current owner generation directly
  instead of leaving a generation-zero job after the retarget pass;
- the first fenced `actor_v2` claim verifies the holder token and live
  `reserved` state, writes matching fence fields to ownership, commits the
  fence, and retargets only matching pending reserved jobs to ownership
  generation one in the same SQLite transaction;
- routing-job selection, route-decision/outbox insertion, outbox selection,
  and mailbox relay all require a live `committed` fence, matching active
  `actor_v2` ownership, and matching fence id/generation. Expiry or revocation
  leaves durable work blocked; it never falls back to legacy automatically.
- ReviewDue dispatch, manual-review admission, recovery discovery, and
  ReviewDue wake-debt selection revalidate the same active owner in their
  write/read boundary. A revoked or expired fenced owner cannot claim a
  schedule, create a manual/recovery mailbox row, advance a recovery case, or
  keep producing bare-key wake attempts. Unfenced Actor v2 test/history rows
  retain their existing active-owner behavior.

Fence identity is relational metadata rather than part of the immutable ingress
payload, so the ownership transaction can retarget a reserved job without
rewriting its signed replay input. The owner repository also revalidates a
committed fence whenever fenced Actor ownership is used. Its generic migration
API rejects both a fenced Actor-to-legacy transition and an unfenced migration
to Actor v2 while fence history exists; those operations require a future
dedicated cutover protocol instead.

The durable canary lease covers only dormant clean-canary isolation. A dormant,
single-request `FencedMailboxHandoffTarget` now consumes an explicitly acquired
wake-target lease, but no publication controller creates it automatically or
binds it to production ingress. There is still no adapter pause, timer/scanner
startup, management action, or runtime ownership cutover wired to the fence.
The current `ActorV2CanaryLifecycleController` is still unmounted.
`MessageIngress` now derives the same bot-scoped ownership key before a
lock-avoiding legacy `wait_for_input` consume: a reservation, Actor owner, or
migrating owner persists durable work and leaves the legacy Future untouched.
Timed-out or cancelled waiters also unregister themselves rather than consuming
a later message. The local registry now gives every framework waiter an opaque
tokenized lease bound to both its legacy base session and canonical routing
identity. A scoped ingress consume must match that identity exactly; an
unscoped compatibility Future can be resolved only through its explicit legacy
API and cannot be consumed by normal ingress. A mismatch, a frozen base
session, or a waiter that ended before its handler cleanup is persisted with a
diagnostic skip and never waits for the legacy session lock.

The registry also exposes a process-local `freeze -> await_quiescent -> thaw`
protocol for a future controller. Freeze is intentionally base-session-wide,
because legacy handler serialization is only base-session-wide. It rejects new
waiters, cancels the open Future, and snapshots its owner task; quiescence is
positive only after the exact lease was released and its owner task exited. A
compatibility waiter, a callback with no owner task, self-drain, timeout, or a
freeze epoch replaced during the wait returns a negative receipt. A receipt is
therefore evidence only for this process's interactive handler, not for
cross-process ingress, plugins, the legacy scheduler, or the adapter.
`thaw` independently rechecks that same snapshot and refuses to reopen the
base-session slot while a handler remains alive, even if that handler already
released its input lease.

This is only an earliest-admission guard and local drain primitive, not a full
quiescence proof for a production cutover. A future controller must still
freeze and drain the relevant local handlers, coordinate an adapter ingress
pause across processes, then acquire/commit durable ownership and publish a
fence-aware wake target. Revocation must eventually fence ingress and
publication, stop the harness, prove `shutdown_complete`, and only then release
a future broader lease. A failed stop or release must remain operator-visible
and blocked, never degrade into legacy execution.

The legacy runtime also exposes an unmounted
`LegacySessionLocalTaskQuiescer` builder on each runtime profile. It composes a
fixed current-process snapshot of the session's active-chat timer, primary
review dispatcher task, review-coordinator background tails, active-chat
semantic-wait/round tasks, and an in-flight per-session review-due dispatch
child. Each owner cancels and awaits only its known task objects;
cancellation-resistant tails remain named in the report rather than being
treated as complete. This helper deliberately excludes the shared review-due
poller loop, scheduler state, unread persistence, session locks, ingress,
adapters, other processes, and external model/tool effects. It is not mounted
on an API, timer, recovery loop, ingress route, or Actor v2 controller, and its
positive report is not a durable cutover receipt.

Task ownership is profile-local while the legacy lock and ingress identity are
base-session-wide. Therefore `AgentRuntime` also exposes
`build_legacy_base_session_local_task_quiescer()`, which composes the default
profile and every configured bot profile for the same base session. A future
controller cannot drain only the profile named by its target `SessionKey` and
declare the shared legacy session clean: another binding may still have a
review tail, active-chat round, or timer dispatch for that same base session.
This remains a current-process observation only.

The runtime now has a separate unmounted signal-admission primitive:
`freeze_legacy_session_signal_admission`,
`await_legacy_session_signal_quiescent`, and
`thaw_legacy_session_signal_admission`. It wraps
`AgentRuntime.handle_agent_signal` before the per-session lock, so a signal
already waiting on that lock remains visible to local drain. After a freeze,
new legacy signals fail closed instead of starting new scheduler work; existing
pre-freeze calls must exit before the receipt is positive. This gate is what
makes a subsequent task snapshot meaningful. It does not persist or reroute a
rejected signal, so it must follow a durable core-ingress admission boundary;
an adapter pause is only needed when a controller also requires an upstream
transport proof. Review-due and active-chat timers inspect the same
read-only gate and skip frozen sessions, so an intentional local lifecycle
freeze is not converted into repeated timer-supervision failures.

`MessageIngress` now has a separate unmounted local ingress participant:
`freeze_legacy_ingress_session`, `await_legacy_ingress_quiescent`, and
`thaw_legacy_ingress_session`. The freeze ticket is bound to one `cutover_id`
and is idempotent only for that operation. Its freeze must follow a durable admission
reservation. Pre-freeze ingress coroutines and direct route-target tasks inherit
one local epoch and must exit before the receipt is positive; a route task
scheduled by a pre-freeze ingress coroutine after the freeze is still included.
Post-freeze ingress is allowed only when existing ownership or an admission
fence makes it durable; it performs a read-only ownership check and refuses to
create a new legacy ownership row. This gives a future controller a local
participant that does not silently lose the dispatch tail between ingress and
Agent scheduling. It still does not pause adapter callbacks, drain adapter
queues or another process, cover durable-routing replay, or discover arbitrary
tasks spawned by plugins and route handlers. It is not an adapter pause receipt
and is not wired into production cutover. Like the legacy session lock, this
participant is base-session-wide: a future controller cannot independently
freeze one bot binding while allowing another binding sharing the same base
session to continue legacy ingress.

`AgentRuntime.build_legacy_session_local_drain_participant(ingress)` combines
these local pieces without mounting a cutover controller. Its request binds a
base session, canonical waiting-input scope, and `cutover_id`. It first makes a
no-yield local preflight of current component tickets and any live waiter scope,
then freezes ingress and waiting input. It waits for pre-freeze ingress and direct route targets before
freezing Agent signal admission, waits for those direct signal calls, drains
every profile's known tasks, and finally confirms the waiter cleanup. That
ordering matters: a route target admitted before ingress freeze may not yet
have entered the Agent runtime and must be allowed to finish its legacy call
before the signal gate closes. A negative ingress or signal receipt skips the
later task snapshot rather than reporting a race-prone partial drain as clean.
An incompatible bot scope, an unmanaged compatibility waiter, or a completed
owner task that still retains a waiter lease is rejected during preflight before
the participant changes ingress state. The aggregate receipt can thaw all three local gates only when every component
is positive. It is still not journal evidence, an adapter pause receipt, a
durable routing/replay barrier, or authority to commit Actor v2 ownership.

### Required Production Cutover State Machine

The missing production controller must be a durable state machine, not a
sequence of best-effort calls across the admission fence, local waiter registry,
and wake-target lease. One cutover epoch is bound to exactly one `SessionKey`,
the corresponding legacy base session, and the adapter instances that can emit
that session's ingress. Its durable journal must retain only opaque capability
digests, immutable identities, phase transitions, and token-free proof
summaries; raw holder or adapter-pause capabilities never enter diagnostics or
application logs.

The required forward path is:

```text
preflighted
  -> admission_reserved
  -> legacy_quiesced
  -> actor_owner_committed
  -> target_published
  -> ingress_resumed
```

`preflighted` proves both that the target session has no Actor v2 residue and
that its legacy scheduler state, unread ranges, active-chat/review work, and
local waiter state are eligible for a new-session cutover. It is not enough to
observe an idle scheduler row: the controller needs positive core-ingress and
task quiescence from every process that can enter this session. The existing
`admission_reserved` path is one durable ingress fence for clean sessions; a
future live migration fence must provide the same earliest durable routing
boundary for an already-legacy owner. `legacy_quiesced` records that exact
boundary plus every local freeze receipt. Missing, expired, or non-matching
proof keeps the epoch blocked.

The currently executable reservation path is deliberately narrower than this
future state machine: `ActorV2AdmissionFenceRepository.reserve()` rejects any
existing ownership row, while generic ownership migration rejects an unfenced
legacy-to-Actor transition and a fenced Actor-to-legacy transition. In other
words, the current system can prepare only a new empty-session canary. The
local drain participant and adapter capability inventory do not silently widen
that into live legacy migration; a dedicated controller must add the durable
migration reservation, process-wide drain proof, and lossless source-state
manifest before that path becomes legal.

`actor_owner_committed` atomically consumes that exact reservation, commits the
Actor v2 ownership generation, and retargets only its buffered routing jobs.
`target_published` requires a live exact wake-target lease, an activated
fenced registry/executor, and a dispatcher binding for the same ownership and
admission-fence incarnation. Only then may `ingress_resumed` release an
optional adapter pause or enable replay of jobs buffered behind the core
ingress barrier. Timer/recovery supervisors and management mailbox admission
must use that same incarnation; a bare `SessionKey` wake, a best-effort
callback, or a process-local registry cannot satisfy the transition.

Every phase has a durable `blocked` terminal-safe branch. A crash, lost lease,
failed adapter drain, failed target retirement, or ambiguous external action
must preserve the fence and journal evidence, stop local Actor work where a
stop proof is available, and require explicit operator recovery. It must never
reopen legacy automatically or reinterpret a later owner as the original
cutover. A replacement epoch is allowed only after an external stop proof for
the prior holder, using fresh admission and target identities. Reverse cutover
is a separate state machine with a lossless migration manifest; it is not the
inverse of these transitions.

The durable journal storage contract is now implemented, including immutable
phase events, token-free evidence digests, exact proof-kind validation, and a
same-transaction recovery-gate fence when a preflight is recorded. It is not a
controller and does not authorize any production transition by itself. Outside
the explicit clean-session atomic methods below, it does not supply a
source-boundary proof, legacy task-quiescence receipts, target activation,
timer/recovery supervision, or ingress resume.

For the narrow clean-session path, the owner-commit gap is now closed by
`commit_clean_actor_owner_and_record()`. It calls the clean-only
same-transaction ownership claim inside the journal's SQLite writer transaction,
consumes the exact reserved admission grant, commits the fence and buffered
routing retarget, appends the immutable `actor_owner_committed` event, and
updates the journal phase together. A phase-write failure rolls every one of
those mutations back, so a crash cannot leave a committed generation-one Actor
owner with a journal still at `legacy_quiesced`. This primitive rejects existing
ownership and does not perform live legacy migration, publish a target, or
resume ingress; it is a necessary atomic boundary for a future clean controller,
not that controller itself.

The preceding clean reservation has the same guarantee through
`reserve_clean_admission_and_record()`: it creates the opaque holder grant and
the token-free `admission_reserved` journal event in one writer transaction.
If the phase cannot advance, no reserved fence remains. Together these two
methods remove the durable split between `preflighted`, admission reservation,
and generation-one owner commit, while deliberately leaving the externally
observed quiescence, target activation/publication, and ingress-resume phases
outside this clean persistence slice.

The state machine and its controller are therefore still not mounted. No
adapter lifecycle currently registers cross-process ingress membership or
services a durable drain request, and the legacy scheduler has no production
per-session durable quiescence receipt. These remain explicit prerequisites;
the journal does not remove readiness blockers or make Actor v2 ownership safe
to activate.

### Core Ingress Barrier Reassessment

The required source boundary for normalized `message-created` traffic is
smaller than a generic adapter pause. Production adapters enter
`ShinBot.on_event()` and then `MessageIngress.process_event()`. That method
first classifies the message in the process-local legacy ingress registry. A
message admitted before a local freeze remains in the tracked pre-freeze task
set; a message that reaches core after the freeze is marked
`requires_durable_admission`, reads the durable ownership/fence row, and is
persisted as a routing job before any normal route target can execute. The
waiting-input fast path applies the same durable decision before it can consume
a legacy waiter.

Consequently, a message that was already queued by an adapter but calls
`MessageIngress` only after the durable migration barrier and every local
ingress freeze does not need an adapter-specific pause to avoid legacy
re-entry. The integration test
`test_local_ingress_freeze_uses_migrating_ownership_for_delayed_message`
proves this path: the delayed event becomes a pending job at the migrating
ownership generation and does not call the legacy Agent handler. The argument
is intentionally bounded to messages observed by normalized core ingress. It
does not claim recovery for an event an adapter had already dropped before
calling the application, malformed payloads rejected before ingress, or any
adapter bypass that does not use `ShinBot.on_event()`.

For a future live legacy-to-Actor cutover, the stronger proof shape is therefore
`durable ownership migration barrier -> every process's local core ingress and
legacy task drain -> Actor target publication -> replay of buffered jobs`.
`AgentRuntimeOwnershipStatus.MIGRATING` already causes ingress to persist jobs
and causes routing recovery to defer their execution, but the generic
`begin_migration()` API has no cutover holder capability, process-membership
snapshot, atomic journal binding, or controlled recovery path. It must not be
mounted as a production controller until those missing authorities are added.

`ActorV2MigrationBarrierRepository` now supplies the first missing ownership
authority as an unmounted primitive. `start_legacy_to_actor_v2()` requires one
exact active unfenced legacy generation and, in one SQLite transaction, enters
the irreversible broad-recovery gate, changes ownership to `migrating` with
pending Actor v2 mode, refences routing work, and persists a holder-token
barrier bound to that migration generation. Before making that transition, it
fails closed when the source has scheduler/review state, unread or
high-priority state, summaries, or another ownership row sharing the same base
legacy session. That preflight deliberately remains in place: the current
barrier start API has no declared materializer/activation plan bound before it
changes ownership, so allowing pre-existing state would still preserve ingress
while risking loss of decision context. A clean idle source is therefore only a
narrow canary candidate, not a live migration implementation. While the
barrier is active, generic `begin_migration()`, `abort_migration()`, and
`complete_migration()` reject the session; only the holder can use the
barrier's controlled abort path. A holder loss has no expiry-based rollback.
The row remains terminal after abort so a new controller cannot silently reuse
the old attempt.

`ActorV2CoreIngressDrainRepository` supplies the next unmounted primitive. An
active barrier holder can seal the exact active ingress participant membership
for every adapter in the barrier, and the
`ActorV2CoreIngressDrainProcessWorker` freezes and drains the shared local
legacy session once for every member held by one process incarnation. It
persists immutable, token-free pairs of `core_ingress_digest` and
`legacy_quiescence_digest`; after every frozen member has acknowledged, the
controller can confirm the request and derive the journal proofs. The cutover
journal now accepts `core_ingress_drain` together with `legacy_quiescence` as
the normalized-core alternative to `adapter_pause_drain`. Once a core-drain
request exists, the barrier cannot be aborted, preventing an ownership-only
rollback from orphaning a frozen local request.

`ActorV2LegacyStateHandoffRepository` is the next unmounted persistence
primitive. After that exact core request is durably drained, the barrier holder
can capture one immutable, versioned source manifest. Its source digest binds
the barrier and drain identities, both drain proof digests, scheduler state and
review plan, unread messages and ranges, high-priority events and recent
mentions, review summaries, prompt summaries, and the complete base-session
ownership scope. Version 1 accepts exactly one profile-scoped owner; a shared
legacy base session fails closed with its complete scope exposed as safe
diagnostic identity, rather than assigning shared legacy state to whichever
profile happened to migrate first.

For every captured unread message, the manifest also records exactly one route
delivery coverage outcome. When a canonical `agent_route_outbox` payload exists
for the same profile/session/message, its rule-independent mailbox payload is
frozen as `verified`; no retained payload becomes `missing`, and contradictory
payloads become `ambiguous`. A later finalizer may consume only `verified`
coverage. It must never fabricate the route identity missing from an older
legacy scheduler row.

The Actor-side `ActorV2LegacyStateSnapshotStager` then produces an immutable,
versioned target staging record from that manifest. It deliberately preserves
the source sections instead of copying them directly into Actor aggregate or
ledger tables. Legacy unread rows alone do not retain the complete
route-delivery identity required by the v2 ledger, and `missing` or `ambiguous`
coverage cannot be repaired by a field-for-field conversion. The staging record
is a complete input to a future semantic materializer and finalizer; it starts
no Actor, writes no live Actor state, and cannot complete ownership on its own.

The first semantic subset is explicit rather than implicit:
`ActorV2LegacyIdleStateTargetPreparer` accepts only an idle scheduler with no
active-chat or active-reply resume state and with `verified` delivery coverage
for every unread message. It emits ownership-unbound review-plan and ledger
seeds, preserving consumption flags and high-priority facts for a later
one-transaction finalizer. It retains recent-mention and summary sections only
as source evidence: the first finalizer rejects a non-empty section until a
dedicated Actor semantic materializer exists. `missing`, `ambiguous`, active
chat, active reply, or malformed delivery payloads produce stable blockers;
they never fall back to a synthetic message event. Active workflow migration
requires its own semantics-preserving materializer.

`SQLiteActorV2LegacyIdleStateFinalizer` is the corresponding unmounted
commit primitive for that narrow idle subset. It starts one SQLite writer
transaction by revalidating the exact barrier-holder capability, drained core
ingress request, immutable manifest, and exact versioned preparation record.
It recomputes the pure preparation output and requires byte-equivalent
canonical payloads before writing anything. It also requires an empty Actor
target and checks every retained live `agent_route_outbox` row against the
frozen rule-independent ledger payload, digest, and canonical delivery
contract. A missing row, a live lease, a completed relay, or any payload drift
blocks the entire transaction.

For a supported source, that transaction writes the initial idle aggregate,
the verified unread ledger entries, terminal migration-provenance consumption
records, and the review schedule plus its schedule journal. Legacy absolute
`next_review_at` is converted to a delay relative to the ownership commit
clock: future deadlines retain their remaining delay and overdue deadlines
become due at commit time. Pending route outbox deliveries already represented
by the seeded ledger are terminalized with
`legacy_state_handoff_materialized`; they cannot be relayed later as duplicate
Actor messages. The transaction then refences the Actor-owned rows to the
immediate active Actor v2 generation and completes the ownership transition.

Completion does not rewrite the original migration barrier row. Instead,
`agent_session_actor_v2_legacy_state_handoff_finalizations` is an immutable
sidecar bound to the barrier, manifest, materializer identity and version,
source and target digests, final ownership generation, reason, and commit time.
The repository projects its presence as the barrier's terminal `completed`
state, so a stale holder cannot abort or finalize the same source twice. Schema
startup validation and immutable insert/update/delete guards verify that the
sidecar names the exact completed Actor owner and materialization; the legacy
source rows remain historical provenance rather than mutable target evidence.

This is deliberately not a production cutover authorization. It does not
publish a fenced wake target, start a worker, activate a registry or executor,
resume ingress, replay buffered routing, supervise timers or recovery, or add a
management action. Its fail-closed subset still excludes non-empty recent
mentions, review summaries, prompt summaries, active workflow state, shared
legacy base sessions, ambiguous/missing route evidence, and overlapping or
suppressed legacy consumption. In addition, barrier start continues to reject
pre-existing legacy scheduler state because no live controller can yet bind a
declared materializer and recovery path before freezing the source. The
finalizer proves the target commit semantics needed by that future controller;
it does not make live legacy migration deployable today.

The barrier and core-drain worker deliberately still have no production
completion path. They do not deliver requests to other processes, start an
Actor target, publish a wake lease, atomically refence a materialized target, or
resume buffered routing. They are prerequisites for the future controller, not
authorization to call one from management or runtime lifecycle code.

Adapter pause-and-drain remains useful only as an optional stronger proof when
the operator needs to extend the guarantee upstream of normalized core ingress
or an adapter has another ingress path. It is not a replacement for the core
ingress freeze. Both evidence paths remain dormant until one supervised
controller binds their proofs to target materialization, publication, and
fenced routing resume.

The platform layer now defines the unmounted
`AdapterIngressPauseParticipant` contract and
`AdapterManager.inspect_ingress_pause_support()`. A valid participant is bound
to one adapter instance and process incarnation, returns an opaque local pause
ticket, drains only pre-pause callback work, and explicitly declares either a
durable buffer or upstream acknowledgement/flow-control guarantee for events
observed after pause. The manager surface is read-only: it never pauses traffic
or treats one process's participant as a cross-process proof. The default
`BaseAdapter` returns no participant, and the current Satori, QQOfficial, and
OneBot adapters therefore report `unsupported`. This is deliberate: their
existing callback/queue behavior cannot establish a no-loss cutover boundary.

### Durable Ingress Membership and Drain Evidence

`ActorV2IngressDrainRepository` now supplies the dormant durable control plane
needed before any process-wide pause can be trusted. A future adapter lifecycle
must register one immutable `(adapter_instance_id, participant_id,
participant_epoch)` membership before it accepts platform callbacks and retain
the returned local holder capability only in that process. Heartbeats are
advisory observations, not leases: a missed or old heartbeat never retires a
member, removes it from a request, or grants the controller permission to
assume the adapter stopped. Normal retirement requires the member's local
capability after adapter shutdown; an unresponsive member requires an exact
external stop proof. Neither terminal record can substitute for a missing
drain acknowledgement.

`begin_drain()` accepts only the exact live admission grant attached to a
journal in `admission_reserved`, including its immutable `cutover_epoch`. In
one writer transaction it snapshots every active member for every adapter named
by that journal and then closes
registration for those adapters. A participant that races registration either
wins and is in the snapshot or loses and is rejected; no late process can
silently bypass the boundary. The request can become `drained` only after every
frozen member uses its holder capability to record both an adapter-pause digest
and a legacy-local-quiescence digest. The stored acknowledgement contains no
pause ticket, holder token, or raw local receipt. The complete request derives
canonical token-free digests for the journal's `adapter_pause_drain` and
`legacy_quiescence` evidence kinds.

`ActorV2IngressDrainProcessWorker` is an optional stronger unmounted local
executor for adapters that can genuinely provide the pause contract. It groups
all request members with one `participant_id`, pauses every adapter owned by
that process incarnation, waits for every pre-pause callback set to be
quiescent, and only then invokes `LegacySessionLocalDrainParticipant` once for
the shared base session. It retains opaque tickets only in that process for
retry and writes one safe acknowledgement per adapter member. This ordering is
essential when a process owns more than one adapter: freezing local legacy
ingress after only the first adapter paused could otherwise reject or skip a
message still arriving through another local adapter. It is not required by the
core-ingress proof above and remains unavailable for the current built-in
adapters.

`DurableActorV2CoreIngressDrainService` now supplies the narrow request-delivery
piece for normalized core ingress. It polls the shared durable request table
only for `open` requests that contain an unacknowledged member for its exact
`participant_id`, then delegates to the process's existing
`ActorV2CoreIngressDrainProcessWorker`. The worker retains its local freeze
ticket across non-quiescent retries and can write acknowledgements only through
the opaque grants it already holds. A local exception records degraded service
health and leaves the request open; the service never confirms the request,
changes ownership, thaws ingress, or publishes an Actor target. This is durable
cross-process request delivery through the control-plane database, not proof
that every registered process has started a service.

Its discovery uses a main keyset cursor plus an independent bounded head-retry
cursor. A negative local drain or local exception requests a periodic head lap
without rewinding the forward cursor: continuous insertion therefore cannot
starve an older frozen request, and retrying that request cannot make newer
barriers disappear behind a reset cursor. The head lap is scheduling fairness
only; it does not fabricate a positive drain receipt or weaken member grants.

This remains an unmounted protocol, not a cutover controller. No adapter
lifecycle currently starts or supervises the local service, no adapter currently
implements the required lossless pause participant, and no resume protocol
verifies delivery of post-pause buffered events to the fenced Actor owner. A
participant heartbeat expiry is intentionally never treated as any of those
proofs. Until those execution paths are implemented and supervised under the
same fence,
`actor_v2_ownership_ingress_cutover_controller_unavailable` remains a real
blocker.

The initial clean preflight is deliberately domain-wide, while a production
canary eventually needs a narrower session/admission scope. `persistence_domain`
object identity remains only an in-process composition check; a future broader
lease must model both scopes explicitly and cannot infer them from a bot id, a
legacy session id, or a process-local database object. Until target publication
and every remaining asynchronous boundary consume the same fence, Actor v2
remains unmounted.

### Incarnation-Bound Wake Requirement

`FencedMailboxWakeRequest` identifies one immutable Actor ownership
incarnation: the `SessionKey`, ownership generation, and, when present, the
admission-fence id and generation are one value. It is not a hint that may be
collapsed to a `SessionKey` after a durable handoff commits.

The current `AgentSessionActorRegistry` is deliberately key-only legacy and
historical-recovery infrastructure. It has no `wake_fenced` or
`wake_handoff` implementation and must not be bound to
`DurableMailboxHandoffDispatcher`. Doing so would let an actor created for an
older generation remain reachable after ownership changes and potentially
observe a newer mailbox row for the same session key.

The actor and SQLite store now have a dormant ownership-binding path for this
future target: an explicitly constructed `AgentSessionActor` can retain one
`FencedMailboxWakeRequest`, and every ensure, recovery, claim, load, commit,
release, failure, and pending-work operation revalidates that exact generation
and fence in its own transaction. A lost binding stops the actor instead of
performing a key-only retry. `FencedSessionActorRegistry` is the matching
dormant lower-level supervisor: it keys actors by the full request and exposes
only `wake_fenced`. It has no bare-key wake and deliberately does not implement
`wake_handoff`, so `DurableMailboxHandoffDispatcher` cannot bind it directly.
Neither registry is constructed or published by `AgentRuntime`; the dispatcher
still has no production target.

`ActorV2FencedWakeTargetLeaseRepository` is the dormant durable publication
primitive for that future target. It leases one `MailboxHandoffTarget` under an
exact fenced request, records only an opaque holder-token digest, and advances
a lease epoch whenever an expired or released publication is replaced. The
same target incarnation cannot be reused for the replacement epoch. Acquisition,
renewal, and validation recheck matching active Actor ownership and a live
committed admission fence in the same SQLite transaction; a revoked or expired
owner can only release its old publication for cleanup. The table independently
prevents owner-identity rewrites, in-place target replacement, and history
deletion, so a later SQL maintenance path cannot silently reopen a prior target
incarnation. This repository is not a process-local registry and does not wake
an actor or claim a handoff.

`FencedActorExecutionBinding` now carries that lease capability beside the
exact owner request into both actor and effect execution. `AgentSessionActor`,
`FencedSessionActorRegistry.wake_leased`, and every SQLite actor-store
ensure/recover/claim/load/commit/release/fail/pending boundary validate the
same target lease transactionally. A released, expired, or replaced lease stops
the old actor instead of allowing it to retry under the still-live owner fence;
a replacement epoch creates a separately bound actor.

The same binding now fences `DurableEffectExecutor` as well. A scoped executor
must use `start_fenced`, validates the lease before it creates workers, cannot
perform broad expired-effect recovery, can claim only the exact
profile/session/ownership generation, and never drains the store-instance
maintenance-notification queue owned by unscoped recovery. It requires
automatic lease renewal and revalidates immediately before every handler task
starts. Every scoped effect lifecycle mutation (claim, renewal, deferred or
retry release, quarantine, completion, failure, shutdown release, and
availability inspection) validates the target lease in its SQLite transaction.
Review and generic model start/finish witnesses, external-action receipt
prepare/claim/renew/reject/unknown/success boundaries, and the final
pre-dispatch renewal receive the same capability. A target-lease loss therefore
stops the executor rather than scheduling another effect retry; a late
model/external outcome remains conservatively durable as existing unknown
evidence instead of authorizing a replay.

`FencedMailboxHandoffTarget` is the intentionally narrow consumer of these
pieces. It owns one complete `FencedMailboxWakeRequest` and one already-acquired
`MailboxHandoffTarget` lease, not a reusable `SessionKey` map. `activate()`
starts only the matching scoped effect executor. `wake_handoff(claim)` checks
the complete handoff id, mailbox identity, claim id/worker/attempt/lease,
target identity, current owner/admission fence, and current target lease in one
transaction; only then may it call `FencedSessionActorRegistry.wake_leased`.
The receipt has three deliberately non-interchangeable outcomes: `accepted`
settles the exact handoff, `stale` settles it only after the target proves that
the owner/admission-fence incarnation is gone, and `deferred` is non-terminal.
An unpublished, stopped, locally blocked, target-lease-lost, foreign, or
claim-raced target returns `deferred`; the dispatcher releases that exact claim
back to pending when its binding is still current, or otherwise retains the
lease until bounded redrive. It must never turn local target unavailability
into a terminal owner verdict. Lease renewal is deliberately caller-driven,
with no hidden target timer. After each accepted actor wake the scoped executor
is signalled, while its polling remains limited to the same request scope.

Retirement is also explicit: a caller first unbinds this identity from the pull
dispatcher, then calls `unpublish()`. `retire()` stops the actor registry and
effect executor, requires a local effect-handler quiescence report, and releases
the durable target lease only when no local handler remains. A cancellation
request alone is not quiescence: an interrupted handler that has not exited
leaves the target `BLOCKED` and retains the lease for a later retry. This is a
process-local stop proof plus existing durable model/receipt evidence, not a
claim that an already-issued external request can be retracted.

`DurableMailboxHandoffDispatcher.dispatch_pending()` can now be narrowed by a
complete expected `FencedMailboxWakeRequest`, not merely profile/session. Its
durable keyset query includes ownership generation and admission-fence id and
generation, and its continuation cursor records that exact filter. A new target
therefore cannot claim, defer, increment, or otherwise perturb an old sidecar
from the same session while it is redriving its own incarnation.

`FencedMailboxHandoffSupervisor` is the corresponding unmounted lifecycle
primitive for one already-composed target. It activates the scoped target,
renews the already-held publication lease, binds the exact dispatcher identity,
and periodically dispatches only the matching full fence scope. Lease renewal,
dispatcher binding changes, target-health loss, and typed dispatch failures are
operator-visible through its local health snapshot. A critical lease or binding
failure fails closed by unbinding, unpublishing, and retiring the target. Normal
shutdown uses the same strict `unbind -> unpublish -> retire` sequence and
never resumes ingress, changes ownership, creates a replacement target, or
settles work outside target receipts. The lease TTL must outlast a bounded
full dispatcher pass plus one polling delay, so a slow target timeout cannot
silently cross an unrenewed publication expiry.

This target remains a dormant integration primitive. No runtime lifecycle,
ingress route, adapter, timer, scanner, API action, or default runtime starts
the supervisor or binds it. A production controller must still coordinate the durable core-ingress
barrier across processes, acquire the ownership/admission fence, construct and
supervise this single-request target, renew or retire it before expiry, unbind it before
retirement, and keep failure states operator-visible. The key-only registry and
ordinary runtime assembly remain unsafe for fenced production traffic until
that broader controller exists.

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
