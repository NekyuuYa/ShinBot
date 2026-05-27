# ShinBot E2E Tests

E2E tests exercise user-like backend flows with real runtime wiring and
temporary data directories. They are slower than unit/API/integration tests and
are intended for release checks, manual CI runs, and regressions that need the
whole message path.

## Platform Simulation

`tests/e2e/platform_sim/` is the current E2E lane. It starts a real `ShinBot`
instance, registers an in-process simulated message platform, emits platform
events from JSON fixtures, and asserts observable output:

- outbound messages and adapter API calls;
- session creation and routing status;
- persisted message logs;
- Agent entry and scheduler state;
- model-runtime audit records.

Every scenario fixture must be registered in
`tests/e2e/platform_sim/manifest.json`. The manifest is the coverage index: it
records the scenario area, tags, and purpose, and tests fail if a fixture is not
registered.

Each run also writes a structured trace to `data-dir/e2e-traces/<scenario>.json`
with step/action events, state snapshots, and expectation analysis results.
This makes failures easier to inspect without re-running the scenario in a
debugger.

## Running

```bash
uv run --group dev python -m pytest -m e2e
uv run --group dev python -m pytest -m e2e --e2e-tag agent
uv run --group dev python -m pytest -m e2e --e2e-scenario "agent_review_*"
uv run --group dev python -m pytest tests/e2e/platform_sim
```

`--e2e-tag` can be repeated and works as an AND filter. `--e2e-scenario`
accepts exact names or shell-style globs against the manifest name, fixture
stem, or fixture filename.

In GitHub Actions, run `Python CI` manually and set `e2e_scenario` or
`e2e_tag` to pass the same filters to the platform simulation job.

## Adding A Scenario

1. Add a declarative JSON fixture under `tests/e2e/platform_sim/fixtures/`.
2. Register it in `tests/e2e/platform_sim/manifest.json` with a clear area,
   tags, and purpose.
3. Keep assertions focused on externally visible behavior. Prefer `expect`
   blocks over adding Python-specific checks to the harness.
4. Extend `fixture_schema.py` and its schema tests when a new fixture field is
   needed.

Use this layer for full message-path behavior. Keep pure parser, policy,
repository, and workflow edge cases in unit or integration tests.

For state-machine scenarios, prefer real message steps plus fake model responses.
Message steps may use `actionsBefore` or `actionsAfter` to interleave timer
signals with platform events, for example `message -> review_due -> message`.
`modelRuntime.fakeCompletion` can provide `texts` and per-call `toolCalls` so
workflow runners exercise the same model-runtime path without paid API calls.
Use `waitAfterSeconds` when the scenario should prove that a real background
timer, rather than a manually injected signal, advances the state machine.

## Startup Smoke

`tests/e2e/startup/` launches `main.py` in a subprocess with an isolated
`--data-dir`, waits for the management API to serve `/api/openapi.json`, then
shuts the process down. Use this lane for regressions that only appear through
the real process entrypoint, boot controller, API app, and database migration
sequence together.
