# ShinBot Test Governance

The test suite is a managed asset. Every test should be easy to classify, run
selectively, and audit before a commit.

## Layers

- `unit`: isolated module behavior with no real API server or cross-runtime boot.
- `api`: HTTP route and API contract tests, usually through `TestClient`.
- `integration`: cross-module behavior such as boot, plugin loading, routing,
  runtime wiring, persistence, adapters, and workflow orchestration.
- `e2e`: full user-like flows or near-production startup scenarios.
- `slow`: tests that are noticeably slower than the rest of their layer.

## Layout

Use the test layer as the first directory level and the subsystem as the second:

```text
tests/
  unit/
    agent/
    core/
    schema/
    utils/
  integration/
    agent/
    boot/
    core/
    plugins/
  api/
    routers/
    model_runtime/
  e2e/
```

API router tests belong in `tests/api/routers/`. Model runtime API tests stay in
`tests/api/model_runtime/` because that area has a larger API surface and its own
fixtures.

Pure unit tests should live under `tests/unit/<subsystem>/...`. Integration
tests should live under `tests/integration/<subsystem>/...`. Root-level
`tests/test_*.py` files are not allowed.

The suite applies a default layer marker during pytest collection based on file
path and filename. Prefer explicit `pytestmark = pytest.mark.<layer>` for new or
high-risk files. The audit script reports inferred files so they can be made
explicit over time.

## Commands

```bash
uv run pytest -m unit
uv run pytest -m api
uv run pytest -m "integration and not slow"
uv run python scripts/test_audit.py
```

For frontend changes, also run:

```bash
cd dashboard && pnpm run build
```

Use Playwright for route, layout, or interaction regressions.

## Expectations

- New API routes need an `api` test for success and at least one failure path.
- Config, boot, routing, plugin, model-runtime, and agent-runtime changes need
  integration coverage that exercises the actual wiring.
- Pure parsing, normalization, validation, and policy logic should have unit
  tests.
- `skip` and `xfail` must include a reason.
- Very large test files should be split when practical. They are harder to
  review and audit.

## Audit

`scripts/test_audit.py` checks:

- test files are discoverable and classifiable;
- layer coverage across the suite;
- `skip` and `xfail` markers include reasons;
- duplicate test names within the same file;
- critical subsystems still have related tests;
- very large test files are called out for follow-up.

The audit is intentionally lightweight and dependency-free. It complements
pytest and coverage rather than replacing them.
