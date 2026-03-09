# PostgreSQL Compatibility Phase 2 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Close the highest-value remaining PostgreSQL compatibility gaps after the Phase 1 roles/settings/catalog fixes by stabilizing extended-protocol `RETURNING`, improving `psql 16.x` `\d <table>` parity, and putting targeted driver/ORM regressions into CI.

**Architecture:** Keep the Phase 1 direction: one source of truth for observable state, regression-first changes, and shared protocol paths instead of parallel special cases. Phase 2 should remove `RETURNING`-specific wire drift by reusing the same row-description and result-format logic used by normal `SELECT`, then lock `psql 16.x` table-describe behavior to real query corpus fixtures, and finally make SQLAlchemy/psycopg regressions part of routine verification.

**Tech Stack:** Rust, SQLite, PostgreSQL wire protocol, `tokio-postgres`, `psql 16.x`, targeted SQLAlchemy/psycopg smoke suites, GitHub Actions

---

### Task 1: Freeze the `RETURNING` failure with a targeted regression

**Files:**
- Modify: `tests/postgres16_compat_regression_test.rs`
- Reference: `src/query/extended.rs`

**Step 1: Write the failing test**

Add a new regression that uses `tokio-postgres` prepared/extended execution for:
- `INSERT ... RETURNING id, name`
- `UPDATE ... RETURNING id`
- `DELETE ... RETURNING name`

Assertions:
- query completes without `UnexpectedMessage`
- returned column names match
- returned OIDs are correct for direct columns
- row values deserialize successfully

**Step 2: Run test to verify it fails**

Run:

```bash
cargo test --quiet --test postgres16_compat_regression_test test_insert_returning_extended_protocol_roundtrip -- --nocapture
```

Expected:
- FAIL with `UnexpectedMessage`, format mismatch, or incorrect OID/value decoding

**Step 3: Write minimal implementation**

Touch only `src/query/extended.rs` for the first fix pass.

Implement the minimum needed to make `RETURNING` statements:
- advertise row metadata only once
- avoid binary/text payload mismatch
- preserve direct-column OIDs

Do not broaden to expression `RETURNING` yet.

**Step 4: Run test to verify it passes**

Run:

```bash
cargo test --quiet --test postgres16_compat_regression_test test_insert_returning_extended_protocol_roundtrip -- --nocapture
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add tests/postgres16_compat_regression_test.rs src/query/extended.rs
git commit -m "fix: stabilize extended returning roundtrip"
```

### Task 2: Unify `RETURNING` Describe and Execute with the shared metadata path

**Files:**
- Modify: `src/query/extended.rs`
- Reference: `src/query/extended_fast_path.rs`
- Reference: `src/query/executor.rs`

**Step 1: Write the failing test**

Add regressions for:
- `prepare("INSERT ... RETURNING id, name")`
- `Describe(statement)`
- `Describe(portal)`
- execution after describe

Verify:
- statement describe and portal describe agree on names/OIDs/formats
- execute does not emit an extra `RowDescription` if describe already did

**Step 2: Run test to verify it fails**

Run:

```bash
cargo test --quiet --test postgres16_compat_regression_test test_returning_describe_execute_metadata_consistency -- --nocapture
```

Expected:
- FAIL from duplicate row description, no-data/row-description mismatch, or type/format drift

**Step 3: Write minimal implementation**

Refactor `src/query/extended.rs` so `RETURNING` uses shared helpers for:
- field description building
- result format normalization
- row-description emission rules

Keep scope limited to:
- `RETURNING column`
- `RETURNING *`
- direct column aliases

Do not add broad expression inference in this task.

**Step 4: Run test to verify it passes**

Run:

```bash
cargo test --quiet --test postgres16_compat_regression_test test_returning_describe_execute_metadata_consistency -- --nocapture
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add tests/postgres16_compat_regression_test.rs src/query/extended.rs
git commit -m "refactor: share returning metadata path"
```

### Task 3: Verify direct-column `RETURNING` against fast-path and non-fast-path execution

**Files:**
- Modify: `tests/postgres16_compat_regression_test.rs`
- Modify: `src/query/extended_fast_path.rs` if needed
- Modify: `src/query/extended.rs` if needed

**Step 1: Write the failing test**

Add one regression that compares:
- simple query `INSERT ... RETURNING id`
- extended prepared `INSERT ... RETURNING id`

Assert:
- same column name
- same OID
- same value decoding

**Step 2: Run test to verify it fails**

Run:

```bash
cargo test --quiet --test postgres16_compat_regression_test test_returning_simple_vs_extended_fidelity -- --nocapture
```

Expected:
- FAIL if fast path and extended path drift

**Step 3: Write minimal implementation**

Only normalize the direct-column `RETURNING` subset. If fast path is already correct, keep changes in the slower path only. If both paths drift, extract a shared helper instead of duplicating fixes.

**Step 4: Run test to verify it passes**

Run:

```bash
cargo test --quiet --test postgres16_compat_regression_test test_returning_simple_vs_extended_fidelity -- --nocapture
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add tests/postgres16_compat_regression_test.rs src/query/extended.rs src/query/extended_fast_path.rs
git commit -m "fix: align returning metadata across execution paths"
```

### Task 4: Capture the real `psql 16.x` `\d <table>` query corpus

**Files:**
- Create: `tests/sql/meta/psql16_d_table_queries.sql`
- Create: `docs/psql-16-d-table-query-notes.md`
- Reference: existing files under `tests/sql/meta/`

**Step 1: Write the fixture**

Capture the real SQL emitted by `psql 16.x` for:
- `\d table_name`
- `\d+ table_name` if useful

Store the raw query corpus in `tests/sql/meta/psql16_d_table_queries.sql`.

In `docs/psql-16-d-table-query-notes.md`, summarize:
- exact `psql` version
- how the queries were captured
- which query shapes are required for parity

**Step 2: Run the existing meta-command checks**

Run the relevant local verification command or script already used for meta-command coverage. If no unified script exists, run the specific regression tests that exercise the same handlers.

Expected:
- current `\d <table>`-equivalent coverage remains partial

**Step 3: Write minimal implementation plan notes in code comments only if needed**

Do not fix behavior in this task. This task exists only to freeze the real corpus and stop relying on assumptions.

**Step 4: Re-run the check to confirm fixture is stable**

Run the same command as Step 2.

Expected:
- same failures, but now backed by a fixed corpus

**Step 5: Commit**

```bash
git add tests/sql/meta/psql16_d_table_queries.sql docs/psql-16-d-table-query-notes.md
git commit -m "test: capture psql 16 d-table query corpus"
```

### Task 5: Add a failing `\d <table>` parity regression

**Files:**
- Modify: `tests/postgres16_compat_regression_test.rs` or add a focused new test file if it gets too large
- Reference: `src/catalog/pg_class.rs`
- Reference: `src/catalog/pg_attribute.rs`
- Reference: `src/catalog/pg_constraint.rs`
- Reference: `src/catalog/pg_index.rs`
- Reference: `src/catalog/query_interceptor.rs`

**Step 1: Write the failing test**

Use the captured `psql 16.x` query shape and assert that:
- the query completes
- expected rows for columns, indexes, and constraints exist
- no key catalog joins return empty when the test table exists

**Step 2: Run test to verify it fails**

Run:

```bash
cargo test --quiet --test postgres16_compat_regression_test test_psql16_d_table_query_shape -- --nocapture
```

Expected:
- FAIL from missing rows, wrong columns, or incomplete catalog subset

**Step 3: Write minimal implementation**

Fix only the catalog readers required for the captured `\d <table>` query shape. Prefer targeted reader changes over generic catalog engines.

Focus files:
- `src/catalog/pg_class.rs`
- `src/catalog/pg_attribute.rs`
- `src/catalog/pg_constraint.rs`
- `src/catalog/pg_index.rs`
- `src/catalog/query_interceptor.rs`

**Step 4: Run test to verify it passes**

Run:

```bash
cargo test --quiet --test postgres16_compat_regression_test test_psql16_d_table_query_shape -- --nocapture
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add tests/postgres16_compat_regression_test.rs src/catalog/pg_class.rs src/catalog/pg_attribute.rs src/catalog/pg_constraint.rs src/catalog/pg_index.rs src/catalog/query_interceptor.rs
git commit -m "fix: improve psql d table catalog parity"
```

### Task 6: Add SQLAlchemy and psycopg targeted verification commands

**Files:**
- Modify: `.github/workflows/ci.yml`
- Modify: `docs/SQLALCHEMY_COMPATIBILITY_REPORT.md`
- Create or modify: lightweight scripts under `tests/python/` if needed

**Step 1: Write the failing CI expectation**

Add a new CI job or job step design that runs only targeted suites covering:
- settings round-trip
- role/catalog reflection
- direct-column prepared describe
- direct-column `RETURNING` if stabilized by Tasks 1-3

If the scripts do not exist yet, first create the minimal script or command wrapper.

**Step 2: Run the targeted suite locally**

Run the exact local command that the CI step will use.

Expected:
- FAIL if the command is wrong or the target scenario is not stable yet

**Step 3: Write minimal implementation**

Limit scope to:
- adding the CI job/step
- making the targeted script deterministic
- tightening docs to the exact tested scope

Do not add full app-matrix CI in this task.

**Step 4: Run the targeted suite locally again**

Run the same command as Step 2.

Expected:
- PASS

**Step 5: Commit**

```bash
git add .github/workflows/ci.yml docs/SQLALCHEMY_COMPATIBILITY_REPORT.md tests/python
git commit -m "ci: add targeted compatibility verification"
```

### Task 7: Refresh public compatibility docs after Phase 2

**Files:**
- Modify: `README.md`
- Modify: `docs/postgresql-compatibility-review-2026-03-10.md`
- Modify: `docs/SQLALCHEMY_COMPATIBILITY_REPORT.md`

**Step 1: Write the doc diff**

Update the compatibility matrix and known gaps to reflect Phase 2 outcomes exactly.

If `RETURNING` is still partial, leave it explicitly partial.
If `\d <table>` is fixed, upgrade only that claim.

**Step 2: Run a documentation sanity pass**

Use:

```bash
rg -n "all PostgreSQL drivers|seamless|complete .* compatibility|full PostgreSQL compatibility" README.md docs tests/rails_app/README.md tests/go_app/README.md
```

Expected:
- no remaining public over-claims for the touched scope

**Step 3: Write minimal implementation**

Keep doc edits narrow and evidence-based. Every stronger claim must correspond to a test or fixture added in this phase.

**Step 4: Run the sanity pass again**

Run:

```bash
rg -n "all PostgreSQL drivers|seamless|complete .* compatibility|full PostgreSQL compatibility" README.md docs tests/rails_app/README.md tests/go_app/README.md
```

Expected:
- no unjustified claims in touched docs

**Step 5: Commit**

```bash
git add README.md docs/postgresql-compatibility-review-2026-03-10.md docs/SQLALCHEMY_COMPATIBILITY_REPORT.md
git commit -m "docs: refresh compatibility claims after phase 2"
```

### Task 8: Run the Phase 2 verification bundle

**Files:**
- No code changes required unless failures reveal missing updates

**Step 1: Run focused Rust regressions**

```bash
cargo test --quiet --test postgres16_compat_regression_test -- --nocapture
cargo test --quiet --test pg_roles_user_test -- --nocapture
```

Expected:
- PASS

**Step 2: Run targeted meta-command verification**

Run the exact `psql 16.x` or fixture-based verification command added in Tasks 4-5.

Expected:
- PASS for the targeted `\d <table>` scope

**Step 3: Run targeted ORM/driver verification**

Run the exact local commands wired into CI in Task 6.

Expected:
- PASS

**Step 4: Record evidence in the review document**

Update:
- `docs/postgresql-compatibility-review-2026-03-10.md`

Add:
- commands run
- exact pass/fail state
- remaining known gaps

**Step 5: Commit**

```bash
git add docs/postgresql-compatibility-review-2026-03-10.md
git commit -m "test: record phase 2 compatibility verification"
```

---

## Milestone Ordering Notes

- Do `RETURNING` before `psql \d <table>`.
- Reason: `RETURNING` is a protocol-correctness issue that can break ordinary app writes and driver behavior, while `\d <table>` is primarily an introspection/completeness problem.
- Do not broaden expression-type inference in Phase 2 unless a regression proves it is required for the targeted suites.
- Keep Rails and GORM as smoke-only unless new deterministic suites are added and wired into CI.

## External Review Notes

- An external-model critique was requested before finalizing this plan.
- The leading hypothesis going into implementation is:
  - first remove protocol drift in `RETURNING`
  - then lock `psql 16.x` table-describe corpus
  - then expand CI around targeted SQLAlchemy/psycopg checks
- If external critique arrives with a materially safer order, update this plan before execution starts.

---

Plan complete and saved to `docs/plans/2026-03-10-postgresql-compatibility-phase-2.md`. Two execution options:

**1. Subagent-Driven (this session)** - I dispatch fresh subagent per task, review between tasks, fast iteration

**2. Parallel Session (separate)** - Open new session with executing-plans, batch execution with checkpoints

Which approach?
