# `psql` `\d <table>` Query Capture Notes

- Capture date: 2026-03-10
- Requested target in plan: `psql 16.x`
- Actual local client available in this workspace: `psql (PostgreSQL) 18.2`
- Capture command source of truth: `psql -E` hidden-query output

## Capture Method

1. Started local `pgsqlite` with:
   - `target/debug/pgsqlite --database <temp>/data --port 55440 --auth trust --log-level error`
2. Connected with:
   - `psql -X -h 127.0.0.1 -p 55440 -d main -U postgres -E`
3. Ran:
   - `CREATE TABLE meta_test_users (...)`
   - `CREATE INDEX meta_test_users_name_idx ON meta_test_users(name)`
   - `\d meta_test_users`
   - `\d+ meta_test_users`

## What Was Captured

- Current `pgsqlite` fails during `\d meta_test_users`.
- Hidden-query capture therefore stopped after the first two queries emitted by `psql`.
- Those exact queries were frozen into:
  - `tests/sql/meta/psql16_d_table_queries.sql`

## Required Query Shapes For Parity

- Initial relation lookup through `pg_catalog.pg_class` joined to `pg_namespace`
- Follow-up relation metadata lookup through `pg_class`, `pg_class tc`, and `pg_am`
- The current observed failures were:
  - `column number 2 is out of range 0..1`
  - `column number 11 is out of range 0..10`

## Verification Run During This Task

- Existing meta-command runner attempt:
  - `timeout 180 tests/runner/run_ssl_tests.sh -m tcp-no-ssl --meta-commands tests/sql/meta/test_meta_commands_working.sql`
  - Result: timed out during release build, so not useful as a quick local signal for this task
- Existing focused meta-command regression:
  - `timeout 60 cargo test --quiet --test pg_proc_test test_pg_proc_psql_df_compatibility -- --nocapture`
  - Result: passed

## Interpretation

- `\df`-style function introspection has a passing focused regression today.
- `\d <table>` remains only partially covered and still fails on the early catalog query path.
- The next task should use the frozen hidden query shapes above, not assumptions about what `psql` emits.
