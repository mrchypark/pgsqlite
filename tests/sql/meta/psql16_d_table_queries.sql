-- Captured hidden queries for `\d meta_test_users` / `\d+ meta_test_users`.
-- Plan target was psql 16.x, but this workspace only had psql 18.2 available on
-- 2026-03-10, so this fixture records the real locally emitted corpus from psql 18.2.
-- The command aborted against current pgsqlite after the second hidden query, so this
-- file currently contains the exact hidden queries observed before failure.

-- Setup used during capture:
-- CREATE TABLE meta_test_users (
--   id INTEGER PRIMARY KEY,
--   name TEXT NOT NULL,
--   email TEXT UNIQUE,
--   created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
-- );
-- CREATE INDEX meta_test_users_name_idx ON meta_test_users(name);

-- `\d meta_test_users` hidden query #1
SELECT c.oid,
  n.nspname,
  c.relname
FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relname OPERATOR(pg_catalog.~) '^(meta_test_users)$' COLLATE pg_catalog.default
  AND pg_catalog.pg_table_is_visible(c.oid)
ORDER BY 2, 3;

-- `\d meta_test_users` hidden query #2
SELECT c.relchecks, c.relkind, c.relhasindex, c.relhasrules, c.relhastriggers, c.relrowsecurity, c.relforcerowsecurity, false AS relhasoids, c.relispartition, '', c.reltablespace, CASE WHEN c.reloftype = 0 THEN '' ELSE c.reloftype::pg_catalog.regtype::pg_catalog.text END, c.relpersistence, c.relreplident, am.amname
FROM pg_catalog.pg_class c
 LEFT JOIN pg_catalog.pg_class tc ON (c.reltoastrelid = tc.oid)
LEFT JOIN pg_catalog.pg_am am ON (c.relam = am.oid)
WHERE c.oid = '55975';
