# SQLAlchemy Compatibility Report

This report reflects targeted SQLAlchemy coverage, not blanket PostgreSQL ORM parity.

## Phase 2 Targeted CI Coverage

The current automated compatibility gate is narrower than the full exploratory suite.

- `tests/python/run_targeted_compat_checks.sh` now runs in CI
- the psycopg3 portion covers:
  - `SET TRANSACTION ...` plus `SHOW` / `current_setting(...)` / `pg_settings`
  - `CREATE ROLE` visibility through `pg_roles` and `pg_user`
  - parameterized direct-column `SELECT` metadata
  - direct-column `INSERT ... RETURNING` metadata
- the SQLAlchemy portion currently covers:
  - engine connection
  - text-based DDL/DML
  - simple readback through SQLAlchemy `text(...)`

This CI job does **not** yet claim full SQLAlchemy reflection parity. Reflection remains partial and broader ORM coverage still relies on the larger targeted app suite described below.

## Test Results with Unified Processor

### Overall Score: 6/8 Tests Passing (75%)

### ✅ Passing Tests

1. **Basic CRUD Operations** - Core CREATE, READ, UPDATE, DELETE paths pass in the targeted suite
2. **Relationship Operations** - Proper handling of foreign keys and cascades
3. **Join Operations** - INNER JOIN and LEFT JOIN work correctly
4. **Transaction Handling** - Proper COMMIT and ROLLBACK support
5. **Bulk Operations** - Bulk insert, update, and delete operations work
6. **DateTime Operations** - Date and DateTime handling works correctly

### ❌ Failing Tests

1. **Complex Queries** - "Unknown PG numeric type: 25" error
   - Appears to be related to aggregate function type mapping
   - Type OID 25 is TEXT in PostgreSQL

2. **Subquery Operations** - Foreign key constraint failure on DELETE
   - Related to cascade delete ordering
   - May need to handle foreign key constraints differently

## Performance Metrics

Average operation time across passing tests: **17.42ms**

This includes network overhead and demonstrates that the unified processor maintains good performance with SQLAlchemy's complex query patterns.

## Key Findings

### What Works Well

1. **ORM Basics** - All fundamental ORM operations work perfectly
2. **Relationships** - Foreign keys, backrefs, and basic cascades work
3. **Transactions** - ACID compliance is maintained
4. **Bulk Operations** - Efficient bulk operations are supported
5. **DateTime Handling** - Proper conversion between PostgreSQL and SQLite datetime formats

### Areas Needing Improvement

1. **Type Mapping** - Some PostgreSQL type OIDs aren't properly mapped
2. **Cascade Delete** - Complex cascade scenarios may have ordering issues
3. **Subqueries** - Some complex subquery patterns need refinement
4. **Extended RETURNING Fidelity** - some direct-column `RETURNING` paths still need wire-format cleanup

## Unified Processor Impact

The unified processor successfully handles:
- **Prepared statements** from SQLAlchemy
- **Complex query patterns** with multiple translations
- **Many RETURNING clauses** used by the targeted suite
- **Zero-allocation fast path** for simple queries

## Recommendations

### Immediate Fixes

1. **Type OID Mapping** - keep narrowing remaining aggregate and expression mismatches
2. **Foreign Key Handling** - review cascade delete order in transaction processing
3. **Extended RETURNING** - finish direct-column wire-format fidelity for all result-format combinations

### Future Enhancements

1. **Query Plan Caching** - Cache prepared statement plans for repeated queries
2. **Connection Pooling** - Optimize for SQLAlchemy's connection pool behavior
3. **Type Inference** - Improve type inference for aggregate functions

## Conclusion

pgsqlite has a useful SQLAlchemy story on tested paths, but the supported scope is still narrower than “full SQLAlchemy compatibility”.

- CI now locks a deterministic SQLAlchemy engine/text smoke path plus companion psycopg3 protocol checks.
- The larger exploratory SQLAlchemy suite is still informative, but it should be read as directional rather than release-gating.
- Reflection-heavy and complex-query behavior remain partial and should stay documented as such.
