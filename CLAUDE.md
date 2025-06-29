# pgsqlite Project Context

## Overview
pgsqlite is a PostgreSQL protocol adapter for SQLite databases. It allows PostgreSQL clients to connect to and query SQLite databases using the PostgreSQL wire protocol.

## Project Structure
- `src/` - Main source code directory
  - `lib.rs` - Main library entry point
  - `protocol/` - PostgreSQL wire protocol implementation
  - `session/` - Session state management
  - `query/` - Query execution handlers
- `tests/` - Test files
- `Cargo.toml` - Rust project configuration
- `TODO.md` - Comprehensive task list for future development

## Build Commands
- `cargo build` - Build the project
- `cargo test` - Run tests
- `cargo run` - Run the project

## Development Workflow
- After implementing any feature, always run the full test suite with `cargo test` to ensure nothing is broken
- **ALWAYS update TODO.md when completing work or discovering new tasks**:
  - Mark completed tasks with `[x]`
  - Add new discovered tasks or subtasks
  - Document partial progress with detailed notes
  - Update task descriptions if implementation reveals complexity
- Check TODO.md for prioritized tasks when planning development work
- Use TODO.md as the authoritative source for tracking all future work

## Code Style
- Follow Rust conventions
- Use existing imports and patterns
- Avoid adding comments unless necessary
- Keep code concise and idiomatic

## Recent Work
- Fixed unused import warning in src/lib.rs:63 (removed unused `info` import)
- Added support for new PostgreSQL types: MONEY, INT4RANGE, INT8RANGE, NUMRANGE, CIDR, INET, MACADDR, MACADDR8, BIT, BIT VARYING
- Refactored CreateTableTranslator to use TypeMapper instead of duplicating type mapping logic
- Enhanced TypeMapper with pg_to_sqlite_for_create_table() method for handling SERIAL types and parametric types
- Improved CREATE TABLE parsing to handle multi-word types like "TIMESTAMP WITH TIME ZONE"
- Fixed TypeMapper SQLite->PostgreSQL mapping consistency to match SchemaTypeMapper (INTEGER maps to int4, not int8)
- Implemented custom DECIMAL type using rust_decimal for NUMERIC, REAL, DOUBLE PRECISION, FLOAT4, and FLOAT8 types
- Added automatic query rewriting to use decimal functions for arithmetic and comparisons
- **Enhanced decimal query rewriting for complex queries**:
  - Fixed correlated subquery context inheritance (test_lateral_join_simulation now passes)
  - Improved aggregate function decimal wrapping to distinguish NUMERIC vs FLOAT types
  - Enhanced derived table decimal type propagation for WHERE clause rewriting
  - Added context merging for subqueries to recognize outer table columns
  - Fixed recursive CTE decimal rewriting by properly handling table aliases in SetExpr processing
- Enhanced support for subqueries and CTEs in decimal query rewriting

## Known Test Issues
- datatype_compat_test: Contains fundamentally broken tests that create INTEGER columns but try to extract as i16 (should be i32)
- array_types_test: Pre-existing array handling issues unrelated to type mapping changes
These tests were failing due to incorrect type expectations or pre-existing implementation limitations, not due to recent changes

## Recently Fixed Tests
- test_create_table_translator_uses_type_mapper: Updated test expectations to match current DECIMAL type support (NUMERIC and DOUBLE PRECISION now correctly map to DECIMAL instead of TEXT for proper decimal arithmetic support)
- test_smallint_metadata_fixed: Fixed by using simple_query instead of parameterized queries for system table access (parameterized queries on __pgsqlite_schema were causing UnexpectedMessage errors)

## Important Design Decisions
- **Type Inference**: NEVER use column names to infer types. Types should be determined from:
  - Explicit PostgreSQL type declarations in CREATE TABLE statements
  - SQLite schema information via PRAGMA table_info
  - Explicit type casts in queries (e.g., $1::int4)
  - Value-based inference only when schema information is unavailable

- **Decimal Query Rewriting**: 
  - Only NUMERIC types (stored as DECIMAL in SQLite) require decimal_from_text wrapping for aggregates
  - FLOAT types (REAL, DOUBLE PRECISION, FLOAT4, FLOAT8) should NOT be wrapped as they're already decimal-compatible
  - Correlated subqueries must inherit outer context to recognize outer table columns
  - Context merging is essential for proper type resolution in nested queries

## Quality Standards
To avoid idiot behavior:
- Write tests that actually verify functionality, not tests that are designed to pass easily
- Only mark tasks as complete when they are actually finished and working
- Test edge cases and error conditions, not just happy paths
- Verify implementations work end-to-end, not just in isolation
- Don't claim something works without actually testing it