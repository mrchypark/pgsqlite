# PostgreSQL Compatibility Review

## 1. Scope

### Review Target
- Project: `pgsqlite`
- Branch / Workspace: current working tree
- Review Date: 2026-03-10
- Reviewer: Codex

### Compatibility Goal
- Goal type:
  - [ ] PostgreSQL engine parity
  - [x] PostgreSQL protocol/tooling compatibility
  - [x] ORM/client compatibility on tested paths
- Target statement:
  - `psql 16.x`, `tokio-postgres`, `psycopg2/3`, SQLAlchemy reflection/CRUD 경로에서 PostgreSQL-like 동작을 최대한 맞추는 것

### Tested Targets
- `psql`: `16.x` 기준 query shape / meta command 범위
- `tokio-postgres`: 회귀 테스트 기준 검증
- `psycopg2`: 기존 프로젝트 문서/테스트 범위 참고, 이번 턴 직접 재검증은 안 함
- `psycopg3`: targeted smoke 직접 재검증
- SQLAlchemy: targeted engine/text smoke 직접 재검증, full reflection 재검증은 안 함
- Rails: smoke-only
- GORM: smoke-only

### Out of Scope
- full PostgreSQL MVCC semantics
- `LISTEN` / `NOTIFY`
- `COPY`
- full `information_schema`
- full extended-protocol `RETURNING` wire-format parity
- engine-level PostgreSQL replacement claim

---

## 2. Executive Summary

### Overall Assessment
- Status:
  - [x] Strong on tested paths
  - [ ] Partially compatible
  - [ ] Development-only compatibility
  - [ ] Not production-viable
- Summary:
  - 현재 기준으로 `roles/settings/catalog` 일관성은 눈에 띄게 좋아졌고, `psql 16.x`/`tokio-postgres`의 핵심 introspection 및 session-setting 경로는 실질적으로 개선됐다.
  - 다만 `RETURNING`의 일부 extended protocol wire-format fidelity는 아직 남아 있어, “일반 PostgreSQL 대체”로 포지셔닝하면 과장이다.

### Recommended Positioning
- “Tested-path PostgreSQL compatibility shim”
- “PostgreSQL replacement”보다는 “tooling/ORM compatibility layer on tested paths”

---

## 3. Tested Matrix

| Area | Target | Result | Notes |
|------|--------|--------|-------|
| Startup / Auth | test server + `tokio-postgres` | Pass | 기본 연결 경로 정상 |
| Simple Query Protocol | regression suite | Pass | session/catalog regression 통과 |
| Extended Protocol | prepared `SELECT`, `SHOW`, catalog reads | Pass | direct-column prepared describe 포함 |
| Prepared Statements | `tokio-postgres` prepare | Pass | direct-column OID regression 통과 |
| Session Settings | `SET`/`SHOW`/`current_setting`/`pg_settings` | Pass | 값 일치하도록 통합 |
| Catalog Introspection | `pg_roles`, `pg_user`, `pg_settings` | Pass | write/read split-brain 제거 |
| `psql` Meta Commands | targeted query shapes | Partial | `\d <table>` hidden query subset은 고정했지만 full command parity는 아직 미완 |
| ORM Reflection | SQLAlchemy-sensitive catalog paths | Partial | engine/text smoke는 직접 재검증했지만 full reflection은 아님 |
| CRUD | core paths | Pass | 기존 범위 유지 |
| Transactions | `SET LOCAL`, `SET TRANSACTION` 수명 | Pass | local override reset 확인 |
| `RETURNING` | common support | Partial | 일부 extended wire-format gap 남음 |
| Type OID Fidelity | prepared direct-column `SELECT` | Pass | direct-column describe OID 검증 완료 |

---

## 4. Findings

### P1
- Title: session settings와 catalog read path의 split-brain
- Area: session semantics / introspection
- Severity: `Resolved`
- Summary:
  - 기존에는 `SET`/`SHOW`/`current_setting`/`pg_settings`가 서로 다른 값 소스를 보거나 하드코딩 응답을 사용했다.
- Evidence:
  - Query / test:
    - `SHOW transaction_isolation`
    - `SELECT current_setting('transaction_isolation')`
    - `SELECT setting FROM pg_settings WHERE name = 'transaction_isolation'`
  - Observed behavior:
    - 변경 전 값 불일치 및 일부 명령 미지원
    - 변경 후 회귀 테스트 통과
  - Expected PostgreSQL behavior:
    - 동일 세션 상태를 일관되게 반영
- Impact:
  - ORM/driver probe가 쉽게 깨짐
- Likely cause:
  - 하드코딩 SHOW 및 정적 `pg_settings`
- Suggested fix:
  - `SessionState`를 단일 truth source로 사용
  - 완료

### P1
- Title: `CREATE ROLE`과 `pg_roles`/`pg_user`의 split-brain
- Area: catalog / role lifecycle
- Severity: `Resolved`
- Summary:
  - 기존에는 role 생성은 `__pgsqlite_roles`에 저장되지만, `pg_roles`/`pg_user`는 정적 데이터만 반환했다.
- Evidence:
  - Query / test:
    - `CREATE ROLE compat_role`
    - `SELECT rolname FROM pg_roles WHERE rolname = 'compat_role'`
    - `SELECT COUNT(*) FROM pg_user WHERE usename = 'compat_role'`
  - Observed behavior:
    - 변경 전 생성 role이 catalog에 보이지 않음
    - 변경 후 회귀 테스트 통과
  - Expected PostgreSQL behavior:
    - `pg_roles`는 role을 보여주고, `NOLOGIN` role은 `pg_user`에 보이지 않아야 함
- Impact:
  - introspection 및 administration path 신뢰도 훼손
- Likely cause:
  - static catalog handler
- Suggested fix:
  - `pg_roles`/`pg_user`가 실제 role backing store를 읽도록 변경
  - 완료

### P2
- Title: extended-protocol `RETURNING` wire-format fidelity
- Area: protocol / metadata fidelity
- Severity: `Open`
- Summary:
  - direct-column prepared `SELECT` describe는 맞췄지만, 일부 `RETURNING` extended path는 advertised metadata와 payload format 정리가 아직 덜 끝났다.
- Evidence:
  - Query / test:
    - `INSERT ... RETURNING id, name`
  - Observed behavior:
    - 일부 경로에서 `UnexpectedMessage` 또는 binary/text mismatch 성격의 failure
  - Expected PostgreSQL behavior:
    - Describe/Execute sequence와 advertised OID/format/payload가 일치해야 함
- Impact:
  - metadata-sensitive driver path에서 깨질 수 있음
- Likely cause:
  - `RETURNING` 전용 execute/describe 경로가 일반 select path와 완전히 통합되지 않음
- Suggested fix:
  - `RETURNING`도 portal describe / row-description emission / payload encoding 경로를 공통화

---

## 5. Session / GUC Consistency

### Tested Parameters
- [x] `search_path`
- [x] `transaction_isolation`
- [x] `default_transaction_isolation`
- [x] `transaction_read_only`
- [x] `default_transaction_read_only`
- [x] `timezone`
- [x] `application_name`
- [x] `client_encoding`

### Consistency Checks

| Parameter | SET/RESET | SHOW | current_setting | pg_settings | Result | Notes |
|-----------|-----------|------|-----------------|-------------|--------|-------|
| `search_path` | Pass | Pass | Pass | Pass | Pass | `SET LOCAL` reset 포함 |
| `transaction_isolation` | Pass | Pass | Pass | Pass | Pass | `SET TRANSACTION` round-trip |
| `timezone` | Pass | Pass | Pass | Pass | Pass | session source 통합 |
| `client_encoding` | Pass | Pass | Pass | Pass | Pass | default/session 조회 일치 |

### Transaction-Local Semantics
- [x] `SET LOCAL` resets on `COMMIT`
- [x] `SET LOCAL` resets on `ROLLBACK`
- [x] `SET TRANSACTION ...` is scoped correctly in current compatibility model
- [ ] failed transaction state matches full PostgreSQL behavior

Notes:
- local override cleanup는 구현됨
- failed transaction semantics는 이번 패스의 핵심 범위는 아니었음

---

## 6. Role / Catalog Consistency

### Role Lifecycle Checks
- [x] `CREATE ROLE` reflected in `pg_roles`
- [x] `CREATE USER` path shares same backing model
- [x] `DROP ROLE` removes from `pg_roles`
- [x] `DROP USER` removes from `pg_user`
- [x] `NOLOGIN` role excluded from `pg_user`
- [x] duplicate create returns PostgreSQL-like error
- [x] missing drop returns PostgreSQL-like error

### Catalog Truth-Source Review
- Write path backing store: `__pgsqlite_roles`
- Read path backing store: `pg_roles` / `pg_user` handler now reads persisted role state
- Result:
  - [x] Single source of truth
  - [ ] Split-brain risk
- Notes:
  - 이전 정적 role arrays 제거 효과가 큼

---

## 7. Protocol Review

### Simple Query Protocol
- Status: `Pass`
- Notes:
  - session-setting 및 catalog regression 기준 정상

### Extended Query Protocol
- Parse: Pass
- Bind: Pass
- Describe: Pass on prepared direct-column `SELECT`
- Execute: Pass on tested catalog/settings paths
- Sync: Pass on tested paths
- Notes:
  - `RETURNING` 일부는 아직 residual risk

### Prepared Statement Fidelity
- [x] prepared `SELECT` direct-column OIDs correct
- [x] `Describe(statement)` metadata correct on tested direct-column path
- [x] `Describe(portal)` metadata correct on tested direct-column path
- [x] simple vs extended results match on tested direct-column path
- [ ] row format matches advertised OID/format for all `RETURNING` paths

Notes:
- prepared `SELECT id AS probe_id, name AS probe_name FROM ...` 경로는 회귀 테스트로 확인

---

## 8. `psql` Compatibility

### Tested Version
- `psql 16.x`

### Meta Commands
| Command | Result | Notes |
|---------|--------|-------|
| `\d` | Partial | targeted query shapes 기준 |
| `\dt` | Pass | 계획 범위 내 |
| `\di` | Pass | 계획 범위 내 |
| `\dv` | Pass | 계획 범위 내 |
| `\dT` | Pass | 계획 범위 내 |
| `\df` | Pass | 기존 테스트 범위 유지 |
| `\d <table>` | Partial | 완전 parity 아님 |

### Query Corpus Source
- [ ] actual `psql` emitted queries freshly recaptured this turn
- [x] fixture / targeted regression 기반 검토
- [x] regression test added/updated

Notes:
- README도 `psql 16.x` targeted support로 낮춰 표기함

---

## 9. ORM / Driver Compatibility

### `tokio-postgres`
- Connection: Pass
- Prepared statements: Pass
- Session state: Pass
- Catalog queries: Pass
- `RETURNING`: Partial
- Result: strong on tested paths

### `psycopg2`
- Connection: Not re-tested this turn
- Binary/text behavior: Partial by project history
- Reflection: Not re-tested this turn
- CRUD: Not re-tested this turn
- Result: partial confidence only

### `psycopg3`
- Connection: Pass
- Binary/text behavior: targeted direct-column metadata smoke pass
- Reflection: N/A
- CRUD: Pass on targeted smoke path
- Result: strong on targeted smoke paths, broader type coverage still partial

### SQLAlchemy
- Reflection: partial confidence only
- CRUD: Pass on engine/text smoke path
- Transactions: Pass on engine/text smoke path
- Joins: partial confidence
- `RETURNING`: known gap remains
- Result: useful compatibility, not full parity

### Rails / ActiveRecord
- Coverage type:
  - [ ] full tested path
  - [x] smoke only
- Result: smoke-only
- Notes:
  - README wording lowered accordingly

### GORM
- Coverage type:
  - [ ] full tested path
  - [x] smoke only
- Result: smoke-only
- Notes:
  - README wording lowered accordingly

---

## 10. Type / OID Fidelity

### Tested Cases
- [x] direct-column prepared `SELECT`
- [ ] direct-column `RETURNING`
- [ ] wildcard `RETURNING *`
- [x] aliased direct columns
- [ ] aggregate projections
- [ ] expression projections

| Query Shape | Expected Type/OID | Observed | Result | Notes |
|-------------|-------------------|----------|--------|-------|
| `SELECT id, name ...` | `int4(23)`, `text(25)` | matched | Pass | regression added |
| `INSERT ... RETURNING id` | `int4(23)` | not fully stabilized | Partial | residual gap |
| `INSERT ... RETURNING *` | table schema OIDs | not verified | Not tested | |
| `SELECT count(*) ...` | integer-ish metadata | catalog count path fixed functionally | Partial | metadata parity not fully audited |

Notes:
- 이번 패스는 prepared direct-column `SELECT` describe fidelity까지를 확실히 고정
- `RETURNING` 전체 wire fidelity는 다음 라운드 권장

---

## 11. Documentation Review

### Claims Checked
- [x] README support claims
- [x] ORM compatibility docs
- [x] test app READMEs
- [x] known gaps documented

### Over-Claims Found
- Claim:
  - “all PostgreSQL drivers”
  - “works seamlessly”
  - “complete Rails/GORM compatibility”
- Why it overstates reality:
  - 실제 검증은 targeted matrix + smoke 범위에 가깝고, 일부 protocol/type gaps가 남아 있음
- Suggested rewrite:
  - tested versions / smoke-only / known gaps를 함께 적는 방식

### Recommended Compatibility Statement
- “pgsqlite provides PostgreSQL-compatible behavior on tested paths for `psql 16.x`, `tokio-postgres`, and selected ORM/client workflows, with documented gaps in some advanced metadata and extended-protocol paths.”

---

## 12. Verification Evidence

### Commands Run
```bash
cargo test --quiet --test postgres16_compat_regression_test -- --nocapture
cargo test --quiet --test pg_roles_user_test -- --nocapture
./tests/python/run_targeted_compat_checks.sh
```

### Key Queries Used
```sql
SHOW transaction_isolation;
SELECT current_setting('transaction_isolation');
SELECT setting FROM pg_settings WHERE name = 'transaction_isolation';

CREATE ROLE compat_role;
SELECT rolname, rolcanlogin FROM pg_roles WHERE rolname = 'compat_role';
SELECT COUNT(*) FROM pg_user WHERE usename = 'compat_role';
```

### Test Files / Fixtures
- `tests/postgres16_compat_regression_test.rs`
- `tests/pg_roles_user_test.rs`
- `tests/python/compat_targeted_smoke.py`
- `tests/python/run_targeted_compat_checks.sh`
- `tests/sql/meta/psql16_d_table_queries.sql`

---

## 13. Final Verdict

### Verdict
- [x] Safe to claim tested-path PostgreSQL compatibility
- [ ] Safe only for development/testing use
- [ ] Needs more work before public compatibility claims

### Top Risks
1. extended protocol `RETURNING` wire-format/type fidelity
2. full `psql \d <table>` parity 미완
3. SQLAlchemy reflection / psycopg full-suite 재검증 부재

### Required Next Steps
1. `RETURNING` describe/execute/format 경로를 공통화해서 OID/format/payload 일치 보장
2. `psql 16.x` 실제 emitted query corpus를 fixture로 고정
3. SQLAlchemy/psycopg targeted suite를 CI에 상시 연결

### Nice-to-Have Next Steps
1. failed transaction semantics를 PostgreSQL에 더 가깝게 정리
2. `pg_class`/`pg_attribute`/`pg_type` coverage를 `\d <table>` 기준으로 추가 보강
3. compatibility matrix를 별도 문서로 분리해서 릴리스 체크리스트에 포함
```
