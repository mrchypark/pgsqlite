# Review Findings Fixes Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 코드 리뷰 지적사항(P0~P3)을 모두 수정하되, “SQLite 위에 Postgres를 흉내내는 서버”라는 프로젝트 목표(완전 대체/완전 호환이 아님)를 유지하면서 안전 기본값, 크래시/DoS 표면 감소, 유지보수성 개선을 달성한다.

**Architecture:** 기존 wire-protocol/translator/fast-path 구조는 유지한다. 보안은 “기본값을 안전하게(로컬 바인딩/로컬 소켓 제한)” + “원격/다중 사용자 접근은 명시적 opt-in” 원칙으로 해결한다. 호환성 측면에서 무거운 기능(SCRAM/MD5 완전 구현, SQL 정식 파서 기반 fast-path 판별 등)은 추가하지 않는다.

**Tech Stack:** Rust 2024, tokio, tokio-util(codec), rusqlite, clap, tracing

---

### Task 1: 테스트용 env var 변경을 직렬화

**Files:**
- Modify: `src/utils/mod.rs`
- Create: `src/utils/test_env.rs`
- Modify: `src/cache/ttl_cache.rs`
- Modify: `src/protocol/buffer_pool.rs`
- Modify: `src/protocol/memory_monitor.rs`
- Modify: `src/cache/memory_aware_manager.rs`
- Modify: `src/security/audit_logger.rs`

**Step 1: env lock 유틸 추가**
- `crate::utils::test_env::lock_env()` 같은 형태로 전역 Mutex 가드를 제공

**Step 2: env var set/remove 하는 테스트에 lock 적용**
- 각 테스트에서 `let _guard = crate::utils::test_env::lock_env();` 후 env 변경

**Step 3: 검증**
- Run: `cargo test --release --lib`
- Expected: PASS

---

### Task 2: connection pool stats(active_connections) 재계산 버그 수정

**Files:**
- Modify: `src/session/pool.rs`

**Step 1: failing 관점 정리(테스트 보강)**
- background health check가 `active_connections = max_connections - idle`로 덮어쓰지 않도록 해야 함.
- 간단히는 background task에서 active_connections를 건드리지 않도록 수정하고, 기존 acquire/drop 기반 카운터를 신뢰.

**Step 2: 구현**
- `background_health_check()` 마지막의 active_connections 재계산 제거
- idle_connections는 `conns.len()`로 맞추되, total/active는 acquire/drop에서만 변경

**Step 3: 검증**
- Run: `cargo test --release --lib session::pool`
- Expected: PASS

---

### Task 3: extended protocol에서 missing prepared statement로 패닉나는 경로 제거

**Files:**
- Modify: `src/query/extended.rs`

**Step 1: failing test 추가**
- “존재하지 않는 statement_name을 가진 portal execute”를 만들기 어렵다면, 최소한 `.expect("Statement should exist")`를 제거하고 `PgSqliteError::Protocol`로 변환하는 경로를 유닛 수준으로 검증(가능하면 핸들러 호출).

**Step 2: 구현**
- `.expect()`를 `ok_or_else(|| PgSqliteError::Protocol(...))?`로 교체

**Step 3: 검증**
- Run: `cargo test --release --lib`
- Expected: PASS

---

### Task 4: fast-path disqualifier 키워드 체크 대소문자 버그 수정

**Files:**
- Modify: `src/query/fast_path.rs`
- Modify: `src/query/extended.rs`
- Test: `src/query/fast_path.rs`(기존 테스트 보강)

**Step 1: failing test 추가**
- 예: `"select * from users join orders on ..."`가 fast-path로 잡히면 안 됨

**Step 2: 구현**
- `to_ascii_uppercase()` 또는 `eq_ignore_ascii_case` 기반으로 keyword 포함 여부 체크
- extended.rs는 이미 `find_keyword_position()`가 있으므로 이를 재사용

**Step 3: 검증**
- Run: `cargo test --release --lib query::fast_path`
- Expected: PASS

---

### Task 5: to_uppercase() 인덱스 기반 슬라이싱(유니코드) 위험 제거

**Files:**
- Modify: `src/cache/statement_pool.rs`
- Modify: `src/cache/enhanced_statement_pool.rs`
- Modify: `src/translator/insert_translator.rs`
- Modify: `src/translator/array_translator.rs`
- Modify: `src/catalog/system_functions.rs`

**Step 1: failing test 추가**
- 유니코드가 앞에 붙은 쿼리(예: `"INSERT INTO t (c) VALUES (1); -- ß"` 형태)에서도 fingerprint/RETURNING 슬라이스가 패닉 없이 동작해야 함.

**Step 2: 구현**
- `.to_uppercase()`를 `.to_ascii_uppercase()`로 교체(키워드는 ASCII)
- 가능한 곳은 uppercasing 자체를 제거하고 `eq_ignore_ascii_case` search로 대체

**Step 3: 검증**
- Run: `cargo test --release --lib`
- Expected: PASS

---

### Task 6: production 경로에 남아있는 특수 디버그 로깅 격리

**Files:**
- Modify: `src/query/extended.rs`

**Step 1: 구현**
- “orders/customer_id” 특수 로깅은 `debug!`로 내리고, `PGSQLITE_DEBUG_EXECUTE=1` 같은 opt-in 플래그가 있을 때만 실행

**Step 2: 검증**
- Run: `cargo test --release --lib`
- Expected: PASS

---

### Task 7: SmallValue/MappedValue public API의 패닉 지뢰 제거

**Files:**
- Modify: `src/protocol/small_value.rs`
- Modify: `src/protocol/memory_mapped.rs`
- Modify: `src/protocol/value_handler.rs`

**Step 1: 설계 선택(최소 변경)**
- “완전한 zero-copy/value-handler 통합”은 목표가 아니므로, 동적 small 값에서 패닉이 나지 않도록 안전한 표현으로 정리한다.

**Step 2: 구현**
- `SmallValue::as_text()`를 static 값만 반환하는 `Option<&'static [u8]>` 형태로 변경(동적은 None)
- `MappedValue`는 동적 small 값을 `SmallBytes`(inline buffer) 또는 `Memory(Vec<u8>)`로 승격해 `as_slice()`가 패닉 없이 동작하도록 수정
- value_handler의 테스트도 새로운 표현에 맞게 업데이트

**Step 3: 검증**
- Run: `cargo test --release --lib protocol::memory_mapped protocol::value_handler protocol::small_value`
- Expected: PASS

---

### Task 8: 보안 기본값 하드닝(로컬 기본 + 원격 opt-in)

**Files:**
- Modify: `src/config.rs`
- Modify: `src/main.rs`
- Modify: `src/protocol/messages.rs`
- Modify: `src/protocol/codec.rs`
- (Optional) Modify: `README.md`

**Step 1: listen address 기본값 변경**
- `--listen-addr`(env: `PGSQLITE_LISTEN_ADDR`) 추가, 기본 `127.0.0.1`
- main TCP bind를 `0.0.0.0` -> `listen_addr`

**Step 2: unix socket permission 기본값 변경**
- `--socket-permissions`(env: `PGSQLITE_SOCKET_PERMISSIONS`) 추가, 기본 `0700`
- main에서 해당 mode 적용

**Step 3: 원격 trust 방지(명시적 opt-in)**
- `listen_addr`가 loopback이 아닌데 auth가 trust면 실행을 거부하거나(또는 `--insecure-allow-remote-trust` 필요) 경고 후 종료
- 목표는 “원격 노출 + 무인증” 조합을 기본적으로 막는 것

**Step 4: (선택) 최소한의 password auth 지원**
- 프로젝트 목표상 SCRAM/MD5 완전 구현은 제외
- cleartext password 모드만 opt-in으로 제공:
  - `--auth password` + `--password`(또는 env)일 때 AuthenticationCleartextPassword + PasswordMessage 처리
  - 나머지는 기존 trust 동작 유지

**Step 5: 검증**
- Run: `cargo test --release --lib`
- Run: `cargo clippy --release --lib -- -D warnings`
- (가능하면) 간단한 auth codec 유닛 테스트 추가 후 PASS 확인

---

### Task 9: 스타일/정적 품질

**Files:**
- 전체(필요 시)

**Step 1: rustfmt 적용**
- Run: `cargo fmt`

**Step 2: 최종 검증**
- Run: `cargo fmt --check`
- Run: `cargo test --release --lib`
- Run: `cargo clippy --release --lib -- -D warnings`

