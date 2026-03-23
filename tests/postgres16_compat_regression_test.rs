mod common;
use common::setup_test_server;
use tokio_postgres::SimpleQueryMessage;

fn first_simple_row_values(messages: &[SimpleQueryMessage]) -> Option<Vec<String>> {
    messages.iter().find_map(|msg| {
        if let SimpleQueryMessage::Row(row) = msg {
            Some(
                (0..row.len())
                    .map(|idx| row.get(idx).unwrap_or_default().to_string())
                    .collect(),
            )
        } else {
            None
        }
    })
}

#[tokio::test]
async fn test_set_transaction_isolation_level_roundtrip() {
    let server = setup_test_server().await;
    let client = &server.client;

    client
        .execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", &[])
        .await
        .unwrap();

    let row = client
        .query_one("SHOW transaction isolation level", &[])
        .await
        .unwrap();
    let value: &str = row.get(0);
    assert_eq!(value, "serializable");

    let row = client
        .query_one("SHOW transaction_isolation", &[])
        .await
        .unwrap();
    let value: &str = row.get(0);
    assert_eq!(value, "serializable");

    let row = client
        .query_one("SELECT current_setting('transaction_isolation')", &[])
        .await
        .unwrap();
    let value: &str = row.get(0);
    assert_eq!(value, "serializable");

    let row = client
        .query_one(
            "SELECT setting FROM pg_settings WHERE name = 'transaction_isolation'",
            &[],
        )
        .await
        .unwrap();
    let value: &str = row.get(0);
    assert_eq!(value, "serializable");
}

#[tokio::test]
async fn test_create_role_reflected_in_pg_roles() {
    let server = setup_test_server().await;
    let client = &server.client;

    client
        .execute("CREATE ROLE compat_role", &[])
        .await
        .unwrap();

    let row = client
        .query_one(
            "SELECT COUNT(*)::int8 FROM __pgsqlite_roles WHERE rolname = 'compat_role'",
            &[],
        )
        .await
        .unwrap();
    let backing_count: i64 = row.get(0);
    assert_eq!(backing_count, 1, "CREATE ROLE should persist into __pgsqlite_roles");

    let row = client
        .query_one(
            "SELECT rolname, rolcanlogin FROM pg_roles WHERE rolname = 'compat_role'",
            &[],
        )
        .await
        .unwrap();

    let rolname: &str = row.get(0);
    let rolcanlogin: &str = row.get(1);
    assert_eq!(rolname, "compat_role");
    assert_eq!(rolcanlogin, "f");

    let row = client
        .query_one(
            "SELECT COUNT(*)::int8 FROM pg_user WHERE usename = 'compat_role'",
            &[],
        )
        .await
        .unwrap();
    let count: i64 = row.get(0);
    assert_eq!(count, 0, "CREATE ROLE without LOGIN should not appear in pg_user");

    client.execute("DROP ROLE compat_role", &[]).await.unwrap();
}

#[tokio::test]
async fn test_set_local_search_path_resets_after_commit() {
    let server = setup_test_server().await;
    let client = &server.client;

    client.batch_execute("BEGIN").await.unwrap();
    client.batch_execute("SET LOCAL search_path = pg_catalog").await.unwrap();

    let row = client
        .query_one("SHOW search_path", &[])
        .await
        .unwrap();
    let value: &str = row.get(0);
    assert_eq!(value, "pg_catalog");

    client.batch_execute("COMMIT").await.unwrap();

    let row = client
        .query_one("SHOW search_path", &[])
        .await
        .unwrap();
    let value: &str = row.get(0);
    assert_eq!(value, "public");
}

#[tokio::test]
async fn test_prepared_select_describe_reports_direct_column_oids() {
    let server = setup_test_server().await;
    let client = &server.client;

    client
        .batch_execute("CREATE TABLE oid_probe (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        .await
        .unwrap();

    let stmt = client
        .prepare("SELECT id AS probe_id, name AS probe_name FROM oid_probe")
        .await
        .unwrap();

    let columns = stmt.columns();
    assert_eq!(columns.len(), 2);
    assert_eq!(columns[0].name(), "probe_id");
    assert_eq!(columns[0].type_().oid(), 23, "id should describe as int4");
    assert_eq!(columns[1].name(), "probe_name");
    assert_eq!(columns[1].type_().oid(), 25, "name should describe as text");
}

#[tokio::test]
async fn test_grant_missing_relation_errors() {
    let server = setup_test_server().await;
    let client = &server.client;

    let err = client
        .execute("GRANT SELECT ON TABLE does_not_exist TO postgres", &[])
        .await
        .expect_err("GRANT on missing relation should fail");

    let msg = err.to_string();
    assert!(
        msg.contains("relation \"does_not_exist\" does not exist"),
        "unexpected error message: {msg}"
    );
}

#[tokio::test]
async fn test_listen_reports_not_supported() {
    let server = setup_test_server().await;
    let client = &server.client;

    let err = client
        .execute("LISTEN chan_test", &[])
        .await
        .expect_err("LISTEN should be rejected as not supported");

    let msg = err.to_string();
    assert!(
        msg.contains("LISTEN is not supported"),
        "unexpected error message: {msg}"
    );
}

#[tokio::test]
async fn test_pg_prepared_statements_from_sql_and_plan_counts() {
    let server = setup_test_server().await;
    let client = &server.client;

    client
        .simple_query("PREPARE p_meta(int4) AS SELECT $1::int4 + 1;")
        .await
        .unwrap();

    let prepared_rows = client
        .simple_query(
            "SELECT from_sql, generic_plans, custom_plans \
             FROM pg_prepared_statements WHERE name = 'p_meta'",
        )
        .await
        .unwrap();
    let values = first_simple_row_values(&prepared_rows).expect("expected one row from catalog");

    let from_sql = values[0].as_str();
    let generic_plans: i64 = values[1].parse().unwrap();
    let custom_plans: i64 = values[2].parse().unwrap();
    assert_eq!(from_sql, "t");
    assert_eq!(generic_plans, 0);
    assert_eq!(custom_plans, 0);

    client.simple_query("EXECUTE p_meta(41);").await.unwrap();

    let prepared_rows = client
        .simple_query(
            "SELECT generic_plans, custom_plans FROM pg_prepared_statements WHERE name = 'p_meta'",
        )
        .await
        .unwrap();
    let values = first_simple_row_values(&prepared_rows).expect("expected one row from catalog");

    let generic_plans: i64 = values[0].parse().unwrap();
    let custom_plans: i64 = values[1].parse().unwrap();
    assert_eq!(generic_plans, 0);
    assert!(custom_plans >= 1);

    client.simple_query("DEALLOCATE p_meta;").await.unwrap();
}

#[tokio::test]
async fn test_pg_stat_io_tracks_read_and_write_counts() {
    let server = setup_test_server().await;
    let client = &server.client;

    let io_rows = client
        .simple_query(
            "SELECT reads, writes \
             FROM pg_stat_io WHERE backend_type = 'client backend' LIMIT 1",
        )
        .await
        .unwrap();
    let values = first_simple_row_values(&io_rows).expect("expected one row from pg_stat_io");
    let reads_before: i64 = values[0].parse().unwrap();
    let writes_before: i64 = values[1].parse().unwrap();

    client
        .execute("CREATE TABLE io_probe (id int primary key, v text)", &[])
        .await
        .unwrap();
    client
        .execute("INSERT INTO io_probe (id, v) VALUES (1, 'x')", &[])
        .await
        .unwrap();
    client
        .simple_query("SELECT v FROM io_probe WHERE id = 1")
        .await
        .unwrap();

    let io_rows = client
        .simple_query(
            "SELECT reads, writes \
             FROM pg_stat_io WHERE backend_type = 'client backend' LIMIT 1",
        )
        .await
        .unwrap();
    let values = first_simple_row_values(&io_rows).expect("expected one row from pg_stat_io");
    let reads_after: i64 = values[0].parse().unwrap();
    let writes_after: i64 = values[1].parse().unwrap();

    assert!(reads_after > reads_before);
    assert!(writes_after > writes_before);
}
