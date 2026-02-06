mod common;
use common::setup_test_server;

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
            "SELECT rolname, rolcanlogin FROM pg_roles WHERE rolname = 'compat_role'",
            &[],
        )
        .await
        .unwrap();

    let rolname: &str = row.get(0);
    let rolcanlogin: &str = row.get(1);
    assert_eq!(rolname, "compat_role");
    assert_eq!(rolcanlogin, "f");

    client.execute("DROP ROLE compat_role", &[]).await.unwrap();
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
