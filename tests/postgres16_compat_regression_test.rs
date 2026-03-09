mod common;
use bytes::{BufMut, BytesMut};
use common::setup_test_server;
use once_cell::sync::Lazy;
use std::sync::Arc;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
};
use tokio_postgres::SimpleQueryMessage;
use uuid::Uuid;

static PROTOCOL_REGRESSION_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

fn reset_protocol_caches() {
    pgsqlite::cache::GLOBAL_PARAMETER_CACHE.clear();
    pgsqlite::cache::GLOBAL_ROW_DESCRIPTION_CACHE.clear();
    pgsqlite::session::GLOBAL_QUERY_CACHE.clear();
}

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

#[derive(Debug, Clone, PartialEq, Eq)]
struct RawFieldDescription {
    name: String,
    type_oid: i32,
    format: i16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RawBackendMessage {
    ParseComplete,
    BindComplete,
    ParameterDescription(Vec<i32>),
    RowDescription(Vec<RawFieldDescription>),
    DataRow(Vec<Option<Vec<u8>>>),
    CommandComplete(String),
    ReadyForQuery(u8),
    NoData,
    Other(u8),
}

struct RawTestServer {
    port: u16,
    server_handle: tokio::task::JoinHandle<()>,
    db_path: String,
}

impl Drop for RawTestServer {
    fn drop(&mut self) {
        self.server_handle.abort();
        if !self.db_path.is_empty() && self.db_path != ":memory:" {
            let _ = std::fs::remove_file(&self.db_path);
            let _ = std::fs::remove_file(format!("{}-journal", self.db_path));
            let _ = std::fs::remove_file(format!("{}-wal", self.db_path));
            let _ = std::fs::remove_file(format!("{}-shm", self.db_path));
        }
    }
}

async fn setup_raw_test_server(init_sql: &[&str]) -> RawTestServer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let test_id = Uuid::new_v4().to_string().replace("-", "");
    let db_path = format!("/tmp/pgsqlite_raw_test_{test_id}.db");
    let db_path_clone = db_path.clone();
    let init_statements: Vec<String> = init_sql.iter().map(|sql| (*sql).to_string()).collect();

    let server_handle = tokio::spawn(async move {
        let db_handler = Arc::new(pgsqlite::session::DbHandler::new(&db_path_clone).unwrap());
        for statement in init_statements {
            db_handler.execute(&statement).await.unwrap();
        }

        let _ = db_handler.execute("PRAGMA schema_version").await;
        let _ = db_handler.execute("PRAGMA table_list").await;
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        let (stream, addr) = listener.accept().await.unwrap();
        if let Err(err) = pgsqlite::handle_test_connection_with_pool(stream, addr, db_handler).await
        {
            eprintln!("raw test server error: {err}");
        }
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    RawTestServer {
        port,
        server_handle,
        db_path,
    }
}

async fn write_startup_message(stream: &mut TcpStream) {
    let mut startup = BytesMut::new();
    startup.put_i32(0);
    startup.put_i32(196608);
    startup.put(&b"user\0postgres\0database\0test\0\0"[..]);
    let len = startup.len() as i32;
    startup[0..4].copy_from_slice(&len.to_be_bytes());
    stream.write_all(&startup).await.unwrap();
}

fn put_cstring(buf: &mut BytesMut, value: &str) {
    buf.extend_from_slice(value.as_bytes());
    buf.put_u8(0);
}

fn build_parse_message(name: &str, query: &str) -> BytesMut {
    let mut msg = BytesMut::new();
    msg.put_u8(b'P');
    msg.put_i32(0);
    put_cstring(&mut msg, name);
    put_cstring(&mut msg, query);
    msg.put_i16(0);
    let len = (msg.len() - 1) as i32;
    msg[1..5].copy_from_slice(&len.to_be_bytes());
    msg
}

fn build_query_message(query: &str) -> BytesMut {
    let mut msg = BytesMut::new();
    msg.put_u8(b'Q');
    msg.put_i32(0);
    put_cstring(&mut msg, query);
    let len = (msg.len() - 1) as i32;
    msg[1..5].copy_from_slice(&len.to_be_bytes());
    msg
}

fn build_describe_message(typ: u8, name: &str) -> BytesMut {
    let mut msg = BytesMut::new();
    msg.put_u8(b'D');
    msg.put_i32(0);
    msg.put_u8(typ);
    put_cstring(&mut msg, name);
    let len = (msg.len() - 1) as i32;
    msg[1..5].copy_from_slice(&len.to_be_bytes());
    msg
}

fn build_bind_message(
    portal: &str,
    statement: &str,
    param_values: &[Option<&[u8]>],
    result_formats: &[i16],
) -> BytesMut {
    let mut msg = BytesMut::new();
    msg.put_u8(b'B');
    msg.put_i32(0);
    put_cstring(&mut msg, portal);
    put_cstring(&mut msg, statement);
    msg.put_i16(0);
    msg.put_i16(param_values.len() as i16);
    for value in param_values {
        match value {
            Some(bytes) => {
                msg.put_i32(bytes.len() as i32);
                msg.extend_from_slice(bytes);
            }
            None => msg.put_i32(-1),
        }
    }
    msg.put_i16(result_formats.len() as i16);
    for format in result_formats {
        msg.put_i16(*format);
    }
    let len = (msg.len() - 1) as i32;
    msg[1..5].copy_from_slice(&len.to_be_bytes());
    msg
}

fn build_execute_message(portal: &str) -> BytesMut {
    let mut msg = BytesMut::new();
    msg.put_u8(b'E');
    msg.put_i32(0);
    put_cstring(&mut msg, portal);
    msg.put_i32(0);
    let len = (msg.len() - 1) as i32;
    msg[1..5].copy_from_slice(&len.to_be_bytes());
    msg
}

fn build_sync_message() -> BytesMut {
    let mut msg = BytesMut::new();
    msg.put_u8(b'S');
    msg.put_i32(4);
    msg
}

fn read_cstring(bytes: &[u8], cursor: &mut usize) -> String {
    let start = *cursor;
    while bytes[*cursor] != 0 {
        *cursor += 1;
    }
    let value = String::from_utf8(bytes[start..*cursor].to_vec()).unwrap();
    *cursor += 1;
    value
}

fn parse_backend_message(msg_type: u8, body: &[u8]) -> RawBackendMessage {
    match msg_type {
        b'1' => RawBackendMessage::ParseComplete,
        b'2' => RawBackendMessage::BindComplete,
        b'n' => RawBackendMessage::NoData,
        b't' => {
            let mut cursor = 0;
            let count = i16::from_be_bytes([body[cursor], body[cursor + 1]]) as usize;
            cursor += 2;
            let mut oids = Vec::with_capacity(count);
            for _ in 0..count {
                oids.push(i32::from_be_bytes([
                    body[cursor],
                    body[cursor + 1],
                    body[cursor + 2],
                    body[cursor + 3],
                ]));
                cursor += 4;
            }
            RawBackendMessage::ParameterDescription(oids)
        }
        b'T' => {
            let mut cursor = 0;
            let count = i16::from_be_bytes([body[cursor], body[cursor + 1]]) as usize;
            cursor += 2;
            let mut fields = Vec::with_capacity(count);
            for _ in 0..count {
                let name = read_cstring(body, &mut cursor);
                let _table_oid = i32::from_be_bytes([
                    body[cursor],
                    body[cursor + 1],
                    body[cursor + 2],
                    body[cursor + 3],
                ]);
                cursor += 4;
                let _column_id = i16::from_be_bytes([body[cursor], body[cursor + 1]]);
                cursor += 2;
                let type_oid = i32::from_be_bytes([
                    body[cursor],
                    body[cursor + 1],
                    body[cursor + 2],
                    body[cursor + 3],
                ]);
                cursor += 4;
                let _type_size = i16::from_be_bytes([body[cursor], body[cursor + 1]]);
                cursor += 2;
                let _type_modifier = i32::from_be_bytes([
                    body[cursor],
                    body[cursor + 1],
                    body[cursor + 2],
                    body[cursor + 3],
                ]);
                cursor += 4;
                let format = i16::from_be_bytes([body[cursor], body[cursor + 1]]);
                cursor += 2;

                fields.push(RawFieldDescription {
                    name,
                    type_oid,
                    format,
                });
            }
            RawBackendMessage::RowDescription(fields)
        }
        b'D' => {
            let mut cursor = 0;
            let count = i16::from_be_bytes([body[cursor], body[cursor + 1]]) as usize;
            cursor += 2;
            let mut values = Vec::with_capacity(count);
            for _ in 0..count {
                let len = i32::from_be_bytes([
                    body[cursor],
                    body[cursor + 1],
                    body[cursor + 2],
                    body[cursor + 3],
                ]);
                cursor += 4;
                if len < 0 {
                    values.push(None);
                } else {
                    let len = len as usize;
                    values.push(Some(body[cursor..cursor + len].to_vec()));
                    cursor += len;
                }
            }
            RawBackendMessage::DataRow(values)
        }
        b'C' => {
            let tag = String::from_utf8(body[..body.len().saturating_sub(1)].to_vec()).unwrap();
            RawBackendMessage::CommandComplete(tag)
        }
        b'Z' => RawBackendMessage::ReadyForQuery(body[0]),
        _ => RawBackendMessage::Other(msg_type),
    }
}

async fn read_backend_messages_until_ready(stream: &mut TcpStream) -> Vec<RawBackendMessage> {
    let mut messages = Vec::new();

    loop {
        let msg_type = stream.read_u8().await.unwrap();
        let len = stream.read_i32().await.unwrap() as usize;
        let mut body = vec![0u8; len - 4];
        stream.read_exact(&mut body).await.unwrap();
        let parsed = parse_backend_message(msg_type, &body);
        let is_ready = matches!(parsed, RawBackendMessage::ReadyForQuery(_));
        messages.push(parsed);
        if is_ready {
            return messages;
        }
    }
}

#[tokio::test]
async fn test_set_transaction_isolation_level_roundtrip() {
    let _guard = PROTOCOL_REGRESSION_LOCK.lock().await;
    reset_protocol_caches();
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
    let _guard = PROTOCOL_REGRESSION_LOCK.lock().await;
    reset_protocol_caches();
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
    assert_eq!(
        backing_count, 1,
        "CREATE ROLE should persist into __pgsqlite_roles"
    );

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
    assert_eq!(
        count, 0,
        "CREATE ROLE without LOGIN should not appear in pg_user"
    );

    client.execute("DROP ROLE compat_role", &[]).await.unwrap();
}

#[tokio::test]
async fn test_set_local_search_path_resets_after_commit() {
    let _guard = PROTOCOL_REGRESSION_LOCK.lock().await;
    reset_protocol_caches();
    let server = setup_test_server().await;
    let client = &server.client;

    client.batch_execute("BEGIN").await.unwrap();
    client
        .batch_execute("SET LOCAL search_path = pg_catalog")
        .await
        .unwrap();

    let row = client.query_one("SHOW search_path", &[]).await.unwrap();
    let value: &str = row.get(0);
    assert_eq!(value, "pg_catalog");

    client.batch_execute("COMMIT").await.unwrap();

    let row = client.query_one("SHOW search_path", &[]).await.unwrap();
    let value: &str = row.get(0);
    assert_eq!(value, "public");
}

#[tokio::test]
async fn test_prepared_select_describe_reports_direct_column_oids() {
    let _guard = PROTOCOL_REGRESSION_LOCK.lock().await;
    reset_protocol_caches();
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
async fn test_insert_returning_extended_protocol_roundtrip() {
    let _guard = PROTOCOL_REGRESSION_LOCK.lock().await;
    reset_protocol_caches();
    let server = setup_test_server().await;
    let client = &server.client;

    client
        .batch_execute(
            "CREATE TABLE returning_probe_task1 (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )",
        )
        .await
        .unwrap();

    let insert_stmt = client
        .prepare("INSERT INTO returning_probe_task1 (name) VALUES ($1) RETURNING id, name")
        .await
        .unwrap();
    assert_eq!(insert_stmt.columns().len(), 2);
    assert_eq!(insert_stmt.columns()[0].name(), "id");
    assert_eq!(insert_stmt.columns()[0].type_().oid(), 23);
    assert_eq!(insert_stmt.columns()[1].name(), "name");
    assert_eq!(insert_stmt.columns()[1].type_().oid(), 25);

    let insert_row = client.query_one(&insert_stmt, &[&"alice"]).await.unwrap();
    let inserted_id: i32 = insert_row.get(0);
    let inserted_name: &str = insert_row.get(1);
    assert_eq!(inserted_id, 1);
    assert_eq!(inserted_name, "alice");

    let update_stmt = client
        .prepare("UPDATE returning_probe_task1 SET name = $1 WHERE id = $2 RETURNING id")
        .await
        .unwrap();
    assert_eq!(update_stmt.columns().len(), 1);
    assert_eq!(update_stmt.columns()[0].name(), "id");
    assert_eq!(update_stmt.columns()[0].type_().oid(), 23);
    let inserted_id_text = inserted_id.to_string();

    let update_row = client
        .query_one(&update_stmt, &[&"alice-updated", &inserted_id_text])
        .await
        .unwrap();
    let updated_id: i32 = update_row.get(0);
    assert_eq!(updated_id, inserted_id);

    let delete_stmt = client
        .prepare("DELETE FROM returning_probe_task1 WHERE id = $1 RETURNING name")
        .await
        .unwrap();
    assert_eq!(delete_stmt.columns().len(), 1);
    assert_eq!(delete_stmt.columns()[0].name(), "name");
    assert_eq!(delete_stmt.columns()[0].type_().oid(), 25);

    let delete_row = client
        .query_one(&delete_stmt, &[&inserted_id_text])
        .await
        .unwrap();
    let deleted_name: &str = delete_row.get(0);
    assert_eq!(deleted_name, "alice-updated");
}

#[tokio::test]
async fn test_returning_describe_execute_metadata_consistency() {
    let _guard = PROTOCOL_REGRESSION_LOCK.lock().await;
    reset_protocol_caches();
    let server = setup_raw_test_server(&[
        "CREATE TABLE returning_probe_task2 (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
    ])
    .await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", server.port))
        .await
        .unwrap();

    write_startup_message(&mut stream).await;
    let _startup_messages = read_backend_messages_until_ready(&mut stream).await;

    let parse = build_parse_message(
        "ret_stmt",
        "INSERT INTO returning_probe_task2 (name) VALUES ($1) RETURNING id, name",
    );
    let describe_statement = build_describe_message(b'S', "ret_stmt");
    let sync = build_sync_message();

    stream.write_all(&parse).await.unwrap();
    stream.write_all(&describe_statement).await.unwrap();
    stream.write_all(&sync).await.unwrap();

    let statement_messages = read_backend_messages_until_ready(&mut stream).await;
    let statement_fields = statement_messages
        .iter()
        .find_map(|msg| match msg {
            RawBackendMessage::RowDescription(fields) => Some(fields.clone()),
            _ => None,
        })
        .expect("statement describe should return row metadata");
    assert_eq!(
        statement_fields,
        vec![
            RawFieldDescription {
                name: "id".to_string(),
                type_oid: 23,
                format: 0,
            },
            RawFieldDescription {
                name: "name".to_string(),
                type_oid: 25,
                format: 0,
            },
        ]
    );

    let bind = build_bind_message("ret_portal", "ret_stmt", &[Some(b"bravo")], &[1]);
    let describe_portal = build_describe_message(b'P', "ret_portal");
    let execute = build_execute_message("ret_portal");

    stream.write_all(&bind).await.unwrap();
    stream.write_all(&describe_portal).await.unwrap();
    stream.write_all(&execute).await.unwrap();
    stream.write_all(&sync).await.unwrap();

    let portal_messages = read_backend_messages_until_ready(&mut stream).await;
    let portal_row_descriptions: Vec<Vec<RawFieldDescription>> = portal_messages
        .iter()
        .filter_map(|msg| match msg {
            RawBackendMessage::RowDescription(fields) => Some(fields.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        portal_row_descriptions.len(),
        1,
        "execute should not emit an extra RowDescription after Describe(portal)"
    );

    let portal_fields = portal_row_descriptions[0].clone();
    assert_eq!(
        portal_fields
            .iter()
            .map(|field| (field.name.clone(), field.type_oid))
            .collect::<Vec<_>>(),
        statement_fields
            .iter()
            .map(|field| (field.name.clone(), field.type_oid))
            .collect::<Vec<_>>()
    );
    assert!(
        portal_fields.iter().all(|field| field.format == 1),
        "portal describe should advertise binary format when binary results are requested: {:?}",
        portal_fields
    );

    let data_row = portal_messages
        .iter()
        .find_map(|msg| match msg {
            RawBackendMessage::DataRow(values) => Some(values.clone()),
            _ => None,
        })
        .expect("execute should return one row");
    assert_eq!(data_row.len(), 2);

    let id_bytes = data_row[0].clone().expect("id should be present");
    assert_eq!(id_bytes.len(), 4, "binary int4 should be 4 bytes");
    assert_eq!(i32::from_be_bytes(id_bytes.try_into().unwrap()), 1);

    let name_bytes = data_row[1].clone().expect("name should be present");
    assert_eq!(name_bytes, b"bravo".to_vec());
}

#[tokio::test]
async fn test_returning_simple_vs_extended_fidelity() {
    let _guard = PROTOCOL_REGRESSION_LOCK.lock().await;
    reset_protocol_caches();
    let simple_server = setup_raw_test_server(&[
        "CREATE TABLE returning_probe_simple (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
    ])
    .await;
    let mut simple_stream = TcpStream::connect(format!("127.0.0.1:{}", simple_server.port))
        .await
        .unwrap();
    write_startup_message(&mut simple_stream).await;
    let _ = read_backend_messages_until_ready(&mut simple_stream).await;

    let simple_query = build_query_message(
        "INSERT INTO returning_probe_simple (name) VALUES ('simple') RETURNING id",
    );
    simple_stream.write_all(&simple_query).await.unwrap();
    let simple_messages = read_backend_messages_until_ready(&mut simple_stream).await;
    let simple_fields = simple_messages
        .iter()
        .find_map(|msg| match msg {
            RawBackendMessage::RowDescription(fields) => Some(fields.clone()),
            _ => None,
        })
        .expect("simple query should emit row description");
    let simple_row = simple_messages
        .iter()
        .find_map(|msg| match msg {
            RawBackendMessage::DataRow(values) => Some(values.clone()),
            _ => None,
        })
        .expect("simple query should emit data row");

    let extended_server = setup_raw_test_server(&[
        "CREATE TABLE returning_probe_extended (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
    ])
    .await;
    let mut extended_stream = TcpStream::connect(format!("127.0.0.1:{}", extended_server.port))
        .await
        .unwrap();
    write_startup_message(&mut extended_stream).await;
    let _ = read_backend_messages_until_ready(&mut extended_stream).await;

    let parse = build_parse_message(
        "fid_stmt",
        "INSERT INTO returning_probe_extended (name) VALUES ('extended') RETURNING id",
    );
    let bind = build_bind_message("fid_portal", "fid_stmt", &[], &[0]);
    let execute = build_execute_message("fid_portal");
    let sync = build_sync_message();

    extended_stream.write_all(&parse).await.unwrap();
    extended_stream.write_all(&bind).await.unwrap();
    extended_stream.write_all(&execute).await.unwrap();
    extended_stream.write_all(&sync).await.unwrap();

    let extended_messages = read_backend_messages_until_ready(&mut extended_stream).await;
    let extended_fields = extended_messages
        .iter()
        .find_map(|msg| match msg {
            RawBackendMessage::RowDescription(fields) => Some(fields.clone()),
            _ => None,
        })
        .expect("extended query should emit row description");
    let extended_row = extended_messages
        .iter()
        .find_map(|msg| match msg {
            RawBackendMessage::DataRow(values) => Some(values.clone()),
            _ => None,
        })
        .expect("extended query should emit data row");

    assert_eq!(simple_fields, extended_fields);
    assert_eq!(simple_fields.len(), 1);
    assert_eq!(simple_fields[0].name, "id");
    assert_eq!(simple_fields[0].type_oid, 23);

    let simple_id = std::str::from_utf8(
        simple_row[0]
            .as_ref()
            .expect("simple returning id should not be null"),
    )
    .unwrap()
    .parse::<i32>()
    .unwrap();
    let extended_id = std::str::from_utf8(
        extended_row[0]
            .as_ref()
            .expect("extended returning id should not be null"),
    )
    .unwrap()
    .parse::<i32>()
    .unwrap();
    assert_eq!(simple_id, 1);
    assert_eq!(extended_id, 1);
}

#[tokio::test]
async fn test_psql16_d_table_query_shape() {
    let _guard = PROTOCOL_REGRESSION_LOCK.lock().await;
    reset_protocol_caches();
    let server = setup_test_server().await;
    let client = &server.client;

    client
        .batch_execute(
            "CREATE TABLE meta_test_users_d (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT UNIQUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX meta_test_users_d_name_idx ON meta_test_users_d(name);",
        )
        .await
        .unwrap();

    let rel_lookup = client
        .query_one(
            "SELECT c.oid, n.nspname, c.relname
             FROM pg_catalog.pg_class c
             LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
             WHERE c.relname OPERATOR(pg_catalog.~) '^(meta_test_users_d)$' COLLATE pg_catalog.default
               AND pg_catalog.pg_table_is_visible(c.oid)
             ORDER BY 2, 3",
            &[],
        )
        .await
        .unwrap();
    let rel_oid: u32 = rel_lookup.get(0);
    let rel_schema: &str = rel_lookup.get(1);
    let rel_name: &str = rel_lookup.get(2);
    assert_eq!(rel_schema, "public");
    assert_eq!(rel_name, "meta_test_users_d");

    let rel_details_query = format!(
        "SELECT c.relchecks, c.relkind, c.relhasindex, c.relhasrules, c.relhastriggers,
                c.relrowsecurity, c.relforcerowsecurity, false AS relhasoids,
                c.relispartition, '', c.reltablespace,
                CASE WHEN c.reloftype = 0 THEN '' ELSE c.reloftype::pg_catalog.regtype::pg_catalog.text END,
                c.relpersistence, c.relreplident, am.amname
         FROM pg_catalog.pg_class c
         LEFT JOIN pg_catalog.pg_class tc ON (c.reltoastrelid = tc.oid)
         LEFT JOIN pg_catalog.pg_am am ON (c.relam = am.oid)
         WHERE c.oid = {rel_oid}"
    );
    let rel_details = client.query_one(&rel_details_query, &[]).await.unwrap();

    let relchecks: i16 = rel_details.get(0);
    let relkind: &str = rel_details.get(1);
    let relhasindex: bool = rel_details.get(2);
    let relpersistence: &str = rel_details.get(12);
    let relreplident: &str = rel_details.get(13);
    let amname: Option<&str> = rel_details.get(14);

    assert_eq!(relchecks, 0);
    assert_eq!(relkind, "r");
    assert!(relhasindex);
    assert_eq!(relpersistence, "p");
    assert_eq!(relreplident, "d");
    assert_eq!(amname, None);
}

#[tokio::test]
async fn test_grant_missing_relation_errors() {
    let _guard = PROTOCOL_REGRESSION_LOCK.lock().await;
    reset_protocol_caches();
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
    let _guard = PROTOCOL_REGRESSION_LOCK.lock().await;
    reset_protocol_caches();
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
    let _guard = PROTOCOL_REGRESSION_LOCK.lock().await;
    reset_protocol_caches();
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
    let _guard = PROTOCOL_REGRESSION_LOCK.lock().await;
    reset_protocol_caches();
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
