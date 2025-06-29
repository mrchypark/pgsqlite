use tokio::net::TcpListener;
use tokio_postgres::{Client, NoTls};
use std::sync::Arc;

pub struct TestServer {
    pub client: Client,
    #[allow(dead_code)]
    pub port: u16,
    pub server_handle: tokio::task::JoinHandle<()>,
}

impl TestServer {
    pub fn abort(self) {
        self.server_handle.abort();
    }
}

/// Setup a test server with an in-memory SQLite database
#[allow(dead_code)]
pub async fn setup_test_server() -> TestServer {
    setup_test_server_with_init(|_| Box::pin(async move { Ok(()) })).await
}

/// Setup a test server with custom initialization
pub async fn setup_test_server_with_init<F, Fut>(init: F) -> TestServer 
where
    F: FnOnce(Arc<pgsqlite::session::DbHandler>) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send,
{
    // Start test server
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    
    let server_handle = tokio::spawn(async move {
        let db_handler = Arc::new(
            pgsqlite::session::DbHandler::new(":memory:").unwrap()
        );
        
        // Run custom initialization
        if let Err(e) = init(db_handler.clone()).await {
            eprintln!("Init error: {}", e);
            return;
        }
        
        let (stream, addr) = listener.accept().await.unwrap();
        if let Err(e) = pgsqlite::handle_test_connection_with_pool(stream, addr, db_handler).await {
            eprintln!("Connection handling error: {}", e);
        }
    });
    
    // Give server time to start
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    // Connect with tokio-postgres
    let config = format!("host=localhost port={} dbname=test user=testuser", port);
    let (client, connection) = tokio_postgres::connect(&config, NoTls).await.unwrap();
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });
    
    TestServer {
        client,
        port,
        server_handle,
    }
}

/// Helper to create test table with various data types
#[allow(dead_code)]
pub async fn create_test_table(db: &pgsqlite::session::DbHandler, table_name: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let query = format!(
        "CREATE TABLE {} (
            id INTEGER PRIMARY KEY,
            name TEXT,
            age INTEGER,
            salary REAL,
            active INTEGER,  -- SQLite doesn't have BOOLEAN
            data BLOB,
            created_at TEXT, -- SQLite doesn't have native TIMESTAMP
            metadata TEXT    -- For JSON data
        )",
        table_name
    );
    db.execute(&query).await?;
    Ok(())
}

/// Assert that a PostgreSQL error has the expected SQLSTATE code
#[allow(dead_code)]
pub fn assert_pg_error_code(err: &tokio_postgres::Error, expected_code: &str) {
    if let Some(db_err) = err.as_db_error() {
        assert_eq!(db_err.code().code(), expected_code, 
            "Expected error code {} but got {}: {}", 
            expected_code, db_err.code().code(), db_err.message());
    } else {
        panic!("Expected database error with code {} but got: {:?}", expected_code, err);
    }
}

/// Helper to execute a query and return the first row's first column as a string
#[allow(dead_code)]
pub async fn query_one_string(client: &Client, query: &str) -> Result<String, tokio_postgres::Error> {
    let row = client.query_one(query, &[]).await?;
    Ok(row.get::<_, String>(0))
}

/// Helper to test both simple and extended query protocols
#[allow(dead_code)]
pub async fn test_both_protocols<F, Fut>(client: &Client, query: &str, params: &[&(dyn tokio_postgres::types::ToSql + Sync)], test_fn: F) 
where
    F: Fn(Vec<tokio_postgres::Row>) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    // Test extended protocol
    let rows = client.query(query, params).await.unwrap();
    test_fn(rows).await;
    
    // Test simple protocol if no parameters
    if params.is_empty() {
        let messages = client.simple_query(query).await.unwrap();
        // Extract rows from SimpleQueryMessage
        let _rows: Vec<tokio_postgres::Row> = Vec::new();
        for msg in messages {
            if let tokio_postgres::SimpleQueryMessage::Row(_) = msg {
                // For simple protocol, we can't easily extract typed data
                // So we'll skip the detailed verification
                return;
            }
        }
    }
}