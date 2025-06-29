use tokio::net::TcpListener;
use tokio_postgres::NoTls;

#[tokio::test]
async fn test_param_type_lookup() {
    // Enable all debug logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("pgsqlite=debug")
        .try_init();
    
    // Start test server
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    
    let server_handle = tokio::spawn(async move {
        let db_handler = std::sync::Arc::new(
            pgsqlite::session::DbHandler::new(":memory:").unwrap()
        );
        
        let (stream, addr) = listener.accept().await.unwrap();
        pgsqlite::handle_test_connection_with_pool(stream, addr, db_handler).await.unwrap();
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
    
    println!("\n=== Starting test - will timeout in 1 second ===");
    
    // Try to prepare a statement with parameter
    match tokio::time::timeout(
        tokio::time::Duration::from_secs(1),
        client.prepare("SELECT $1::int4")
    ).await {
        Ok(Ok(stmt)) => {
            println!("Statement prepared successfully!");
            println!("Param types: {:?}", stmt.params());
        }
        Ok(Err(e)) => {
            println!("Statement preparation failed: {:?}", e);
        }
        Err(_) => {
            println!("Statement preparation timed out");
        }
    }
    
    server_handle.abort();
}