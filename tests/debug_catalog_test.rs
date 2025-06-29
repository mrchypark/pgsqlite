use tokio::net::TcpListener;
use tokio_postgres::NoTls;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

#[tokio::test]
async fn test_debug_catalog_queries() {
    // Enable debug logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("pgsqlite::query::extended=info,pgsqlite::catalog=debug")
        .try_init();
    
    // Counter for catalog queries
    let _catalog_query_count = Arc::new(AtomicUsize::new(0));
    
    // Start test server
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    
    let server_handle = tokio::spawn(async move {
        let db_handler = std::sync::Arc::new(
            pgsqlite::session::DbHandler::new(":memory:").unwrap()
        );
        
        let (stream, addr) = listener.accept().await.unwrap();
        
        // Wrap the connection handler to count catalog queries
        match pgsqlite::handle_test_connection_with_pool(stream, addr, db_handler).await {
            Ok(_) => println!("Connection handled successfully"),
            Err(e) => println!("Connection error: {}", e),
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
    
    // Try a parameterized query and see what happens
    println!("\n=== Starting parameterized query ===");
    
    // Set a limit on how many queries we'll allow
    let start = std::time::Instant::now();
    let timeout = tokio::time::Duration::from_secs(2);
    
    match tokio::time::timeout(
        timeout,
        client.query("SELECT $1::int4", &[&42i32])
    ).await {
        Ok(Ok(rows)) => {
            println!("Query successful! Got {} rows", rows.len());
        }
        Ok(Err(e)) => {
            println!("Query failed: {:?}", e);
        }
        Err(_) => {
            println!("Query timed out after {:?}", start.elapsed());
            println!("This indicates an infinite loop in catalog queries");
        }
    }
    
    server_handle.abort();
}