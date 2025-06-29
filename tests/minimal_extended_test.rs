use tokio::net::TcpListener;
use tokio_postgres::NoTls;

#[tokio::test]
async fn test_minimal_extended() {
    // Enable debug logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("pgsqlite=info")
        .try_init();
    
    // Start test server
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    
    let server_handle = tokio::spawn(async move {
        let db_handler = std::sync::Arc::new(
            pgsqlite::session::DbHandler::new(":memory:").unwrap()
        );
        
        // Create simple test table
        db_handler.execute(
            "CREATE TABLE test (id INTEGER)"
        ).await.unwrap();
        
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
    
    // Try the simplest possible extended query - no parameters
    println!("Running simple extended query...");
    match client.query("SELECT 1", &[]).await {
        Ok(rows) => {
            println!("Query successful, got {} rows", rows.len());
            assert_eq!(rows.len(), 1);
        }
        Err(e) => {
            println!("Query failed: {:?}", e);
            panic!("Query failed");
        }
    }
    
    // Now try with a parameter
    println!("Running query with parameter...");
    match tokio::time::timeout(
        tokio::time::Duration::from_secs(2),
        client.query("SELECT $1::int4", &[&42i32])
    ).await {
        Ok(Ok(rows)) => {
            println!("Parameterized query successful, got {} rows", rows.len());
            assert_eq!(rows.len(), 1);
        }
        Ok(Err(e)) => {
            println!("Parameterized query failed: {:?}", e);
            panic!("Parameterized query failed");
        }
        Err(_) => {
            println!("Parameterized query timed out - likely infinite loop in catalog queries");
            panic!("Timeout");
        }
    }
    
    println!("Test passed!");
    
    server_handle.abort();
}