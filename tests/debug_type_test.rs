mod common;
use common::*;

#[tokio::test]
async fn test_debug_type_inference() {
    // Enable debug logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("pgsqlite=debug")
        .try_init();
    
    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute(
                "CREATE TABLE test_table (
                    id INTEGER PRIMARY KEY,
                    data BLOB
                )"
            ).await?;
            
            Ok(())
        })
    }).await;
    
    let client = &server.client;
    
    println!("Testing bytea parameter type inference...");
    
    // Test inserting with explicit parameter types
    let test_data = vec![0u8, 1, 2, 3];
    
    // First, let's see what types are inferred
    match client.prepare("INSERT INTO test_table (id, data) VALUES ($1, $2)").await {
        Ok(stmt) => {
            println!("Statement prepared successfully");
            println!("Parameter types: {:?}", stmt.params());
            
            // Now try to execute
            match client.execute(&stmt, &[&1i32, &test_data]).await {
                Ok(rows) => println!("Insert successful: {} rows", rows),
                Err(e) => println!("Insert failed: {:?}", e),
            }
        }
        Err(e) => println!("Prepare failed: {:?}", e),
    }
    
    server.abort();
}