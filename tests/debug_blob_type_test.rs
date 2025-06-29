mod common;
use common::*;

#[tokio::test]
async fn test_debug_blob_type_inference() {
    // Enable debug logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("pgsqlite=debug")
        .try_init();
    
    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute(
                "CREATE TABLE null_test (
                    id INTEGER PRIMARY KEY,
                    opt_int INTEGER,
                    opt_text TEXT,
                    opt_blob BLOB
                )"
            ).await?;
            
            db.execute(
                "INSERT INTO null_test VALUES (1, NULL, NULL, NULL)"
            ).await?;
            
            db.execute(
                "INSERT INTO null_test VALUES (2, 42, 'hello', X'010203')"
            ).await?;
            
            Ok(())
        })
    }).await;
    
    let client = &server.client;
    
    println!("Testing SELECT query type inference...");
    
    // First, test with prepare to see what types are inferred
    match client.prepare("SELECT opt_int, opt_text, opt_blob FROM null_test WHERE id = $1").await {
        Ok(stmt) => {
            println!("Statement prepared successfully");
            println!("Parameter types: {:?}", stmt.params());
            println!("Column types: {:?}", stmt.columns());
            
            // Try to query
            match client.query(&stmt, &[&2i32]).await {
                Ok(rows) => {
                    println!("Query successful: {} rows", rows.len());
                    if !rows.is_empty() {
                        let row = &rows[0];
                        println!("Row columns: {:?}", row.columns());
                        
                        // Try to get values
                        match row.try_get::<_, i32>(0) {
                            Ok(v) => println!("opt_int: {}", v),
                            Err(e) => println!("opt_int error: {:?}", e),
                        }
                        
                        match row.try_get::<_, String>(1) {
                            Ok(v) => println!("opt_text: {}", v),
                            Err(e) => println!("opt_text error: {:?}", e),
                        }
                        
                        match row.try_get::<_, Vec<u8>>(2) {
                            Ok(v) => println!("opt_blob: {:?}", v),
                            Err(e) => println!("opt_blob error: {:?}", e),
                        }
                    }
                },
                Err(e) => println!("Query failed: {:?}", e),
            }
        }
        Err(e) => println!("Prepare failed: {:?}", e),
    }
    
    server.abort();
}