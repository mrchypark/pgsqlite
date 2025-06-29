mod common;
use common::*;

#[tokio::test]
async fn test_smallint_metadata_fixed() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create table with SMALLINT
    client.execute(
        "CREATE TABLE test_smallint (id INTEGER PRIMARY KEY, val SMALLINT)",
        &[]
    ).await.unwrap();
    
    // Insert a value
    client.execute(
        "INSERT INTO test_smallint VALUES (1, 42)",
        &[]
    ).await.unwrap();
    
    // Query the value - this should return correct type info
    let row = client.query_one("SELECT val FROM test_smallint WHERE id = 1", &[]).await.unwrap();
    
    let col_type = row.columns()[0].type_();
    println!("Column type returned: {:?} (OID: {})", col_type, col_type.oid());
    
    // This should be int2 (OID 21), not int4 (OID 23)
    assert_eq!(col_type.oid(), 21, "Should return int2 (SMALLINT) type");
    
    // Should be able to get as i16
    let val: i16 = row.get(0);
    assert_eq!(val, 42);
    
    // Now let's verify the metadata was stored correctly using a simple query
    // Use simple_query to avoid parameterized query issues with system tables
    let meta_rows = client.simple_query(
        "SELECT pg_type FROM __pgsqlite_schema WHERE table_name = 'test_smallint' AND column_name = 'val'"
    ).await.unwrap();
    
    let mut found_type = false;
    for msg in meta_rows {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            if let Some(pg_type) = row.get(0) {
                println!("Stored PostgreSQL type: {}", pg_type);
                assert_eq!(pg_type, "SMALLINT", "PostgreSQL type should be SMALLINT in metadata");
                found_type = true;
            }
        }
    }
    
    assert!(found_type, "Should find metadata for test_smallint.val");
    
    server.abort();
}