mod common;
use common::*;

#[tokio::test]
async fn test_smallint_simple() {
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
    
    // Query and check type
    let row = client.query_one("SELECT val FROM test_smallint WHERE id = 1", &[]).await.unwrap();
    
    let col_type = row.columns()[0].type_();
    println!("Column type returned: {:?} (OID: {})", col_type, col_type.oid());
    
    // This should be int2 (OID 21), not int4 (OID 23)
    assert_eq!(col_type.oid(), 21, "Should return int2 (SMALLINT) type");
    
    // Should be able to get as i16
    let val: i16 = row.get(0);
    assert_eq!(val, 42);
    
    server.abort();
}