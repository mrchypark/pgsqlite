mod common;

#[tokio::test]
async fn test_explicit_cast_types() {
    let server = common::setup_test_server().await;
    let client = &server.client;

    // Create a test table with various types
    client.execute(
        "CREATE TABLE cast_test (
            id INTEGER PRIMARY KEY,
            bit_val BIT(8),
            int_val INTEGER,
            text_val TEXT
        )",
        &[]
    ).await.unwrap();

    // Insert test data
    client.execute(
        "INSERT INTO cast_test (id, bit_val, int_val, text_val) VALUES ($1, $2, $3, $4)",
        &[&1i32, &"10101010", &42i32, &"hello"]
    ).await.unwrap();

    // Test explicit casts in prepared statements
    let stmt = client.prepare(
        "SELECT 
            bit_val::text,
            int_val::text,
            text_val::int4,
            bit_val::bit
        FROM cast_test WHERE id = 1"
    ).await.unwrap();

    // Verify the column types are what we cast them to
    assert_eq!(stmt.columns()[0].type_().name(), "text", "bit_val::text should be text type");
    assert_eq!(stmt.columns()[1].type_().name(), "text", "int_val::text should be text type");
    assert_eq!(stmt.columns()[2].type_().name(), "int4", "text_val::int4 should be int4 type");
    assert_eq!(stmt.columns()[3].type_().name(), "bit", "bit_val::bit should be bit type");

    // Execute and verify we can retrieve the values with the cast types
    let row = client.query_one(&stmt, &[]).await.unwrap();
    
    let bit_as_text: String = row.get(0);
    assert_eq!(bit_as_text, "10101010");
    
    let int_as_text: String = row.get(1);
    assert_eq!(int_as_text, "42");
    
    // text_val contains "hello" which can't be cast to int4, so this would be NULL or error
    // Skip this check as it depends on SQLite's behavior
    
    // For bit type, we can't directly get as String in tokio_postgres
    // but the type should be reported correctly

    server.abort();
}

#[tokio::test]
async fn test_cast_with_aliases() {
    let server = common::setup_test_server().await;
    let client = &server.client;

    // Create a test table
    client.execute(
        "CREATE TABLE alias_test (
            id INTEGER PRIMARY KEY,
            num_val NUMERIC
        )",
        &[]
    ).await.unwrap();

    client.execute(
        "INSERT INTO alias_test (id, num_val) VALUES (1, 123.45)",
        &[]
    ).await.unwrap();

    // Test casts with column aliases
    let stmt = client.prepare(
        "SELECT 
            num_val::text AS text_version,
            num_val::int4 AS int_version,
            num_val AS original
        FROM alias_test WHERE id = 1"
    ).await.unwrap();

    // Verify column names and types
    assert_eq!(stmt.columns()[0].name(), "text_version");
    assert_eq!(stmt.columns()[0].type_().name(), "text");
    
    assert_eq!(stmt.columns()[1].name(), "int_version");
    assert_eq!(stmt.columns()[1].type_().name(), "int4");
    
    assert_eq!(stmt.columns()[2].name(), "original");
    // Original type might be inferred as float8 or text depending on value

    server.abort();
}