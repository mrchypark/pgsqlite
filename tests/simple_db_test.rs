#[tokio::test]
async fn test_db_handler() {
    use pgsqlite::session::DbHandler;
    
    let db = DbHandler::new(":memory:").unwrap();
    
    // Create table
    db.execute("CREATE TABLE test (id INTEGER, name TEXT)").await.unwrap();
    
    // Insert data
    db.execute("INSERT INTO test VALUES (1, 'Alice')").await.unwrap();
    
    // Query data
    let result = db.query("SELECT * FROM test").await.unwrap();
    
    assert_eq!(result.columns, vec!["id", "name"]);
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Some(b"1".to_vec()));
    assert_eq!(result.rows[0][1], Some(b"Alice".to_vec()));
}