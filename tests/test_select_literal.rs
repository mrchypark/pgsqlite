use pgsqlite::session::DbHandler;

#[tokio::test]
async fn test_select_literal() {
    let db = DbHandler::new(":memory:").unwrap();
    
    // Test what SQLite returns for SELECT 42
    let result = db.query("SELECT 42").await.unwrap();
    
    println!("Columns: {:?}", result.columns);
    println!("Rows: {:?}", result.rows);
    
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.columns.len(), 1);
    
    // Check the column name
    println!("Column name: {}", result.columns[0]);
    
    // Check the value
    if let Some(val) = &result.rows[0][0] {
        let str_val = String::from_utf8_lossy(val);
        println!("Value as string: {}", str_val);
    }
}