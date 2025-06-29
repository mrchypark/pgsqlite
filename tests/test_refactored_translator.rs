use pgsqlite::translator::CreateTableTranslator;

#[test]
fn test_create_table_translator_uses_type_mapper() {
    // Test that the refactored translator produces the same results as before
    
    let test_cases = vec![
        // Basic types
        (
            "CREATE TABLE users (id INTEGER, name TEXT, active BOOLEAN)",
            "CREATE TABLE users (id INTEGER, name TEXT, active INTEGER)"
        ),
        // SERIAL types should get AUTOINCREMENT
        (
            "CREATE TABLE posts (id SERIAL PRIMARY KEY, title VARCHAR(255))",
            "CREATE TABLE posts (id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT)"
        ),
        (
            "CREATE TABLE logs (id BIGSERIAL, message TEXT)",
            "CREATE TABLE logs (id INTEGER PRIMARY KEY AUTOINCREMENT, message TEXT)"
        ),
        // Parametric types
        (
            "CREATE TABLE products (name VARCHAR(100), price NUMERIC(10,2))",
            "CREATE TABLE products (name TEXT, price DECIMAL)"
        ),
        // Multi-word types
        (
            "CREATE TABLE events (created_at TIMESTAMP WITH TIME ZONE, duration DOUBLE PRECISION)",
            "CREATE TABLE events (created_at TEXT, duration DECIMAL)"
        ),
        // New types from recent work
        (
            "CREATE TABLE network (ip INET, mac MACADDR, price MONEY)",
            "CREATE TABLE network (ip TEXT, mac TEXT, price TEXT)"
        ),
        (
            "CREATE TABLE ranges (int_range INT4RANGE, bits BIT(8))",
            "CREATE TABLE ranges (int_range TEXT, bits TEXT)"
        ),
    ];
    
    for (pg_sql, expected_sqlite) in test_cases {
        let (result, _mappings) = CreateTableTranslator::translate(pg_sql).unwrap();
        assert_eq!(result, expected_sqlite, "Failed for input: {}", pg_sql);
    }
}

#[test]
fn test_create_table_translator_type_mappings() {
    // Test that type mappings are stored correctly
    let pg_sql = "CREATE TABLE test (id SERIAL, name VARCHAR(255), price MONEY, ip INET)";
    let (_sqlite_sql, mappings) = CreateTableTranslator::translate(pg_sql).unwrap();
    
    // Check that mappings are stored correctly
    assert_eq!(mappings.get("test.id").unwrap().pg_type, "SERIAL");
    assert_eq!(mappings.get("test.id").unwrap().sqlite_type, "INTEGER PRIMARY KEY AUTOINCREMENT");
    
    assert_eq!(mappings.get("test.name").unwrap().pg_type, "VARCHAR(255)");
    assert_eq!(mappings.get("test.name").unwrap().sqlite_type, "TEXT");
    
    assert_eq!(mappings.get("test.price").unwrap().pg_type, "MONEY");
    assert_eq!(mappings.get("test.price").unwrap().sqlite_type, "TEXT");
    
    assert_eq!(mappings.get("test.ip").unwrap().pg_type, "INET");
    assert_eq!(mappings.get("test.ip").unwrap().sqlite_type, "TEXT");
}

#[test]
fn test_serial_primary_key_handling() {
    // Test that SERIAL PRIMARY KEY doesn't duplicate PRIMARY KEY
    let test_cases = vec![
        (
            "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT)",
            "CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)"
        ),
        (
            "CREATE TABLE posts (id BIGSERIAL PRIMARY KEY, title TEXT)",
            "CREATE TABLE posts (id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT)"
        ),
        // SERIAL without explicit PRIMARY KEY should still get it
        (
            "CREATE TABLE logs (id SERIAL, message TEXT)",
            "CREATE TABLE logs (id INTEGER PRIMARY KEY AUTOINCREMENT, message TEXT)"
        ),
    ];
    
    for (pg_sql, expected_sqlite) in test_cases {
        let (result, _mappings) = CreateTableTranslator::translate(pg_sql).unwrap();
        assert_eq!(result, expected_sqlite, "Failed for input: {}", pg_sql);
    }
}