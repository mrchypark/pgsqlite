use pgsqlite::catalog::CatalogInterceptor;

#[test]
fn test_catalog_interceptor() {
    // Test simple pg_type query
    let query = "SELECT oid, typname FROM pg_catalog.pg_type WHERE oid = 25";
    let result = CatalogInterceptor::intercept_query(query);
    assert!(result.is_some());
    
    let response = result.unwrap().unwrap();
    assert_eq!(response.columns, vec!["oid", "typname"]);
    assert_eq!(response.rows.len(), 1);
    assert_eq!(response.rows[0][0], Some("25".as_bytes().to_vec()));
    assert_eq!(response.rows[0][1], Some("text".as_bytes().to_vec()));
    
    // Test pg_type query with parameter placeholder
    let query = "SELECT oid, typname FROM pg_catalog.pg_type WHERE oid = $1";
    let result = CatalogInterceptor::intercept_query(query);
    assert!(result.is_some());
    
    let response = result.unwrap().unwrap();
    assert_eq!(response.columns, vec!["oid", "typname"]);
    assert_eq!(response.rows.len(), 36); // Should return all types (18 basic + 18 array types)
    
    // Test complex JOIN query
    let query = "SELECT t.typname, t.typtype, n.nspname 
                 FROM pg_catalog.pg_type t 
                 INNER JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid 
                 WHERE t.oid = $1";
    let result = CatalogInterceptor::intercept_query(query);
    assert!(result.is_some());
    
    let response = result.unwrap().unwrap();
    assert_eq!(response.columns, vec!["typname", "typtype", "nspname"]);
    
    // Test non-catalog query
    let query = "SELECT * FROM users";
    let result = CatalogInterceptor::intercept_query(query);
    assert!(result.is_none());
}

#[test]
fn test_catalog_with_joins() {
    let query = "SELECT t.typname, t.typtype, t.typelem, r.rngsubtype, t.typbasetype, n.nspname, t.typrelid
                 FROM pg_catalog.pg_type t
                 LEFT OUTER JOIN pg_catalog.pg_range r ON r.rngtypid = t.oid
                 INNER JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid
                 WHERE t.oid = $1";
    
    let result = CatalogInterceptor::intercept_query(query);
    assert!(result.is_some());
    
    let response = result.unwrap().unwrap();
    assert_eq!(response.columns.len(), 7);
    assert_eq!(response.columns[0], "typname");
    assert_eq!(response.columns[1], "typtype");
    assert_eq!(response.columns[2], "typelem");
    assert_eq!(response.columns[3], "rngsubtype");
    assert_eq!(response.columns[4], "typbasetype");
    assert_eq!(response.columns[5], "nspname");
    assert_eq!(response.columns[6], "typrelid");
    
    // Should return all types since we can't filter by parameter
    assert!(response.rows.len() > 0);
}