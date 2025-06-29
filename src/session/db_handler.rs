use rusqlite::Connection;
use tokio::sync::{mpsc, oneshot};
use std::thread;
use tracing::{info, error};
use sqlparser::parser::Parser;
use sqlparser::dialect::PostgreSqlDialect;

pub enum DbCommand {
    Execute {
        query: String,
        response: oneshot::Sender<Result<DbResponse, rusqlite::Error>>,
    },
    Query {
        query: String,
        response: oneshot::Sender<Result<DbResponse, rusqlite::Error>>,
    },
    GetSchemaType {
        table_name: String,
        column_name: String,
        response: oneshot::Sender<Result<Option<String>, rusqlite::Error>>,
    },
    Shutdown,
}

pub struct DbResponse {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Option<Vec<u8>>>>,
    pub rows_affected: usize,
}

pub struct DbHandler {
    sender: mpsc::Sender<DbCommand>,
}

impl DbHandler {
    pub fn new(db_path: &str) -> Result<Self, rusqlite::Error> {
        let (sender, mut receiver) = mpsc::channel(100);
        let db_path = db_path.to_string();
        
        // Spawn a dedicated thread for SQLite operations
        thread::spawn(move || {
            let conn = if db_path == ":memory:" {
                Connection::open_in_memory()
            } else {
                Connection::open(&db_path)
            };
            
            let conn = match conn {
                Ok(c) => c,
                Err(e) => {
                    error!("Failed to open database: {}", e);
                    return;
                }
            };
            
            // Set pragmas
            if let Err(e) = conn.execute_batch(
                "PRAGMA journal_mode=WAL;
                 PRAGMA synchronous=NORMAL;
                 PRAGMA cache_size=-64000;
                 PRAGMA temp_store=MEMORY;"
            ) {
                error!("Failed to set pragmas: {}", e);
            }
            
            // Register custom functions
            if let Err(e) = crate::functions::register_all_functions(&conn) {
                error!("Failed to register custom functions: {}", e);
            }
            
            // Initialize metadata table
            if let Err(e) = crate::metadata::TypeMetadata::init(&conn) {
                error!("Failed to initialize metadata table: {}", e);
            }
            
            
            // Main command loop
            while let Some(cmd) = receiver.blocking_recv() {
                match cmd {
                    DbCommand::Execute { query, response } => {
                        let result = execute_dml(&conn, &query);
                        let _ = response.send(result);
                    }
                    DbCommand::Query { query, response } => {
                        let result = execute_query(&conn, &query);
                        let _ = response.send(result);
                    }
                    DbCommand::GetSchemaType { table_name, column_name, response } => {
                        let result = crate::metadata::TypeMetadata::get_pg_type(&conn, &table_name, &column_name);
                        let _ = response.send(result);
                    }
                    DbCommand::Shutdown => break,
                }
            }
            
            info!("Database handler thread shutting down");
        });
        
        Ok(DbHandler { sender })
    }
    
    pub async fn execute(&self, query: &str) -> Result<DbResponse, rusqlite::Error> {
        let (tx, rx) = oneshot::channel();
        
        self.sender.send(DbCommand::Execute {
            query: query.to_string(),
            response: tx,
        }).await.map_err(|_| rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_MISUSE),
            Some("Database handler unavailable".to_string())
        ))?;
        
        rx.await.map_err(|_| rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_MISUSE),
            Some("Failed to receive response".to_string())
        ))?
    }
    
    pub async fn query(&self, query: &str) -> Result<DbResponse, rusqlite::Error> {
        let (tx, rx) = oneshot::channel();
        
        self.sender.send(DbCommand::Query {
            query: query.to_string(),
            response: tx,
        }).await.map_err(|_| rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_MISUSE),
            Some("Database handler unavailable".to_string())
        ))?;
        
        rx.await.map_err(|_| rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_MISUSE),
            Some("Failed to receive response".to_string())
        ))?
    }
    
    pub async fn get_schema_type(&self, table_name: &str, column_name: &str) -> Result<Option<String>, rusqlite::Error> {
        let (tx, rx) = oneshot::channel();
        
        self.sender.send(DbCommand::GetSchemaType {
            table_name: table_name.to_string(),
            column_name: column_name.to_string(),
            response: tx,
        }).await.map_err(|_| rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_MISUSE),
            Some("Database handler unavailable".to_string())
        ))?;
        
        rx.await.map_err(|_| rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_MISUSE),
            Some("Failed to receive response".to_string())
        ))?
    }

    pub async fn shutdown(&self) {
        let _ = self.sender.send(DbCommand::Shutdown).await;
    }
}

fn execute_query(conn: &Connection, query: &str) -> Result<DbResponse, rusqlite::Error> {
    // Rewrite query to use decimal functions if needed
    let rewritten_query = rewrite_query_if_needed(conn, query)?;
    let mut stmt = conn.prepare(&rewritten_query)?;
    let column_count = stmt.column_count();
    
    // Get column names
    let mut columns = Vec::new();
    for i in 0..column_count {
        columns.push(stmt.column_name(i)?.to_string());
    }
    
    // Get rows
    let mut rows = Vec::new();
    let result_rows = stmt.query_map([], |row| {
        let mut values = Vec::new();
        for i in 0..column_count {
            use rusqlite::types::ValueRef;
            match row.get_ref(i)? {
                ValueRef::Null => values.push(None),
                ValueRef::Integer(i) => {
                    // For now, still use text format since we're using format=0 (text)
                    values.push(Some(i.to_string().into_bytes()))
                },
                ValueRef::Real(f) => values.push(Some(f.to_string().into_bytes())),
                ValueRef::Text(s) => values.push(Some(s.to_vec())),
                ValueRef::Blob(b) => values.push(Some(b.to_vec())),
            }
        }
        Ok(values)
    })?;
    
    for row in result_rows {
        rows.push(row?);
    }
    
    let rows_affected = rows.len();
    Ok(DbResponse {
        columns,
        rows,
        rows_affected,
    })
}

fn execute_dml(conn: &Connection, query: &str) -> Result<DbResponse, rusqlite::Error> {
    // Rewrite query to use decimal functions if needed
    let rewritten_query = rewrite_query_if_needed(conn, query)?;
    let rows_affected = conn.execute(&rewritten_query, [])?;
    
    Ok(DbResponse {
        columns: Vec::new(),
        rows: Vec::new(),
        rows_affected,
    })
}

/// Rewrite query to use decimal functions if it contains NUMERIC operations
fn rewrite_query_if_needed(conn: &Connection, query: &str) -> Result<String, rusqlite::Error> {
    // Parse the query
    let dialect = PostgreSqlDialect {};
    let mut statements = match Parser::parse_sql(&dialect, query) {
        Ok(stmts) => stmts,
        Err(e) => {
            // If we can't parse it, just return the original query
            error!("Failed to parse query for decimal rewriting: {}", e);
            return Ok(query.to_string());
        }
    };
    
    // If we have a statement, try to rewrite it
    if let Some(stmt) = statements.first_mut() {
        let mut rewriter = crate::rewriter::DecimalQueryRewriter::new(conn);
        if let Err(e) = rewriter.rewrite_statement(stmt) {
            error!("Failed to rewrite query for decimal operations: {}", e);
            return Ok(query.to_string());
        }
        
        // Return the rewritten query
        Ok(stmt.to_string())
    } else {
        Ok(query.to_string())
    }
}