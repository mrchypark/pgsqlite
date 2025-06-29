use rusqlite::{Connection, Result};
use std::sync::{Arc, Mutex};
use tokio::sync::Semaphore;

pub struct SqlitePool {
    path: String,
    connections: Arc<Mutex<Vec<Connection>>>,
    semaphore: Arc<Semaphore>,
    _max_connections: usize,
}

impl SqlitePool {
    pub fn new(path: &str) -> Result<Self> {
        let max_connections = 10;
        let pool = SqlitePool {
            path: path.to_string(),
            connections: Arc::new(Mutex::new(Vec::new())),
            semaphore: Arc::new(Semaphore::new(max_connections)),
            _max_connections: max_connections,
        };
        
        // Pre-create some connections
        let mut conns = pool.connections.lock().unwrap();
        for _ in 0..5.min(max_connections) {
            let conn = if path == ":memory:" {
                Connection::open_in_memory()?
            } else {
                Connection::open(&pool.path)?
            };
            
            // Set pragmas for better performance
            conn.execute_batch(
                "PRAGMA journal_mode=WAL;
                 PRAGMA synchronous=NORMAL;
                 PRAGMA cache_size=-64000;
                 PRAGMA temp_store=MEMORY;"
            )?;
            
            conns.push(conn);
        }
        drop(conns);
        
        Ok(pool)
    }
    
    pub async fn acquire(&self) -> Result<PooledConnection> {
        let permit = self.semaphore.clone().acquire_owned().await.unwrap();
        
        let conn = {
            let mut conns = self.connections.lock().unwrap();
            conns.pop()
        };
        
        let conn = match conn {
            Some(c) => c,
            None => {
                if self.path == ":memory:" {
                    let conn = Connection::open_in_memory()?;
                    conn.execute_batch(
                        "PRAGMA journal_mode=WAL;
                         PRAGMA synchronous=NORMAL;
                         PRAGMA cache_size=-64000;
                         PRAGMA temp_store=MEMORY;"
                    )?;
                    conn
                } else {
                    let conn = Connection::open(&self.path)?;
                    conn.execute_batch(
                        "PRAGMA journal_mode=WAL;
                         PRAGMA synchronous=NORMAL;
                         PRAGMA cache_size=-64000;
                         PRAGMA temp_store=MEMORY;"
                    )?;
                    conn
                }
            }
        };
        
        Ok(PooledConnection {
            conn: Some(conn),
            pool: self.connections.clone(),
            _permit: permit,
        })
    }
}

pub struct PooledConnection {
    conn: Option<Connection>,
    pool: Arc<Mutex<Vec<Connection>>>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl std::ops::Deref for PooledConnection {
    type Target = Connection;
    
    fn deref(&self) -> &Self::Target {
        self.conn.as_ref().unwrap()
    }
}

impl std::ops::DerefMut for PooledConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.conn.as_mut().unwrap()
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            let mut conns = self.pool.lock().unwrap();
            conns.push(conn);
        }
    }
}