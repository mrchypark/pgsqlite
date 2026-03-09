use crate::cache::QueryCache;
use crate::config::global_config;
use crate::protocol::TransactionStatus;
use crate::session::DbHandler;
use once_cell::sync::Lazy;
use parking_lot::Mutex as ParkingMutex;
use rusqlite::Connection;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::{Mutex, RwLock};

// Global query cache shared across all sessions
pub static GLOBAL_QUERY_CACHE: Lazy<Arc<QueryCache>> = Lazy::new(|| {
    let cfg = global_config();
    Arc::new(QueryCache::new(cfg.query_cache_size, cfg.query_cache_ttl))
});

// Global session counter for WAL mode isolation optimization
static ACTIVE_SESSION_COUNT: AtomicUsize = AtomicUsize::new(0);

pub struct SessionState {
    pub id: uuid::Uuid,
    pub database: String,
    pub user: String,
    pub parameters: RwLock<HashMap<String, String>>,
    pub local_parameters: RwLock<HashMap<String, String>>,
    pub prepared_statements: RwLock<HashMap<String, PreparedStatement>>,
    pub prepared_statement_meta: RwLock<HashMap<String, PreparedStatementMeta>>,
    pub portals: RwLock<HashMap<String, Portal>>,
    pub portal_meta: RwLock<HashMap<String, PortalMeta>>,
    pub transaction_status: RwLock<TransactionStatus>,
    pub portal_manager: Arc<super::PortalManager>,
    pub python_param_mapping: RwLock<HashMap<String, Vec<String>>>, // Maps statement name to Python parameter names
    pub db_handler: Mutex<Option<Arc<DbHandler>>>, // Reference to the database handler for session lifecycle management
    pub cached_connection: ParkingMutex<Option<Arc<ParkingMutex<Connection>>>>, // Cached connection for fast access
}

pub struct PreparedStatement {
    pub query: String,
    pub translated_query: Option<String>, // Cached translation of the query
    pub param_types: Vec<i32>,
    pub param_formats: Vec<i16>,
    pub field_descriptions: Vec<crate::protocol::FieldDescription>,
    pub translation_metadata: Option<crate::translator::TranslationMetadata>, // Type hints from query translation
}

#[derive(Clone, Debug)]
pub struct PreparedStatementMeta {
    pub prepare_time: std::time::SystemTime,
    pub from_sql: bool,
    pub generic_plans: u64,
    pub custom_plans: u64,
}

#[derive(Clone)]
pub struct Portal {
    pub statement_name: String,
    pub query: String,
    pub translated_query: Option<String>, // Cached translation from prepared statement
    pub bound_values: Vec<Option<Vec<u8>>>,
    pub param_formats: Vec<i16>,
    pub result_formats: Vec<i16>,
    pub inferred_param_types: Option<Vec<i32>>, // Types inferred from actual values
    pub row_description_sent: bool,
}

#[derive(Clone, Debug)]
pub struct PortalMeta {
    pub created_at: std::time::SystemTime,
    pub is_holdable: bool,
    pub is_scrollable: bool,
}

impl SessionState {
    pub fn new(database: String, user: String) -> Self {
        let parameters = default_parameters();

        // Increment active session count
        ACTIVE_SESSION_COUNT.fetch_add(1, Ordering::Relaxed);

        SessionState {
            id: uuid::Uuid::new_v4(),
            database,
            user,
            parameters: RwLock::new(parameters),
            local_parameters: RwLock::new(HashMap::new()),
            prepared_statements: RwLock::new(HashMap::new()),
            prepared_statement_meta: RwLock::new(HashMap::new()),
            portals: RwLock::new(HashMap::new()),
            portal_meta: RwLock::new(HashMap::new()),
            transaction_status: RwLock::new(TransactionStatus::Idle),
            portal_manager: Arc::new(super::PortalManager::new(100)), // Allow up to 100 concurrent portals
            python_param_mapping: RwLock::new(HashMap::new()),
            db_handler: Mutex::new(None), // Will be set after session is created
            cached_connection: ParkingMutex::new(None), // Initialize as None
        }
    }

    /// Create a new session with default database and user (for testing)
    #[cfg(test)]
    pub fn new_test() -> Self {
        Self::new("test".to_string(), "test".to_string())
    }

    /// Check if the session is currently in a transaction
    pub async fn in_transaction(&self) -> bool {
        matches!(
            *self.transaction_status.read().await,
            TransactionStatus::InTransaction | TransactionStatus::InFailedTransaction
        )
    }

    /// Set the transaction status
    pub async fn set_transaction_status(&self, status: TransactionStatus) {
        *self.transaction_status.write().await = status;
    }

    /// Get the transaction status
    pub async fn get_transaction_status(&self) -> TransactionStatus {
        *self.transaction_status.read().await
    }

    /// Get the current number of active sessions
    pub async fn get_session_count(&self) -> usize {
        ACTIVE_SESSION_COUNT.load(Ordering::Relaxed)
    }

    /// Set the database handler for this session
    /// This should be called after the session is created and a connection is established
    pub async fn set_db_handler(&self, db_handler: Arc<DbHandler>) {
        *self.db_handler.lock().await = Some(db_handler);
    }

    /// Get the database handler for this session
    pub async fn get_db_handler(&self) -> Option<Arc<DbHandler>> {
        self.db_handler.lock().await.clone()
    }

    /// Initialize the session connection with the database handler
    /// This ensures the session has its dedicated connection
    pub async fn initialize_connection(&self) -> Result<(), crate::PgSqliteError> {
        if let Some(ref db_handler) = *self.db_handler.lock().await {
            db_handler.create_session_connection(self.id).await?;
        }
        Ok(())
    }

    /// Clean up the session connection
    /// This should be called when the session is being terminated
    pub async fn cleanup_connection(&self) {
        // Clear the cached connection first
        self.cached_connection.lock().take();

        if let Some(ref db_handler) = *self.db_handler.lock().await {
            db_handler.remove_session_connection(&self.id);
        }
    }

    /// Cache a connection for fast access
    pub fn cache_connection(&self, connection: Arc<ParkingMutex<Connection>>) {
        *self.cached_connection.lock() = Some(connection);
    }

    /// Get the cached connection if available
    pub fn get_cached_connection(&self) -> Option<Arc<ParkingMutex<Connection>>> {
        self.cached_connection.lock().clone()
    }

    pub fn canonical_parameter_name(name: &str) -> String {
        let normalized = name
            .trim()
            .trim_matches('\'')
            .trim_matches('"')
            .replace('-', "_")
            .replace(' ', "_")
            .to_uppercase();

        match normalized.as_str() {
            "TRANSACTION_ISOLATION_LEVEL" => "TRANSACTION_ISOLATION".to_string(),
            "TIME_ZONE" => "TIMEZONE".to_string(),
            "DATE_STYLE" => "DATESTYLE".to_string(),
            "INTERVAL_STYLE" => "INTERVALSTYLE".to_string(),
            other => other.to_string(),
        }
    }

    pub fn parameter_default(name: &str) -> Option<&'static str> {
        match Self::canonical_parameter_name(name).as_str() {
            "SERVER_VERSION" => Some("16.0"),
            "SERVER_VERSION_NUM" => Some("160000"),
            "SERVER_ENCODING" => Some("UTF8"),
            "CLIENT_ENCODING" => Some("UTF8"),
            "DATESTYLE" => Some("ISO, MDY"),
            "TIMEZONE" => Some("UTC"),
            "TIMEZONE_ABBREVIATIONS" => Some("Default"),
            "INTERVALSTYLE" => Some("postgres"),
            "INTEGER_DATETIMES" => Some("on"),
            "SEARCH_PATH" => Some("public"),
            "DEFAULT_TRANSACTION_ISOLATION" => Some("read committed"),
            "TRANSACTION_ISOLATION" => Some("read committed"),
            "DEFAULT_TRANSACTION_READ_ONLY" => Some("off"),
            "TRANSACTION_READ_ONLY" => Some("off"),
            "APPLICATION_NAME" => Some(""),
            "STANDARD_CONFORMING_STRINGS" => Some("on"),
            "SESSION_AUTHORIZATION" => Some("postgres"),
            "IS_SUPERUSER" => Some("on"),
            _ => None,
        }
    }

    pub async fn get_parameter(&self, name: &str) -> Option<String> {
        let canonical = Self::canonical_parameter_name(name);

        {
            let local = self.local_parameters.read().await;
            for key in parameter_lookup_keys(&canonical) {
                if let Some(value) = local.get(&key) {
                    return Some(value.clone());
                }
            }
        }

        let params = self.parameters.read().await;
        for key in parameter_lookup_keys(&canonical) {
            if let Some(value) = params.get(&key) {
                return Some(value.clone());
            }
        }

        Self::parameter_default(&canonical).map(str::to_string)
    }

    pub async fn set_parameter(&self, name: &str, value: String) {
        let canonical = Self::canonical_parameter_name(name);
        let mut params = self.parameters.write().await;
        remove_parameter_aliases(&mut params, &canonical);
        params.insert(canonical, value);
    }

    pub async fn set_local_parameter(&self, name: &str, value: String) {
        let canonical = Self::canonical_parameter_name(name);
        let mut params = self.local_parameters.write().await;
        remove_parameter_aliases(&mut params, &canonical);
        params.insert(canonical, value);
    }

    pub async fn reset_parameter(&self, name: &str) {
        let canonical = Self::canonical_parameter_name(name);
        {
            let mut local = self.local_parameters.write().await;
            remove_parameter_aliases(&mut local, &canonical);
        }

        let mut params = self.parameters.write().await;
        remove_parameter_aliases(&mut params, &canonical);
        if let Some(default) = Self::parameter_default(&canonical) {
            params.insert(canonical, default.to_string());
        }
    }

    pub async fn reset_all_parameters(&self) {
        *self.parameters.write().await = default_parameters();
        self.clear_local_parameters().await;
    }

    pub async fn clear_local_parameters(&self) {
        self.local_parameters.write().await.clear();
    }
}

fn default_parameters() -> HashMap<String, String> {
    let mut parameters = HashMap::new();
    parameters.insert("server_version".to_string(), "16.0 (pgsqlite)".to_string());
    parameters.insert("server_encoding".to_string(), "UTF8".to_string());
    parameters.insert("client_encoding".to_string(), "UTF8".to_string());
    parameters.insert("DateStyle".to_string(), "ISO, MDY".to_string());
    parameters.insert("TimeZone".to_string(), "UTC".to_string());
    parameters.insert("IntervalStyle".to_string(), "postgres".to_string());
    parameters.insert("integer_datetimes".to_string(), "on".to_string());
    parameters.insert("SEARCH_PATH".to_string(), "public".to_string());
    parameters.insert(
        "DEFAULT_TRANSACTION_ISOLATION".to_string(),
        "read committed".to_string(),
    );
    parameters.insert(
        "TRANSACTION_ISOLATION".to_string(),
        "read committed".to_string(),
    );
    parameters.insert(
        "DEFAULT_TRANSACTION_READ_ONLY".to_string(),
        "off".to_string(),
    );
    parameters.insert("TRANSACTION_READ_ONLY".to_string(), "off".to_string());
    parameters.insert("application_name".to_string(), "".to_string());
    parameters.insert("standard_conforming_strings".to_string(), "on".to_string());
    parameters
}

fn parameter_lookup_keys(canonical: &str) -> Vec<String> {
    let mut keys = vec![canonical.to_string()];
    match canonical {
        "SERVER_VERSION" => keys.push("server_version".to_string()),
        "SERVER_VERSION_NUM" => keys.push("server_version_num".to_string()),
        "SERVER_ENCODING" => keys.push("server_encoding".to_string()),
        "CLIENT_ENCODING" => keys.push("client_encoding".to_string()),
        "DATESTYLE" => {
            keys.push("DateStyle".to_string());
            keys.push("datestyle".to_string());
        }
        "TIMEZONE" => {
            keys.push("TimeZone".to_string());
            keys.push("timezone".to_string());
        }
        "INTERVALSTYLE" => {
            keys.push("IntervalStyle".to_string());
            keys.push("intervalstyle".to_string());
        }
        "INTEGER_DATETIMES" => keys.push("integer_datetimes".to_string()),
        "SEARCH_PATH" => keys.push("search_path".to_string()),
        "APPLICATION_NAME" => keys.push("application_name".to_string()),
        "STANDARD_CONFORMING_STRINGS" => keys.push("standard_conforming_strings".to_string()),
        _ => {}
    }
    keys
}

fn remove_parameter_aliases(map: &mut HashMap<String, String>, canonical: &str) {
    for key in parameter_lookup_keys(canonical) {
        map.remove(&key);
    }
}

impl Drop for SessionState {
    fn drop(&mut self) {
        // Note: We can't do async operations in Drop, so cleanup is handled
        // explicitly when the session ends or via a background task
        // For now, just decrement the session count

        // Decrement active session count when session is destroyed
        ACTIVE_SESSION_COUNT.fetch_sub(1, Ordering::Relaxed);
    }
}
