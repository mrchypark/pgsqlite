// Module for session management
pub mod connection_manager;
pub mod db_handler;
pub mod db_handler_registry;
pub mod message_loop;
pub mod pool;
pub mod portal_manager;
pub mod query_router;
pub mod read_only_handler;
pub mod state;
pub mod thread_local_cache;

pub use connection_manager::ConnectionManager;
pub use db_handler::{DbHandler, DbResponse};
pub use db_handler_registry::get_or_create_handler;
pub use pool::PoolStats;
pub use pool::{PooledConnection, SqlitePool};
pub use portal_manager::{
    CachedQueryResult, ManagedPortal, PortalExecutionState, PortalExecutor, PortalManager,
};
pub use query_router::{QueryRoute, QueryRouter, QueryType, RouterError, RouterStats};
pub use read_only_handler::{ReadOnlyDbHandler, ReadOnlyError};
pub use state::{
    GLOBAL_QUERY_CACHE, Portal, PortalMeta, PreparedStatement, PreparedStatementMeta, SessionState,
};
pub use thread_local_cache::ThreadLocalConnectionCache;
