// Module for PostgreSQL wire protocol implementation
pub mod auth;
pub mod binary;
pub mod buffer_pool;
pub mod codec;
pub mod memory_mapped;
pub mod memory_monitor;
pub mod messages;
pub mod parser;
pub mod rate_limiter;
pub mod small_value;
pub mod value_handler;

pub use auth::{AuthResult, ServerAuth, perform_authentication};
pub use binary::{BinaryEncoder, ZeroCopyBinaryEncoder};
pub use buffer_pool::{
    BufferPool, BufferPoolConfig, BufferPoolStats, PooledBytesMut, get_pooled_buffer,
    global_buffer_pool,
};
pub use codec::PostgresCodec;
pub use memory_mapped::{MappedValue, MappedValueFactory, MappedValueReader, MemoryMappedConfig};
pub use memory_monitor::{
    MemoryMonitor, MemoryMonitorConfig, MemoryPressure, MemoryStats, global_memory_monitor,
};
pub use messages::*;
pub use parser::{
    AuthenticationRequest, MessageParser, ParseError, PostgresMessage, PostgresMessageType,
    ProtocolState,
};
pub use rate_limiter::{
    CircuitBreakerConfig, CircuitState, RateLimitConfig, RateLimitError, RateLimiter,
    check_global_rate_limit, global_rate_limiter, record_global_failure,
};
pub use small_value::SmallValue;
pub use value_handler::{ValueHandler, ValueHandlerConfig, ValueHandlerStats};
