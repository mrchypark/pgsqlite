use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

pub mod enhanced_statement_pool;
pub mod enum_cache;
pub mod execution;
pub mod lazy_schema_loader;
pub mod parameter_cache;
pub mod query;
pub mod query_fingerprint;
pub mod result_cache;
pub mod row_description;
pub mod schema;
pub mod statement_pool;
pub mod status;
pub mod translation_cache;
pub mod ttl_cache;
pub mod wire_protocol_cache;

pub use enhanced_statement_pool::{
    EnhancedStatementPool, PoolStats, StatementMetadata as EnhancedStatementMetadata,
};
pub use enum_cache::{EnumCache, global_enum_cache};
pub use execution::{
    ExecutionCache, ExecutionMetadata, global_execution_cache, global_type_converter_table,
};
pub use lazy_schema_loader::LazySchemaLoader;
pub use parameter_cache::{
    CachedParameterInfo, GLOBAL_PARAM_VALUE_CACHE, GLOBAL_PARAMETER_CACHE, ParameterTypeCache,
};
pub use query::{CacheMetrics, CachedQuery, QueryCache};
pub use query_fingerprint::QueryFingerprint;
pub use result_cache::{CachedResultSet, ResultCacheKey, ResultSetCache, global_result_cache};
pub use row_description::{
    CachedRowDescription, GLOBAL_ROW_DESCRIPTION_CACHE, RowDescriptionCache, RowDescriptionKey,
};
pub use schema::SchemaCache;
pub use statement_pool::{StatementMetadata, StatementPool, StatementPoolStats};
pub use status::{CacheStatus, format_cache_status_as_table, get_cache_status, log_cache_status};
pub use translation_cache::{TranslationCache, global_translation_cache};
pub use ttl_cache::{CacheStats, TtlCache, TtlCacheConfig, TtlCacheFactory};
pub use wire_protocol_cache::{
    CachedWireResponse, WIRE_PROTOCOL_CACHE, WireProtocolCache, encode_data_row,
    is_cacheable_for_wire_protocol,
};

/// Simple LRU cache with TTL support
pub struct LruCache<K, V> {
    cache: Arc<RwLock<HashMap<K, CacheEntry<V>>>>,
    capacity: usize,
    ttl: Duration,
}

struct CacheEntry<V> {
    value: V,
    last_accessed: Instant,
}

impl<K: Eq + std::hash::Hash + Clone, V: Clone> LruCache<K, V> {
    pub fn new(capacity: usize, ttl: Duration) -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::with_capacity(capacity))),
            capacity,
            ttl,
        }
    }

    pub fn get(&self, key: &K) -> Option<V> {
        let mut cache = self.cache.write().unwrap();

        if let Some(entry) = cache.get_mut(key) {
            if entry.last_accessed.elapsed() < self.ttl {
                entry.last_accessed = Instant::now();
                return Some(entry.value.clone());
            }
            cache.remove(key);
        }

        None
    }

    pub fn insert(&self, key: K, value: V) {
        let mut cache = self.cache.write().unwrap();

        // Simple eviction: remove oldest entry if at capacity
        if cache.len() >= self.capacity
            && !cache.contains_key(&key)
            && let Some((oldest_key, _)) = cache.iter().min_by_key(|(_, entry)| entry.last_accessed)
        {
            let oldest_key = oldest_key.clone();
            cache.remove(&oldest_key);
        }

        cache.insert(
            key,
            CacheEntry {
                value,
                last_accessed: Instant::now(),
            },
        );
    }

    pub fn invalidate(&self, key: &K) {
        self.cache.write().unwrap().remove(key);
    }

    pub fn clear(&self) {
        self.cache.write().unwrap().clear();
    }
}
