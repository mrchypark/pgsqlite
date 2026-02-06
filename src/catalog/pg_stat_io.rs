use super::where_evaluator::WhereEvaluator;
use crate::PgSqliteError;
use crate::session::db_handler::{DbHandler, DbResponse};
use chrono::{DateTime, TimeZone, Utc};
use once_cell::sync::Lazy;
use sqlparser::ast::{Expr, Select, SelectItem};
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::debug;

struct PgStatIoCounters {
    reads: AtomicU64,
    read_time_micros: AtomicU64,
    writes: AtomicU64,
    write_time_micros: AtomicU64,
    writebacks: AtomicU64,
    writeback_time_micros: AtomicU64,
    extends: AtomicU64,
    extend_time_micros: AtomicU64,
    op_bytes: AtomicU64,
    hits: AtomicU64,
    evictions: AtomicU64,
    reuses: AtomicU64,
    fsyncs: AtomicU64,
    fsync_time_micros: AtomicU64,
    stats_reset_unix_seconds: AtomicI64,
}

impl PgStatIoCounters {
    fn new() -> Self {
        Self {
            reads: AtomicU64::new(0),
            read_time_micros: AtomicU64::new(0),
            writes: AtomicU64::new(0),
            write_time_micros: AtomicU64::new(0),
            writebacks: AtomicU64::new(0),
            writeback_time_micros: AtomicU64::new(0),
            extends: AtomicU64::new(0),
            extend_time_micros: AtomicU64::new(0),
            op_bytes: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            reuses: AtomicU64::new(0),
            fsyncs: AtomicU64::new(0),
            fsync_time_micros: AtomicU64::new(0),
            stats_reset_unix_seconds: AtomicI64::new(current_unix_seconds()),
        }
    }
}

static PG_STAT_IO_COUNTERS: Lazy<PgStatIoCounters> = Lazy::new(PgStatIoCounters::new);

fn current_unix_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

enum StatementIoKind {
    Read,
    Write,
    Ignore,
}

fn classify_statement(query: &str) -> StatementIoKind {
    let upper = query.trim_start().to_uppercase();
    if upper.is_empty()
        || upper.contains("PG_STAT_IO")
        || upper.starts_with("PREPARE ")
        || upper.starts_with("DEALLOCATE ")
        || upper.starts_with("EXECUTE ")
        || upper.starts_with("LISTEN ")
        || upper.starts_with("UNLISTEN ")
    {
        return StatementIoKind::Ignore;
    }

    let keyword = upper.split_whitespace().next().unwrap_or_default();
    match keyword {
        "SELECT" | "WITH" | "SHOW" | "VALUES" | "EXPLAIN" | "PRAGMA" => StatementIoKind::Read,
        "INSERT" | "UPDATE" | "DELETE" | "REPLACE" | "CREATE" | "ALTER" | "DROP" | "TRUNCATE"
        | "VACUUM" | "ANALYZE" => StatementIoKind::Write,
        _ => StatementIoKind::Ignore,
    }
}

pub struct StatementIoRecordGuard {
    query: String,
    started_at: Instant,
    enabled: bool,
}

impl StatementIoRecordGuard {
    pub fn start(query: &str) -> Self {
        let enabled = !matches!(classify_statement(query), StatementIoKind::Ignore);
        Self {
            query: query.to_string(),
            started_at: Instant::now(),
            enabled,
        }
    }
}

impl Drop for StatementIoRecordGuard {
    fn drop(&mut self) {
        if self.enabled {
            record_statement_io(&self.query, self.started_at.elapsed());
        }
    }
}

pub fn record_statement_io(query: &str, elapsed: Duration) {
    let elapsed_micros = elapsed.as_micros() as u64;
    let query_bytes = query.len() as u64;
    match classify_statement(query) {
        StatementIoKind::Read => {
            PG_STAT_IO_COUNTERS.reads.fetch_add(1, Ordering::Relaxed);
            PG_STAT_IO_COUNTERS
                .read_time_micros
                .fetch_add(elapsed_micros, Ordering::Relaxed);
            PG_STAT_IO_COUNTERS.hits.fetch_add(1, Ordering::Relaxed);
            PG_STAT_IO_COUNTERS
                .op_bytes
                .fetch_add(query_bytes, Ordering::Relaxed);
        }
        StatementIoKind::Write => {
            PG_STAT_IO_COUNTERS.writes.fetch_add(1, Ordering::Relaxed);
            PG_STAT_IO_COUNTERS
                .write_time_micros
                .fetch_add(elapsed_micros, Ordering::Relaxed);
            PG_STAT_IO_COUNTERS
                .writebacks
                .fetch_add(1, Ordering::Relaxed);
            PG_STAT_IO_COUNTERS
                .writeback_time_micros
                .fetch_add(elapsed_micros, Ordering::Relaxed);
            PG_STAT_IO_COUNTERS.extends.fetch_add(1, Ordering::Relaxed);
            PG_STAT_IO_COUNTERS
                .extend_time_micros
                .fetch_add(elapsed_micros, Ordering::Relaxed);
            PG_STAT_IO_COUNTERS
                .op_bytes
                .fetch_add(query_bytes, Ordering::Relaxed);
        }
        StatementIoKind::Ignore => {}
    }
}

/// Handler for pg_stat_io view - PostgreSQL 16+ I/O statistics
pub struct PgStatIoHandler;

impl PgStatIoHandler {
    pub async fn handle_query(
        select: &Select,
        _db: &DbHandler,
    ) -> Result<DbResponse, PgSqliteError> {
        debug!("Handling pg_stat_io query");

        let all_columns = vec![
            "backend_type".to_string(),
            "object".to_string(),
            "context".to_string(),
            "reads".to_string(),
            "read_time".to_string(),
            "writes".to_string(),
            "write_time".to_string(),
            "writebacks".to_string(),
            "writeback_time".to_string(),
            "extends".to_string(),
            "extend_time".to_string(),
            "op_bytes".to_string(),
            "hits".to_string(),
            "evictions".to_string(),
            "reuses".to_string(),
            "fsyncs".to_string(),
            "fsync_time".to_string(),
            "stats_reset".to_string(),
        ];

        let selected_columns = Self::get_selected_columns(&select.projection, &all_columns);
        let stats = Self::get_io_statistics();

        let filtered_stats = if let Some(where_clause) = &select.selection {
            Self::apply_where_filter(&stats, where_clause, &selected_columns)?
        } else {
            stats
        };

        let mut rows = Vec::new();
        for stat in filtered_stats {
            let mut row = Vec::new();
            for column in &selected_columns {
                let value = stat.get(column).cloned().unwrap_or_else(|| b"".to_vec());
                row.push(Some(value));
            }
            rows.push(row);
        }

        let rows_count = rows.len();
        Ok(DbResponse {
            columns: selected_columns,
            rows,
            rows_affected: rows_count,
        })
    }

    fn get_selected_columns(projection: &[SelectItem], all_columns: &[String]) -> Vec<String> {
        let mut selected = Vec::new();

        for item in projection {
            match item {
                SelectItem::Wildcard(_) => {
                    selected.extend_from_slice(all_columns);
                    break;
                }
                SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                    let col_name = ident.value.to_lowercase();
                    if all_columns.contains(&col_name) {
                        selected.push(col_name);
                    }
                }
                SelectItem::ExprWithAlias {
                    expr: Expr::Identifier(ident),
                    alias,
                } => {
                    let col_name = ident.value.to_lowercase();
                    if all_columns.contains(&col_name) {
                        selected.push(alias.value.clone());
                    }
                }
                SelectItem::QualifiedWildcard(_, _) => {
                    selected.extend_from_slice(all_columns);
                    break;
                }
                _ => {}
            }
        }

        selected
    }

    fn get_io_statistics() -> Vec<HashMap<String, Vec<u8>>> {
        let reads = PG_STAT_IO_COUNTERS.reads.load(Ordering::Relaxed);
        let read_time_micros = PG_STAT_IO_COUNTERS.read_time_micros.load(Ordering::Relaxed);
        let writes = PG_STAT_IO_COUNTERS.writes.load(Ordering::Relaxed);
        let write_time_micros = PG_STAT_IO_COUNTERS
            .write_time_micros
            .load(Ordering::Relaxed);
        let writebacks = PG_STAT_IO_COUNTERS.writebacks.load(Ordering::Relaxed);
        let writeback_time_micros = PG_STAT_IO_COUNTERS
            .writeback_time_micros
            .load(Ordering::Relaxed);
        let extends = PG_STAT_IO_COUNTERS.extends.load(Ordering::Relaxed);
        let extend_time_micros = PG_STAT_IO_COUNTERS
            .extend_time_micros
            .load(Ordering::Relaxed);
        let op_bytes = PG_STAT_IO_COUNTERS.op_bytes.load(Ordering::Relaxed);
        let hits = PG_STAT_IO_COUNTERS.hits.load(Ordering::Relaxed);
        let evictions = PG_STAT_IO_COUNTERS.evictions.load(Ordering::Relaxed);
        let reuses = PG_STAT_IO_COUNTERS.reuses.load(Ordering::Relaxed);
        let fsyncs = PG_STAT_IO_COUNTERS.fsyncs.load(Ordering::Relaxed);
        let fsync_time_micros = PG_STAT_IO_COUNTERS
            .fsync_time_micros
            .load(Ordering::Relaxed);
        let stats_reset_unix_seconds = PG_STAT_IO_COUNTERS
            .stats_reset_unix_seconds
            .load(Ordering::Relaxed);
        let stats_reset = Self::format_stats_reset(stats_reset_unix_seconds).into_bytes();

        vec![
            {
                let mut map = HashMap::new();
                map.insert("backend_type".to_string(), b"client backend".to_vec());
                map.insert("object".to_string(), b"relation".to_vec());
                map.insert("context".to_string(), b"normal".to_vec());
                map.insert("reads".to_string(), reads.to_string().into_bytes());
                map.insert(
                    "read_time".to_string(),
                    Self::micros_to_millis_text(read_time_micros).into_bytes(),
                );
                map.insert("writes".to_string(), writes.to_string().into_bytes());
                map.insert(
                    "write_time".to_string(),
                    Self::micros_to_millis_text(write_time_micros).into_bytes(),
                );
                map.insert(
                    "writebacks".to_string(),
                    writebacks.to_string().into_bytes(),
                );
                map.insert(
                    "writeback_time".to_string(),
                    Self::micros_to_millis_text(writeback_time_micros).into_bytes(),
                );
                map.insert("extends".to_string(), extends.to_string().into_bytes());
                map.insert(
                    "extend_time".to_string(),
                    Self::micros_to_millis_text(extend_time_micros).into_bytes(),
                );
                map.insert("op_bytes".to_string(), op_bytes.to_string().into_bytes());
                map.insert("hits".to_string(), hits.to_string().into_bytes());
                map.insert("evictions".to_string(), evictions.to_string().into_bytes());
                map.insert("reuses".to_string(), reuses.to_string().into_bytes());
                map.insert("fsyncs".to_string(), fsyncs.to_string().into_bytes());
                map.insert(
                    "fsync_time".to_string(),
                    Self::micros_to_millis_text(fsync_time_micros).into_bytes(),
                );
                map.insert("stats_reset".to_string(), stats_reset.clone());
                map
            },
            Self::make_zero_row("background writer", "relation", &stats_reset),
            Self::make_zero_row("checkpointer", "relation", &stats_reset),
            Self::make_zero_row("walwriter", "wal", &stats_reset),
        ]
    }

    fn make_zero_row(
        backend_type: &str,
        object: &str,
        stats_reset: &[u8],
    ) -> HashMap<String, Vec<u8>> {
        let mut map = HashMap::new();
        map.insert("backend_type".to_string(), backend_type.as_bytes().to_vec());
        map.insert("object".to_string(), object.as_bytes().to_vec());
        map.insert("context".to_string(), b"normal".to_vec());
        map.insert("reads".to_string(), b"0".to_vec());
        map.insert("read_time".to_string(), b"0.000".to_vec());
        map.insert("writes".to_string(), b"0".to_vec());
        map.insert("write_time".to_string(), b"0.000".to_vec());
        map.insert("writebacks".to_string(), b"0".to_vec());
        map.insert("writeback_time".to_string(), b"0.000".to_vec());
        map.insert("extends".to_string(), b"0".to_vec());
        map.insert("extend_time".to_string(), b"0.000".to_vec());
        map.insert("op_bytes".to_string(), b"0".to_vec());
        map.insert("hits".to_string(), b"0".to_vec());
        map.insert("evictions".to_string(), b"0".to_vec());
        map.insert("reuses".to_string(), b"0".to_vec());
        map.insert("fsyncs".to_string(), b"0".to_vec());
        map.insert("fsync_time".to_string(), b"0.000".to_vec());
        map.insert("stats_reset".to_string(), stats_reset.to_vec());
        map
    }

    fn micros_to_millis_text(micros: u64) -> String {
        format!("{:.3}", micros as f64 / 1000.0)
    }

    fn format_stats_reset(unix_seconds: i64) -> String {
        Utc.timestamp_opt(unix_seconds, 0)
            .single()
            .unwrap_or_else(|| {
                let fallback: DateTime<Utc> = SystemTime::now().into();
                fallback
            })
            .format("%Y-%m-%d %H:%M:%S+00")
            .to_string()
    }

    fn apply_where_filter(
        stats: &[HashMap<String, Vec<u8>>],
        where_clause: &Expr,
        _selected_columns: &[String],
    ) -> Result<Vec<HashMap<String, Vec<u8>>>, PgSqliteError> {
        let mut filtered = Vec::new();

        for stat in stats {
            let mut string_data = HashMap::new();
            for (key, value) in stat {
                if let Ok(string_val) = String::from_utf8(value.clone()) {
                    string_data.insert(key.clone(), string_val);
                }
            }

            let column_mapping = HashMap::new();
            if WhereEvaluator::evaluate(where_clause, &string_data, &column_mapping) {
                filtered.push(stat.clone());
            }
        }

        Ok(filtered)
    }
}
