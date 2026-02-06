use super::where_evaluator::WhereEvaluator;
use crate::PgSqliteError;
use crate::session::SessionState;
use crate::session::db_handler::{DbHandler, DbResponse};
use crate::types::SchemaTypeMapper;
use chrono::{DateTime, Utc};
use sqlparser::ast::{Expr, Select, SelectItem};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

/// Handler for pg_prepared_statements view - shows prepared statements
pub struct PgPreparedStatementsHandler;

impl PgPreparedStatementsHandler {
    pub async fn handle_query(
        select: &Select,
        _db: &DbHandler,
        session: Option<Arc<SessionState>>,
    ) -> Result<DbResponse, PgSqliteError> {
        debug!("Handling pg_prepared_statements query");

        // pg_prepared_statements columns
        let all_columns = vec![
            "name".to_string(),
            "statement".to_string(),
            "prepare_time".to_string(),
            "parameter_types".to_string(),
            "result_types".to_string(),
            "from_sql".to_string(),
            "generic_plans".to_string(),
            "custom_plans".to_string(),
        ];

        let selected_columns = Self::get_selected_columns(&select.projection, &all_columns);

        // Get prepared statements (empty for now - would need session state tracking)
        let statements = Self::get_prepared_statements(session).await;

        // Apply WHERE clause filtering if present
        let filtered_statements = if let Some(where_clause) = &select.selection {
            Self::apply_where_filter(&statements, where_clause, &selected_columns)?
        } else {
            statements
        };

        // Build response
        let mut rows = Vec::new();
        for stmt in filtered_statements {
            let mut row = Vec::new();
            for column in &selected_columns {
                let value = stmt.get(column).cloned().unwrap_or_else(|| b"".to_vec());
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

    async fn get_prepared_statements(
        session: Option<Arc<SessionState>>,
    ) -> Vec<HashMap<String, Vec<u8>>> {
        let Some(session) = session else {
            return vec![];
        };

        let statements = session.prepared_statements.read().await;
        let statement_meta = session.prepared_statement_meta.read().await;
        let mut rows = Vec::new();

        for (name, stmt) in statements.iter() {
            // Hide unnamed extended-protocol statements from SQL catalog probes.
            if name.is_empty() {
                continue;
            }

            let mut row = HashMap::new();
            row.insert("name".to_string(), name.as_bytes().to_vec());
            row.insert("statement".to_string(), stmt.query.as_bytes().to_vec());
            row.insert(
                "prepare_time".to_string(),
                statement_meta
                    .get(name)
                    .map(|meta| Self::format_timestamptz(meta.prepare_time))
                    .unwrap_or_else(|| "1970-01-01 00:00:00+00".to_string())
                    .into_bytes(),
            );
            row.insert(
                "parameter_types".to_string(),
                Self::format_regtype_array(&stmt.param_types).into_bytes(),
            );
            row.insert("result_types".to_string(), b"{}".to_vec());
            row.insert(
                "from_sql".to_string(),
                statement_meta
                    .get(name)
                    .map(|meta| {
                        if meta.from_sql {
                            b"t".to_vec()
                        } else {
                            b"f".to_vec()
                        }
                    })
                    .unwrap_or_else(|| b"f".to_vec()),
            );
            row.insert(
                "generic_plans".to_string(),
                statement_meta
                    .get(name)
                    .map(|meta| meta.generic_plans.to_string().into_bytes())
                    .unwrap_or_else(|| b"0".to_vec()),
            );
            row.insert(
                "custom_plans".to_string(),
                statement_meta
                    .get(name)
                    .map(|meta| meta.custom_plans.to_string().into_bytes())
                    .unwrap_or_else(|| b"0".to_vec()),
            );
            rows.push(row);
        }

        rows.sort_by(|a, b| {
            a.get("name")
                .unwrap_or(&Vec::new())
                .cmp(b.get("name").unwrap_or(&Vec::new()))
        });
        rows
    }

    fn format_regtype_array(param_types: &[i32]) -> String {
        if param_types.is_empty() {
            return "{}".to_string();
        }

        let values: Vec<String> = param_types
            .iter()
            .map(|oid| SchemaTypeMapper::pg_oid_to_type_name(*oid))
            .map(Self::escape_array_item)
            .collect();
        format!("{{{}}}", values.join(","))
    }

    fn format_timestamptz(ts: std::time::SystemTime) -> String {
        let dt: DateTime<Utc> = ts.into();
        dt.format("%Y-%m-%d %H:%M:%S%.6f+00").to_string()
    }

    fn escape_array_item(item: &str) -> String {
        if item.contains(' ')
            || item.contains(',')
            || item.contains('"')
            || item.contains('\\')
            || item.contains('{')
            || item.contains('}')
        {
            format!("\"{}\"", item.replace('\\', "\\\\").replace('"', "\\\""))
        } else {
            item.to_string()
        }
    }

    fn apply_where_filter(
        statements: &[HashMap<String, Vec<u8>>],
        where_clause: &Expr,
        _selected_columns: &[String],
    ) -> Result<Vec<HashMap<String, Vec<u8>>>, PgSqliteError> {
        let mut filtered = Vec::new();

        for stmt in statements {
            let mut string_data = HashMap::new();
            for (key, value) in stmt {
                if let Ok(string_val) = String::from_utf8(value.clone()) {
                    string_data.insert(key.clone(), string_val);
                }
            }

            let column_mapping = HashMap::new();
            if WhereEvaluator::evaluate(where_clause, &string_data, &column_mapping) {
                filtered.push(stmt.clone());
            }
        }

        Ok(filtered)
    }
}
