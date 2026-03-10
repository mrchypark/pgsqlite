use super::where_evaluator::WhereEvaluator;
use crate::PgSqliteError;
use crate::session::SessionState;
use crate::session::db_handler::{DbHandler, DbResponse};
use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, Select, SelectItem};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;
use uuid::Uuid;

pub struct PgUserHandler;

impl PgUserHandler {
    pub async fn handle_query(
        select: &Select,
        db: &DbHandler,
        session: Option<&Arc<SessionState>>,
    ) -> Result<DbResponse, PgSqliteError> {
        debug!("Handling pg_user query");

        // Define all available columns for PostgreSQL pg_user view
        let all_columns = vec![
            "usename".to_string(),
            "usesysid".to_string(),
            "usecreatedb".to_string(),
            "usesuper".to_string(),
            "userepl".to_string(),
            "usebypassrls".to_string(),
            "passwd".to_string(),
            "valuntil".to_string(),
            "useconfig".to_string(),
        ];

        // Determine which columns to return
        let selected_columns = Self::get_selected_columns(&select.projection, &all_columns);

        let users = Self::load_users(db, session).await?;

        // Apply WHERE clause filtering if present
        let filtered_users = if let Some(where_clause) = &select.selection {
            Self::apply_where_filter(&users, where_clause, &selected_columns)?
        } else {
            users
        };

        if let Some(count_column) = Self::count_projection_name(&select.projection) {
            return Ok(DbResponse {
                columns: vec![count_column],
                rows: vec![vec![Some(filtered_users.len().to_string().into_bytes())]],
                rows_affected: 1,
            });
        }

        // Build response
        let mut rows = Vec::new();
        for user in filtered_users {
            let mut row = Vec::new();
            for column in &selected_columns {
                let value = user.get(column).cloned().unwrap_or_else(|| b"".to_vec());
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
                    // For qualified wildcard like pg_user.*, return all columns
                    selected.extend_from_slice(all_columns);
                    break;
                }
                _ => {}
            }
        }

        selected
    }

    async fn load_users(
        db: &DbHandler,
        session: Option<&Arc<SessionState>>,
    ) -> Result<Vec<HashMap<String, Vec<u8>>>, PgSqliteError> {
        let read_users = |conn: &rusqlite::Connection| {
            let exists: i64 = conn.query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = '__pgsqlite_roles'",
                [],
                |row| row.get(0),
            )?;
            if exists == 0 {
                return Ok(Self::get_default_users());
            }

            Self::read_users_from_connection(conn)
        };

        if let Some(session) = session {
            return db.with_session_connection(&session.id, read_users).await;
        }

        let temp_session = Uuid::new_v4();
        db.create_session_connection(temp_session).await?;

        let result = db.with_session_connection(&temp_session, read_users).await;

        db.remove_session_connection(&temp_session);
        result
    }

    fn read_users_from_connection(
        conn: &rusqlite::Connection,
    ) -> Result<Vec<HashMap<String, Vec<u8>>>, rusqlite::Error> {
        let mut stmt = conn.prepare(
            "SELECT rolname, oid, rolcreatedb, rolsuper, rolreplication, rolbypassrls, \
             rolpassword, rolvaliduntil, rolconfig \
             FROM __pgsqlite_roles WHERE rolcanlogin = 't' ORDER BY oid",
        )?;
        let mut rows = stmt.query([])?;
        let mut users = Vec::new();

        while let Some(row) = rows.next()? {
            let mut user = HashMap::new();
            user.insert("usename".to_string(), row.get::<_, String>(0)?.into_bytes());
            user.insert(
                "usesysid".to_string(),
                row.get::<_, i64>(1)?.to_string().into_bytes(),
            );
            user.insert(
                "usecreatedb".to_string(),
                row.get::<_, String>(2)?.into_bytes(),
            );
            user.insert(
                "usesuper".to_string(),
                row.get::<_, String>(3)?.into_bytes(),
            );
            user.insert("userepl".to_string(), row.get::<_, String>(4)?.into_bytes());
            user.insert(
                "usebypassrls".to_string(),
                row.get::<_, String>(5)?.into_bytes(),
            );
            user.insert(
                "passwd".to_string(),
                row.get::<_, Option<String>>(6)?
                    .unwrap_or_default()
                    .into_bytes(),
            );
            user.insert(
                "valuntil".to_string(),
                row.get::<_, Option<String>>(7)?
                    .unwrap_or_default()
                    .into_bytes(),
            );
            user.insert(
                "useconfig".to_string(),
                row.get::<_, Option<String>>(8)?
                    .unwrap_or_default()
                    .into_bytes(),
            );
            users.push(user);
        }

        Ok(users)
    }

    fn get_default_users() -> Vec<HashMap<String, Vec<u8>>> {
        let mut users = Vec::new();

        // Default superuser (corresponds to postgres role)
        let mut postgres_user = HashMap::new();
        postgres_user.insert("usename".to_string(), b"postgres".to_vec());
        postgres_user.insert("usesysid".to_string(), b"10".to_vec()); // Standard postgres user OID
        postgres_user.insert("usecreatedb".to_string(), b"t".to_vec()); // true
        postgres_user.insert("usesuper".to_string(), b"t".to_vec()); // true
        postgres_user.insert("userepl".to_string(), b"t".to_vec()); // true
        postgres_user.insert("usebypassrls".to_string(), b"t".to_vec()); // true
        postgres_user.insert("passwd".to_string(), b"********".to_vec()); // hidden
        postgres_user.insert("valuntil".to_string(), b"".to_vec()); // NULL
        postgres_user.insert("useconfig".to_string(), b"".to_vec()); // NULL
        users.push(postgres_user);

        // Default current user (corresponds to pgsqlite_user role)
        let mut current_user = HashMap::new();
        current_user.insert("usename".to_string(), b"pgsqlite_user".to_vec());
        current_user.insert("usesysid".to_string(), b"100".to_vec()); // Default user OID
        current_user.insert("usecreatedb".to_string(), b"t".to_vec()); // true
        current_user.insert("usesuper".to_string(), b"t".to_vec()); // true for simplicity
        current_user.insert("userepl".to_string(), b"f".to_vec()); // false
        current_user.insert("usebypassrls".to_string(), b"t".to_vec()); // true
        current_user.insert("passwd".to_string(), b"********".to_vec()); // hidden
        current_user.insert("valuntil".to_string(), b"".to_vec()); // NULL
        current_user.insert("useconfig".to_string(), b"".to_vec()); // NULL
        users.push(current_user);

        users
    }

    fn count_projection_name(projection: &[SelectItem]) -> Option<String> {
        for item in projection {
            match item {
                SelectItem::UnnamedExpr(expr) if Self::is_count_star_expr(expr) => {
                    return Some("count".to_string());
                }
                SelectItem::ExprWithAlias { expr, alias } if Self::is_count_star_expr(expr) => {
                    return Some(alias.value.clone());
                }
                _ => {}
            }
        }

        None
    }

    fn is_count_star(function: &sqlparser::ast::Function) -> bool {
        if !function.name.to_string().eq_ignore_ascii_case("count") {
            return false;
        }
        let args = match &function.args {
            sqlparser::ast::FunctionArguments::List(list) => &list.args,
            _ => return false,
        };
        args.len() == 1 && matches!(&args[0], FunctionArg::Unnamed(FunctionArgExpr::Wildcard))
    }

    fn is_count_star_expr(expr: &Expr) -> bool {
        match expr {
            Expr::Function(function) => Self::is_count_star(function),
            Expr::Cast { expr, .. } => Self::is_count_star_expr(expr),
            _ => false,
        }
    }

    fn apply_where_filter(
        users: &[HashMap<String, Vec<u8>>],
        where_clause: &Expr,
        _selected_columns: &[String],
    ) -> Result<Vec<HashMap<String, Vec<u8>>>, PgSqliteError> {
        let mut filtered = Vec::new();

        for user in users {
            // Convert Vec<u8> to String for WhereEvaluator
            let mut string_data = HashMap::new();
            for (key, value) in user {
                if let Ok(string_val) = String::from_utf8(value.clone()) {
                    string_data.insert(key.clone(), string_val);
                }
            }

            let column_mapping = HashMap::new(); // Empty mapping for now
            if WhereEvaluator::evaluate(where_clause, &string_data, &column_mapping) {
                filtered.push(user.clone());
            }
        }

        Ok(filtered)
    }
}
