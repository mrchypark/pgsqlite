use super::where_evaluator::WhereEvaluator;
use crate::PgSqliteError;
use crate::session::SessionState;
use crate::session::db_handler::{DbHandler, DbResponse};
use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, Select, SelectItem};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;
use uuid::Uuid;

pub struct PgRolesHandler;

impl PgRolesHandler {
    pub async fn handle_query(
        select: &Select,
        db: &DbHandler,
        session: Option<&Arc<SessionState>>,
    ) -> Result<DbResponse, PgSqliteError> {
        debug!("Handling pg_roles query");

        // Define all available columns for PostgreSQL pg_roles view
        let all_columns = vec![
            "oid".to_string(),
            "rolname".to_string(),
            "rolsuper".to_string(),
            "rolinherit".to_string(),
            "rolcreaterole".to_string(),
            "rolcreatedb".to_string(),
            "rolcanlogin".to_string(),
            "rolreplication".to_string(),
            "rolconnlimit".to_string(),
            "rolpassword".to_string(),
            "rolvaliduntil".to_string(),
            "rolbypassrls".to_string(),
            "rolconfig".to_string(),
        ];

        // Determine which columns to return
        let selected_columns = Self::get_selected_columns(&select.projection, &all_columns);

        let roles = Self::load_roles(db, session).await?;

        // Apply WHERE clause filtering if present
        let filtered_roles = if let Some(where_clause) = &select.selection {
            Self::apply_where_filter(&roles, where_clause, &selected_columns)?
        } else {
            roles
        };

        if let Some(count_column) = Self::count_projection_name(&select.projection) {
            return Ok(DbResponse {
                columns: vec![count_column],
                rows: vec![vec![Some(filtered_roles.len().to_string().into_bytes())]],
                rows_affected: 1,
            });
        }

        // Build response
        let mut rows = Vec::new();
        for role in filtered_roles {
            let mut row = Vec::new();
            for column in &selected_columns {
                let value = role.get(column).cloned().unwrap_or_else(|| b"".to_vec());
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
                    // For qualified wildcard like pg_roles.*, return all columns
                    selected.extend_from_slice(all_columns);
                    break;
                }
                _ => {}
            }
        }

        selected
    }

    async fn load_roles(
        db: &DbHandler,
        session: Option<&Arc<SessionState>>,
    ) -> Result<Vec<HashMap<String, Vec<u8>>>, PgSqliteError> {
        let read_roles = |conn: &rusqlite::Connection| {
            let exists: i64 = conn.query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = '__pgsqlite_roles'",
                [],
                |row| row.get(0),
            )?;
            if exists == 0 {
                return Ok(Self::get_default_roles());
            }

            Self::read_roles_from_connection(conn)
        };

        if let Some(session) = session {
            return db.with_session_connection(&session.id, read_roles).await;
        }

        let temp_session = Uuid::new_v4();
        db.create_session_connection(temp_session).await?;

        let result = db.with_session_connection(&temp_session, read_roles).await;

        db.remove_session_connection(&temp_session);
        result
    }

    fn read_roles_from_connection(
        conn: &rusqlite::Connection,
    ) -> Result<Vec<HashMap<String, Vec<u8>>>, rusqlite::Error> {
        let mut stmt = conn.prepare(
            "SELECT oid, rolname, rolsuper, rolinherit, rolcreaterole, rolcreatedb, \
             rolcanlogin, rolreplication, rolconnlimit, rolpassword, rolvaliduntil, \
             rolbypassrls, rolconfig FROM __pgsqlite_roles ORDER BY oid",
        )?;
        let mut rows = stmt.query([])?;
        let mut roles = Vec::new();

        while let Some(row) = rows.next()? {
            let mut role = HashMap::new();
            role.insert(
                "oid".to_string(),
                row.get::<_, i64>(0)?.to_string().into_bytes(),
            );
            role.insert("rolname".to_string(), row.get::<_, String>(1)?.into_bytes());
            role.insert(
                "rolsuper".to_string(),
                row.get::<_, String>(2)?.into_bytes(),
            );
            role.insert(
                "rolinherit".to_string(),
                row.get::<_, String>(3)?.into_bytes(),
            );
            role.insert(
                "rolcreaterole".to_string(),
                row.get::<_, String>(4)?.into_bytes(),
            );
            role.insert(
                "rolcreatedb".to_string(),
                row.get::<_, String>(5)?.into_bytes(),
            );
            role.insert(
                "rolcanlogin".to_string(),
                row.get::<_, String>(6)?.into_bytes(),
            );
            role.insert(
                "rolreplication".to_string(),
                row.get::<_, String>(7)?.into_bytes(),
            );
            role.insert(
                "rolconnlimit".to_string(),
                row.get::<_, i64>(8)?.to_string().into_bytes(),
            );
            role.insert(
                "rolpassword".to_string(),
                row.get::<_, Option<String>>(9)?
                    .unwrap_or_default()
                    .into_bytes(),
            );
            role.insert(
                "rolvaliduntil".to_string(),
                row.get::<_, Option<String>>(10)?
                    .unwrap_or_default()
                    .into_bytes(),
            );
            role.insert(
                "rolbypassrls".to_string(),
                row.get::<_, String>(11)?.into_bytes(),
            );
            role.insert(
                "rolconfig".to_string(),
                row.get::<_, Option<String>>(12)?
                    .unwrap_or_default()
                    .into_bytes(),
            );
            roles.push(role);
        }

        Ok(roles)
    }

    fn get_default_roles() -> Vec<HashMap<String, Vec<u8>>> {
        let mut roles = Vec::new();

        // Default superuser role (simulating PostgreSQL's postgres role)
        let mut postgres_role = HashMap::new();
        postgres_role.insert("oid".to_string(), b"10".to_vec()); // Standard postgres role OID
        postgres_role.insert("rolname".to_string(), b"postgres".to_vec());
        postgres_role.insert("rolsuper".to_string(), b"t".to_vec()); // true
        postgres_role.insert("rolinherit".to_string(), b"t".to_vec()); // true
        postgres_role.insert("rolcreaterole".to_string(), b"t".to_vec()); // true
        postgres_role.insert("rolcreatedb".to_string(), b"t".to_vec()); // true
        postgres_role.insert("rolcanlogin".to_string(), b"t".to_vec()); // true
        postgres_role.insert("rolreplication".to_string(), b"t".to_vec()); // true
        postgres_role.insert("rolconnlimit".to_string(), b"-1".to_vec()); // unlimited
        postgres_role.insert("rolpassword".to_string(), b"********".to_vec()); // hidden
        postgres_role.insert("rolvaliduntil".to_string(), b"".to_vec()); // NULL
        postgres_role.insert("rolbypassrls".to_string(), b"t".to_vec()); // true
        postgres_role.insert("rolconfig".to_string(), b"".to_vec()); // NULL
        roles.push(postgres_role);

        // Default public role (for compatibility)
        let mut public_role = HashMap::new();
        public_role.insert("oid".to_string(), b"0".to_vec()); // Public role OID
        public_role.insert("rolname".to_string(), b"public".to_vec());
        public_role.insert("rolsuper".to_string(), b"f".to_vec()); // false
        public_role.insert("rolinherit".to_string(), b"t".to_vec()); // true
        public_role.insert("rolcreaterole".to_string(), b"f".to_vec()); // false
        public_role.insert("rolcreatedb".to_string(), b"f".to_vec()); // false
        public_role.insert("rolcanlogin".to_string(), b"f".to_vec()); // false
        public_role.insert("rolreplication".to_string(), b"f".to_vec()); // false
        public_role.insert("rolconnlimit".to_string(), b"-1".to_vec()); // unlimited
        public_role.insert("rolpassword".to_string(), b"".to_vec()); // NULL
        public_role.insert("rolvaliduntil".to_string(), b"".to_vec()); // NULL
        public_role.insert("rolbypassrls".to_string(), b"f".to_vec()); // false
        public_role.insert("rolconfig".to_string(), b"".to_vec()); // NULL
        roles.push(public_role);

        // Default current user role (matches connection user)
        let mut current_user_role = HashMap::new();
        current_user_role.insert("oid".to_string(), b"100".to_vec()); // Default user OID
        current_user_role.insert("rolname".to_string(), b"pgsqlite_user".to_vec());
        current_user_role.insert("rolsuper".to_string(), b"t".to_vec()); // true for simplicity
        current_user_role.insert("rolinherit".to_string(), b"t".to_vec()); // true
        current_user_role.insert("rolcreaterole".to_string(), b"t".to_vec()); // true
        current_user_role.insert("rolcreatedb".to_string(), b"t".to_vec()); // true
        current_user_role.insert("rolcanlogin".to_string(), b"t".to_vec()); // true
        current_user_role.insert("rolreplication".to_string(), b"f".to_vec()); // false
        current_user_role.insert("rolconnlimit".to_string(), b"-1".to_vec()); // unlimited
        current_user_role.insert("rolpassword".to_string(), b"********".to_vec()); // hidden
        current_user_role.insert("rolvaliduntil".to_string(), b"".to_vec()); // NULL
        current_user_role.insert("rolbypassrls".to_string(), b"t".to_vec()); // true
        current_user_role.insert("rolconfig".to_string(), b"".to_vec()); // NULL
        roles.push(current_user_role);

        roles
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
        roles: &[HashMap<String, Vec<u8>>],
        where_clause: &Expr,
        _selected_columns: &[String],
    ) -> Result<Vec<HashMap<String, Vec<u8>>>, PgSqliteError> {
        let mut filtered = Vec::new();

        for role in roles {
            // Convert Vec<u8> to String for WhereEvaluator
            let mut string_data = HashMap::new();
            for (key, value) in role {
                if let Ok(string_val) = String::from_utf8(value.clone()) {
                    string_data.insert(key.clone(), string_val);
                }
            }

            let column_mapping = HashMap::new(); // Empty mapping for now
            if WhereEvaluator::evaluate(where_clause, &string_data, &column_mapping) {
                filtered.push(role.clone());
            }
        }

        Ok(filtered)
    }
}
