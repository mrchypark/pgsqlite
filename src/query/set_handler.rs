use crate::PgSqliteError;
use crate::protocol::BackendMessage;
use crate::session::SessionState;
use futures::SinkExt;
use once_cell::sync::Lazy;
use regex::Regex;
use std::sync::Arc;
use tokio_util::codec::Framed;
use tracing::{debug, info};

static SET_TIMEZONE_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)^\s*SET\s+(LOCAL\s+)?TIME\s*ZONE\s+(.+?)\s*;?\s*$").unwrap());

static SET_TRANSACTION_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"(?i)^\s*SET\s+TRANSACTION\s+(?:(ISOLATION\s+LEVEL)\s+([A-Za-z\s]+)|(READ\s+ONLY|READ\s+WRITE))\s*;?\s*$",
    )
    .unwrap()
});

static SET_PARAMETER_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)^\s*SET\s+(LOCAL\s+)?(\w+)\s*(?:TO|=)\s*(.+?)\s*;?\s*$").unwrap());

static RESET_PARAMETER_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)^\s*RESET\s+(.+?)\s*;?\s*$").unwrap());

static SHOW_PARAMETER_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)^\s*SHOW\s+(.+?)\s*$").unwrap());

pub struct SetHandler;

impl SetHandler {
    /// Check if this is a SET command that we need to handle
    pub fn is_set_command(query: &str) -> bool {
        let trimmed = query.trim();
        let upper = trimmed.to_uppercase();
        upper.starts_with("SET ") || upper.starts_with("SHOW ")
    }

    /// Handle SET and SHOW commands
    pub async fn handle_set_command<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        session: &Arc<SessionState>,
        query: &str,
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        Self::handle_set_command_with_options(framed, session, query, false).await
    }

    /// Handle SET and SHOW commands with extended protocol support
    pub async fn handle_set_command_extended<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        session: &Arc<SessionState>,
        query: &str,
        skip_row_description: bool,
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        Self::handle_set_command_with_options(framed, session, query, skip_row_description).await
    }

    async fn handle_set_command_with_options<T>(
        framed: &mut Framed<T, crate::protocol::PostgresCodec>,
        session: &Arc<SessionState>,
        query: &str,
        skip_row_description: bool,
    ) -> Result<(), PgSqliteError>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let trimmed = query.trim();
        debug!("Handling SET command: {}", trimmed);

        // Handle SET TIME ZONE
        if let Some(caps) = SET_TIMEZONE_PATTERN.captures(trimmed) {
            let is_local = caps.get(1).is_some();
            let timezone = caps[2].trim().trim_matches('\'').trim_matches('"');
            info!("Setting timezone to: {}", timezone);
            Self::set_timezone(session, timezone, is_local).await?;

            framed
                .send(BackendMessage::CommandComplete {
                    tag: "SET".to_string(),
                })
                .await
                .map_err(PgSqliteError::Io)?;

            return Ok(());
        }

        if let Some(caps) = SET_TRANSACTION_PATTERN.captures(trimmed) {
            if caps.get(1).is_some() {
                let isolation_level = caps[2].trim().to_lowercase();
                Self::set_parameter_value(
                    session,
                    "TRANSACTION_ISOLATION",
                    isolation_level,
                    true,
                )
                .await;
            } else {
                let read_mode = caps[3].trim().to_lowercase();
                let read_only = if read_mode == "read only" { "on" } else { "off" };
                Self::set_parameter_value(
                    session,
                    "TRANSACTION_READ_ONLY",
                    read_only.to_string(),
                    true,
                )
                .await;
            }

            framed
                .send(BackendMessage::CommandComplete {
                    tag: "SET".to_string(),
                })
                .await
                .map_err(PgSqliteError::Io)?;

            return Ok(());
        }

        // Handle general SET parameter
        if let Some(caps) = SET_PARAMETER_PATTERN.captures(trimmed) {
            let is_local = caps.get(1).is_some();
            let param_name = SessionState::canonical_parameter_name(&caps[2]);
            let param_value = caps[3].trim().trim_matches('\'').trim_matches('"');

            let normalized_value = if param_name == "SEARCH_PATH" {
                normalize_search_path(param_value, session).await
            } else {
                param_value.to_string()
            };

            Self::set_parameter_value(session, &param_name, normalized_value, is_local).await;

            framed
                .send(BackendMessage::CommandComplete {
                    tag: "SET".to_string(),
                })
                .await
                .map_err(PgSqliteError::Io)?;

            return Ok(());
        }

        if let Some(caps) = RESET_PARAMETER_PATTERN.captures(trimmed) {
            let name = caps[1].trim();
            if name.eq_ignore_ascii_case("ALL") {
                session.reset_all_parameters().await;
            } else {
                session.reset_parameter(name).await;
            }

            framed
                .send(BackendMessage::CommandComplete {
                    tag: "RESET".to_string(),
                })
                .await
                .map_err(PgSqliteError::Io)?;

            return Ok(());
        }

        // Handle SHOW parameter
        if let Some(caps) = SHOW_PARAMETER_PATTERN.captures(trimmed) {
            let requested_name = caps[1].trim();
            let param_name = SessionState::canonical_parameter_name(requested_name);
            info!("SHOW parameter: {}", param_name);

            let value = session
                .get_parameter(&param_name)
                .await
                .unwrap_or_else(|| "unset".to_string());
            info!("Parameter {} = {}", param_name, value);

            // Send row description only if not in extended protocol with pre-described statement
            if !skip_row_description {
                let field = crate::protocol::FieldDescription {
                    name: requested_name.to_lowercase(),
                    table_oid: 0,
                    column_id: 1,
                    type_oid: crate::types::PgType::Text.to_oid(),
                    type_size: -1,
                    type_modifier: -1,
                    format: 0,
                };

                framed
                    .send(BackendMessage::RowDescription(vec![field]))
                    .await
                    .map_err(PgSqliteError::Io)?;
            }

            // Send data row
            let row = vec![Some(value.as_bytes().to_vec())];
            framed
                .send(BackendMessage::DataRow(row))
                .await
                .map_err(PgSqliteError::Io)?;

            framed
                .send(BackendMessage::CommandComplete {
                    tag: "SHOW".to_string(),
                })
                .await
                .map_err(PgSqliteError::Io)?;

            return Ok(());
        }

        Err(PgSqliteError::Protocol(format!(
            "Unrecognized SET command: {query}"
        )))
    }

    /// Set the session timezone
    async fn set_timezone(
        session: &Arc<SessionState>,
        timezone: &str,
        is_local: bool,
    ) -> Result<(), PgSqliteError> {
        // Validate timezone (basic validation)
        let valid_timezone = match timezone.to_uppercase().as_str() {
            "UTC" | "GMT" => "UTC",
            "EST" => "America/New_York",
            "PST" => "America/Los_Angeles",
            "CST" => "America/Chicago",
            "MST" => "America/Denver",
            _ => {
                // Check if it's a numeric offset like '+05:30' or '-08:00'
                if Self::is_valid_offset(timezone) {
                    timezone
                } else {
                    // For now, default to the provided value
                    // In a full implementation, we'd validate against a timezone database
                    timezone
                }
            }
        };

        Self::set_parameter_value(session, "TIMEZONE", valid_timezone.to_string(), is_local).await;

        Ok(())
    }

    /// Check if a string is a valid timezone offset
    fn is_valid_offset(offset: &str) -> bool {
        let offset_pattern = Regex::new(r"^[+-]\d{2}:\d{2}$").unwrap();
        offset_pattern.is_match(offset)
    }

    async fn set_parameter_value(
        session: &Arc<SessionState>,
        name: &str,
        value: String,
        is_local: bool,
    ) {
        if is_local && session.in_transaction().await {
            session.set_local_parameter(name, value).await;
        } else {
            session.set_parameter(name, value).await;
        }
    }
}

async fn normalize_search_path(value: &str, session: &Arc<SessionState>) -> String {
    let raw = value
        .trim()
        .trim_matches('\'')
        .trim_matches('"')
        .to_string();
    if raw.eq_ignore_ascii_case("default") {
        return "public".to_string();
    }

    if raw.is_empty() {
        return "public".to_string();
    }

    let user = session.user.clone();
    let mut parts = Vec::new();
    for part in raw.split(',') {
        let mut item = part.trim().trim_matches('\'').trim_matches('"').to_string();
        if item.is_empty() {
            continue;
        }
        if item == "$user" {
            item = user.clone();
        }
        parts.push(item);
    }

    if parts.is_empty() {
        "public".to_string()
    } else {
        // Keep a stable, whitespace-free representation so SHOW/current_setting
        // comparisons are predictable (and easy to parse by splitting on ',').
        parts.join(",")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_set_command() {
        assert!(SetHandler::is_set_command("SET TIME ZONE 'UTC'"));
        assert!(SetHandler::is_set_command("set time zone 'UTC'"));
        assert!(SetHandler::is_set_command("SET search_path TO public"));
        assert!(SetHandler::is_set_command("SHOW TimeZone"));
        assert!(SetHandler::is_set_command("show timezone"));

        assert!(!SetHandler::is_set_command("SELECT * FROM users"));
        assert!(!SetHandler::is_set_command("INSERT INTO test VALUES (1)"));
    }

    #[test]
    fn test_set_timezone_pattern() {
        let query = "SET TIME ZONE 'America/New_York'";
        assert!(SET_TIMEZONE_PATTERN.is_match(query));

        let query = "set time zone UTC";
        assert!(SET_TIMEZONE_PATTERN.is_match(query));

        let query = "SET TIME ZONE '+05:30'";
        assert!(SET_TIMEZONE_PATTERN.is_match(query));
    }

    #[test]
    fn test_show_parameter_pattern() {
        let query = "SHOW TimeZone";
        assert!(SHOW_PARAMETER_PATTERN.is_match(query));

        let query = "show search_path";
        assert!(SHOW_PARAMETER_PATTERN.is_match(query));
    }
}
