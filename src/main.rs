use anyhow::Result;
use clap::Parser;
use futures::SinkExt;
use futures::StreamExt;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_util::codec::Framed;
use tracing::{error, info};

use pgsqlite::protocol::{
    AuthenticationMessage, BackendMessage, ErrorResponse, FrontendMessage, PostgresCodec,
    TransactionStatus,
};
use pgsqlite::query::{ExtendedQueryHandler, QueryExecutor};
use pgsqlite::session::{DbHandler, SessionState};

#[derive(Parser, Debug)]
#[command(name = "pgsqlite")]
#[command(about = "pgsqlite - 🐘 PostgreSQL + 🪶 SQLite = ♥\nPostgreSQL wire protocol server on top of SQLite", long_about = None)]
struct Config {
    #[arg(short, long, default_value = "5432")]
    port: u16,

    #[arg(short, long, default_value = "sqlite.db")]
    database: String,

    #[arg(long, default_value = "info")]
    log_level: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::parse();

    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(config.log_level)
        .init();

    // Initialize database handler
    let db_handler = Arc::new(
        DbHandler::new(&config.database)
            .map_err(|e| anyhow::anyhow!("Failed to create database handler: {}", e))?,
    );

    // Start TCP listener
    let listener = TcpListener::bind(("0.0.0.0", config.port)).await?;
    info!(
        "PostgreSQL-compatible server listening on port {}",
        config.port
    );
    info!("Using database: {}", config.database);

    loop {
        let (stream, addr) = listener.accept().await?;
        info!("New connection from {}", addr);

        let db_handler = db_handler.clone();

        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, addr, db_handler).await {
                error!("Connection error from {}: {}", addr, e);
            }
        });
    }
}

async fn handle_connection(
    stream: tokio::net::TcpStream,
    addr: std::net::SocketAddr,
    db_handler: Arc<DbHandler>,
) -> Result<()> {
    info!("Handling connection from {}", addr);

    let codec = PostgresCodec::new();
    let mut framed = Framed::new(stream, codec);

    // Wait for startup message
    let startup = match framed.next().await {
        Some(Ok(FrontendMessage::StartupMessage(msg))) => msg,
        Some(Ok(other)) => {
            error!("Expected startup message, got {:?}", other);
            return Err(anyhow::anyhow!("Protocol error: expected startup message"));
        }
        Some(Err(e)) => return Err(e.into()),
        None => return Err(anyhow::anyhow!("Connection closed unexpectedly")),
    };

    info!("Received startup message from {}: {:?}", addr, startup);

    // Extract session parameters
    let mut database = "main".to_string();
    let mut user = "postgres".to_string();

    for (key, value) in &startup.parameters {
        match key.as_str() {
            "database" => database = value.clone(),
            "user" => user = value.clone(),
            _ => {}
        }
    }

    let session = Arc::new(SessionState::new(database, user));

    // Send authentication OK
    framed
        .send(BackendMessage::Authentication(AuthenticationMessage::Ok))
        .await?;

    // Send parameter status messages
    for (key, value) in session.parameters.read().await.iter() {
        framed
            .send(BackendMessage::ParameterStatus {
                name: key.clone(),
                value: value.clone(),
            })
            .await?;
    }

    // Send backend key data
    framed
        .send(BackendMessage::BackendKeyData {
            process_id: std::process::id() as i32,
            secret_key: rand::random::<i32>(),
        })
        .await?;

    // Send ready for query
    framed
        .send(BackendMessage::ReadyForQuery {
            status: TransactionStatus::Idle,
        })
        .await?;

    info!("Sent authentication and ready response to {}", addr);

    // Main message loop
    while let Some(msg) = framed.next().await {
        match msg? {
            FrontendMessage::Query(sql) => {
                info!("Received query from {}: {}", addr, sql);

                // Execute the query
                match QueryExecutor::execute_query(&mut framed, &db_handler, &sql).await {
                    Ok(()) => {
                        // Query executed successfully
                    }
                    Err(e) => {
                        error!("Query execution error: {}", e);
                        let err = ErrorResponse::new(
                            "ERROR".to_string(),
                            "42000".to_string(),
                            format!("Query execution failed: {}", e),
                        );
                        framed.send(BackendMessage::ErrorResponse(err)).await?;
                    }
                }

                // Always send ReadyForQuery after handling the query
                framed
                    .send(BackendMessage::ReadyForQuery {
                        status: *session.transaction_status.read().await,
                    })
                    .await?;
            }
            FrontendMessage::Parse {
                name,
                query,
                param_types,
            } => {
                match ExtendedQueryHandler::handle_parse(
                    &mut framed,
                    &db_handler,
                    &session,
                    name,
                    query,
                    param_types,
                )
                .await
                {
                    Ok(()) => {}
                    Err(e) => {
                        error!("Parse error: {}", e);
                        let err = ErrorResponse::new(
                            "ERROR".to_string(),
                            "42000".to_string(),
                            format!("Parse failed: {}", e),
                        );
                        framed.send(BackendMessage::ErrorResponse(err)).await?;
                        framed
                            .send(BackendMessage::ReadyForQuery {
                                status: *session.transaction_status.read().await,
                            })
                            .await?;
                    }
                }
            }
            FrontendMessage::Bind {
                portal,
                statement,
                formats,
                values,
                result_formats,
            } => {
                match ExtendedQueryHandler::handle_bind(
                    &mut framed,
                    &session,
                    portal,
                    statement,
                    formats,
                    values,
                    result_formats,
                )
                .await
                {
                    Ok(()) => {}
                    Err(e) => {
                        error!("Bind error: {}", e);
                        let err = ErrorResponse::new(
                            "ERROR".to_string(),
                            "42000".to_string(),
                            format!("Bind failed: {}", e),
                        );
                        framed.send(BackendMessage::ErrorResponse(err)).await?;
                        framed
                            .send(BackendMessage::ReadyForQuery {
                                status: *session.transaction_status.read().await,
                            })
                            .await?;
                    }
                }
            }
            FrontendMessage::Execute { portal, max_rows } => {
                match ExtendedQueryHandler::handle_execute(
                    &mut framed,
                    &db_handler,
                    &session,
                    portal,
                    max_rows,
                )
                .await
                {
                    Ok(()) => {}
                    Err(e) => {
                        error!("Execute error: {}", e);
                        let err = ErrorResponse::new(
                            "ERROR".to_string(),
                            "42000".to_string(),
                            format!("Execute failed: {}", e),
                        );
                        framed.send(BackendMessage::ErrorResponse(err)).await?;
                        framed
                            .send(BackendMessage::ReadyForQuery {
                                status: *session.transaction_status.read().await,
                            })
                            .await?;
                    }
                }
            }
            FrontendMessage::Describe { typ, name } => {
                match ExtendedQueryHandler::handle_describe(&mut framed, &session, typ, name).await
                {
                    Ok(()) => {}
                    Err(e) => {
                        error!("Describe error: {}", e);
                        let err = ErrorResponse::new(
                            "ERROR".to_string(),
                            "42000".to_string(),
                            format!("Describe failed: {}", e),
                        );
                        framed.send(BackendMessage::ErrorResponse(err)).await?;
                        framed
                            .send(BackendMessage::ReadyForQuery {
                                status: *session.transaction_status.read().await,
                            })
                            .await?;
                    }
                }
            }
            FrontendMessage::Close { typ, name } => {
                match ExtendedQueryHandler::handle_close(&mut framed, &session, typ, name).await {
                    Ok(()) => {}
                    Err(e) => {
                        error!("Close error: {}", e);
                        let err = ErrorResponse::new(
                            "ERROR".to_string(),
                            "42000".to_string(),
                            format!("Close failed: {}", e),
                        );
                        framed.send(BackendMessage::ErrorResponse(err)).await?;
                        framed
                            .send(BackendMessage::ReadyForQuery {
                                status: *session.transaction_status.read().await,
                            })
                            .await?;
                    }
                }
            }
            FrontendMessage::Sync => {
                // Send ReadyForQuery to indicate we're ready for more commands
                framed
                    .send(BackendMessage::ReadyForQuery {
                        status: *session.transaction_status.read().await,
                    })
                    .await?;
            }
            FrontendMessage::Flush => {
                // Flush any pending messages
                framed.flush().await?;
            }
            FrontendMessage::Terminate => {
                info!("Client {} requested termination", addr);
                break;
            }
            other => {
                info!("Received unhandled message from {}: {:?}", addr, other);
            }
        }
    }

    info!("Connection from {} closed", addr);
    Ok(())
}
