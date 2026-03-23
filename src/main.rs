#![forbid(unsafe_code)]
#![deny(clippy::all)]
#![deny(unreachable_pub)]
#![deny(clippy::pedantic)]

mod auth;
mod config;
mod error;
mod handlers;
mod models;
mod router;
mod storage;

use std::sync::Arc;

use auth::AuthConfig;
use config::Config;
use storage::memory::MemoryStorage;
use storage::redis::RedisStorage;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize structured logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let config = Config::load()?;
    let auth_config = Arc::new(AuthConfig::from_config(&config)?);

    info!("{config}");
    info!("Active auth modes: {:?}", config.auth_modes);

    // Select storage backend based on config and build router
    let app = match config.storage {
        config::StorageBackend::Memory => {
            let storage = Arc::new(MemoryStorage::new());
            router::build_router(storage, auth_config)
        }
        config::StorageBackend::Redis => {
            let storage = Arc::new(
                RedisStorage::new(&config.redis_url)
                    .await
                    .map_err(|e| anyhow::anyhow!("{e}"))?,
            );
            router::build_router(storage, auth_config)
        }
    };

    let bind_addr = format!("{}:{}", config.host, config.port);
    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    info!("Server listening on {bind_addr}");

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await?;

    info!("Server shut down gracefully");
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c().await.ok();
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
}
