use std::future::Future;

use crate::error::AppError;
use crate::models::{StartFrame, SyncData};

pub(crate) mod memory;
pub(crate) mod redis;

/// Async storage trait combining both ingest (write) and broadcast (read) operations.
pub(crate) trait Storage: Send + Sync + 'static {
    /// Check storage backend health. Default implementation always succeeds.
    fn health_check(&self) -> impl Future<Output = Result<(), AppError>> + Send {
        async { Ok(()) }
    }

    /// Validate authentication for a token.
    #[allow(dead_code)]
    fn auth(&self, token: &str, auth: &str) -> impl Future<Output = Result<(), AppError>> + Send;

    /// Store a start frame for a match.
    fn start(
        &self,
        token: &str,
        fragment: i32,
        frame: StartFrame,
    ) -> impl Future<Output = Result<(), AppError>> + Send;

    /// Store a full snapshot for a fragment.
    fn full(
        &self,
        token: &str,
        fragment: i32,
        tick: i32,
        body: Vec<u8>,
    ) -> impl Future<Output = Result<(), AppError>> + Send;

    /// Store a delta for a fragment.
    fn delta(
        &self,
        token: &str,
        fragment: i32,
        end_tick: i32,
        is_final: bool,
        body: Vec<u8>,
    ) -> impl Future<Output = Result<(), AppError>> + Send;

    /// Get sync metadata for a specific fragment, or latest if fragment is None.
    fn get_sync(
        &self,
        token: &str,
        fragment: Option<i32>,
    ) -> impl Future<Output = Result<SyncData, AppError>> + Send;

    /// Get start frame bytes.
    fn get_start(
        &self,
        token: &str,
        fragment: i32,
    ) -> impl Future<Output = Result<Vec<u8>, AppError>> + Send;

    /// Get full snapshot bytes.
    fn get_full(
        &self,
        token: &str,
        fragment: i32,
    ) -> impl Future<Output = Result<Vec<u8>, AppError>> + Send;

    /// Get delta bytes.
    fn get_delta(
        &self,
        token: &str,
        fragment: i32,
    ) -> impl Future<Output = Result<Vec<u8>, AppError>> + Send;
}
