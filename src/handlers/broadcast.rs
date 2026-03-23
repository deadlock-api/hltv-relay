use std::sync::Arc;

use axum::extract::{Path, Query};
use axum::http::header;
use axum::response::IntoResponse;
use serde::Deserialize;

use super::MatchFragmentPath;
use crate::error::AppError;
use crate::storage::Storage;

/// Path extractor for routes that only need the token (e.g., sync).
#[derive(Deserialize)]
pub(crate) struct TokenPath {
    pub(crate) token: String,
}

/// Optional query parameter for `GET /:token/sync`.
#[derive(Deserialize)]
pub(crate) struct SyncQuery {
    pub(crate) fragment: Option<i32>,
}

/// GET `/:token/sync?fragment=N`
///
/// Returns JSON sync metadata. The `fragment` query parameter is optional;
/// when omitted (or zero), returns the latest sync data with delay applied.
pub(crate) async fn get_sync<S: Storage>(
    Path(path): Path<TokenPath>,
    Query(query): Query<SyncQuery>,
    axum::extract::State(storage): axum::extract::State<Arc<S>>,
) -> Result<impl IntoResponse, AppError> {
    // Treat fragment=0 the same as omitted, matching Go behavior
    let fragment = query.fragment.filter(|&f| f != 0);

    let sync_data = storage.get_sync(&path.token, fragment).await?;

    Ok(axum::Json(sync_data))
}

/// GET `/:token/:fragment_number/start`
///
/// Returns the raw start frame bytes for a fragment.
pub(crate) async fn get_start<S: Storage>(
    Path(path): Path<MatchFragmentPath>,
    axum::extract::State(storage): axum::extract::State<Arc<S>>,
) -> Result<impl IntoResponse, AppError> {
    let data = storage.get_start(&path.token, path.fragment_number).await?;

    Ok(([(header::CONTENT_TYPE, "application/octet-stream")], data))
}

/// GET `/:token/:fragment_number/full`
///
/// Returns the raw full snapshot bytes for a fragment.
pub(crate) async fn get_full<S: Storage>(
    Path(path): Path<MatchFragmentPath>,
    axum::extract::State(storage): axum::extract::State<Arc<S>>,
) -> Result<impl IntoResponse, AppError> {
    let data = storage.get_full(&path.token, path.fragment_number).await?;

    Ok(([(header::CONTENT_TYPE, "application/octet-stream")], data))
}

/// GET `/:token/:fragment_number/delta`
///
/// Returns the raw delta bytes for a fragment.
pub(crate) async fn get_delta<S: Storage>(
    Path(path): Path<MatchFragmentPath>,
    axum::extract::State(storage): axum::extract::State<Arc<S>>,
) -> Result<impl IntoResponse, AppError> {
    let data = storage.get_delta(&path.token, path.fragment_number).await?;

    Ok(([(header::CONTENT_TYPE, "application/octet-stream")], data))
}
