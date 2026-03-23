use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, Query};
use axum::response::IntoResponse;
use serde::Deserialize;

use super::MatchFragmentPath;
use crate::error::AppError;
use crate::models::StartFrame;
use crate::storage::Storage;

/// Query parameters for `POST /:token/:fragment_number/start`.
#[derive(Deserialize)]
pub(crate) struct StartQuery {
    #[allow(dead_code)]
    tick: i32,
    tps: f64,
    map: String,
    protocol: i32,
}

/// Query parameters for `POST /:token/:fragment_number/full`.
#[derive(Deserialize)]
pub(crate) struct FullQuery {
    tick: i32,
}

/// Query parameters for `POST /:token/:fragment_number/delta`.
#[derive(Deserialize)]
pub(crate) struct DeltaQuery {
    endtick: i32,
    #[serde(rename = "final")]
    is_final: bool,
}

/// POST `/:token/:fragment_number/start?tick=X&tps=Y&map=Z&protocol=P`
///
/// Stores the start frame for a match broadcast.
pub(crate) async fn post_start<S: Storage>(
    Path(path): Path<MatchFragmentPath>,
    Query(query): Query<StartQuery>,
    axum::extract::State(storage): axum::extract::State<Arc<S>>,
    body: Bytes,
) -> Result<impl IntoResponse, AppError> {
    let frame = StartFrame {
        tps: query.tps,
        protocol: query.protocol,
        map_name: query.map,
        body: body.to_vec(),
    };

    storage
        .start(&path.token, path.fragment_number, frame)
        .await?;

    Ok(())
}

/// POST `/:token/:fragment_number/full?tick=X`
///
/// Stores a full snapshot for a fragment.
pub(crate) async fn post_full<S: Storage>(
    Path(path): Path<MatchFragmentPath>,
    Query(query): Query<FullQuery>,
    axum::extract::State(storage): axum::extract::State<Arc<S>>,
    body: Bytes,
) -> Result<impl IntoResponse, AppError> {
    storage
        .full(&path.token, path.fragment_number, query.tick, body.to_vec())
        .await?;

    Ok(())
}

/// POST `/:token/:fragment_number/delta?endtick=X&final=true|false`
///
/// Stores a delta for a fragment.
pub(crate) async fn post_delta<S: Storage>(
    Path(path): Path<MatchFragmentPath>,
    Query(query): Query<DeltaQuery>,
    axum::extract::State(storage): axum::extract::State<Arc<S>>,
    body: Bytes,
) -> Result<impl IntoResponse, AppError> {
    storage
        .delta(
            &path.token,
            path.fragment_number,
            query.endtick,
            query.is_final,
            body.to_vec(),
        )
        .await?;

    Ok(())
}
