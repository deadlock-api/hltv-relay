use std::sync::Arc;

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::middleware;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use tower_http::cors::CorsLayer;

use crate::auth::{AuthConfig, auth_middleware};
use crate::handlers::{broadcast, ingest};
use crate::storage::Storage;

/// Build the application router with all routes wired.
///
/// POST (ingest) routes are protected by auth middleware.
/// GET (broadcast) routes are public.
pub(crate) fn build_router<S: Storage>(storage: Arc<S>, auth_config: Arc<AuthConfig>) -> Router {
    // GET routes — public, no auth
    let broadcast_routes = Router::new()
        .route("/{token}/sync", get(broadcast::get_sync::<S>))
        .route(
            "/{token}/{fragment_number}/start",
            get(broadcast::get_start::<S>),
        )
        .route(
            "/{token}/{fragment_number}/full",
            get(broadcast::get_full::<S>),
        )
        .route(
            "/{token}/{fragment_number}/delta",
            get(broadcast::get_delta::<S>),
        );

    // POST routes — protected by auth middleware
    let ingest_routes = Router::new()
        .route(
            "/{token}/{fragment_number}/start",
            post(ingest::post_start::<S>),
        )
        .route(
            "/{token}/{fragment_number}/full",
            post(ingest::post_full::<S>),
        )
        .route(
            "/{token}/{fragment_number}/delta",
            post(ingest::post_delta::<S>),
        )
        .layer(middleware::from_fn(auth_middleware))
        .layer(axum::Extension(auth_config));

    // Health check — verifies storage backend connectivity
    let health = Router::new().route("/health", get(health_check::<S>));

    Router::new()
        .merge(broadcast_routes)
        .merge(ingest_routes)
        .merge(health)
        .with_state(storage)
        .layer(CorsLayer::permissive())
}

async fn health_check<S: Storage>(
    State(storage): State<Arc<S>>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    storage
        .health_check()
        .await
        .map_err(|e| (StatusCode::SERVICE_UNAVAILABLE, e.to_string()))?;
    Ok("ok")
}

#[cfg(test)]
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
mod tests {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    use super::*;
    use crate::auth::{AuthConfig, AuthMode};
    use crate::models::SyncData;
    use crate::storage::memory::MemoryStorage;

    fn test_app(auth_key: &str) -> Router {
        let storage = Arc::new(MemoryStorage::new(8));
        let auth_config = Arc::new(AuthConfig::new(vec![AuthMode::Key(auth_key.to_owned())]));
        build_router(storage, auth_config)
    }

    fn test_app_allow_all() -> Router {
        let storage = Arc::new(MemoryStorage::new(8));
        let auth_config = Arc::new(AuthConfig::new(vec![AuthMode::AllowAll]));
        build_router(storage, auth_config)
    }

    async fn body_bytes(body: Body) -> Vec<u8> {
        body.collect()
            .await
            .expect("collect body")
            .to_bytes()
            .to_vec()
    }

    // ── POST start then GET it back — bytes match ────────────────────

    #[tokio::test]
    async fn post_start_then_get_returns_same_bytes() {
        let app = test_app_allow_all();
        let start_body = vec![0xDE, 0xAD, 0xBE, 0xEF];

        // POST start frame
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/s111t222/0/start?tick=0&tps=64.0&map=de_dust2&protocol=4")
                    .header("x-origin-auth", "unused")
                    .body(Body::from(start_body.clone()))
                    .expect("build request"),
            )
            .await
            .expect("send request");
        assert_eq!(resp.status(), StatusCode::OK);

        // GET start frame
        let resp = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/s111t222/0/start")
                    .body(Body::empty())
                    .expect("build request"),
            )
            .await
            .expect("send request");
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(body_bytes(resp.into_body()).await, start_body);
    }

    // ── POST full + delta, GET sync returns correct fragment info ────

    #[tokio::test]
    async fn post_full_delta_then_get_sync() {
        let app = test_app_allow_all();
        let token = "s111t222";

        // POST start to create the match
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!(
                        "/{token}/0/start?tick=0&tps=128.0&map=de_mirage&protocol=14"
                    ))
                    .body(Body::from(vec![1, 2, 3]))
                    .expect("build request"),
            )
            .await
            .expect("send request");
        assert_eq!(resp.status(), StatusCode::OK);

        // POST full + delta for fragments 0..=10 so sync (latest - 8) returns fragment 2
        for i in 0..=10_i32 {
            let resp = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(format!("/{token}/{i}/full?tick={}", i * 100))
                        .body(Body::from(vec![i as u8]))
                        .expect("build request"),
                )
                .await
                .expect("send request");
            assert_eq!(resp.status(), StatusCode::OK);

            let resp = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(format!(
                            "/{token}/{i}/delta?endtick={}&final={}",
                            i * 100 + 50,
                            i == 10
                        ))
                        .body(Body::from(vec![i as u8 + 100]))
                        .expect("build request"),
                )
                .await
                .expect("send request");
            assert_eq!(resp.status(), StatusCode::OK);
        }

        // GET sync (no fragment param — should return latest - 8 = fragment 2)
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/{token}/sync"))
                    .body(Body::empty())
                    .expect("build request"),
            )
            .await
            .expect("send request");
        assert_eq!(resp.status(), StatusCode::OK);

        let bytes = body_bytes(resp.into_body()).await;
        let sync: SyncData = serde_json::from_slice(&bytes).expect("parse sync JSON");
        assert_eq!(sync.fragment, 2);
        assert_eq!(sync.tick, 200);
        assert_eq!(sync.endtick, 250);
        assert_eq!(sync.map, "de_mirage");
        assert_eq!(sync.protocol, 14);
        assert!((sync.tps - 128.0).abs() < f64::EPSILON);

        // GET sync with specific fragment
        let resp = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/{token}/sync?fragment=5"))
                    .body(Body::empty())
                    .expect("build request"),
            )
            .await
            .expect("send request");
        assert_eq!(resp.status(), StatusCode::OK);

        let bytes = body_bytes(resp.into_body()).await;
        let sync: SyncData = serde_json::from_slice(&bytes).expect("parse sync JSON");
        assert_eq!(sync.fragment, 5);
        assert_eq!(sync.tick, 500);
    }

    // ── Auth rejection with wrong key returns 401 ───────────────────

    #[tokio::test]
    async fn auth_wrong_key_returns_401() {
        let app = test_app("correct-key");

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/s111t222/0/start?tick=0&tps=64.0&map=de_dust2&protocol=4")
                    .header("x-origin-auth", "wrong-key")
                    .body(Body::from(vec![1, 2, 3]))
                    .expect("build request"),
            )
            .await
            .expect("send request");
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // ── Auth acceptance with correct key returns 200 ────────────────

    #[tokio::test]
    async fn auth_correct_key_returns_200() {
        let app = test_app("my-secret");

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/s111t222/0/start?tick=0&tps=64.0&map=de_dust2&protocol=4")
                    .header("x-origin-auth", "my-secret")
                    .body(Body::from(vec![1, 2, 3]))
                    .expect("build request"),
            )
            .await
            .expect("send request");
        assert_eq!(resp.status(), StatusCode::OK);
    }

    // ── Auth missing header returns 401 ─────────────────────────────

    #[tokio::test]
    async fn auth_missing_header_returns_401() {
        let app = test_app("some-key");

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/s111t222/0/start?tick=0&tps=64.0&map=de_dust2&protocol=4")
                    .body(Body::from(vec![1, 2, 3]))
                    .expect("build request"),
            )
            .await
            .expect("send request");
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // ── Unknown token returns 404 ───────────────────────────────────

    #[tokio::test]
    async fn unknown_token_get_sync_returns_404() {
        let app = test_app_allow_all();

        let resp = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/s999t999/sync")
                    .body(Body::empty())
                    .expect("build request"),
            )
            .await
            .expect("send request");
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn unknown_token_get_start_returns_404() {
        let app = test_app_allow_all();

        let resp = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/s999t999/0/start")
                    .body(Body::empty())
                    .expect("build request"),
            )
            .await
            .expect("send request");
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn unknown_token_get_full_returns_404() {
        let app = test_app_allow_all();

        let resp = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/s999t999/0/full")
                    .body(Body::empty())
                    .expect("build request"),
            )
            .await
            .expect("send request");
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn unknown_token_get_delta_returns_404() {
        let app = test_app_allow_all();

        let resp = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/s999t999/0/delta")
                    .body(Body::empty())
                    .expect("build request"),
            )
            .await
            .expect("send request");
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    // ── Sync endpoint returns valid JSON matching expected schema ────

    #[tokio::test]
    async fn sync_returns_valid_json_with_all_fields() {
        let app = test_app_allow_all();
        let token = "s111t222";

        // Create match and ingest enough fragments
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!(
                        "/{token}/0/start?tick=0&tps=64.0&map=de_ancient&protocol=5"
                    ))
                    .body(Body::from(vec![1]))
                    .expect("build request"),
            )
            .await
            .expect("send request");
        assert_eq!(resp.status(), StatusCode::OK);

        // Ingest fragment 5 with full + delta
        for i in 0..=10_i32 {
            let _ = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(format!("/{token}/{i}/full?tick={}", i * 50))
                        .body(Body::from(vec![i as u8]))
                        .expect("build request"),
                )
                .await
                .expect("send request");
            let _ = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(format!(
                            "/{token}/{i}/delta?endtick={}&final=false",
                            i * 50 + 25
                        ))
                        .body(Body::from(vec![i as u8]))
                        .expect("build request"),
                )
                .await
                .expect("send request");
        }

        let resp = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/{token}/sync?fragment=5"))
                    .body(Body::empty())
                    .expect("build request"),
            )
            .await
            .expect("send request");
        assert_eq!(resp.status(), StatusCode::OK);

        let bytes = body_bytes(resp.into_body()).await;
        let value: serde_json::Value = serde_json::from_slice(&bytes).expect("parse JSON");

        // Verify all expected fields are present
        assert!(value.get("tick").is_some());
        assert!(value.get("endtick").is_some());
        assert!(value.get("rtdelay").is_some());
        assert!(value.get("rcvage").is_some());
        assert!(value.get("fragment").is_some());
        assert!(value.get("signup_fragment").is_some());
        assert!(value.get("tps").is_some());
        assert!(value.get("keyframe_interval").is_some());
        assert!(value.get("map").is_some());
        assert!(value.get("protocol").is_some());

        // Verify types and values
        assert_eq!(value["fragment"], 5);
        assert_eq!(value["tick"], 250);
        assert_eq!(value["endtick"], 275);
        assert_eq!(value["map"], "de_ancient");
        assert_eq!(value["protocol"], 5);
    }

    // ── GET endpoints are public (no auth required) ─────────────────

    #[tokio::test]
    async fn get_endpoints_require_no_auth() {
        // Even with key-based auth, GET endpoints should work without a key
        let app = test_app("super-secret");

        // POST a start frame with correct auth
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/s111t222/0/start?tick=0&tps=64.0&map=de_dust2&protocol=4")
                    .header("x-origin-auth", "super-secret")
                    .body(Body::from(vec![0xAB]))
                    .expect("build request"),
            )
            .await
            .expect("send request");
        assert_eq!(resp.status(), StatusCode::OK);

        // GET without any auth header should succeed
        let resp = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/s111t222/0/start")
                    .body(Body::empty())
                    .expect("build request"),
            )
            .await
            .expect("send request");
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(body_bytes(resp.into_body()).await, vec![0xAB]);
    }

    // ── Full and delta round-trip ───────────────────────────────────

    #[tokio::test]
    async fn post_full_and_delta_then_get_returns_same_bytes() {
        let app = test_app_allow_all();
        let token = "s111t222";
        let full_body = vec![0x01, 0x02, 0x03, 0x04, 0x05];
        let delta_body = vec![0xA0, 0xB0, 0xC0];

        // Create match
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!(
                        "/{token}/0/start?tick=0&tps=64.0&map=de_dust2&protocol=4"
                    ))
                    .body(Body::from(vec![1]))
                    .expect("build request"),
            )
            .await
            .expect("send request");
        assert_eq!(resp.status(), StatusCode::OK);

        // POST full
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/{token}/1/full?tick=100"))
                    .body(Body::from(full_body.clone()))
                    .expect("build request"),
            )
            .await
            .expect("send request");
        assert_eq!(resp.status(), StatusCode::OK);

        // POST delta
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/{token}/1/delta?endtick=150&final=false"))
                    .body(Body::from(delta_body.clone()))
                    .expect("build request"),
            )
            .await
            .expect("send request");
        assert_eq!(resp.status(), StatusCode::OK);

        // GET full
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/{token}/1/full"))
                    .body(Body::empty())
                    .expect("build request"),
            )
            .await
            .expect("send request");
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(body_bytes(resp.into_body()).await, full_body);

        // GET delta
        let resp = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/{token}/1/delta"))
                    .body(Body::empty())
                    .expect("build request"),
            )
            .await
            .expect("send request");
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(body_bytes(resp.into_body()).await, delta_body);
    }

    // ── Health check returns 200 ────────────────────────────────────

    #[tokio::test]
    async fn health_check_returns_200() {
        let app = test_app_allow_all();

        let resp = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/health")
                    .body(Body::empty())
                    .expect("build request"),
            )
            .await
            .expect("send request");
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
