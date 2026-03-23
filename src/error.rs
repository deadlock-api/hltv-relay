use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

#[derive(Debug, thiserror::Error)]
pub(crate) enum AppError {
    #[error("invalid authentication")]
    InvalidAuth,

    #[error("fragment not found")]
    FragmentNotFound,

    #[error("match not found")]
    MatchNotFound,

    #[error("storage error: {0}")]
    StorageError(String),

    #[error("configuration error: {0}")]
    ConfigError(String),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status = match &self {
            Self::InvalidAuth => StatusCode::UNAUTHORIZED,
            Self::FragmentNotFound | Self::MatchNotFound => StatusCode::NOT_FOUND,
            Self::StorageError(_) | Self::ConfigError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, self.to_string()).into_response()
    }
}
