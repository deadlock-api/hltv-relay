use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use tracing::{error, warn};

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

        match &self {
            Self::StorageError(_) | Self::ConfigError(_) => {
                error!(status = status.as_u16(), error = %self, "internal error");
            }
            Self::InvalidAuth => {
                warn!(status = status.as_u16(), "unauthorized request");
            }
            _ => {}
        }

        (status, self.to_string()).into_response()
    }
}
