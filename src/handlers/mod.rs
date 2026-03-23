pub(crate) mod broadcast;
pub(crate) mod ingest;

use serde::Deserialize;

/// Shared path extractor for `/:token/:fragment_number` routes.
#[derive(Debug, Deserialize)]
pub(crate) struct MatchFragmentPath {
    pub(crate) token: String,
    pub(crate) fragment_number: i32,
}
