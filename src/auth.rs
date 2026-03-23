use std::net::IpAddr;
use std::sync::Arc;

use axum::extract::ConnectInfo;
use axum::http::Request;
use axum::middleware::Next;
use axum::response::Response;
use ipnetwork::IpNetwork;

use crate::config::Config;
use crate::error::AppError;

/// Individual auth mode that can be combined with OR logic.
#[derive(Debug, Clone)]
pub(crate) enum AuthMode {
    /// Accept any request without auth check.
    AllowAll,
    /// Validate `X-Origin-Auth` header against a configured key.
    Key(String),
    /// Accept requests from specified CIDR networks.
    Network(Vec<IpNetwork>),
}

/// Configured authentication — a list of modes checked with OR logic.
/// If the list is empty, all requests are denied by default.
#[derive(Debug, Clone)]
pub(crate) struct AuthConfig {
    modes: Vec<AuthMode>,
}

impl AuthConfig {
    /// Create an [`AuthConfig`] from a list of auth modes directly.
    #[cfg(test)]
    pub(crate) fn new(modes: Vec<AuthMode>) -> Self {
        Self { modes }
    }

    /// Build an [`AuthConfig`] from the application [`Config`].
    pub(crate) fn from_config(config: &Config) -> Result<Self, AppError> {
        let mut modes = Vec::new();

        for mode_str in &config.auth_modes {
            match mode_str.as_str() {
                "allow-all" => modes.push(AuthMode::AllowAll),
                "key" => {
                    let key = config.auth_key.clone().ok_or_else(|| {
                        AppError::ConfigError(
                            "auth mode 'key' requires --auth-key to be set".to_owned(),
                        )
                    })?;
                    modes.push(AuthMode::Key(key));
                }
                "network" => {
                    if config.allowed_networks.is_empty() {
                        return Err(AppError::ConfigError(
                            "auth mode 'network' requires --allowed-networks to be set".to_owned(),
                        ));
                    }
                    let networks = config
                        .allowed_networks
                        .iter()
                        .map(|s| {
                            s.parse::<IpNetwork>().map_err(|e| {
                                AppError::ConfigError(format!("invalid CIDR '{s}': {e}"))
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    modes.push(AuthMode::Network(networks));
                }
                other => {
                    return Err(AppError::ConfigError(format!(
                        "unknown auth mode: '{other}', expected 'key', 'allow-all', or 'network'"
                    )));
                }
            }
        }

        Ok(Self { modes })
    }

    /// Check whether a request is authorized.
    ///
    /// Returns `Ok(())` if any configured mode accepts the request.
    /// Returns `Err(InvalidAuth)` if no mode accepts (or none are configured).
    pub(crate) fn check(
        &self,
        auth_header: Option<&str>,
        client_ip: Option<IpAddr>,
    ) -> Result<(), AppError> {
        if self.modes.is_empty() {
            // Deny by default when no auth mode is configured.
            return Err(AppError::InvalidAuth);
        }

        for mode in &self.modes {
            match mode {
                AuthMode::AllowAll => return Ok(()),
                AuthMode::Key(expected) => {
                    if let Some(provided) = auth_header
                        && provided == expected
                    {
                        return Ok(());
                    }
                }
                AuthMode::Network(networks) => {
                    if let Some(ip) = client_ip
                        && networks.iter().any(|net| net.contains(ip))
                    {
                        return Ok(());
                    }
                }
            }
        }

        Err(AppError::InvalidAuth)
    }
}

/// Extract the client IP from `X-Forwarded-For` header, falling back to the
/// connected peer address.
fn extract_client_ip(req: &Request<axum::body::Body>) -> Option<IpAddr> {
    // Try X-Forwarded-For first (first IP in the list is the original client).
    if let Some(forwarded) = req.headers().get("x-forwarded-for")
        && let Ok(value) = forwarded.to_str()
        && let Some(first) = value.split(',').next()
        && let Ok(ip) = first.trim().parse::<IpAddr>()
    {
        return Some(ip);
    }

    // Fall back to peer address from ConnectInfo.
    req.extensions()
        .get::<ConnectInfo<std::net::SocketAddr>>()
        .map(|ci| ci.0.ip())
}

/// Axum middleware that enforces authentication on ingest (POST) routes.
pub(crate) async fn auth_middleware(
    req: Request<axum::body::Body>,
    next: Next,
) -> Result<Response, AppError> {
    let auth_config = req
        .extensions()
        .get::<Arc<AuthConfig>>()
        .cloned()
        .ok_or(AppError::InvalidAuth)?;

    let auth_header = req
        .headers()
        .get("x-origin-auth")
        .and_then(|v| v.to_str().ok());

    let client_ip = extract_client_ip(&req);

    auth_config.check(auth_header, client_ip)?;

    Ok(next.run(req).await)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_config(
        auth_modes: Vec<&str>,
        auth_key: Option<&str>,
        allowed_networks: Vec<&str>,
    ) -> Config {
        Config {
            port: 3000,
            host: "0.0.0.0".to_owned(),
            storage: crate::config::StorageBackend::Memory,
            redis_url: String::new(),
            auth_modes: auth_modes
                .into_iter()
                .map(std::borrow::ToOwned::to_owned)
                .collect(),
            auth_key: auth_key.map(std::borrow::ToOwned::to_owned),
            allowed_networks: allowed_networks
                .into_iter()
                .map(std::borrow::ToOwned::to_owned)
                .collect(),
        }
    }

    #[test]
    fn deny_by_default_when_no_modes() {
        let ac = AuthConfig::from_config(&make_config(vec![], None, vec![])).expect("should build");
        assert!(ac.check(None, None).is_err());
    }

    #[test]
    fn allow_all_mode_accepts_anything() {
        let ac = AuthConfig::from_config(&make_config(vec!["allow-all"], None, vec![]))
            .expect("should build");
        assert!(ac.check(None, None).is_ok());
    }

    #[test]
    fn key_mode_accepts_correct_key() {
        let ac = AuthConfig::from_config(&make_config(vec!["key"], Some("secret123"), vec![]))
            .expect("should build");
        assert!(ac.check(Some("secret123"), None).is_ok());
    }

    #[test]
    fn key_mode_rejects_wrong_key() {
        let ac = AuthConfig::from_config(&make_config(vec!["key"], Some("secret123"), vec![]))
            .expect("should build");
        assert!(ac.check(Some("wrong"), None).is_err());
    }

    #[test]
    fn key_mode_rejects_missing_header() {
        let ac = AuthConfig::from_config(&make_config(vec!["key"], Some("secret123"), vec![]))
            .expect("should build");
        assert!(ac.check(None, None).is_err());
    }

    #[test]
    fn key_mode_requires_auth_key_configured() {
        let result = AuthConfig::from_config(&make_config(vec!["key"], None, vec![]));
        assert!(result.is_err());
    }

    #[test]
    fn network_mode_accepts_allowed_ip() {
        let ac = AuthConfig::from_config(&make_config(vec!["network"], None, vec!["10.0.0.0/8"]))
            .expect("should build");
        let ip: IpAddr = "10.1.2.3".parse().expect("valid ip");
        assert!(ac.check(None, Some(ip)).is_ok());
    }

    #[test]
    fn network_mode_rejects_disallowed_ip() {
        let ac = AuthConfig::from_config(&make_config(vec!["network"], None, vec!["10.0.0.0/8"]))
            .expect("should build");
        let ip: IpAddr = "192.168.1.1".parse().expect("valid ip");
        assert!(ac.check(None, Some(ip)).is_err());
    }

    #[test]
    fn network_mode_requires_networks_configured() {
        let result = AuthConfig::from_config(&make_config(vec!["network"], None, vec![]));
        assert!(result.is_err());
    }

    #[test]
    fn or_logic_key_or_network() {
        let ac = AuthConfig::from_config(&make_config(
            vec!["key", "network"],
            Some("mykey"),
            vec!["10.0.0.0/8"],
        ))
        .expect("should build");

        // Key alone works
        assert!(ac.check(Some("mykey"), None).is_ok());

        // Network alone works
        let ip: IpAddr = "10.5.5.5".parse().expect("valid ip");
        assert!(ac.check(None, Some(ip)).is_ok());

        // Neither works
        let bad_ip: IpAddr = "192.168.1.1".parse().expect("valid ip");
        assert!(ac.check(Some("wrong"), Some(bad_ip)).is_err());
    }

    #[test]
    fn unknown_auth_mode_is_error() {
        let result = AuthConfig::from_config(&make_config(vec!["bogus"], None, vec![]));
        assert!(result.is_err());
    }

    #[test]
    fn invalid_cidr_is_error() {
        let result =
            AuthConfig::from_config(&make_config(vec!["network"], None, vec!["not-a-cidr"]));
        assert!(result.is_err());
    }

    #[test]
    fn multiple_networks_or_logic() {
        let ac = AuthConfig::from_config(&make_config(
            vec!["network"],
            None,
            vec!["10.0.0.0/8", "172.16.0.0/12"],
        ))
        .expect("should build");

        let ip1: IpAddr = "10.1.1.1".parse().expect("valid ip");
        let ip2: IpAddr = "172.16.5.5".parse().expect("valid ip");
        let ip3: IpAddr = "192.168.1.1".parse().expect("valid ip");

        assert!(ac.check(None, Some(ip1)).is_ok());
        assert!(ac.check(None, Some(ip2)).is_ok());
        assert!(ac.check(None, Some(ip3)).is_err());
    }
}
