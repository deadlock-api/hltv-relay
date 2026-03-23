use serde::{Deserialize, Serialize};

use crate::error::AppError;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct StartFrame {
    pub(crate) tps: f64,
    pub(crate) protocol: i32,
    pub(crate) map_name: String,
    pub(crate) body: Vec<u8>,
}

#[derive(Debug, Clone)]
#[allow(dead_code, clippy::struct_field_names)]
pub(crate) struct Fragment {
    pub(crate) tick: i32,
    pub(crate) final_fragment: bool,
    pub(crate) end_tick: i32,
    pub(crate) full: Option<Vec<u8>>,
    pub(crate) delta: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[allow(dead_code)]
pub(crate) struct SyncData {
    pub(crate) tick: i32,
    pub(crate) endtick: i32,
    pub(crate) rtdelay: f64,
    pub(crate) rcvage: f64,
    pub(crate) fragment: i32,
    pub(crate) signup_fragment: i32,
    pub(crate) tps: f64,
    pub(crate) keyframe_interval: f64,
    pub(crate) map: String,
    pub(crate) protocol: i32,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(crate) struct TokenInfo {
    pub(crate) steam_id: String,
    pub(crate) timestamp: String,
}

#[allow(dead_code)]
pub(crate) fn parse_token(token: &str) -> Result<TokenInfo, AppError> {
    let rest = token.strip_prefix('s').ok_or(AppError::InvalidAuth)?;

    let t_pos = rest.find('t').ok_or(AppError::InvalidAuth)?;

    let steam_id = &rest[..t_pos];
    let timestamp = &rest[t_pos + 1..];

    if steam_id.is_empty() || timestamp.is_empty() {
        return Err(AppError::InvalidAuth);
    }

    Ok(TokenInfo {
        steam_id: steam_id.to_string(),
        timestamp: timestamp.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sync_data_json_round_trip() {
        let sync = SyncData {
            tick: 1000,
            endtick: 1500,
            rtdelay: 10.0,
            rcvage: 0.5,
            fragment: 42,
            signup_fragment: 1,
            tps: 64.0,
            keyframe_interval: 3.0,
            map: "de_dust2".to_string(),
            protocol: 14,
        };

        let json = serde_json::to_string(&sync).unwrap();
        let deserialized: SyncData = serde_json::from_str(&json).unwrap();
        assert_eq!(sync, deserialized);
    }

    #[test]
    fn test_sync_data_json_field_names() {
        let sync = SyncData {
            tick: 100,
            endtick: 200,
            rtdelay: 5.0,
            rcvage: 1.0,
            fragment: 10,
            signup_fragment: 0,
            tps: 128.0,
            keyframe_interval: 2.0,
            map: "de_mirage".to_string(),
            protocol: 13,
        };

        let json = serde_json::to_string(&sync).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();

        // Verify exact JSON field names match Go server
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
    }

    #[test]
    fn test_parse_token_valid() {
        let result = parse_token("s76561198000000001t1679000000").unwrap();
        assert_eq!(result.steam_id, "76561198000000001");
        assert_eq!(result.timestamp, "1679000000");
    }

    #[test]
    fn test_parse_token_missing_s_prefix() {
        assert!(parse_token("76561198000000001t1679000000").is_err());
    }

    #[test]
    fn test_parse_token_missing_t_separator() {
        assert!(parse_token("s76561198000000001").is_err());
    }

    #[test]
    fn test_parse_token_empty_steam_id() {
        assert!(parse_token("st1679000000").is_err());
    }

    #[test]
    fn test_parse_token_empty_timestamp() {
        assert!(parse_token("s76561198000000001t").is_err());
    }

    #[test]
    fn test_parse_token_empty_string() {
        assert!(parse_token("").is_err());
    }
}
