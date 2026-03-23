use async_compression::tokio::bufread::{ZstdDecoder, ZstdEncoder};
use redis::AsyncCommands;
use tokio::io::{AsyncReadExt, BufReader};
use tracing::{debug, info};

use crate::error::AppError;
use crate::models::{StartFrame, SyncData};
use crate::storage::Storage;

/// Default TTL for all Redis keys (2 hours in seconds).
const DEFAULT_TTL_SECS: u64 = 2 * 60 * 60;

/// Compress bytes using zstd.
async fn compress(data: &[u8]) -> Result<Vec<u8>, AppError> {
    let mut encoder = ZstdEncoder::new(BufReader::new(data));
    let mut compressed = Vec::new();
    encoder
        .read_to_end(&mut compressed)
        .await
        .map_err(|e| AppError::StorageError(format!("zstd compression error: {e}")))?;
    Ok(compressed)
}

/// Decompress zstd-compressed bytes.
async fn decompress(data: &[u8]) -> Result<Vec<u8>, AppError> {
    let mut decoder = ZstdDecoder::new(BufReader::new(data));
    let mut decompressed = Vec::new();
    decoder
        .read_to_end(&mut decompressed)
        .await
        .map_err(|e| AppError::StorageError(format!("zstd decompression error: {e}")))?;
    Ok(decompressed)
}

/// Convert a Redis error into an `AppError::StorageError`.
#[allow(clippy::needless_pass_by_value)] // Used as map_err callback which requires FnOnce(E)
fn redis_err(e: redis::RedisError) -> AppError {
    AppError::StorageError(format!("redis error: {e}"))
}

/// Parse a `Vec<u8>` as a little-endian i32.
fn read_i32_le(bytes: Vec<u8>) -> Result<i32, AppError> {
    let arr: [u8; 4] = bytes
        .try_into()
        .map_err(|_| AppError::StorageError("invalid i32 bytes".to_owned()))?;
    Ok(i32::from_le_bytes(arr))
}

/// Redis storage backend using a connection manager (auto-reconnecting multiplexed connection).
pub(crate) struct RedisStorage {
    conn: redis::aio::ConnectionManager,
    ttl: u64,
    delay: i32,
}

impl RedisStorage {
    /// Create a new Redis storage backend by connecting to the given URL.
    pub(crate) async fn new(redis_url: &str, fragment_delay: i32) -> Result<Self, AppError> {
        Self::with_ttl(redis_url, DEFAULT_TTL_SECS, fragment_delay).await
    }

    /// Create a new Redis storage backend with a custom TTL (in seconds).
    pub(crate) async fn with_ttl(
        redis_url: &str,
        ttl: u64,
        fragment_delay: i32,
    ) -> Result<Self, AppError> {
        let client = redis::Client::open(redis_url).map_err(redis_err)?;
        let conn = redis::aio::ConnectionManager::new(client)
            .await
            .map_err(redis_err)?;
        info!("connected to Redis");
        Ok(Self {
            conn,
            ttl,
            delay: fragment_delay,
        })
    }

    /// Check that the Redis connection is healthy (used by health check endpoint).
    pub(crate) async fn ping(&self) -> Result<(), AppError> {
        let mut conn = self.conn.clone();
        redis::cmd("PING")
            .query_async::<String>(&mut conn)
            .await
            .map_err(redis_err)?;
        Ok(())
    }

    /// Verify a match exists by checking for its metadata key.
    async fn ensure_match_exists(
        conn: &mut redis::aio::ConnectionManager,
        token: &str,
    ) -> Result<(), AppError> {
        let meta_key = format!("{token}:meta");
        let exists: bool = conn.exists(&meta_key).await.map_err(redis_err)?;
        if !exists {
            return Err(AppError::MatchNotFound);
        }
        Ok(())
    }

    /// Get a fragment's raw bytes by suffix (start, full, or delta).
    /// Returns `MatchNotFound` if the match doesn't exist, `FragmentNotFound` if the key is missing.
    async fn get_fragment_bytes(
        &self,
        token: &str,
        fragment: i32,
        suffix: &str,
    ) -> Result<Vec<u8>, AppError> {
        let mut conn = self.conn.clone();
        let key = format!("{token}:{fragment}:{suffix}");
        let val: Option<Vec<u8>> = conn.get(&key).await.map_err(redis_err)?;
        // A missing key means either the match or fragment doesn't exist.
        // Distinguish by checking for metadata only when the key is absent.
        if let Some(bytes) = val {
            decompress(&bytes).await
        } else {
            Self::ensure_match_exists(&mut conn, token).await?;
            Err(AppError::FragmentNotFound)
        }
    }
}

impl Storage for RedisStorage {
    async fn health_check(&self) -> Result<(), AppError> {
        self.ping().await
    }

    async fn start(&self, token: &str, fragment: i32, frame: StartFrame) -> Result<(), AppError> {
        debug!(token, fragment, map = %frame.map_name, "redis: storing start frame");
        let mut conn = self.conn.clone();

        let body_key = format!("{token}:{fragment}:start");
        let meta = serde_json::json!({
            "tps": frame.tps,
            "protocol": frame.protocol,
            "map_name": frame.map_name,
            "signup_fragment": fragment,
        });
        let meta_key = format!("{token}:meta");
        let meta_bytes = serde_json::to_vec(&meta)
            .map_err(|e| AppError::StorageError(format!("json serialize error: {e}")))?;
        let compressed_body = compress(&frame.body).await?;

        redis::pipe()
            .cmd("SET")
            .arg(&body_key)
            .arg(&compressed_body)
            .arg("EX")
            .arg(self.ttl)
            .ignore()
            .cmd("SET")
            .arg(&meta_key)
            .arg(&meta_bytes)
            .arg("EX")
            .arg(self.ttl)
            .ignore()
            .query_async::<()>(&mut conn)
            .await
            .map_err(redis_err)?;

        Ok(())
    }

    async fn full(
        &self,
        token: &str,
        fragment: i32,
        tick: i32,
        body: Vec<u8>,
    ) -> Result<(), AppError> {
        let mut conn = self.conn.clone();
        Self::ensure_match_exists(&mut conn, token).await?;

        let body_key = format!("{token}:{fragment}:full");
        let tick_key = format!("{token}:{fragment}:tick");
        let latest_key = format!("{token}:latest");
        let compressed_body = compress(&body).await?;

        redis::pipe()
            .cmd("SET")
            .arg(&body_key)
            .arg(&compressed_body)
            .arg("EX")
            .arg(self.ttl)
            .ignore()
            .cmd("SET")
            .arg(&tick_key)
            .arg(tick.to_le_bytes().as_slice())
            .arg("EX")
            .arg(self.ttl)
            .ignore()
            .cmd("SET")
            .arg(&latest_key)
            .arg(fragment.to_le_bytes().as_slice())
            .arg("EX")
            .arg(self.ttl)
            .ignore()
            .query_async::<()>(&mut conn)
            .await
            .map_err(redis_err)?;

        Ok(())
    }

    async fn delta(
        &self,
        token: &str,
        fragment: i32,
        end_tick: i32,
        is_final: bool,
        body: Vec<u8>,
    ) -> Result<(), AppError> {
        let mut conn = self.conn.clone();
        Self::ensure_match_exists(&mut conn, token).await?;

        let body_key = format!("{token}:{fragment}:delta");
        let end_tick_key = format!("{token}:{fragment}:endtick");
        let final_key = format!("{token}:{fragment}:final");
        let final_byte: u8 = u8::from(is_final);
        let compressed_body = compress(&body).await?;

        redis::pipe()
            .cmd("SET")
            .arg(&body_key)
            .arg(&compressed_body)
            .arg("EX")
            .arg(self.ttl)
            .ignore()
            .cmd("SET")
            .arg(&end_tick_key)
            .arg(end_tick.to_le_bytes().as_slice())
            .arg("EX")
            .arg(self.ttl)
            .ignore()
            .cmd("SET")
            .arg(&final_key)
            .arg(&[final_byte])
            .arg("EX")
            .arg(self.ttl)
            .ignore()
            .query_async::<()>(&mut conn)
            .await
            .map_err(redis_err)?;

        Ok(())
    }

    async fn get_sync(&self, token: &str, fragment: Option<i32>) -> Result<SyncData, AppError> {
        let mut conn = self.conn.clone();

        // Load match metadata
        let meta_key = format!("{token}:meta");
        let meta_bytes: Option<Vec<u8>> = conn.get(&meta_key).await.map_err(redis_err)?;
        let meta_bytes = meta_bytes.ok_or(AppError::MatchNotFound)?;
        let meta: serde_json::Value = serde_json::from_slice(&meta_bytes)
            .map_err(|e| AppError::StorageError(format!("json deserialize error: {e}")))?;

        let frag_num = if let Some(f) = fragment {
            f
        } else {
            let latest_key = format!("{token}:latest");
            let latest_bytes: Option<Vec<u8>> = conn.get(&latest_key).await.map_err(redis_err)?;
            let latest = read_i32_le(latest_bytes.ok_or(AppError::FragmentNotFound)?)?;
            latest - self.delay
        };

        // Load tick and endtick in a single pipeline
        let tick_key = format!("{token}:{frag_num}:tick");
        let end_tick_key = format!("{token}:{frag_num}:endtick");

        let (tick_bytes, end_tick_bytes): (Option<Vec<u8>>, Option<Vec<u8>>) = redis::pipe()
            .cmd("GET")
            .arg(&tick_key)
            .cmd("GET")
            .arg(&end_tick_key)
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;

        let tick = read_i32_le(tick_bytes.ok_or(AppError::FragmentNotFound)?)?;
        let end_tick = match end_tick_bytes {
            Some(b) => read_i32_le(b)?,
            None => 0,
        };

        let tps = meta["tps"].as_f64().unwrap_or(0.0);
        let protocol = meta["protocol"].as_i64().unwrap_or(0);
        let map_name = meta["map_name"].as_str().unwrap_or("");
        let signup_fragment = meta["signup_fragment"].as_i64().unwrap_or(0);

        Ok(SyncData {
            tick,
            endtick: end_tick,
            rtdelay: 0.0,
            rcvage: 0.0,
            fragment: frag_num,
            #[allow(clippy::cast_possible_truncation)]
            signup_fragment: signup_fragment as i32,
            tps,
            keyframe_interval: 3.0,
            map: map_name.to_owned(),
            #[allow(clippy::cast_possible_truncation)]
            protocol: protocol as i32,
        })
    }

    async fn get_start(&self, token: &str, fragment: i32) -> Result<Vec<u8>, AppError> {
        self.get_fragment_bytes(token, fragment, "start").await
    }

    async fn get_full(&self, token: &str, fragment: i32) -> Result<Vec<u8>, AppError> {
        self.get_fragment_bytes(token, fragment, "full").await
    }

    async fn get_delta(&self, token: &str, fragment: i32) -> Result<Vec<u8>, AppError> {
        self.get_fragment_bytes(token, fragment, "delta").await
    }
}

#[cfg(test)]
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
mod tests {
    use super::*;
    use testcontainers_modules::redis::{REDIS_PORT, Redis};
    use testcontainers_modules::testcontainers::runners::AsyncRunner;

    async fn start_redis() -> (testcontainers::ContainerAsync<Redis>, RedisStorage) {
        let container = Redis::default().start().await.unwrap();
        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(REDIS_PORT).await.unwrap();
        let url = format!("redis://{host}:{port}");
        let storage = RedisStorage::new(&url, 8)
            .await
            .expect("redis connection failed");
        (container, storage)
    }

    #[tokio::test]
    async fn test_redis_start_and_get_start() {
        let (_container, storage) = start_redis().await;
        let token = "s12345t6789_redis_test_start";

        storage
            .start(
                token,
                0,
                StartFrame {
                    tps: 128.0,
                    protocol: 4,
                    map_name: "de_dust2".to_owned(),
                    body: vec![1, 2, 3, 4],
                },
            )
            .await
            .expect("start failed");

        let body = storage.get_start(token, 0).await.expect("get_start failed");
        assert_eq!(body, vec![1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn test_redis_full_requires_match() {
        let (_container, storage) = start_redis().await;
        let result = storage
            .full("nonexistent_redis_test", 1, 100, vec![5, 6])
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_redis_full_and_get_full() {
        let (_container, storage) = start_redis().await;
        let token = "s12345t6789_redis_test_full";

        storage
            .start(
                token,
                0,
                StartFrame {
                    tps: 128.0,
                    protocol: 4,
                    map_name: "de_dust2".to_owned(),
                    body: vec![1, 2, 3],
                },
            )
            .await
            .expect("start failed");

        storage
            .full(token, 1, 100, vec![10, 20, 30])
            .await
            .expect("full failed");

        let body = storage.get_full(token, 1).await.expect("get_full failed");
        assert_eq!(body, vec![10, 20, 30]);
    }

    #[tokio::test]
    async fn test_redis_delta_and_get_delta() {
        let (_container, storage) = start_redis().await;
        let token = "s12345t6789_redis_test_delta";

        storage
            .start(
                token,
                0,
                StartFrame {
                    tps: 128.0,
                    protocol: 4,
                    map_name: "de_dust2".to_owned(),
                    body: vec![1, 2, 3],
                },
            )
            .await
            .expect("start failed");

        storage
            .delta(token, 1, 200, false, vec![40, 50, 60])
            .await
            .expect("delta failed");

        let body = storage.get_delta(token, 1).await.expect("get_delta failed");
        assert_eq!(body, vec![40, 50, 60]);
    }

    #[tokio::test]
    async fn test_redis_get_sync() {
        let (_container, storage) = start_redis().await;
        let token = "s12345t6789_redis_test_sync";

        storage
            .start(
                token,
                0,
                StartFrame {
                    tps: 128.0,
                    protocol: 4,
                    map_name: "de_dust2".to_owned(),
                    body: vec![1],
                },
            )
            .await
            .expect("start failed");

        // Ingest fragments 0 through 10
        for i in 0..=10 {
            storage
                .full(token, i, i * 100, vec![i as u8])
                .await
                .expect("full failed");
            storage
                .delta(token, i, i * 100 + 50, i == 10, vec![i as u8 + 100])
                .await
                .expect("delta failed");
        }

        // Latest is 10, delay is 8, so sync should return fragment 2
        let sync = storage
            .get_sync(token, None)
            .await
            .expect("get_sync failed");
        assert_eq!(sync.fragment, 2);
        assert_eq!(sync.tick, 200);
        assert_eq!(sync.map, "de_dust2");
        assert_eq!(sync.protocol, 4);
    }

    #[tokio::test]
    async fn test_redis_ping() {
        let (_container, storage) = start_redis().await;
        storage.ping().await.expect("ping failed");
    }
}
