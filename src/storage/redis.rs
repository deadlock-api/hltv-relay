use redis::AsyncCommands;

use crate::error::AppError;
use crate::models::{StartFrame, SyncData};
use crate::storage::Storage;

/// Default TTL for all Redis keys (1 hour in seconds).
const DEFAULT_TTL_SECS: u64 = 3600;

/// Default fragment delay for sync endpoint (matches Go reference and memory backend).
const DEFAULT_DELAY: i32 = 8;

/// Redis storage backend using a connection manager (auto-reconnecting multiplexed connection).
pub(crate) struct RedisStorage {
    conn: redis::aio::ConnectionManager,
    ttl: u64,
    delay: i32,
}

impl RedisStorage {
    /// Create a new Redis storage backend by connecting to the given URL.
    pub(crate) async fn new(redis_url: &str) -> Result<Self, AppError> {
        Self::with_ttl(redis_url, DEFAULT_TTL_SECS).await
    }

    /// Create a new Redis storage backend with a custom TTL (in seconds).
    pub(crate) async fn with_ttl(redis_url: &str, ttl: u64) -> Result<Self, AppError> {
        let client = redis::Client::open(redis_url)
            .map_err(|e| AppError::StorageError(format!("redis client error: {e}")))?;
        let conn = redis::aio::ConnectionManager::new(client)
            .await
            .map_err(|e| AppError::StorageError(format!("redis connection error: {e}")))?;
        Ok(Self {
            conn,
            ttl,
            delay: DEFAULT_DELAY,
        })
    }

    /// Check that the Redis connection is healthy (used by health check endpoint).
    pub(crate) async fn ping(&self) -> Result<(), AppError> {
        let mut conn = self.conn.clone();
        redis::cmd("PING")
            .query_async::<String>(&mut conn)
            .await
            .map_err(|e| AppError::StorageError(format!("redis ping failed: {e}")))?;
        Ok(())
    }

    /// Set a key with the configured TTL.
    async fn set_with_ttl(
        conn: &mut redis::aio::ConnectionManager,
        key: &str,
        value: &[u8],
        ttl: u64,
    ) -> Result<(), AppError> {
        conn.set_ex::<_, _, ()>(key, value, ttl)
            .await
            .map_err(|e| AppError::StorageError(format!("redis set error: {e}")))?;
        Ok(())
    }

    /// Get raw bytes from a key.
    async fn get_bytes(
        conn: &mut redis::aio::ConnectionManager,
        key: &str,
    ) -> Result<Vec<u8>, AppError> {
        let val: Option<Vec<u8>> = conn
            .get(key)
            .await
            .map_err(|e| AppError::StorageError(format!("redis get error: {e}")))?;
        val.ok_or(AppError::FragmentNotFound)
    }
}

impl Storage for RedisStorage {
    async fn health_check(&self) -> Result<(), AppError> {
        self.ping().await
    }

    async fn auth(&self, token: &str, auth: &str) -> Result<(), AppError> {
        let mut conn = self.conn.clone();
        let key = format!("{token}:auth");
        let stored: Option<String> = conn
            .get(&key)
            .await
            .map_err(|e| AppError::StorageError(format!("redis get error: {e}")))?;

        match stored {
            Some(ref stored_key) if stored_key != auth => Err(AppError::InvalidAuth),
            _ => Ok(()),
        }
    }

    async fn start(&self, token: &str, fragment: i32, frame: StartFrame) -> Result<(), AppError> {
        let mut conn = self.conn.clone();

        // Store start frame body
        let body_key = format!("{token}:{fragment}:start");
        Self::set_with_ttl(&mut conn, &body_key, &frame.body, self.ttl).await?;

        // Store match metadata as JSON
        let meta = serde_json::json!({
            "tps": frame.tps,
            "protocol": frame.protocol,
            "map_name": frame.map_name,
            "signup_fragment": fragment,
        });
        let meta_key = format!("{token}:meta");
        let meta_bytes = serde_json::to_vec(&meta)
            .map_err(|e| AppError::StorageError(format!("json serialize error: {e}")))?;
        Self::set_with_ttl(&mut conn, &meta_key, &meta_bytes, self.ttl).await?;

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

        // Check match exists
        let meta_key = format!("{token}:meta");
        let exists: bool = conn
            .exists(&meta_key)
            .await
            .map_err(|e| AppError::StorageError(format!("redis exists error: {e}")))?;
        if !exists {
            return Err(AppError::MatchNotFound);
        }

        // Store full snapshot
        let body_key = format!("{token}:{fragment}:full");
        Self::set_with_ttl(&mut conn, &body_key, &body, self.ttl).await?;

        // Store fragment tick metadata
        let tick_key = format!("{token}:{fragment}:tick");
        let tick_bytes = tick.to_le_bytes();
        Self::set_with_ttl(&mut conn, &tick_key, &tick_bytes, self.ttl).await?;

        // Update latest fragment number
        let latest_key = format!("{token}:latest");
        Self::set_with_ttl(&mut conn, &latest_key, &fragment.to_le_bytes(), self.ttl).await?;

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

        // Check match exists
        let meta_key = format!("{token}:meta");
        let exists: bool = conn
            .exists(&meta_key)
            .await
            .map_err(|e| AppError::StorageError(format!("redis exists error: {e}")))?;
        if !exists {
            return Err(AppError::MatchNotFound);
        }

        // Store delta
        let body_key = format!("{token}:{fragment}:delta");
        Self::set_with_ttl(&mut conn, &body_key, &body, self.ttl).await?;

        // Store end_tick and final_fragment metadata
        let end_tick_key = format!("{token}:{fragment}:endtick");
        Self::set_with_ttl(&mut conn, &end_tick_key, &end_tick.to_le_bytes(), self.ttl).await?;

        let final_key = format!("{token}:{fragment}:final");
        let final_byte: u8 = u8::from(is_final);
        Self::set_with_ttl(&mut conn, &final_key, &[final_byte], self.ttl).await?;

        Ok(())
    }

    async fn get_sync(&self, token: &str, fragment: Option<i32>) -> Result<SyncData, AppError> {
        let mut conn = self.conn.clone();

        // Load match metadata
        let meta_key = format!("{token}:meta");
        let meta_bytes: Option<Vec<u8>> = conn
            .get(&meta_key)
            .await
            .map_err(|e| AppError::StorageError(format!("redis get error: {e}")))?;
        let meta_bytes = meta_bytes.ok_or(AppError::MatchNotFound)?;
        let meta: serde_json::Value = serde_json::from_slice(&meta_bytes)
            .map_err(|e| AppError::StorageError(format!("json deserialize error: {e}")))?;

        let frag_num = if let Some(f) = fragment {
            f
        } else {
            // Get latest fragment
            let latest_key = format!("{token}:latest");
            let latest_bytes: Option<Vec<u8>> = conn
                .get(&latest_key)
                .await
                .map_err(|e| AppError::StorageError(format!("redis get error: {e}")))?;
            let latest_bytes = latest_bytes.ok_or(AppError::FragmentNotFound)?;
            let latest = i32::from_le_bytes(
                latest_bytes
                    .try_into()
                    .map_err(|_| AppError::StorageError("invalid latest bytes".to_owned()))?,
            );
            latest - self.delay
        };

        // Load fragment metadata
        let tick_key = format!("{token}:{frag_num}:tick");
        let tick_bytes: Option<Vec<u8>> = conn
            .get(&tick_key)
            .await
            .map_err(|e| AppError::StorageError(format!("redis get error: {e}")))?;
        let tick_bytes = tick_bytes.ok_or(AppError::FragmentNotFound)?;
        let tick = i32::from_le_bytes(
            tick_bytes
                .try_into()
                .map_err(|_| AppError::StorageError("invalid tick bytes".to_owned()))?,
        );

        let end_tick_key = format!("{token}:{frag_num}:endtick");
        let end_tick_bytes: Option<Vec<u8>> = conn
            .get(&end_tick_key)
            .await
            .map_err(|e| AppError::StorageError(format!("redis get error: {e}")))?;
        let end_tick = match end_tick_bytes {
            Some(b) => i32::from_le_bytes(
                b.try_into()
                    .map_err(|_| AppError::StorageError("invalid endtick bytes".to_owned()))?,
            ),
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
        let mut conn = self.conn.clone();

        // Check match exists
        let meta_key = format!("{token}:meta");
        let exists: bool = conn
            .exists(&meta_key)
            .await
            .map_err(|e| AppError::StorageError(format!("redis exists error: {e}")))?;
        if !exists {
            return Err(AppError::MatchNotFound);
        }

        let key = format!("{token}:{fragment}:start");
        Self::get_bytes(&mut conn, &key).await
    }

    async fn get_full(&self, token: &str, fragment: i32) -> Result<Vec<u8>, AppError> {
        let mut conn = self.conn.clone();

        let meta_key = format!("{token}:meta");
        let exists: bool = conn
            .exists(&meta_key)
            .await
            .map_err(|e| AppError::StorageError(format!("redis exists error: {e}")))?;
        if !exists {
            return Err(AppError::MatchNotFound);
        }

        let key = format!("{token}:{fragment}:full");
        Self::get_bytes(&mut conn, &key).await
    }

    async fn get_delta(&self, token: &str, fragment: i32) -> Result<Vec<u8>, AppError> {
        let mut conn = self.conn.clone();

        let meta_key = format!("{token}:meta");
        let exists: bool = conn
            .exists(&meta_key)
            .await
            .map_err(|e| AppError::StorageError(format!("redis exists error: {e}")))?;
        if !exists {
            return Err(AppError::MatchNotFound);
        }

        let key = format!("{token}:{fragment}:delta");
        Self::get_bytes(&mut conn, &key).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore]
    async fn test_redis_start_and_get_start() {
        let storage = RedisStorage::new("redis://127.0.0.1:6379")
            .await
            .expect("redis connection failed");
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
    #[ignore]
    async fn test_redis_full_requires_match() {
        let storage = RedisStorage::new("redis://127.0.0.1:6379")
            .await
            .expect("redis connection failed");
        let result = storage
            .full("nonexistent_redis_test", 1, 100, vec![5, 6])
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    #[ignore]
    async fn test_redis_full_and_get_full() {
        let storage = RedisStorage::new("redis://127.0.0.1:6379")
            .await
            .expect("redis connection failed");
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
    #[ignore]
    async fn test_redis_delta_and_get_delta() {
        let storage = RedisStorage::new("redis://127.0.0.1:6379")
            .await
            .expect("redis connection failed");
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
    #[ignore]
    async fn test_redis_get_sync() {
        let storage = RedisStorage::new("redis://127.0.0.1:6379")
            .await
            .expect("redis connection failed");
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
    #[ignore]
    async fn test_redis_ping() {
        let storage = RedisStorage::new("redis://127.0.0.1:6379")
            .await
            .expect("redis connection failed");
        storage.ping().await.expect("ping failed");
    }
}
