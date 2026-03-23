use std::collections::HashMap;
use std::time::Instant;

use tokio::sync::RwLock;

use crate::error::AppError;
use crate::models::{Fragment, StartFrame, SyncData};
use crate::storage::Storage;

/// Default fragment delay for sync endpoint (matches Go reference implementation).
const DEFAULT_DELAY: i32 = 8;

/// Maximum number of fragments to retain per match to prevent unbounded memory growth.
/// Keeps ~1200 fragments (~1 hour of data). Fragment 0 and start frames are always retained.
const MAX_RETAINED_FRAGMENTS: i32 = 1200;

/// Per-match state stored in memory.
struct Match {
    receive_age: Instant,
    latest: i32,
    signup_fragment: i32,
    tps: f64,
    protocol: i32,
    map_name: String,
    start_frames: HashMap<i32, StartFrame>,
    fragments: HashMap<i32, Fragment>,
    #[allow(dead_code)]
    auth_key: Option<String>,
}

impl Match {
    fn new() -> Self {
        Self {
            receive_age: Instant::now(),
            latest: 0,
            signup_fragment: 0,
            tps: 0.0,
            protocol: 0,
            map_name: String::new(),
            start_frames: HashMap::new(),
            fragments: HashMap::new(),
            auth_key: None,
        }
    }
}

/// In-memory storage backend using `tokio::sync::RwLock`.
pub(crate) struct MemoryStorage {
    matches: RwLock<HashMap<String, Match>>,
    delay: i32,
}

impl MemoryStorage {
    pub(crate) fn new() -> Self {
        Self {
            matches: RwLock::new(HashMap::new()),
            delay: DEFAULT_DELAY,
        }
    }

    /// Clean up old fragments beyond the retention window.
    fn cleanup_old_fragments(m: &mut Match) {
        if m.latest <= MAX_RETAINED_FRAGMENTS {
            return;
        }
        let cutoff = m.latest - MAX_RETAINED_FRAGMENTS;
        m.fragments.retain(|&frag, _| frag >= cutoff);
    }
}

impl Storage for MemoryStorage {
    async fn auth(&self, token: &str, auth: &str) -> Result<(), AppError> {
        let matches = self.matches.read().await;
        if let Some(m) = matches.get(token)
            && let Some(ref stored_key) = m.auth_key
        {
            if stored_key != auth {
                return Err(AppError::InvalidAuth);
            }
            return Ok(());
        }
        // No auth stored for this token yet — accept (auth will be set on first ingest)
        Ok(())
    }

    async fn start(&self, token: &str, fragment: i32, frame: StartFrame) -> Result<(), AppError> {
        let mut matches = self.matches.write().await;
        let m = matches.entry(token.to_owned()).or_insert_with(Match::new);

        m.signup_fragment = fragment;
        m.tps = frame.tps;
        m.protocol = frame.protocol;
        m.map_name.clone_from(&frame.map_name);
        m.start_frames.insert(fragment, frame);
        Ok(())
    }

    async fn full(
        &self,
        token: &str,
        fragment: i32,
        tick: i32,
        body: Vec<u8>,
    ) -> Result<(), AppError> {
        let mut matches = self.matches.write().await;
        let m = matches.get_mut(token).ok_or(AppError::MatchNotFound)?;

        let frag = m.fragments.entry(fragment).or_insert_with(|| Fragment {
            tick: 0,
            final_fragment: false,
            end_tick: 0,
            full: None,
            delta: None,
        });
        frag.tick = tick;
        frag.full = Some(body);

        m.latest = fragment;
        m.receive_age = Instant::now();

        Self::cleanup_old_fragments(m);
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
        let mut matches = self.matches.write().await;
        let m = matches.get_mut(token).ok_or(AppError::MatchNotFound)?;

        let frag = m.fragments.entry(fragment).or_insert_with(|| Fragment {
            tick: 0,
            final_fragment: false,
            end_tick: 0,
            full: None,
            delta: None,
        });
        frag.end_tick = end_tick;
        frag.final_fragment = is_final;
        frag.delta = Some(body);

        Ok(())
    }

    async fn get_sync(&self, token: &str, fragment: Option<i32>) -> Result<SyncData, AppError> {
        let matches = self.matches.read().await;
        let m = matches.get(token).ok_or(AppError::MatchNotFound)?;

        let frag_num = match fragment {
            Some(f) => f,
            None => m.latest - self.delay,
        };

        let frag = m
            .fragments
            .get(&frag_num)
            .ok_or(AppError::FragmentNotFound)?;

        Ok(SyncData {
            tick: frag.tick,
            endtick: frag.end_tick,
            rtdelay: 0.0,
            rcvage: m.receive_age.elapsed().as_secs_f64(),
            fragment: frag_num,
            signup_fragment: m.signup_fragment,
            tps: m.tps,
            keyframe_interval: 3.0,
            map: m.map_name.clone(),
            protocol: m.protocol,
        })
    }

    async fn get_start(&self, token: &str, fragment: i32) -> Result<Vec<u8>, AppError> {
        let matches = self.matches.read().await;
        let m = matches.get(token).ok_or(AppError::MatchNotFound)?;
        let start = m
            .start_frames
            .get(&fragment)
            .ok_or(AppError::FragmentNotFound)?;
        Ok(start.body.clone())
    }

    async fn get_full(&self, token: &str, fragment: i32) -> Result<Vec<u8>, AppError> {
        let matches = self.matches.read().await;
        let m = matches.get(token).ok_or(AppError::MatchNotFound)?;
        let frag = m
            .fragments
            .get(&fragment)
            .ok_or(AppError::FragmentNotFound)?;
        frag.full.clone().ok_or(AppError::FragmentNotFound)
    }

    async fn get_delta(&self, token: &str, fragment: i32) -> Result<Vec<u8>, AppError> {
        let matches = self.matches.read().await;
        let m = matches.get(token).ok_or(AppError::MatchNotFound)?;
        let frag = m
            .fragments
            .get(&fragment)
            .ok_or(AppError::FragmentNotFound)?;
        frag.delta.clone().ok_or(AppError::FragmentNotFound)
    }
}

#[cfg(test)]
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
mod tests {
    use std::sync::Arc;

    use super::*;

    fn test_start_frame() -> StartFrame {
        StartFrame {
            tps: 128.0,
            protocol: 4,
            map_name: "de_dust2".to_owned(),
            body: vec![1, 2, 3, 4],
        }
    }

    #[tokio::test]
    async fn test_start_and_get_start() {
        let storage = MemoryStorage::new();
        let token = "s12345t6789";

        storage.start(token, 0, test_start_frame()).await.unwrap();

        let body = storage.get_start(token, 0).await.unwrap();
        assert_eq!(body, vec![1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn test_full_requires_match() {
        let storage = MemoryStorage::new();
        let result = storage.full("nonexistent", 1, 100, vec![5, 6]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_full_and_get_full() {
        let storage = MemoryStorage::new();
        let token = "s12345t6789";

        storage.start(token, 0, test_start_frame()).await.unwrap();
        storage.full(token, 1, 100, vec![10, 20, 30]).await.unwrap();

        let body = storage.get_full(token, 1).await.unwrap();
        assert_eq!(body, vec![10, 20, 30]);
    }

    #[tokio::test]
    async fn test_delta_and_get_delta() {
        let storage = MemoryStorage::new();
        let token = "s12345t6789";

        storage.start(token, 0, test_start_frame()).await.unwrap();
        storage
            .delta(token, 1, 200, false, vec![40, 50, 60])
            .await
            .unwrap();

        let body = storage.get_delta(token, 1).await.unwrap();
        assert_eq!(body, vec![40, 50, 60]);
    }

    #[tokio::test]
    async fn test_get_sync_latest_with_delay() {
        let storage = MemoryStorage::new();
        let token = "s12345t6789";

        storage.start(token, 0, test_start_frame()).await.unwrap();

        // Ingest fragments 0 through 10
        for i in 0..=10 {
            storage
                .full(token, i, i * 100, vec![i as u8])
                .await
                .unwrap();
            storage
                .delta(token, i, i * 100 + 50, i == 10, vec![i as u8 + 100])
                .await
                .unwrap();
        }

        // Latest is 10, delay is 8, so sync should return fragment 2
        let sync = storage.get_sync(token, None).await.unwrap();
        assert_eq!(sync.fragment, 2);
        assert_eq!(sync.tick, 200);
        assert_eq!(sync.map, "de_dust2");
        assert_eq!(sync.protocol, 4);
    }

    #[tokio::test]
    async fn test_get_sync_specific_fragment() {
        let storage = MemoryStorage::new();
        let token = "s12345t6789";

        storage.start(token, 0, test_start_frame()).await.unwrap();
        storage.full(token, 5, 500, vec![55]).await.unwrap();
        storage.delta(token, 5, 550, false, vec![56]).await.unwrap();

        let sync = storage.get_sync(token, Some(5)).await.unwrap();
        assert_eq!(sync.fragment, 5);
        assert_eq!(sync.tick, 500);
        assert_eq!(sync.endtick, 550);
    }

    #[tokio::test]
    async fn test_get_sync_unknown_token() {
        let storage = MemoryStorage::new();
        let result = storage.get_sync("nonexistent", None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_sync_fragment_not_ready() {
        let storage = MemoryStorage::new();
        let token = "s12345t6789";

        storage.start(token, 0, test_start_frame()).await.unwrap();
        // Only fragment 0 ingested, latest=0, so latest-delay = -8 won't exist
        storage.full(token, 0, 0, vec![1]).await.unwrap();

        let result = storage.get_sync(token, None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_fragment_not_found_for_missing() {
        let storage = MemoryStorage::new();
        let token = "s12345t6789";
        storage.start(token, 0, test_start_frame()).await.unwrap();

        assert!(storage.get_full(token, 99).await.is_err());
        assert!(storage.get_delta(token, 99).await.is_err());
        assert!(storage.get_start(token, 99).await.is_err());
    }

    #[tokio::test]
    async fn test_match_not_found() {
        let storage = MemoryStorage::new();

        assert!(storage.get_start("nope", 0).await.is_err());
        assert!(storage.get_full("nope", 0).await.is_err());
        assert!(storage.get_delta("nope", 0).await.is_err());
    }

    #[tokio::test]
    async fn test_concurrent_read_write() {
        let storage = Arc::new(MemoryStorage::new());
        let token = "s12345t6789";

        // Start the match first
        storage.start(token, 0, test_start_frame()).await.unwrap();

        let storage_writer = Arc::clone(&storage);
        let storage_reader = Arc::clone(&storage);

        let writer = tokio::spawn(async move {
            for i in 1..=20 {
                storage_writer
                    .full(token, i, i * 100, vec![i as u8])
                    .await
                    .unwrap();
                storage_writer
                    .delta(token, i, i * 100 + 50, false, vec![i as u8 + 100])
                    .await
                    .unwrap();
            }
        });

        let reader = tokio::spawn(async move {
            // Read operations should never panic, even during concurrent writes
            for _ in 0..50 {
                let _ = storage_reader.get_sync(token, Some(0)).await;
                let _ = storage_reader.get_full(token, 0).await;
                let _ = storage_reader.get_start(token, 0).await;
                tokio::task::yield_now().await;
            }
        });

        writer.await.unwrap();
        reader.await.unwrap();

        // Verify final state
        let body = storage.get_full(token, 20).await.unwrap();
        assert_eq!(body, vec![20]);
    }

    #[tokio::test]
    async fn test_cleanup_old_fragments() {
        let storage = MemoryStorage::new();
        let token = "s12345t6789";

        storage.start(token, 0, test_start_frame()).await.unwrap();

        // Ingest enough fragments to trigger cleanup
        for i in 0..=(MAX_RETAINED_FRAGMENTS + 100) {
            storage.full(token, i, i * 10, vec![1]).await.unwrap();
        }

        // Old fragments should be cleaned up
        let result = storage.get_full(token, 0).await;
        assert!(result.is_err(), "fragment 0 should have been cleaned up");

        // Recent fragments should still exist
        let latest = MAX_RETAINED_FRAGMENTS + 100;
        let result = storage.get_full(token, latest).await;
        assert!(result.is_ok(), "latest fragment should still exist");
    }
}
