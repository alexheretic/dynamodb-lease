use std::{
    collections::{HashMap, hash_map::Entry},
    sync::Arc,
};
use tokio::sync::{Mutex, OwnedMutexGuard, TryLockError};

/// Map of local key locks.
/// These can eliminate db contention for leases acquired by the same client,
/// ie within the same process.
#[derive(Debug, Clone, Default)]
pub(crate) struct LocalLocks(Arc<std::sync::Mutex<HashMap<String, Arc<Mutex<()>>>>>);

impl LocalLocks {
    pub(crate) fn try_lock(&self, key: String) -> Result<OwnedMutexGuard<()>, TryLockError> {
        self.key_mutex(key).try_lock_owned()
    }

    pub(crate) async fn lock(&self, key: String) -> OwnedMutexGuard<()> {
        self.key_mutex(key).lock_owned().await
    }

    fn key_mutex(&self, key: String) -> Arc<Mutex<()>> {
        let mut locks = self.0.lock().unwrap();
        locks.entry(key).or_default().clone()
    }

    pub(crate) fn try_remove(&self, key: String) {
        let mut locks = self.0.lock().unwrap();
        if let Entry::Occupied(entry) = locks.entry(key) {
            if Arc::strong_count(entry.get()) == 1 {
                entry.remove();
            }
        }
    }
}
