use crate::Client;
use std::sync::Arc;
use tokio::sync::{Mutex, OwnedMutexGuard};
use uuid::Uuid;

/// Represents a held distributed lease & background task to
/// continuously try to extend it until dropped.
///
/// On drop asynchronously releases the underlying lock.
#[derive(Debug)]
pub struct Lease {
    client: Client,
    /// Note: The extension tasks holds an **exclusive** weak references to
    /// this (that exclusivity is used to indicate that the task is still alive).
    key_lease_v: Arc<(String, Mutex<Uuid>)>,
    /// A local guard to avoid db contention for leases within the same client.
    local_guard: Option<OwnedMutexGuard<()>>,
    release_on_drop: bool,
}

impl Lease {
    pub(crate) fn new(client: Client, key: String, lease_v: Uuid) -> Self {
        let lease = Self {
            client,
            key_lease_v: Arc::new((key, Mutex::new(lease_v))),
            local_guard: None,
            release_on_drop: true,
        };

        start_periodically_extending(&lease);

        lease
    }

    pub(crate) fn with_local_guard(mut self, guard: OwnedMutexGuard<()>) -> Self {
        self.local_guard = Some(guard);
        self
    }

    /// Releases the lease returning `Ok(())` after successful deletion.
    ///
    /// Note: The local guard is unlocked **first** before deleting the lease.
    /// This avoids other concurrent acquires in the same process being unfairly
    /// advantaged in acquiring subsequent leases and potentially causing other
    /// processes to be starved.
    ///
    /// If you await this method then immediately acquire a lease,
    /// e.g. inside a loop, you are acquiring with an unfair advantage vs other process
    /// attempts. This may lead to other process being starved of leases.
    pub async fn release(mut self) -> anyhow::Result<()> {
        // disable release on drop since we're doing that now
        self.release_on_drop = false;

        let (key, lease_v) = &*self.key_lease_v;

        drop(self.local_guard.take());
        self.client.try_clean_local_lock(key.clone());

        let lease_v = lease_v.lock().await;
        self.client.delete_lease(key.clone(), *lease_v).await?;
        drop(lease_v); // hold v-lock during deletion to ensure no race with `extend_lease`
        Ok(())
    }

    /// Returns `true` if the lease periodic extension task is still running.
    ///
    /// If lease extension fails, e.g. due to lost contact with the db, this
    /// will return `false`.
    pub fn is_healthy(&self) -> bool {
        // The `start_periodically_extending` task holds an exclusive weak ref
        // to this field, so if the weak count is zero we know this task has died.
        Arc::weak_count(&self.key_lease_v) != 0
    }
}

fn start_periodically_extending(lease: &Lease) {
    let key_lease_v = Arc::downgrade(&lease.key_lease_v);
    let client = lease.client.clone();
    tokio::spawn(async move {
        let mut extend_interval = tokio::time::interval(client.extend_period);
        extend_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        extend_interval.reset();
        loop {
            extend_interval.tick().await;

            match key_lease_v.upgrade() {
                Some(key_lease_v) => {
                    let mut lease_v = key_lease_v.1.lock().await;
                    let key = key_lease_v.0.clone();
                    match client.extend_lease(key, *lease_v).await {
                        Ok(new_lease_v) => {
                            *lease_v = new_lease_v;
                        }
                        // stop on error, TODO retries, logs?
                        Err(_) => break,
                    }
                }
                // lease dropped
                None => break,
            }
        }
    });
}

impl Drop for Lease {
    /// Asynchronously releases the underlying lock.
    fn drop(&mut self) {
        if self.release_on_drop {
            // Clone necessary data before moving self into the spawned task
            let lease = Lease {
                client: self.client.clone(),
                key_lease_v: Arc::clone(&self.key_lease_v),
                local_guard: self.local_guard.take(), // Take ownership of the guard
                release_on_drop: false,
            };
            tokio::spawn(async move {
                // TODO retries, logs?
                _ = lease.release().await;
            });
        }
    }
}
