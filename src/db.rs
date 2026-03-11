use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use tokio::sync::{Notify, broadcast};
use tokio::time::{self, Duration, Instant};
use tracing::debug;

use crate::Frame;
use crate::aof;

/// A wrapper around a `Db` instance. This exists to allow orderly cleanup
/// of the `Db` by signalling the background purge task to shut down when
/// this struct is dropped.
#[derive(Debug)]
pub(crate) struct DbDropGuard {
    /// The `Db` instance that will be shut down when this `DbDropGuard` struct
    /// is dropped.
    db: Db,
}

/// Server state shared across all connections.
///
/// `Db` contains a `HashMap` storing the key/value data and all
/// `broadcast::Sender` values for active pub/sub channels.
///
/// A `Db` instance is a handle to shared state. Cloning `Db` is shallow and
/// only incurs an atomic ref count increment.
///
/// When a `Db` value is created, a background task is spawned. This task is
/// used to expire values after the requested duration has elapsed. The task
/// runs until all instances of `Db` are dropped, at which point the task
/// terminates.
#[derive(Debug, Clone)]
pub(crate) struct Db {
    /// Handle to shared state. The background task will also have an
    /// `Arc<Shared>`.
    shared: Arc<Shared>,
}

#[derive(Debug)]
struct Shared {
    /// The shared state is guarded by a mutex. This is a `std::sync::Mutex` and
    /// not a Tokio mutex. This is because there are no asynchronous operations
    /// being performed while holding the mutex. Additionally, the critical
    /// sections are very small.
    ///
    /// A Tokio mutex is mostly intended to be used when locks need to be held
    /// across `.await` yield points. All other cases are **usually** best
    /// served by a std mutex. If the critical section does not include any
    /// async operations but is long (CPU intensive or performing blocking
    /// operations), then the entire operation, including waiting for the mutex,
    /// is considered a "blocking" operation and `tokio::task::spawn_blocking`
    /// should be used.
    state: Mutex<State>,

    /// Notifies the background task handling entry expiration. The background
    /// task waits on this to be notified, then checks for expired values or the
    /// shutdown signal.
    background_task: Notify,
    change_tx: broadcast::Sender<Bytes>,
    next_offset: AtomicU64,
    repl_applied_offset: AtomicI64,
}

#[derive(Debug)]
struct State {
    /// The key-value data. We are not trying to do anything fancy so a
    /// `std::collections::HashMap` works fine.
    entries: HashMap<String, Entry>,

    /// The pub/sub key-space. Redis uses a **separate** key space for key-value
    /// and pub/sub. `db-rs` handles this by using a separate `HashMap`.
    pub_sub: HashMap<String, broadcast::Sender<Bytes>>,

    /// Tracks key TTLs.
    ///
    /// A `BTreeSet` is used to maintain expirations sorted by when they expire.
    /// This allows the background task to iterate this map to find the value
    /// expiring next.
    ///
    /// While highly unlikely, it is possible for more than one expiration to be
    /// created for the same instant. Because of this, the `Instant` is
    /// insufficient for the key. A unique key (`String`) is used to
    /// break these ties.
    expirations: BTreeSet<(Instant, String)>,

    /// True when the Db instance is shutting down. This happens when all `Db`
    /// values drop. Setting this to `true` signals to the background task to
    /// exit.
    shutdown: bool,
}

/// Entry in the key-value store
#[derive(Debug)]
struct Entry {
    /// Stored data
    data: Bytes,

    /// Instant at which the entry expires and should be removed from the
    /// database.
    expires_at: Option<Instant>,
}

#[derive(Debug)]
pub(crate) struct SnapshotEntry {
    pub(crate) key: String,
    pub(crate) value: Bytes,
    pub(crate) expire_in: Option<Duration>,
}

impl DbDropGuard {
    /// Create a new `DbDropGuard`, wrapping a `Db` instance. When this is
    /// dropped the `Db`'s purge task will be shut down.
    pub(crate) fn new() -> DbDropGuard {
        DbDropGuard { db: Db::new() }
    }

    /// Get the shared database. Internally, this is an
    /// `Arc`, so a clone only increments the ref count.
    pub(crate) fn db(&self) -> Db {
        self.db.clone()
    }
}

impl Drop for DbDropGuard {
    fn drop(&mut self) {
        // Signal the 'Db' instance to shut down the task that purges expired keys
        self.db.shutdown_purge_task();
    }
}

impl Db {
    /// Create a new, empty, `Db` instance. Allocates shared state and spawns a
    /// background task to manage key expiration.
    pub(crate) fn new() -> Db {
        let (change_tx, _) = broadcast::channel(1024);
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                entries: HashMap::new(),
                pub_sub: HashMap::new(),
                expirations: BTreeSet::new(),
                shutdown: false,
            }),
            background_task: Notify::new(),
            change_tx,
            next_offset: AtomicU64::new(0),
            repl_applied_offset: AtomicI64::new(-1),
        });

        // Start the background task.
        tokio::spawn(purge_expired_tasks(shared.clone()));

        Db { shared }
    }

    pub(crate) fn subscribe_changes(&self) -> broadcast::Receiver<Bytes> {
        self.shared.change_tx.subscribe()
    }

    pub(crate) fn current_offset(&self) -> i64 {
        let n = self.shared.next_offset.load(Ordering::SeqCst);
        let leader = n.saturating_sub(1) as i64;
        let repl = self.shared.repl_applied_offset.load(Ordering::SeqCst);
        leader.max(repl)
    }

    pub(crate) fn set_applied_offset(&self, offset: i64) {
        self.shared
            .repl_applied_offset
            .store(offset, Ordering::SeqCst);
    }

    /// Get the value associated with a key.
    ///
    /// Returns `None` if there is no value associated with the key. This may be
    /// due to never having assigned a value to the key or a previously assigned
    /// value expired.
    pub(crate) fn get(&self, key: &str) -> Option<Bytes> {
        // Acquire the lock, get the entry and clone the value.
        //
        // Because data is stored using `Bytes`, a clone here is a shallow
        // clone. Data is not copied.
        let state = self.shared.state.lock().unwrap();
        state.entries.get(key).map(|entry| entry.data.clone())
    }

    /// Delete one or more keys and return the number of deleted keys.
    pub(crate) fn del_many(&self, keys: &[String]) -> usize {
        let mut state = self.shared.state.lock().unwrap();
        let mut removed_keys = Vec::new();

        for key in keys {
            if let Some(prev) = state.entries.remove(key) {
                removed_keys.push(key.clone());

                if let Some(when) = prev.expires_at {
                    state.expirations.remove(&(when, key.clone()));
                }
            }
        }

        drop(state);
        for key in &removed_keys {
            self.shared.emit_change("del", key, None);
        }

        removed_keys.len()
    }

    /// Return the number of keys that currently exist.
    pub(crate) fn exists_many(&self, keys: &[String]) -> usize {
        let state = self.shared.state.lock().unwrap();
        keys.iter()
            .filter(|key| state.entries.contains_key(*key))
            .count()
    }

    /// Set a timeout on key.
    ///
    /// Returns `true` if the timeout was set, `false` if the key does not
    /// exist.
    pub(crate) fn expire(&self, key: &str, duration: Duration) -> bool {
        let mut state = self.shared.state.lock().unwrap();
        let key = key.to_string();
        let now = Instant::now();

        let prev_expires_at = match state.entries.get(&key) {
            Some(entry) => {
                if let Some(when) = entry.expires_at
                    && when <= now
                {
                    state.entries.remove(&key);
                    state.expirations.remove(&(when, key));
                    return false;
                }

                entry.expires_at
            }
            None => return false,
        };

        let when = now + duration;
        let notify = state
            .next_expiration()
            .map(|expiration| expiration > when)
            .unwrap_or(true);

        if let Some(prev) = prev_expires_at {
            state.expirations.remove(&(prev, key.clone()));
        }

        if let Some(entry) = state.entries.get_mut(&key) {
            entry.expires_at = Some(when);
        }

        state.expirations.insert((when, key.clone()));

        drop(state);
        self.shared.emit_change(
            "expire",
            &key,
            Some(Bytes::from(duration.as_millis().to_string())),
        );
        if notify {
            self.shared.background_task.notify_one();
        }

        true
    }

    /// Return the remaining time to live of a key, in seconds.
    pub(crate) fn ttl(&self, key: &str) -> i64 {
        self.remaining_ttl(key, false)
    }

    /// Return the remaining time to live of a key, in milliseconds.
    pub(crate) fn pttl(&self, key: &str) -> i64 {
        self.remaining_ttl(key, true)
    }

    fn remaining_ttl(&self, key: &str, in_millis: bool) -> i64 {
        let mut state = self.shared.state.lock().unwrap();
        let key = key.to_string();
        let now = Instant::now();

        let expires_at = match state.entries.get(&key) {
            Some(entry) => entry.expires_at,
            None => return -2,
        };

        let Some(expires_at) = expires_at else {
            return -1;
        };

        if expires_at <= now {
            state.entries.remove(&key);
            state.expirations.remove(&(expires_at, key));
            return -2;
        }

        let remaining = expires_at.duration_since(now);
        if in_millis {
            remaining.as_millis().try_into().unwrap_or(i64::MAX)
        } else {
            remaining.as_secs().try_into().unwrap_or(i64::MAX)
        }
    }

    /// Set the value associated with a key along with an optional expiration
    /// Duration.
    ///
    /// If a value is already associated with the key, it is removed.
    pub(crate) fn set(&self, key: String, value: Bytes, expire: Option<Duration>) {
        let mut state = self.shared.state.lock().unwrap();

        // If this `set` becomes the key that expires **next**, the background
        // task needs to be notified so it can update its state.
        //
        // Whether or not the task needs to be notified is computed during the
        // `set` routine.
        let mut notify = false;

        let expires_at = expire.map(|duration| {
            // `Instant` at which the key expires.
            let when = Instant::now() + duration;

            // Only notify the worker task if the newly inserted expiration is the
            // **next** key to evict. In this case, the worker needs to be woken up
            // to update its state.
            notify = state
                .next_expiration()
                .map(|expiration| expiration > when)
                .unwrap_or(true);

            when
        });

        // Insert the entry into the `HashMap`.
        let prev = state.entries.insert(
            key.clone(),
            Entry {
                data: value.clone(),
                expires_at,
            },
        );

        // If there was a value previously associated with the key **and** it
        // had an expiration time. The associated entry in the `expirations` map
        // must also be removed. This avoids leaking data.
        if let Some(prev) = prev
            && let Some(when) = prev.expires_at
        {
            // clear expiration
            state.expirations.remove(&(when, key.clone()));
        }

        // Track the expiration. If we insert before remove that will cause bug
        // when current `(when, key)` equals prev `(when, key)`. Remove then insert
        // can avoid this.
        if let Some(when) = expires_at {
            state.expirations.insert((when, key.clone()));
        }

        // Release the mutex before notifying the background task. This helps
        // reduce contention by avoiding the background task waking up only to
        // be unable to acquire the mutex due to this function still holding it.
        drop(state);
        self.shared.emit_change("set", &key, Some(value));
        if notify {
            // Finally, only notify the background task if it needs to update
            // its state to reflect a new expiration.
            self.shared.background_task.notify_one();
        }
    }

    /// Compare-and-set: set key to value only if current value equals expected.
    /// Returns true if set, false on mismatch.
    pub(crate) fn cas(&self, key: &str, expected: Option<&[u8]>, value: Bytes) -> bool {
        let mut state = self.shared.state.lock().unwrap();
        let key = key.to_string();
        let current = state.entries.get(&key).map(|e| e.data.as_ref());
        if current != expected {
            return false;
        }
        let prev = state.entries.insert(
            key.clone(),
            Entry {
                data: value.clone(),
                expires_at: None,
            },
        );
        if let Some(prev) = prev
            && let Some(when) = prev.expires_at
        {
            state.expirations.remove(&(when, key.clone()));
        }
        drop(state);
        self.shared.emit_change("set", &key, Some(value));
        true
    }

    /// Apply a change from replication without emitting to the change stream.
    pub(crate) fn apply_change_quiet(
        &self,
        op: &str,
        key: &str,
        value: Option<Bytes>,
        offset: i64,
    ) {
        self.set_applied_offset(offset);
        use std::str;
        let mut state = self.shared.state.lock().unwrap();
        let key = key.to_string();
        let now = Instant::now();

        match op {
            "set" => {
                if let Some(v) = value {
                    let prev = state.entries.insert(
                        key.clone(),
                        Entry {
                            data: v,
                            expires_at: None,
                        },
                    );
                    if let Some(prev) = prev
                        && let Some(when) = prev.expires_at
                    {
                        state.expirations.remove(&(when, key));
                    }
                }
            }
            "del" => {
                if let Some(prev) = state.entries.remove(&key)
                    && let Some(when) = prev.expires_at
                {
                    state.expirations.remove(&(when, key));
                }
            }
            "expire" => {
                let Some(ref v) = value else { return };
                let ms: u64 = str::from_utf8(v)
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
                if ms == 0 {
                    return;
                }
                let prev_expires_at = match state.entries.get(&key) {
                    Some(entry) => {
                        if let Some(when) = entry.expires_at
                            && when <= now
                        {
                            state.entries.remove(&key);
                            state.expirations.remove(&(when, key));
                            return;
                        }
                        entry.expires_at
                    }
                    None => return,
                };
                let duration = Duration::from_millis(ms);
                let when = now + duration;
                let notify = state
                    .next_expiration()
                    .map(|expiration| expiration > when)
                    .unwrap_or(true);
                if let Some(prev) = prev_expires_at {
                    state.expirations.remove(&(prev, key.clone()));
                }
                if let Some(entry) = state.entries.get_mut(&key) {
                    entry.expires_at = Some(when);
                }
                state.expirations.insert((when, key));
                drop(state);
                if notify {
                    self.shared.background_task.notify_one();
                }
            }
            _ => {}
        }
    }

    pub(crate) fn snapshot_entries(&self) -> Vec<SnapshotEntry> {
        let mut state = self.shared.state.lock().unwrap();
        let now = Instant::now();

        let mut entries = Vec::with_capacity(state.entries.len());
        let mut expired = Vec::new();

        for (key, entry) in &state.entries {
            match entry.expires_at {
                Some(when) if when <= now => expired.push((key.clone(), when)),
                Some(when) => entries.push(SnapshotEntry {
                    key: key.clone(),
                    value: entry.data.clone(),
                    expire_in: Some(when.duration_since(now)),
                }),
                None => entries.push(SnapshotEntry {
                    key: key.clone(),
                    value: entry.data.clone(),
                    expire_in: None,
                }),
            }
        }

        for (key, when) in expired {
            state.entries.remove(&key);
            state.expirations.remove(&(when, key));
        }

        entries
    }

    /// Returns a `Receiver` for the requested channel.
    ///
    /// The returned `Receiver` is used to receive values broadcast by `PUBLISH`
    /// commands.
    pub(crate) fn subscribe(&self, key: String) -> broadcast::Receiver<Bytes> {
        use std::collections::hash_map::Entry;

        // Acquire the mutex
        let mut state = self.shared.state.lock().unwrap();

        // If there is no entry for the requested channel, then create a new
        // broadcast channel and associate it with the key. If one already
        // exists, return an associated receiver.
        match state.pub_sub.entry(key) {
            Entry::Occupied(e) => e.get().subscribe(),
            Entry::Vacant(e) => {
                // No broadcast channel exists yet, so create one.
                //
                // The channel is created with a capacity of `1024` messages. A
                // message is stored in the channel until **all** subscribers
                // have seen it. This means that a slow subscriber could result
                // in messages being held indefinitely.
                //
                // When the channel's capacity fills up, publishing will result
                // in old messages being dropped. This prevents slow consumers
                // from blocking the entire system.
                let (tx, rx) = broadcast::channel(1024);
                e.insert(tx);
                rx
            }
        }
    }

    /// Publish a message to the channel. Returns the number of subscribers
    /// listening on the channel.
    pub(crate) fn publish(&self, key: &str, value: Bytes) -> usize {
        let state = self.shared.state.lock().unwrap();

        state
            .pub_sub
            .get(key)
            // On a successful message send on the broadcast channel, the number
            // of subscribers is returned. An error indicates there are no
            // receivers, in which case, `0` should be returned.
            .map(|tx| tx.send(value).unwrap_or(0))
            // If there is no entry for the channel key, then there are no
            // subscribers. In this case, return `0`.
            .unwrap_or(0)
    }

    /// Signals the purge background task to shut down. This is called by the
    /// `DbShutdown`s `Drop` implementation.
    fn shutdown_purge_task(&self) {
        // The background task must be signaled to shut down. This is done by
        // setting `State::shutdown` to `true` and signalling the task.
        let mut state = self.shared.state.lock().unwrap();
        state.shutdown = true;

        // Drop the lock before signalling the background task. This helps
        // reduce lock contention by ensuring the background task doesn't
        // wake up only to be unable to acquire the mutex.
        drop(state);
        self.shared.background_task.notify_one();
    }
}

impl Shared {
    fn emit_change(&self, op: &str, key: &str, value: Option<Bytes>) {
        let offset = self.next_offset.fetch_add(1, Ordering::SeqCst) as i64;
        let value_frame = value.map_or(Frame::Null, Frame::Bulk);
        let frame = Frame::Array(vec![
            Frame::Integer(offset),
            Frame::Simple(op.to_string()),
            Frame::Bulk(Bytes::from(key.to_string())),
            value_frame,
        ]);
        let mut encoded = Vec::new();
        if aof::encode_frame(&frame, &mut encoded).is_ok() {
            let _ = self.change_tx.send(Bytes::from(encoded));
        }
    }

    /// Purge all expired keys and return the `Instant` at which the **next**
    /// key will expire. The background task will sleep until this instant.
    fn purge_expired_keys(&self) -> Option<Instant> {
        let (expired_keys, next) = {
            let mut state = self.state.lock().unwrap();
            if state.shutdown {
                // The database is shutting down. All handles to the shared state
                // have dropped. The background task should exit.
                return None;
            }

            // This is needed to make the borrow checker happy. In short, `lock()`
            // returns a `MutexGuard` and not a `&mut State`. The borrow checker is
            // not able to see "through" the mutex guard and determine that it is
            // safe to access both `state.expirations` and `state.entries` mutably,
            // so we get a "real" mutable reference to `State` outside of the loop.
            let state = &mut *state;

            // Find all keys scheduled to expire **before** now.
            let now = Instant::now();
            let mut expired_keys: Vec<String> = Vec::new();
            let mut next_expiration = None;

            while let Some(&(when, ref key)) = state.expirations.iter().next() {
                if when > now {
                    // Done purging, `when` is the instant at which the next key expires.
                    // The worker task will wait until this instant.
                    next_expiration = Some(when);
                    break;
                }
                expired_keys.push(key.clone());
                // The key expired, remove it
                state.entries.remove(key);
                state.expirations.remove(&(when, key.clone()));
            }
            (expired_keys, next_expiration)
        };

        for key in expired_keys {
            self.emit_change("del", &key, None);
        }
        next
    }

    /// Returns `true` if the database is shutting down
    ///
    /// The `shutdown` flag is set when all `Db` values have dropped, indicating
    /// that the shared state can no longer be accessed.
    fn is_shutdown(&self) -> bool {
        self.state.lock().unwrap().shutdown
    }
}

impl State {
    fn next_expiration(&self) -> Option<Instant> {
        self.expirations
            .iter()
            .next()
            .map(|expiration| expiration.0)
    }
}

/// Routine executed by the background task.
///
/// Wait to be notified. On notification, purge any expired keys from the shared
/// state handle. If `shutdown` is set, terminate the task.
async fn purge_expired_tasks(shared: Arc<Shared>) {
    // If the shutdown flag is set, then the task should exit.
    while !shared.is_shutdown() {
        // Purge all keys that are expired. The function returns the instant at
        // which the **next** key will expire. The worker should wait until the
        // instant has passed then purge again.
        if let Some(when) = shared.purge_expired_keys() {
            // Wait until the next key expires **or** until the background task
            // is notified. If the task is notified, then it must reload its
            // state as new keys have been set to expire early. This is done by
            // looping.
            tokio::select! {
                _ = time::sleep_until(when) => {}
                _ = shared.background_task.notified() => {}
            }
        } else {
            // There are no keys expiring in the future. Wait until the task is
            // notified.
            shared.background_task.notified().await;
        }
    }

    debug!("Purge background task shut down")
}
