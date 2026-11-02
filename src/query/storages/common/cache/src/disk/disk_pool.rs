// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! One disk pool: an LRU index in memory over the files of one directory, the legacy disk
//! cache's design. The disk holds nothing but the entry files; a restart rebuilds the index by
//! scanning the directory.
//!
//! The pool lock guards the index and the byte counts only. File IO happens outside it: a writer
//! reserves room under the lock (taking victims out of the index), deletes the victims' files and
//! writes its own without the lock, then publishes the entry under the lock again.
//!
//! A file is never deleted while a new one may be written under its name, and never written
//! while it may be deleted: otherwise the delete could remove the new file and leave an indexed
//! entry without one. So an entry taken out of the index stays marked as being deleted until
//! `delete` has removed its file, a key reserved for writing stays marked until it is published
//! or released, and a key marked either way cannot be reserved; the restart scan leaves the
//! files of marked keys alone, and marks a file it rejects before deleting it.
//!
//! The pool also keeps the entries of each object, by object id, so `remove_object` can drop
//! all of them when the object is rewritten or deleted, and the points the objects were
//! invalidated at, so `publish` rejects an entry read before (see `fill_fence`). Until the
//! restart scan has run, every invalidation is kept, so the scan does not index the files of
//! an object invalidated before it got to them.
//!
//! `publish` checks the capacity again: it may have shrunk while the entry was written. A kind
//! turned off is refused by `reserve`, `publish` and the restart scan alike, so no entry of it
//! comes back from a write queued or a file scanned before.

use std::collections::HashMap;
use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use databend_common_cache::Cache;
use databend_common_cache::LruCache;
use log::info;
use log::warn;
use parking_lot::Mutex;
use rayon::prelude::*;

use crate::CacheKind;
use crate::CacheValue;
use crate::Pool;
use crate::disk::disk_file::TMP_SUFFIX;
use crate::disk::disk_file::is_own_tmp;
use crate::disk::key::object_of;

/// Entries larger than this share of the pool are not cached, as in the memory pools.
const OVERSIZED_DIVISOR: u64 = 4;

/// Bytes of one entry file, the unit the index accounts in.
struct FileSize(u64);

impl From<FileSize> for CacheValue<FileSize> {
    fn from(value: FileSize) -> Self {
        let bytes = value.0 as usize;
        CacheValue::new(value, bytes)
    }
}

/// The outcome of `DiskPool::reserve`. When `reserved`, the room is held until `publish` or
/// `release`. Either way, `victims` were taken out of the index and must be passed to
/// `DiskPool::delete`.
#[derive(Debug)]
#[must_use]
pub struct Reservation {
    pub reserved: bool,
    pub victims: Vec<String>,
}

/// The outcome of `DiskPool::publish`. `victims`, possibly the entry itself, were taken out of
/// the index and must be passed to `DiskPool::delete`.
#[derive(Debug)]
#[must_use]
pub struct Published {
    pub indexed: bool,
    pub victims: Vec<String>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DiskPoolStats {
    pub items: u64,
    pub bytes: u64,
    /// Bytes reserved by writes not yet published.
    pub writing: u64,
    pub cap: u64,
    pub hit: u64,
    pub miss: u64,
}

struct State {
    /// Keyed by the entry's path relative to the pool root, `<kind>/<prefix>/<hash>`. Its own
    /// capacity is unbounded: the pool enforces `cap`, so the LRU never drops an entry whose
    /// file would then stay on disk.
    index: LruCache<String, CacheValue<FileSize>>,
    cap: u64,
    /// Bytes reserved by writers and not yet published.
    writing: u64,
    /// Items and bytes per kind, by kind index, kept with the index.
    kinds: Vec<(u64, u64)>,
    /// Entries taken out whose files `delete` has not removed yet.
    deleting: HashSet<String>,
    /// Keys reserved by writers and not yet published or released.
    in_flight: HashSet<String>,
    /// The entries of each object, by object id.
    objects: HashMap<String, Vec<String>>,
    /// The last point each object was invalidated at, while fills started before it may still
    /// publish, or while the restart scan has not run.
    invalidated: HashMap<String, u64>,
    /// Whether the restart scan has run.
    reloaded: bool,
    /// Kinds turned off, by kind index.
    kinds_off: Vec<bool>,
}

impl State {
    fn kind_off(&self, rel: &str) -> bool {
        kind_of(rel).is_some_and(|kind| self.kinds_off[kind.index()])
    }
}

impl State {
    fn used(&self) -> u64 {
        self.index.bytes_size() + self.writing
    }

    fn insert(&mut self, rel: String, size: u64) {
        let kind = kind_of(&rel);
        match self.index.insert(rel.clone(), FileSize(size).into()) {
            Some(old) => self.account(kind, old.get_inner().0, false),
            None => {
                if let Some(object) = object_of(&rel) {
                    self.objects
                        .entry(object.to_string())
                        .or_default()
                        .push(rel);
                }
            }
        }
        self.account(kind, size, true);
    }

    /// Takes the least recently used entry out, marking it as being deleted.
    fn pop_lru(&mut self) -> Option<(String, u64)> {
        let (rel, size) = self.index.pop_by_policy()?;
        let size = size.get_inner().0;
        self.taken(&rel, size);
        Some((rel, size))
    }

    /// Takes `rel` out, marking it as being deleted.
    fn pop(&mut self, rel: &str) -> Option<u64> {
        let size = self.index.pop(rel)?.get_inner().0;
        self.taken(rel, size);
        Some(size)
    }

    /// Accounts an entry taken out of the index.
    fn taken(&mut self, rel: &str, size: u64) {
        self.account(kind_of(rel), size, false);
        self.deleting.insert(rel.to_string());
        if let Some(object) = object_of(rel) {
            if let Some(rels) = self.objects.get_mut(object) {
                rels.retain(|r| r != rel);
                if rels.is_empty() {
                    self.objects.remove(object);
                }
            }
        }
    }

    fn account(&mut self, kind: Option<CacheKind>, size: u64, add: bool) {
        let Some(kind) = kind else { return };
        let (items, bytes) = &mut self.kinds[kind.index()];
        if add {
            *items += 1;
            *bytes += size;
        } else {
            *items = items.saturating_sub(1);
            *bytes = bytes.saturating_sub(size);
        }
    }
}

/// The kind of an entry, from the first component of its index key.
fn kind_of(rel: &str) -> Option<CacheKind> {
    rel.split('/').next().and_then(CacheKind::from_name)
}

pub struct DiskPool {
    pool: Pool,
    root: PathBuf,
    state: Mutex<State>,
    hit: AtomicU64,
    miss: AtomicU64,
}

impl DiskPool {
    /// A pool of `cap` bytes over `root`. The index starts empty; `reload` fills it from the
    /// files under `root`.
    pub fn new(pool: Pool, root: PathBuf, cap: u64) -> Self {
        DiskPool {
            pool,
            root,
            state: Mutex::new(State {
                index: LruCache::with_bytes_capacity(usize::MAX),
                cap,
                writing: 0,
                kinds: CacheKind::all().map(|_| (0, 0)).collect(),
                deleting: HashSet::new(),
                in_flight: HashSet::new(),
                objects: HashMap::new(),
                invalidated: HashMap::new(),
                reloaded: false,
                kinds_off: CacheKind::all().map(|_| false).collect(),
            }),
            hit: AtomicU64::new(0),
            miss: AtomicU64::new(0),
        }
    }

    pub fn pool(&self) -> Pool {
        self.pool
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The entry's file and size, marking it most recently used.
    pub fn lookup(&self, rel: &str) -> Option<(PathBuf, u64)> {
        let size = self.state.lock().index.get(rel).map(|v| v.get_inner().0)?;
        Some((self.root.join(rel), size))
    }

    /// Counts a lookup for the pool's row in `system.caches`.
    pub fn record(&self, hit: bool) {
        let counter = if hit { &self.hit } else { &self.miss };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub fn contains(&self, rel: &str) -> bool {
        self.state.lock().index.contains(rel)
    }

    /// Reserves `bytes` for `rel`, taking least recently used entries out of the index until
    /// they fit. Rejects an entry larger than a quarter of the pool, any entry when the pool is
    /// off, and `rel` while it is indexed (found by the restart scan since it was queued), its
    /// previous file is being deleted, or another write of it runs.
    pub fn reserve(&self, rel: &str, bytes: u64) -> Reservation {
        let mut state = self.state.lock();
        let mut victims = Vec::new();
        if state.cap == 0
            || bytes > state.cap / OVERSIZED_DIVISOR
            || state.index.contains(rel)
            || state.deleting.contains(rel)
            || state.in_flight.contains(rel)
            || state.kind_off(rel)
        {
            return Reservation {
                reserved: false,
                victims,
            };
        }
        while state.used() + bytes > state.cap {
            match state.pop_lru() {
                Some((rel, _)) => victims.push(rel),
                // Only bytes being written are left; they are published or released soon.
                None => {
                    return Reservation {
                        reserved: false,
                        victims,
                    };
                }
            }
        }
        state.writing += bytes;
        state.in_flight.insert(rel.to_string());
        Reservation {
            reserved: true,
            victims,
        }
    }

    /// Publishes the entry written under a reservation by a fill started at `started` (see
    /// `fill_fence`). A key reserved is neither indexed nor scanned meanwhile, so the entry is
    /// new to the index. The entry is rejected, as a victim,
    /// when its object was invalidated after the fill started, or when it no longer fits the
    /// capacity; entries are evicted when the capacity shrank below what is held.
    pub fn publish(&self, rel: String, reserved: u64, written: u64, started: u64) -> Published {
        let mut state = self.state.lock();
        state.writing -= reserved;
        state.in_flight.remove(&rel);
        let stale = object_of(&rel)
            .and_then(|object| state.invalidated.get(object))
            .is_some_and(|at| *at > started);
        if stale
            || state.cap == 0
            || written > state.cap / OVERSIZED_DIVISOR
            || state.kind_off(&rel)
        {
            // Marked until deleted, as an evicted entry is.
            state.deleting.insert(rel.clone());
            return Published {
                indexed: false,
                victims: vec![rel],
            };
        }
        state.insert(rel.clone(), written);
        let mut victims = Vec::new();
        while state.used() > state.cap {
            match state.pop_lru() {
                Some((victim, _)) => victims.push(victim),
                None => break,
            }
        }
        Published {
            indexed: !victims.contains(&rel),
            victims,
        }
    }

    /// Gives back the reservation of `rel`, whose write failed or was abandoned.
    pub fn release(&self, rel: &str, reserved: u64) {
        let mut state = self.state.lock();
        state.writing -= reserved;
        state.in_flight.remove(rel);
    }

    /// Takes every entry of the object `object` (see `DiskKey::object_id`) out of the index, in
    /// any kind, and rejects the entries of fills started before `at` (see `fill_fence`);
    /// returns them, for `delete`. `oldest_fill` is the start of the oldest fill running.
    pub fn remove_object(&self, object: &str, at: u64, oldest_fill: Option<u64>) -> Vec<String> {
        let mut state = self.state.lock();
        if state.reloaded {
            // Points no fill running at `at` started before reject nothing any more. Points
            // after `at` are left alone: invalidations reach the pools out of clock order, and
            // `oldest_fill` says nothing of fills started after `at`.
            state.invalidated.retain(|_, point| {
                *point > at || oldest_fill.is_some_and(|oldest| oldest < *point)
            });
        }
        if !state.reloaded || oldest_fill.is_some_and(|oldest| oldest < at) {
            state.invalidated.insert(object.to_string(), at);
        }
        let rels = state.objects.get(object).cloned().unwrap_or_default();
        rels.into_iter()
            .filter(|rel| state.pop(rel).is_some())
            .collect()
    }

    /// Takes the entry out of the index and deletes its file. Returns whether it was indexed.
    pub fn remove(&self, rel: &str) -> bool {
        let removed = self.state.lock().pop(rel).is_some();
        if removed {
            self.delete(&[rel.to_string()]);
        }
        removed
    }

    /// Deletes the files of entries taken out of the index, then lets their keys be written
    /// again.
    pub fn delete(&self, victims: &[String]) {
        for rel in victims {
            let path = self.root.join(rel);
            if let Err(e) = fs::remove_file(&path) {
                if e.kind() != std::io::ErrorKind::NotFound {
                    warn!("[DiskCache] failed to remove {path:?}: {e}");
                }
            }
        }
        let mut state = self.state.lock();
        for rel in victims {
            state.deleting.remove(rel);
        }
    }

    /// Changes the capacity; returns the entries taken out to fit it, for `delete`.
    pub fn set_cap(&self, cap: u64) -> Vec<String> {
        let mut state = self.state.lock();
        state.cap = cap;
        let mut victims = Vec::new();
        while state.used() > cap {
            match state.pop_lru() {
                Some((rel, _)) => victims.push(rel),
                None => break,
            }
        }
        victims
    }

    /// Takes at least `bytes` of the least recently used entries out, or all of them; returns
    /// them, for `delete`.
    pub fn evict_bytes(&self, bytes: u64) -> Vec<String> {
        let mut state = self.state.lock();
        let mut freed = 0;
        let mut victims = Vec::new();
        while freed < bytes {
            let Some((rel, size)) = state.pop_lru() else {
                break;
            };
            freed += size;
            victims.push(rel);
        }
        victims
    }

    pub fn cap(&self) -> u64 {
        self.state.lock().cap
    }

    pub fn stats(&self) -> DiskPoolStats {
        let state = self.state.lock();
        DiskPoolStats {
            items: state.index.len() as u64,
            bytes: state.index.bytes_size(),
            writing: state.writing,
            cap: state.cap,
            hit: self.hit.load(Ordering::Relaxed),
            miss: self.miss.load(Ordering::Relaxed),
        }
    }

    /// Turns `kind` on or off. Turning it off takes its entries out, returned for `delete`,
    /// and refuses its writes and files from then on.
    pub fn set_kind_enabled(&self, kind: CacheKind, enabled: bool) -> Vec<String> {
        self.state.lock().kinds_off[kind.index()] = !enabled;
        if enabled {
            Vec::new()
        } else {
            self.remove_kind(kind)
        }
    }

    /// Takes every entry of `kind` out; returns them, for `delete`.
    pub fn remove_kind(&self, kind: CacheKind) -> Vec<String> {
        let prefix = format!("{}/", kind.name());
        let mut state = self.state.lock();
        let rels: Vec<String> = state
            .index
            .iter()
            .filter(|(rel, _)| rel.starts_with(&prefix))
            .map(|(rel, _)| rel.clone())
            .collect();
        rels.into_iter()
            .filter(|rel| state.pop(rel).is_some())
            .collect()
    }

    /// The last point `object` was invalidated at, while it is kept (see `remove_object`).
    pub fn invalidated_at(&self, object: &str) -> Option<u64> {
        self.state.lock().invalidated.get(object).copied()
    }

    /// Items and bytes of `kind` in the pool.
    pub fn kind_usage(&self, kind: CacheKind) -> (u64, u64) {
        self.state.lock().kinds[kind.index()]
    }

    /// Rebuilds the index from the files under the root, in scan order, and deletes what does
    /// not belong: files of unknown kinds or kinds turned off, files left half written, and files beyond the
    /// capacity, or of objects invalidated meanwhile. Returns the entries loaded and the files
    /// deleted. Runs concurrently with reads and writes: a key indexed meanwhile, being deleted
    /// or being written is skipped.
    pub fn reload(&self) -> (u64, u64) {
        let loaded = AtomicU64::new(0);
        let deleted = AtomicU64::new(0);
        let entries = scan_files(&self.root);
        entries.par_iter().for_each(|path| {
            let Some(rel) = rel_key(&self.root, path) else {
                remove(path, &deleted);
                return;
            };
            if rel.ends_with(TMP_SUFFIX) {
                // Left by a crashed process; a write of this one is still in progress.
                if !is_own_tmp(&rel) {
                    remove(path, &deleted);
                }
                return;
            }
            // Files of unknown kinds, or not named `<object>.<unit>`.
            if kind_of(&rel).is_none() || object_of(&rel).is_none() {
                remove(path, &deleted);
                return;
            }
            let Ok(size) = fs::metadata(path).map(|m| m.len()) else {
                return;
            };
            let admitted = {
                let mut state = self.state.lock();
                if state.index.contains(&rel)
                    || state.deleting.contains(&rel)
                    || state.in_flight.contains(&rel)
                {
                    // Indexed by a write meanwhile, being deleted, or being written: its file
                    // is not the scan's to keep or delete.
                    true
                } else if state.used() + size <= state.cap
                    && size <= state.cap / OVERSIZED_DIVISOR
                    && object_of(&rel).is_none_or(|o| !state.invalidated.contains_key(o))
                    && !state.kind_off(&rel)
                {
                    state.insert(rel.clone(), size);
                    loaded.fetch_add(1, Ordering::Relaxed);
                    true
                } else {
                    // Marked until deleted, so no write of the key starts meanwhile.
                    state.deleting.insert(rel.clone());
                    false
                }
            };
            if !admitted {
                remove(path, &deleted);
                self.state.lock().deleting.remove(&rel);
            }
        });
        self.state.lock().reloaded = true;
        let (loaded, deleted) = (loaded.into_inner(), deleted.into_inner());
        info!(
            "[DiskCache] pool {}: reloaded {loaded} entries, deleted {deleted} files",
            self.pool.name()
        );
        (loaded, deleted)
    }
}

/// Every file under `root`, recursively, scanning directories in parallel.
fn scan_files(root: &Path) -> Vec<PathBuf> {
    let Ok(entries) = fs::read_dir(root) else {
        return Vec::new();
    };
    entries
        .filter_map(|e| e.ok())
        .collect::<Vec<_>>()
        .into_par_iter()
        .flat_map(|entry| {
            let path = entry.path();
            match entry.file_type() {
                Ok(t) if t.is_dir() => scan_files(&path),
                Ok(_) => vec![path],
                Err(_) => Vec::new(),
            }
        })
        .collect()
}

fn rel_key(root: &Path, path: &Path) -> Option<String> {
    path.strip_prefix(root)
        .ok()
        .and_then(|rel| rel.to_str())
        .map(|rel| rel.replace(std::path::MAIN_SEPARATOR, "/"))
}

fn remove(path: &Path, deleted: &AtomicU64) {
    match fs::remove_file(path) {
        Ok(()) => {
            deleted.fetch_add(1, Ordering::Relaxed);
        }
        Err(e) => warn!("[DiskCache] failed to remove {path:?}: {e}"),
    }
}
