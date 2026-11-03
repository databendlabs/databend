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
//! writes its own under a temporary name without the lock, then publishes the entry under the
//! lock again, renaming the file into place only if the entry is accepted. A file under its
//! final name is so always an entry the pool accepted, also for the restart scan after a crash.
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
//! restart scan has finished, every invalidation is kept, so the scan does not index the files
//! of an object invalidated before it got to them. A scan that could not read every file
//! leaves the pool not reloaded: changes then delete an object's unindexed files by name, and
//! the invalidations are pruned as usual, no scan being left for them to exclude files from.
//!
//! An object being rewritten or deleted (`begin_change` to `end_change`) has no entry: they are
//! taken out when the change begins, and none is reserved, published or scanned until it ends.
//! A crash during the change leaves no file of the object to be read after the restart.
//!
//! `publish` checks the capacity again: it may have shrunk while the entry was written. A kind
//! turned off is refused by `reserve`, `publish` and the restart scan alike, so no entry of it
//! comes back from a write queued or a file scanned before.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
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
    /// Entries, and their bytes, evicted since the pool started, by `Eviction` reason.
    pub evicted: [u64; 2],
    pub evicted_bytes: [u64; 2],
}

/// What happened to the entries of one kind in a pool since it started.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DiskKindEvents {
    /// Entries written and indexed, and their bytes.
    pub written: u64,
    pub written_bytes: u64,
    /// Entries evicted, and their bytes, by `Eviction` reason.
    pub evicted: [u64; 2],
    pub evicted_bytes: [u64; 2],
    /// Entries dropped because their object was rewritten or deleted (`begin_change`).
    pub invalidated: u64,
    pub invalidated_bytes: u64,
}

/// Why least recently used entries leave a pool.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Eviction {
    /// For room within the pool's capacity: a write, or a lowered capacity.
    Capacity,
    /// The file system is over the watermark (`cache.disk.watermark_ratio`).
    DiskPressure,
}

impl Eviction {
    pub const ALL: [Eviction; 2] = [Eviction::Capacity, Eviction::DiskPressure];

    pub fn name(self) -> &'static str {
        match self {
            Eviction::Capacity => "capacity",
            Eviction::DiskPressure => "disk_pressure",
        }
    }

    fn index(self) -> usize {
        self as usize
    }
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
    /// The same, by object id, for `begin_change` to finish them.
    deleting_objects: HashMap<String, HashSet<String>>,
    /// Those whose delete failed: no delete is pending for them, so the next change of their
    /// object deletes them and clears their marks.
    undeleted: HashSet<String>,
    /// Keys reserved by writers and not yet published or released.
    in_flight: HashSet<String>,
    /// Files of a previous run the restart scan left alone because their keys were being
    /// written, by object id: untracked until a write of the key replaces them, so a change of
    /// the object deletes them.
    shadowed: HashMap<String, HashSet<String>>,
    /// The entries of each object, by object id.
    objects: HashMap<String, HashSet<String>>,
    /// The last point each object was invalidated at, while fills started before it may still
    /// publish, or while the restart scan has not finished.
    invalidated: HashMap<String, u64>,
    /// The same, by point, for pruning in order.
    invalidation_order: BTreeMap<u64, String>,
    /// Objects being rewritten or deleted, with the changes of each under way.
    changing: HashMap<String, usize>,
    /// Whether the restart scan has indexed every file it found: until then, changes look for
    /// an object's files by name too.
    reloaded: bool,
    /// Whether the restart scan has finished, complete or not: until then, every invalidation
    /// is kept for it.
    scanned: bool,
    /// Kinds turned off, by kind index.
    kinds_off: Vec<bool>,
    /// By kind index.
    events: Vec<DiskKindEvents>,
}

impl State {
    fn kind_off(&self, rel: &str) -> bool {
        kind_of(rel).is_some_and(|kind| self.kinds_off[kind.index()])
    }

    fn changing(&self, rel: &str) -> bool {
        object_of(rel).is_some_and(|object| self.changing.contains_key(object))
    }

    /// Takes every entry of `object` out of the index.
    fn take_object(&mut self, object: &str) -> Vec<String> {
        // Detached first: taking each entry out then finds no set to update.
        let rels = self.objects.remove(object).unwrap_or_default();
        rels.into_iter()
            .filter(|rel| {
                let Some(size) = self.pop(rel) else {
                    return false;
                };
                if let Some(events) = self.events_of(rel) {
                    events.invalidated += 1;
                    events.invalidated_bytes += size;
                }
                true
            })
            .collect()
    }

    fn events_of(&mut self, rel: &str) -> Option<&mut DiskKindEvents> {
        kind_of(rel).map(|kind| &mut self.events[kind.index()])
    }

    /// Records `object` as invalidated at `at`, unless at a later point already: invalidations
    /// reach the pools out of clock order.
    fn invalidate(&mut self, object: &str, at: u64) {
        if let Some(point) = self.invalidated.get(object).copied() {
            if point >= at {
                return;
            }
            self.invalidation_order.remove(&point);
        }
        self.invalidated.insert(object.to_string(), at);
        self.invalidation_order.insert(at, object.to_string());
    }

    /// Drops the points up to `bound`, in order.
    fn prune_invalidations(&mut self, bound: u64) {
        while let Some(entry) = self.invalidation_order.first_entry() {
            if *entry.key() > bound {
                break;
            }
            let object = entry.remove();
            self.invalidated.remove(&object);
        }
    }

    /// See `DiskPool::remove_object`.
    fn remove_object(&mut self, object: &str, at: u64, oldest_fill: Option<u64>) -> Vec<String> {
        if self.scanned {
            // Points no fill running at `at` started before reject nothing any more: those up
            // to `at` and to the oldest fill. Points after `at` are left alone: invalidations
            // reach the pools out of clock order, and `oldest_fill` says nothing of fills
            // started after `at`.
            self.prune_invalidations(oldest_fill.map_or(at, |oldest| oldest.min(at)));
        }
        if !self.scanned || oldest_fill.is_some_and(|oldest| oldest < at) {
            self.invalidate(object, at);
        }
        self.take_object(object)
    }

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
                        .insert(rel);
                }
            }
        }
        self.account(kind, size, true);
    }

    /// Takes the least recently used entry out, marking it as being deleted.
    fn pop_lru(&mut self, reason: Eviction) -> Option<(String, u64)> {
        let (rel, size) = self.index.pop_by_policy()?;
        let size = size.get_inner().0;
        self.taken(&rel, size);
        if let Some(events) = self.events_of(&rel) {
            events.evicted[reason.index()] += 1;
            events.evicted_bytes[reason.index()] += size;
        }
        Some((rel, size))
    }

    /// Takes `rel` out, marking it as being deleted.
    fn pop(&mut self, rel: &str) -> Option<u64> {
        let size = self.index.pop(rel)?.get_inner().0;
        self.taken(rel, size);
        Some(size)
    }

    fn mark_deleting(&mut self, rel: &str) {
        self.deleting.insert(rel.to_string());
        if let Some(object) = object_of(rel) {
            self.deleting_objects
                .entry(object.to_string())
                .or_default()
                .insert(rel.to_string());
        }
    }

    /// The file of a previous run the restart scan left under `rel` (see `shadowed`), whose
    /// write is abandoned: marked for `delete`, as no write replaces it now.
    fn take_shadowed(&mut self, rel: &str) -> Option<String> {
        let object = object_of(rel)?;
        let rels = self.shadowed.get_mut(object)?;
        if !rels.remove(rel) {
            return None;
        }
        if rels.is_empty() {
            self.shadowed.remove(object);
        }
        self.mark_deleting(rel);
        Some(rel.to_string())
    }

    fn unmark_deleting(&mut self, rel: &str) {
        self.deleting.remove(rel);
        self.undeleted.remove(rel);
        if let Some(object) = object_of(rel) {
            if let Some(rels) = self.deleting_objects.get_mut(object) {
                rels.remove(rel);
                if rels.is_empty() {
                    self.deleting_objects.remove(object);
                }
            }
        }
    }

    /// Accounts an entry taken out of the index.
    fn taken(&mut self, rel: &str, size: u64) {
        self.account(kind_of(rel), size, false);
        self.mark_deleting(rel);
        if let Some(object) = object_of(rel) {
            if let Some(rels) = self.objects.get_mut(object) {
                rels.remove(rel);
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
                deleting_objects: HashMap::new(),
                undeleted: HashSet::new(),
                in_flight: HashSet::new(),
                shadowed: HashMap::new(),
                objects: HashMap::new(),
                invalidated: HashMap::new(),
                invalidation_order: BTreeMap::new(),
                changing: HashMap::new(),
                reloaded: false,
                scanned: false,
                kinds_off: CacheKind::all().map(|_| false).collect(),
                events: CacheKind::all()
                    .map(|_| DiskKindEvents::default())
                    .collect(),
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

    /// Whether `publish` would accept, as things stand, the entry `rel` read by a fill started at
    /// `started`: its object is not being changed nor invalidated since, and its kind is on.
    /// Bytes still waiting to be written are served only then.
    pub fn accepts(&self, rel: &str, started: u64) -> bool {
        let state = self.state.lock();
        let stale = object_of(rel)
            .and_then(|object| state.invalidated.get(object))
            .is_some_and(|at| *at > started);
        !stale && state.cap > 0 && !state.changing(rel) && !state.kind_off(rel)
    }

    /// Whether an entry of `len` bytes, its file `len` plus the checksum, is small enough to be
    /// kept: no larger than a quarter of the pool.
    pub fn admits(&self, len: u64) -> bool {
        let cap = self.state.lock().cap;
        cap > 0 && len.saturating_add(4) <= cap / OVERSIZED_DIVISOR
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
            || state.changing(rel)
        {
            return Reservation {
                reserved: false,
                victims,
            };
        }
        while state.used() + bytes > state.cap {
            match state.pop_lru(Eviction::Capacity) {
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
    /// `fill_fence`), and gives the reservation back. A key reserved is neither indexed nor
    /// scanned meanwhile, so the entry is new to the index. The entry is rejected when its
    /// object was invalidated after the fill started or is being changed, when its kind is off,
    /// or when it no longer fits the capacity; entries are evicted when the capacity shrank
    /// below what is held. `commit` makes the entry's file visible under its name, and runs
    /// only for an accepted entry, under the lock: an invalidation of the object waits for it,
    /// then finds the entry indexed. Its error is returned, the entry not indexed.
    pub fn publish(
        &self,
        rel: String,
        reserved: u64,
        written: u64,
        started: u64,
        commit: impl FnOnce() -> std::io::Result<()>,
    ) -> std::io::Result<Published> {
        let mut state = self.state.lock();
        state.writing -= reserved;
        state.in_flight.remove(&rel);
        let stale = object_of(&rel)
            .and_then(|object| state.invalidated.get(object))
            .is_some_and(|at| *at > started);
        if stale
            || state.changing(&rel)
            || state.cap == 0
            || written > state.cap / OVERSIZED_DIVISOR
            || state.kind_off(&rel)
        {
            return Ok(Published {
                indexed: false,
                victims: state.take_shadowed(&rel).into_iter().collect(),
            });
        }
        if let Err(e) = commit() {
            // Not replaced: kept marked, for the next change of its object to delete it.
            if let Some(rel) = state.take_shadowed(&rel) {
                state.undeleted.insert(rel);
            }
            return Err(e);
        }
        // The file the restart scan left under the key, if any, is replaced.
        if let Some(object) = object_of(&rel) {
            if let Some(rels) = state.shadowed.get_mut(object) {
                rels.remove(&rel);
                if rels.is_empty() {
                    state.shadowed.remove(object);
                }
            }
        }
        state.insert(rel.clone(), written);
        if let Some(events) = state.events_of(&rel) {
            events.written += 1;
            events.written_bytes += written;
        }
        let mut victims = Vec::new();
        while state.used() > state.cap {
            match state.pop_lru(Eviction::Capacity) {
                Some((victim, _)) => victims.push(victim),
                None => break,
            }
        }
        Ok(Published {
            indexed: !victims.contains(&rel),
            victims,
        })
    }

    /// Gives back the reservation of `rel`, whose write failed or was abandoned.
    pub fn release(&self, rel: &str, reserved: u64) {
        let shadowed = {
            let mut state = self.state.lock();
            state.writing -= reserved;
            state.in_flight.remove(rel);
            state.take_shadowed(rel)
        };
        if let Some(rel) = shadowed {
            self.delete(&[rel]);
        }
    }

    /// Takes every entry of the object `object` (see `DiskKey::object_id`) out of the index, in
    /// any kind, and rejects the entries of fills started before `at` (see `fill_fence`);
    /// returns them, for `delete`. `oldest_fill` is the start of the oldest fill running.
    pub fn remove_object(&self, object: &str, at: u64, oldest_fill: Option<u64>) -> Vec<String> {
        self.state.lock().remove_object(object, at, oldest_fill)
    }

    /// `object` is about to be rewritten or deleted: deletes the files of its entries, and
    /// refuses its entries until `end_change`. So are deleted the files of its entries taken
    /// out earlier whose delete is still pending, as after a resize in the background, the
    /// files of a previous run under its keys being written when the restart scan ran, and,
    /// before the restart scan has run, its files not indexed yet: none may be left for a scan
    /// after a crash. Returns the entries dropped, or the first file that could not be
    /// deleted: the change must not proceed then. Either way `end_change` ends it.
    pub fn begin_change(&self, object: &str) -> std::io::Result<usize> {
        let (pending, victims, shadowed, reloaded) = {
            let mut state = self.state.lock();
            *state.changing.entry(object.to_string()).or_default() += 1;
            // Taken before the object's own entries join them.
            let pending: Vec<String> = state
                .deleting_objects
                .get(object)
                .map(|rels| rels.iter().cloned().collect())
                .unwrap_or_default();
            // No write of the object publishes while it changes, so none replaces them. Marked
            // until deleted, as the entries taken: a change of the object meanwhile deletes
            // them too, and one whose delete fails keeps them for the next.
            let shadowed: Vec<String> = state
                .shadowed
                .remove(object)
                .map(|rels| rels.into_iter().collect())
                .unwrap_or_default();
            for rel in &shadowed {
                state.mark_deleting(rel);
            }
            (pending, state.take_object(object), shadowed, state.reloaded)
        };
        // The pending deletes keep their marks for their deletes to clear; no file of those keys
        // is written meanwhile. Those whose delete failed have none pending: their marks are
        // cleared here once their files are gone.
        let (pending_deleted, failed) = match remove_files(&self.root, &pending) {
            Ok(()) => (Ok(()), HashSet::new()),
            Err((e, failed)) => (Err(e), failed.into_iter().collect::<HashSet<_>>()),
        };
        {
            let mut state = self.state.lock();
            for rel in &pending {
                if failed.contains(rel) {
                    state.undeleted.insert(rel.clone());
                } else if state.undeleted.contains(rel) {
                    state.unmark_deleting(rel);
                }
            }
        }
        let taken = self.try_delete(&victims);
        let shadowed_deleted = self.try_delete(&shadowed);
        let mut deleted = pending_deleted.and(taken).and(shadowed_deleted);
        if !reloaded {
            // The scan refuses the object's files from now on, and no write of it is put in
            // place, so the files left under its name are unindexed ones.
            deleted = deleted.and(self.delete_unindexed_files(object));
        }
        deleted.map(|()| victims.len())
    }

    /// Deletes the files named after `object` (see `DiskKey::rel_path`) in every kind.
    fn delete_unindexed_files(&self, object: &str) -> std::io::Result<()> {
        let kinds = match fs::read_dir(&self.root) {
            Ok(kinds) => kinds,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e),
        };
        let prefix = format!("{object}.");
        let mut found = Vec::new();
        for kind in kinds {
            let dir = kind?.path().join(&object[..3.min(object.len())]);
            let files = match fs::read_dir(&dir) {
                Ok(files) => files,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e),
            };
            for file in files {
                let path = file?.path();
                let name = path.file_name().map(|n| n.to_string_lossy().into_owned());
                if name.is_some_and(|n| n.starts_with(&prefix) && !n.ends_with(TMP_SUFFIX)) {
                    found.extend(rel_key(&self.root, &path));
                }
            }
        }
        remove_files(&self.root, &found).map_err(|(e, failed)| {
            // Kept as pending deletes, for a retry of the change to find them once the scan
            // has run.
            let mut state = self.state.lock();
            for rel in failed {
                state.mark_deleting(&rel);
                state.undeleted.insert(rel);
            }
            e
        })
    }

    /// The change of `object` begun by `begin_change` is done, or abandoned: as
    /// `remove_object` at `at`, so fills started before are rejected from then on, at once
    /// with the end of the change.
    pub fn end_change(&self, object: &str, at: u64, oldest_fill: Option<u64>) -> Vec<String> {
        let mut state = self.state.lock();
        if let Some(changes) = state.changing.get_mut(object) {
            *changes -= 1;
            if *changes == 0 {
                state.changing.remove(object);
            }
        }
        state.remove_object(object, at, oldest_fill)
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
        if let Err(e) = self.try_delete(victims) {
            warn!(
                "[DiskCache] failed to remove a file of pool {}: {e}",
                self.pool.name()
            );
        }
    }

    /// `delete`, returning the first file that could not be deleted. All are tried; a file
    /// not deleted stays marked as being deleted, its key not written again, for a change of
    /// its object to delete it before the object changes.
    fn try_delete(&self, victims: &[String]) -> std::io::Result<()> {
        let (deleted, failed) = match remove_files(&self.root, victims) {
            Ok(()) => (Ok(()), HashSet::new()),
            Err((e, failed)) => (Err(e), failed.into_iter().collect()),
        };
        let mut state = self.state.lock();
        for rel in victims {
            if failed.contains(rel) {
                state.undeleted.insert(rel.clone());
            } else {
                state.unmark_deleting(rel);
            }
        }
        deleted
    }

    /// Changes the capacity; returns the entries taken out to fit it, for `delete`.
    pub fn set_cap(&self, cap: u64) -> Vec<String> {
        let mut state = self.state.lock();
        state.cap = cap;
        let mut victims = Vec::new();
        while state.used() > cap {
            match state.pop_lru(Eviction::Capacity) {
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
            let Some((rel, size)) = state.pop_lru(Eviction::DiskPressure) else {
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
            evicted: [0, 1].map(|i| state.events.iter().map(|e| e.evicted[i]).sum()),
            evicted_bytes: [0, 1].map(|i| state.events.iter().map(|e| e.evicted_bytes[i]).sum()),
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

    /// Drops the invalidation points up to `point`, before which no running fill started. All
    /// are kept until the restart scan has finished.
    pub fn prune_invalidations(&self, point: u64) {
        let mut state = self.state.lock();
        if state.scanned {
            state.prune_invalidations(point);
        }
    }

    /// The last point `object` was invalidated at, while it is kept (see `remove_object`).
    pub fn invalidated_at(&self, object: &str) -> Option<u64> {
        self.state.lock().invalidated.get(object).copied()
    }

    /// Items and bytes of `kind` in the pool.
    pub fn kind_usage(&self, kind: CacheKind) -> (u64, u64) {
        self.state.lock().kinds[kind.index()]
    }

    /// See `DiskKindEvents`.
    pub fn kind_events(&self, kind: CacheKind) -> DiskKindEvents {
        self.state.lock().events[kind.index()]
    }

    /// Rebuilds the index from the files under the root, in scan order, and deletes what does
    /// not belong: files of unknown kinds or kinds turned off, files left half written, and files beyond the
    /// capacity, or of objects invalidated meanwhile. Returns the entries loaded and the files
    /// deleted. Runs concurrently with reads and writes: a key indexed meanwhile, being deleted
    /// or being written is skipped.
    pub fn reload(&self) -> (u64, u64) {
        let loaded = AtomicU64::new(0);
        let deleted = AtomicU64::new(0);
        let complete = AtomicBool::new(true);
        let entries = scan_files(&self.root, &complete);
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
            let size = match fs::metadata(path) {
                Ok(meta) => meta.len(),
                // Deleted meanwhile, by a change or an eviction.
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
                Err(e) => {
                    warn!("[DiskCache] failed to scan {path:?}: {e}");
                    complete.store(false, Ordering::Relaxed);
                    return;
                }
            };
            let admitted = {
                let mut state = self.state.lock();
                if state.in_flight.contains(&rel) {
                    // Being written: the write replaces the file when published. Until then
                    // it is not the scan's to keep or delete, but a change of its object
                    // deletes it.
                    if let Some(object) = object_of(&rel) {
                        state
                            .shadowed
                            .entry(object.to_string())
                            .or_default()
                            .insert(rel.clone());
                    }
                    true
                } else if state.index.contains(&rel) || state.deleting.contains(&rel) {
                    // Indexed by a write meanwhile, or being deleted: its file is not the
                    // scan's to keep or delete.
                    true
                } else if state.used() + size <= state.cap
                    && size <= state.cap / OVERSIZED_DIVISOR
                    && object_of(&rel).is_none_or(|o| !state.invalidated.contains_key(o))
                    && !state.kind_off(&rel)
                    && !state.changing(&rel)
                {
                    state.insert(rel.clone(), size);
                    loaded.fetch_add(1, Ordering::Relaxed);
                    true
                } else {
                    // Marked until deleted, so no write of the key starts meanwhile.
                    state.mark_deleting(&rel);
                    false
                }
            };
            if !admitted {
                let gone = remove(path, &deleted);
                let mut state = self.state.lock();
                if gone {
                    state.unmark_deleting(&rel);
                } else {
                    // Kept marked, for a change of its object to delete it.
                    state.undeleted.insert(rel.clone());
                }
            }
        });
        // A file the scan could not see is not known either way: the pool stays as before the
        // scan, a change of an object then looking for its files by name.
        let complete = complete.into_inner();
        {
            let mut state = self.state.lock();
            state.reloaded |= complete;
            state.scanned = true;
        }
        if !complete {
            warn!(
                "[DiskCache] pool {}: the restart scan is incomplete; changes of objects keep \
                 looking for their files on disk",
                self.pool.name()
            );
        }
        let (loaded, deleted) = (loaded.into_inner(), deleted.into_inner());
        info!(
            "[DiskCache] pool {}: reloaded {loaded} entries, deleted {deleted} files",
            self.pool.name()
        );
        (loaded, deleted)
    }
}

/// Deletes the files of `rels` under `root`, all of them; a file already gone is deleted.
/// Fails with the first error and the files not deleted.
fn remove_files(root: &Path, rels: &[String]) -> Result<(), (std::io::Error, Vec<String>)> {
    let mut first = None;
    let mut failed = Vec::new();
    for rel in rels {
        let path = root.join(rel);
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                first
                    .get_or_insert_with(|| std::io::Error::new(e.kind(), format!("{path:?}: {e}")));
                failed.push(rel.clone());
            }
        }
    }
    match first {
        None => Ok(()),
        Some(e) => Err((e, failed)),
    }
}

/// Every file under `root`, recursively, scanning directories in parallel. `complete` is
/// cleared when a directory or entry could not be read.
fn scan_files(root: &Path, complete: &AtomicBool) -> Vec<PathBuf> {
    let entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(e) => {
            if e.kind() != std::io::ErrorKind::NotFound {
                warn!("[DiskCache] failed to scan {root:?}: {e}");
                complete.store(false, Ordering::Relaxed);
            }
            return Vec::new();
        }
    };
    entries
        .filter_map(|e| match e {
            Ok(entry) => Some(entry),
            Err(e) => {
                warn!("[DiskCache] failed to scan {root:?}: {e}");
                complete.store(false, Ordering::Relaxed);
                None
            }
        })
        .collect::<Vec<_>>()
        .into_par_iter()
        .flat_map(|entry| {
            let path = entry.path();
            match entry.file_type() {
                Ok(t) if t.is_dir() => scan_files(&path, complete),
                Ok(_) => vec![path],
                Err(e) => {
                    warn!("[DiskCache] failed to scan {path:?}: {e}");
                    complete.store(false, Ordering::Relaxed);
                    Vec::new()
                }
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

/// Whether the file is gone.
fn remove(path: &Path, deleted: &AtomicU64) -> bool {
    match fs::remove_file(path) {
        Ok(()) => {
            deleted.fetch_add(1, Ordering::Relaxed);
            true
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => true,
        Err(e) => {
            warn!("[DiskCache] failed to remove {path:?}: {e}");
            false
        }
    }
}
