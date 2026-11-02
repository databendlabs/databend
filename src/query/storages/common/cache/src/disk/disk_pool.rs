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
//! An entry taken out of the index stays marked as being deleted until `delete` has removed its
//! file. Meanwhile it cannot be reserved again, and the restart scan leaves its file alone:
//! otherwise a new file written under the same name could be deleted in place of the old one,
//! leaving an indexed entry without a file.

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
}

impl State {
    fn used(&self) -> u64 {
        self.index.bytes_size() + self.writing
    }

    fn insert(&mut self, rel: String, size: u64) {
        let kind = kind_of(&rel);
        if let Some(old) = self.index.insert(rel, FileSize(size).into()) {
            self.account(kind, old.get_inner().0, false);
        }
        self.account(kind, size, true);
    }

    /// Takes the least recently used entry out, marking it as being deleted.
    fn pop_lru(&mut self) -> Option<(String, u64)> {
        let (rel, size) = self.index.pop_by_policy()?;
        let size = size.get_inner().0;
        self.account(kind_of(&rel), size, false);
        self.deleting.insert(rel.clone());
        Some((rel, size))
    }

    /// Takes `rel` out, marking it as being deleted.
    fn pop(&mut self, rel: &str) -> Option<u64> {
        let size = self.index.pop(rel)?.get_inner().0;
        self.account(kind_of(rel), size, false);
        self.deleting.insert(rel.to_string());
        Some(size)
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
    /// off, and `rel` while its previous file is being deleted.
    pub fn reserve(&self, rel: &str, bytes: u64) -> Reservation {
        let mut state = self.state.lock();
        let mut victims = Vec::new();
        if state.cap == 0 || bytes > state.cap / OVERSIZED_DIVISOR || state.deleting.contains(rel) {
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
        Reservation {
            reserved: true,
            victims,
        }
    }

    /// Publishes the entry written under a reservation. An entry already indexed under `rel`
    /// (written concurrently, or found by the restart scan) is replaced, not counted twice.
    pub fn publish(&self, rel: String, reserved: u64, written: u64) {
        let mut state = self.state.lock();
        state.writing -= reserved;
        state.insert(rel, written);
    }

    /// Gives back a reservation whose write failed or was abandoned.
    pub fn release(&self, reserved: u64) {
        self.state.lock().writing -= reserved;
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

    /// Items and bytes of `kind` in the pool.
    pub fn kind_usage(&self, kind: CacheKind) -> (u64, u64) {
        self.state.lock().kinds[kind.index()]
    }

    /// Rebuilds the index from the files under the root, in scan order, and deletes what does
    /// not belong: files of unknown kinds, files left half written, and files beyond the
    /// capacity. Returns the entries loaded and the files deleted. Runs concurrently with reads
    /// and writes: a key indexed meanwhile, or being deleted, is skipped.
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
            if kind_of(&rel).is_none() {
                remove(path, &deleted);
                return;
            }
            let Ok(size) = fs::metadata(path).map(|m| m.len()) else {
                return;
            };
            let admitted = {
                let mut state = self.state.lock();
                if state.index.contains(&rel) || state.deleting.contains(&rel) {
                    true
                } else if state.used() + size <= state.cap && size <= state.cap / OVERSIZED_DIVISOR
                {
                    state.insert(rel, size);
                    loaded.fetch_add(1, Ordering::Relaxed);
                    true
                } else {
                    false
                }
            };
            if !admitted {
                remove(path, &deleted);
            }
        });
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
