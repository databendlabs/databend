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
pub(crate) struct FileSize(pub u64);

impl From<FileSize> for CacheValue<FileSize> {
    fn from(value: FileSize) -> Self {
        let bytes = value.0 as usize;
        CacheValue::new(value, bytes)
    }
}

/// The outcome of `DiskPool::reserve`. When `reserved`, the room is held until `publish` or
/// `release`. Either way, `victims` were taken out of the index and their files must be
/// deleted.
pub(crate) struct Reservation {
    pub reserved: bool,
    pub victims: Vec<PathBuf>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DiskPoolStats {
    pub items: u64,
    pub bytes: u64,
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

    fn pop_lru(&mut self) -> Option<String> {
        let (rel, size) = self.index.pop_by_policy()?;
        self.account(kind_of(&rel), size.get_inner().0, false);
        Some(rel)
    }

    fn pop(&mut self, rel: &str) -> Option<u64> {
        let size = self.index.pop(rel)?.get_inner().0;
        self.account(kind_of(rel), size, false);
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
    /// A pool of `cap` bytes over `root`. The index starts empty; `reload` or `reset` brings the
    /// directory in line with it.
    pub(crate) fn new(pool: Pool, root: PathBuf, cap: u64) -> Self {
        DiskPool {
            pool,
            root,
            state: Mutex::new(State {
                index: LruCache::with_bytes_capacity(usize::MAX),
                cap,
                writing: 0,
                kinds: CacheKind::all().map(|_| (0, 0)).collect(),
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
    pub(crate) fn lookup(&self, rel: &str) -> Option<(PathBuf, u64)> {
        let size = self.state.lock().index.get(rel).map(|v| v.get_inner().0)?;
        Some((self.root.join(rel), size))
    }

    /// Counts a lookup for the pool's row in `system.caches`.
    pub(crate) fn record(&self, hit: bool) {
        let counter = if hit { &self.hit } else { &self.miss };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn contains(&self, rel: &str) -> bool {
        self.state.lock().index.contains(rel)
    }

    /// Reserves `bytes`, taking least recently used entries out of the index until they fit.
    /// Rejects an entry larger than a quarter of the pool, and any entry when the pool is off.
    pub(crate) fn reserve(&self, bytes: u64) -> Reservation {
        let mut state = self.state.lock();
        let mut victims = Vec::new();
        if state.cap == 0 || bytes > state.cap / OVERSIZED_DIVISOR {
            return Reservation {
                reserved: false,
                victims,
            };
        }
        while state.used() + bytes > state.cap {
            match state.pop_lru() {
                Some(rel) => victims.push(self.root.join(rel)),
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
    pub(crate) fn publish(&self, rel: String, reserved: u64, written: u64) {
        let mut state = self.state.lock();
        state.writing -= reserved;
        state.insert(rel, written);
    }

    /// Gives back a reservation whose write failed or was abandoned.
    pub(crate) fn release(&self, reserved: u64) {
        self.state.lock().writing -= reserved;
    }

    /// Takes the entry out of the index; returns its file to delete.
    pub(crate) fn remove(&self, rel: &str) -> Option<PathBuf> {
        let mut state = self.state.lock();
        state.pop(rel).map(|_| self.root.join(rel))
    }

    /// Changes the capacity; returns the files of the entries taken out to fit it.
    pub(crate) fn set_cap(&self, cap: u64) -> Vec<PathBuf> {
        let mut state = self.state.lock();
        state.cap = cap;
        let mut victims = Vec::new();
        while state.used() > cap {
            match state.pop_lru() {
                Some(rel) => victims.push(self.root.join(rel)),
                None => break,
            }
        }
        victims
    }

    /// Takes up to `bytes` of the least recently used entries out; returns their files.
    pub(crate) fn evict_bytes(&self, bytes: u64) -> Vec<PathBuf> {
        let mut state = self.state.lock();
        let mut freed = 0;
        let mut victims = Vec::new();
        while freed < bytes {
            let Some((rel, size)) = state
                .index
                .peek_by_policy()
                .map(|(k, v)| (k.clone(), v.get_inner().0))
            else {
                break;
            };
            state.pop(&rel);
            freed += size;
            victims.push(self.root.join(rel));
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
            cap: state.cap,
            hit: self.hit.load(Ordering::Relaxed),
            miss: self.miss.load(Ordering::Relaxed),
        }
    }

    /// Takes every entry of `kind` out; returns their files.
    pub(crate) fn remove_kind(&self, kind: CacheKind) -> Vec<PathBuf> {
        let prefix = format!("{}/", kind.name());
        let mut state = self.state.lock();
        let rels: Vec<String> = state
            .index
            .iter()
            .filter(|(rel, _)| rel.starts_with(&prefix))
            .map(|(rel, _)| rel.clone())
            .collect();
        rels.into_iter()
            .filter_map(|rel| state.pop(&rel).map(|_| self.root.join(rel)))
            .collect()
    }

    /// Items and bytes of `kind` in the pool.
    pub(crate) fn kind_usage(&self, kind: CacheKind) -> (u64, u64) {
        self.state.lock().kinds[kind.index()]
    }

    /// Rebuilds the index from the files under the root, in scan order, and deletes what does
    /// not belong: files of unknown kinds, files left half written, and files beyond the
    /// capacity. Runs concurrently with reads and writes: a key indexed meanwhile is skipped.
    pub(crate) fn reload(&self) -> (u64, u64) {
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
                if state.index.contains(&rel) {
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
