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

//! The IO entry of readers: the disk cache in front of object storage. A read looks the unit up
//! on disk, reads storage on a miss, and, when the caller allows it, queues the bytes to be
//! written to disk. Concurrent misses of one unit read storage once.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use databend_common_base::base::GlobalInstance;
use databend_common_base::runtime::ThreadTracker;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_storages_common_io::MergeIOReadResult;
use databend_storages_common_io::MergeIOReader;
use databend_storages_common_io::OwnerMemory;
use databend_storages_common_io::ReadSettings;
use futures::future::try_join_all;
use opendal::Buffer;
use opendal::Operator;

use crate::CacheKind;
use crate::CacheManager;
use crate::DiskCache;
use crate::DiskKey;

/// What a read may do with the disk cache.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DiskFill {
    /// Read the disk cache; write the bytes read from storage to it.
    ReadWrite,
    /// Read the disk cache; do not write to it. Maintenance and background tasks read this way.
    ReadOnly,
    /// Neither read nor write the disk cache.
    Bypass,
}

impl DiskFill {
    fn reads(self) -> bool {
        self != DiskFill::Bypass
    }

    fn writes(self) -> bool {
        self == DiskFill::ReadWrite
    }
}

/// Object storage behind the disk cache of the node. Cheap to clone.
#[derive(Clone)]
pub struct DiskCachedStorage {
    op: Operator,
    disk: Option<Arc<DiskCache>>,
    /// Prepended to paths in disk cache keys. Fuse paths are unique within a tenant; paths of
    /// external files are relative to their stage, so their keys carry the operator root.
    key_prefix: Option<String>,
}

impl DiskCachedStorage {
    /// `op` behind the node's disk cache, if the node has one.
    pub fn new(op: Operator) -> Self {
        let disk = GlobalInstance::try_get::<Arc<CacheManager>>().and_then(|m| m.disk());
        Self::with_disk(op, disk)
    }

    pub fn with_disk(op: Operator, disk: Option<Arc<DiskCache>>) -> Self {
        DiskCachedStorage {
            op,
            disk,
            key_prefix: None,
        }
    }

    /// Keys of this storage carry `prefix`, e.g. the operator root of external files.
    pub fn with_key_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.key_prefix = Some(prefix.into());
        self
    }

    /// The object at `path` as the disk cache names it: the key prefix, then the path without
    /// leading slashes, which the operator ignores. Reads and evictions of the object so name
    /// one object alike whichever way its path is written.
    fn object_path(&self, path: &str) -> String {
        let path = path.trim_start_matches('/');
        match &self.key_prefix {
            Some(prefix) => format!("{prefix}{path}"),
            None => path.to_string(),
        }
    }

    fn key(&self, path: &str, range: Option<Range<u64>>) -> DiskKey {
        let path = self.object_path(path);
        match range {
            Some(range) => DiskKey::range(path, range),
            None => DiskKey::whole(path),
        }
    }

    /// Drops what the disk cache holds of the objects at `paths`, after they were rewritten in
    /// place or deleted (see `DiskCache::evict_object`). Paths of fuse tables are never reused
    /// otherwise; the memory caches of the decoded values are evicted by their own handles.
    /// Deleting the cached files is file system work, on a blocking thread, which runs to its
    /// end when the caller is cancelled: the objects are gone from storage either way.
    pub async fn evict_objects(&self, paths: &[String]) -> Result<()> {
        let Some(disk) = self.disk.clone() else {
            return Ok(());
        };
        let objects: Vec<String> = paths.iter().map(|p| self.object_path(p)).collect();
        databend_common_base::runtime::spawn_blocking(move || {
            for object in &objects {
                disk.evict_object(object);
            }
        })
        .await
        .map_err(|e| ErrorCode::Internal(format!("disk cache eviction task failed: {e}")))
    }

    /// For writes, listing and deletes, which do not go through the disk cache.
    pub fn operator(&self) -> &Operator {
        &self.op
    }

    /// Whether a read of `len` bytes of `kind` with `fill` may be kept by the disk cache.
    pub fn admits(&self, kind: CacheKind, len: u64, fill: DiskFill) -> bool {
        self.disk_for(kind, fill)
            .is_some_and(|disk| disk.admits(kind, len))
    }

    /// Whether reads of `kind` with `fill` go through the disk cache.
    pub fn uses_disk(&self, kind: CacheKind, fill: DiskFill) -> bool {
        self.disk_for(kind, fill).is_some()
    }

    /// `fill`, or no disk cache at all for maintenance statements
    /// (`ThreadTracker::is_maintenance`): the disk cache serves queries.
    fn statement_fill(fill: DiskFill) -> DiskFill {
        if ThreadTracker::is_maintenance() {
            DiskFill::Bypass
        } else {
            fill
        }
    }

    fn disk_for(&self, kind: CacheKind, fill: DiskFill) -> Option<&Arc<DiskCache>> {
        let fill = Self::statement_fill(fill);
        self.disk
            .as_ref()
            .filter(|disk| fill.reads() && disk.is_enabled(kind))
    }

    /// The whole object at `path`, or `range` of it.
    pub async fn read(
        &self,
        kind: CacheKind,
        path: &str,
        range: Option<Range<u64>>,
        fill: DiskFill,
    ) -> Result<Bytes> {
        let fill = Self::statement_fill(fill);
        let key = self.key(path, range.clone());
        let Some(disk) = self.disk_for(kind, fill) else {
            return self.read_storage(path, range).await;
        };
        if let Some(bytes) = disk.get(kind, &key).await {
            Profile::record_usize_profile(
                ProfileStatisticsName::ScanBytesFromLocalDisk,
                bytes.len(),
            );
            return Ok(bytes);
        }
        let load = async {
            let bytes = self.read_storage(path, range.clone()).await?;
            disk.record_storage_read(kind, bytes.len() as u64);
            if fill.writes() {
                disk.put(kind, &key, bytes.clone()).await;
            }
            Ok(bytes)
        };
        disk.flight().run(&format!("{kind:?}/{key}"), load).await
    }

    async fn read_storage(&self, path: &str, range: Option<Range<u64>>) -> Result<Bytes> {
        let buffer = match range {
            Some(range) => self.op.read_with(path).range(range).await?,
            None => self.op.read(path).await?,
        };
        Ok(buffer.to_bytes())
    }

    /// The units of `path` given by `units`, each a range taken from the object's metadata.
    /// Units on disk are returned in `cached`; the others are read from storage with merge IO,
    /// returned in `fetched`, and queued to disk unit by unit.
    pub async fn read_units(
        &self,
        kind: CacheKind,
        path: &str,
        units: &[(ColumnId, Range<u64>)],
        fill: DiskFill,
        settings: &ReadSettings,
    ) -> Result<UnitsRead> {
        let fill = Self::statement_fill(fill);
        let Some(disk) = self.disk_for(kind, fill) else {
            let fetched =
                MergeIOReader::merge_io_read(settings, self.op.clone(), path, units).await?;
            return Ok(UnitsRead {
                cached: HashMap::new(),
                fetched,
            });
        };
        let lookups = units.iter().map(|(id, range)| async move {
            let key = self.key(path, Some(range.clone()));
            (*id, range.clone(), disk.get(kind, &key).await)
        });
        let mut cached = HashMap::new();
        let mut misses = Vec::new();
        for (id, range, hit) in futures::future::join_all(lookups).await {
            match hit {
                Some(bytes) => {
                    Profile::record_usize_profile(
                        ProfileStatisticsName::ScanBytesFromLocalDisk,
                        bytes.len(),
                    );
                    cached.insert(id, bytes);
                }
                None => misses.push((id, range)),
            }
        }
        if misses.is_empty() {
            let fetched = MergeIOReadResult::create(
                OwnerMemory::create(vec![]),
                HashMap::new(),
                path.to_string(),
            );
            return Ok(UnitsRead { cached, fetched });
        }
        let load = self.fetch_units(kind, path, &misses, fill, settings, disk);
        // Readers missing the same units at once, as scans of the same columns do, share one
        // storage read. The object as the disk cache names it, prefix included: two storages
        // sharing the disk cache read different objects under one relative path.
        let object = self.key(path, None);
        let mut flight_key = format!("{kind:?}/{object}");
        for (id, range) in &misses {
            flight_key.push_str(&format!(":{id}={}-{}", range.start, range.end));
        }
        let fetched = disk.units_flight().run(&flight_key, load).await?;
        Ok(UnitsRead { cached, fetched })
    }

    /// Reads `misses` from storage with merge IO and queues them to disk unit by unit.
    async fn fetch_units(
        &self,
        kind: CacheKind,
        path: &str,
        misses: &[(ColumnId, Range<u64>)],
        fill: DiskFill,
        settings: &ReadSettings,
        disk: &DiskCache,
    ) -> Result<MergeIOReadResult> {
        let fetched = MergeIOReader::merge_io_read(settings, self.op.clone(), path, misses).await?;
        let miss_bytes: u64 = misses.iter().map(|(_, r)| r.end - r.start).sum();
        disk.record_storage_read(kind, miss_bytes);
        if fill.writes() {
            let fetched = &fetched;
            let puts = misses.iter().map(|(id, range)| async move {
                let Ok(unit) = fetched_unit_buffer(fetched, *id) else {
                    return;
                };
                // Copied out of the merged read buffer, so that a queued unit does not keep
                // the whole buffer alive, the queue accounting for the unit's bytes only; and
                // once the queue has room for it, so that units waiting for room hold no copy.
                let key = self.key(path, Some(range.clone()));
                // One copy, also of a buffer of several chunks.
                let copy = || Bytes::from(unit.to_vec());
                disk.put_with(kind, &key, unit.len() as u64, copy).await;
            });
            futures::future::join_all(puts).await;
        }
        Ok(fetched)
    }

    /// Several ranges of `path`, each its own unit, read concurrently.
    pub async fn read_ranges(
        &self,
        kind: CacheKind,
        path: &str,
        ranges: Vec<Range<u64>>,
        fill: DiskFill,
    ) -> Result<HashMap<Range<u64>, Bytes>> {
        let reads = ranges.into_iter().map(|range| async move {
            let bytes = self.read(kind, path, Some(range.clone()), fill).await?;
            Ok::<_, databend_common_exception::ErrorCode>((range, bytes))
        });
        Ok(try_join_all(reads).await?.into_iter().collect())
    }
}

/// Units of one object, from disk and from storage.
pub struct UnitsRead {
    pub cached: HashMap<ColumnId, Bytes>,
    pub fetched: MergeIOReadResult,
}

impl UnitsRead {
    /// The bytes of unit `id`, wherever they were read from.
    pub fn unit(&self, id: ColumnId) -> Option<Bytes> {
        match self.cached.get(&id) {
            Some(bytes) => Some(bytes.clone()),
            None => fetched_unit(&self.fetched, id).ok(),
        }
    }
}

fn fetched_unit(fetched: &MergeIOReadResult, id: ColumnId) -> Result<Bytes> {
    Ok(fetched_unit_buffer(fetched, id)?.to_bytes())
}

/// The bytes of unit `id` in `fetched`, as a view of the merged read buffer: no copy.
fn fetched_unit_buffer(fetched: &MergeIOReadResult, id: ColumnId) -> Result<Buffer> {
    let (chunk, range) = fetched.columns_chunk_offsets.get(&id).ok_or_else(|| {
        databend_common_exception::ErrorCode::Internal(format!(
            "unit {id} of {} was not read",
            fetched.block_path
        ))
    })?;
    let chunk = fetched
        .owner_memory
        .get_chunk(*chunk, &fetched.block_path)?;
    Ok(chunk.slice(range.clone()))
}
