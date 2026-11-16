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

// Inspired by Quickwit's CachingDirectory.
// Copyright 2021-Present Datadog, Inc.
// Modified by Datafuse Labs into a search-scoped, non-evicting range pin.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt;
use std::io;
use std::ops::Range;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock;

use async_trait::async_trait;
use tantivy::Directory;
use tantivy::HasLen;
use tantivy::directory::FileHandle;
use tantivy::directory::OwnedBytes;
use tantivy::directory::error::OpenReadError;

use super::read_only_directory;

#[derive(Clone, Debug)]
struct CachedRange {
    range: Range<usize>,
    bytes: OwnedBytes,
}

/// The pinned ranges of one file, by start. No range contains another, so the ranges have
/// distinct starts, and their ends grow with their starts: the ranges that start at or before an
/// offset end furthest with the last of them.
#[derive(Clone, Debug, Default)]
struct FileRangeCache(Arc<RwLock<BTreeMap<usize, CachedRange>>>);

impl FileRangeCache {
    /// The range that starts last at or before `offset`: of those, the one that ends last.
    fn last_from(ranges: &BTreeMap<usize, CachedRange>, offset: usize) -> Option<&CachedRange> {
        ranges
            .range(..=offset)
            .next_back()
            .map(|(_, cached)| cached)
    }

    fn insert(&self, range: Range<usize>, bytes: OwnedBytes) -> io::Result<()> {
        if range.len() != bytes.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cached range and byte lengths differ",
            ));
        }
        let mut ranges = self.0.write().expect("search pin lock poisoned");
        if Self::last_from(&ranges, range.start).is_some_and(|cached| cached.range.end >= range.end)
        {
            return Ok(());
        }
        // The ranges it contains start at or after it and, ends growing, are consecutive.
        let contained = ranges
            .range(range.start..)
            .take_while(|(_, cached)| cached.range.end <= range.end)
            .map(|(&start, _)| start)
            .collect::<Vec<_>>();
        for start in contained {
            ranges.remove(&start);
        }
        ranges.insert(range.start, CachedRange { range, bytes });
        Ok(())
    }

    fn get(&self, range: Range<usize>) -> Option<OwnedBytes> {
        if range.is_empty() {
            return Some(OwnedBytes::empty());
        }
        let ranges = self.0.read().expect("search pin lock poisoned");
        if let Some(cached) =
            Self::last_from(&ranges, range.start).filter(|cached| cached.range.end >= range.end)
        {
            let start = range.start - cached.range.start;
            return Some(cached.bytes.slice(start..start + range.len()));
        }

        // Tantivy may asynchronously warm adjacent slices and later synchronously request their
        // union. Assemble that request from already-warm slices without touching object storage.
        let mut cursor = range.start;
        let mut output = Vec::with_capacity(range.len());
        while cursor < range.end {
            let cached =
                Self::last_from(&ranges, cursor).filter(|cached| cached.range.end > cursor)?;
            let end = cached.range.end.min(range.end);
            let start_in_cached = cursor - cached.range.start;
            output.extend_from_slice(
                &cached.bytes[start_in_cached..start_in_cached + (end - cursor)],
            );
            cursor = end;
        }
        Some(OwnedBytes::new(output))
    }
}

/// Directory that pins asynchronously warmed ranges for one index search.
///
/// Lookup/Payload page caches provide capacity-bounded cross-query reuse. This layer prevents
/// those pages from being evicted between asynchronous warmup and synchronous Tantivy search.
/// Sync misses return `WouldBlock`; pinned ranges are released when this directory is dropped.
#[derive(Clone)]
pub struct SearchPinDirectory {
    underlying: Arc<dyn Directory>,
    files: Arc<RwLock<HashMap<PathBuf, FileRangeCache>>>,
}

impl SearchPinDirectory {
    /// Creates an empty range pin for one index search.
    pub fn new(underlying: Arc<dyn Directory>) -> Self {
        Self {
            underlying,
            files: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn file_cache(&self, path: &Path) -> FileRangeCache {
        if let Some(cache) = self
            .files
            .read()
            .expect("search pin lock poisoned")
            .get(path)
            .cloned()
        {
            return cache;
        }
        self.files
            .write()
            .expect("search pin lock poisoned")
            .entry(path.to_path_buf())
            .or_default()
            .clone()
    }

    /// Inserts an externally fetched range during search warmup.
    pub fn insert(
        &self,
        path: impl Into<PathBuf>,
        range: Range<usize>,
        bytes: OwnedBytes,
    ) -> io::Result<()> {
        let path = path.into();
        self.file_cache(&path).insert(range, bytes)
    }
}

impl fmt::Debug for SearchPinDirectory {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("SearchPinDirectory")
            .field("underlying", &self.underlying)
            .finish_non_exhaustive()
    }
}

struct SearchPinFileHandle {
    underlying: Arc<dyn FileHandle>,
    cache: FileRangeCache,
    path: PathBuf,
}

impl fmt::Debug for SearchPinFileHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("SearchPinFileHandle")
            .field("path", &self.path)
            .finish()
    }
}

impl HasLen for SearchPinFileHandle {
    fn len(&self) -> usize {
        self.underlying.len()
    }
}

#[async_trait]
impl FileHandle for SearchPinFileHandle {
    fn read_bytes(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        self.cache.get(range.clone()).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::WouldBlock,
                format!(
                    "search range {}..{} for {} was not warmed",
                    range.start,
                    range.end,
                    self.path.display()
                ),
            )
        })
    }

    async fn read_bytes_async(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        if let Some(bytes) = self.cache.get(range.clone()) {
            return Ok(bytes);
        }
        let bytes = self.underlying.read_bytes_async(range.clone()).await?;
        self.cache.insert(range, bytes.clone())?;
        Ok(bytes)
    }
}

impl Directory for SearchPinDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        Ok(Arc::new(SearchPinFileHandle {
            underlying: self.underlying.get_file_handle(path)?,
            cache: self.file_cache(path),
            path: path.to_path_buf(),
        }))
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        let handle = self.get_file_handle(path)?;
        handle
            .read_bytes(0..handle.len())
            .map(|bytes| bytes.as_ref().to_vec())
            .map_err(|error| OpenReadError::wrap_io_error(error, path.to_path_buf()))
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        self.underlying.exists(path)
    }

    read_only_directory!();
}

#[cfg(test)]
mod tests {
    use tantivy::directory::RamDirectory;

    use super::*;

    #[test]
    fn test_sync_reads_require_warmup() {
        let directory = RamDirectory::default();
        directory
            .atomic_write(Path::new("file"), b"abcdef")
            .unwrap();
        let pin = SearchPinDirectory::new(Arc::new(directory));
        let file = pin.open_read(Path::new("file")).unwrap();
        assert_eq!(
            file.read_bytes_slice(2..4).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        pin.insert("file", 1..5, OwnedBytes::new(b"bcde".to_vec()))
            .unwrap();
        assert_eq!(file.read_bytes_slice(2..4).unwrap().as_ref(), b"cd");
    }

    /// The pinned ranges as a list scanned in full: what `FileRangeCache` must answer.
    #[derive(Default)]
    struct ListModel(Vec<Range<usize>>);

    impl ListModel {
        fn insert(&mut self, range: Range<usize>) {
            if self
                .0
                .iter()
                .any(|cached| cached.start <= range.start && cached.end >= range.end)
            {
                return;
            }
            self.0
                .retain(|cached| !(range.start <= cached.start && range.end >= cached.end));
            self.0.push(range);
        }

        /// Whether `range` is covered, by one range or by adjacent ones.
        fn covers(&self, range: Range<usize>) -> bool {
            let mut cursor = range.start;
            while cursor < range.end {
                match self
                    .0
                    .iter()
                    .filter(|cached| cached.start <= cursor && cached.end > cursor)
                    .map(|cached| cached.end)
                    .max()
                {
                    Some(end) => cursor = end,
                    None => return false,
                }
            }
            true
        }
    }

    #[test]
    fn test_file_range_cache_matches_list() {
        const LEN: usize = 64;
        let data = (0..LEN).map(|i| i as u8).collect::<Vec<_>>();
        // A fixed linear congruential sequence: the test is deterministic.
        let mut state = 0x9e37_79b9_7f4a_7c15_u64;
        let mut next = |bound: usize| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (state >> 33) as usize % bound
        };
        for _ in 0..200 {
            let cache = FileRangeCache::default();
            let mut model = ListModel::default();
            for _ in 0..40 {
                let start = next(LEN + 1);
                let end = start + next(LEN + 1 - start).min(12);
                if next(2) == 0 {
                    cache
                        .insert(start..end, OwnedBytes::new(data[start..end].to_vec()))
                        .unwrap();
                    model.insert(start..end);
                    let pinned = cache.0.read().unwrap();
                    let mut want = model.0.clone();
                    want.sort_by_key(|r| r.start);
                    let got = pinned.values().map(|c| c.range.clone()).collect::<Vec<_>>();
                    assert_eq!(got, want);
                } else {
                    let got = cache.get(start..end);
                    if start == end || model.covers(start..end) {
                        assert_eq!(got.unwrap().as_ref(), &data[start..end], "{start}..{end}");
                    } else {
                        assert!(got.is_none(), "{start}..{end}");
                    }
                }
            }
        }
    }
}
