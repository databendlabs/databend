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

/// The pinned ranges of each file, by path.
#[derive(Clone, Debug, Default)]
struct PinnedFiles(Arc<RwLock<HashMap<PathBuf, FileRangeCache>>>);

impl PinnedFiles {
    fn file_cache(&self, path: &Path) -> FileRangeCache {
        if let Some(cache) = self
            .0
            .read()
            .expect("search pin lock poisoned")
            .get(path)
            .cloned()
        {
            return cache;
        }
        self.0
            .write()
            .expect("search pin lock poisoned")
            .entry(path.to_path_buf())
            .or_default()
            .clone()
    }
}

/// The ranges that the searches through a shared pin warm in the files it does not retain,
/// for the search running in this task, by pin.
#[derive(Default)]
struct SearchScope(RwLock<HashMap<usize, PinnedFiles>>);

impl SearchScope {
    fn files(&self, pin: usize) -> PinnedFiles {
        if let Some(files) = self.0.read().expect("search scope lock poisoned").get(&pin) {
            return files.clone();
        }
        self.0
            .write()
            .expect("search scope lock poisoned")
            .entry(pin)
            .or_default()
            .clone()
    }
}

tokio::task_local! {
    static SEARCH_SCOPE: SearchScope;
}

/// Runs one search: the ranges it warms through shared pins, in the files they do not retain,
/// are pinned until it completes. The synchronous search must run inside `search` too.
pub async fn with_search_scope<F: Future>(search: F) -> F::Output {
    SEARCH_SCOPE.scope(SearchScope::default(), search).await
}

/// Directory that pins asynchronously warmed ranges for index searches.
///
/// Lookup/Payload page caches provide capacity-bounded cross-query reuse. This layer prevents
/// those pages from being evicted between asynchronous warmup and synchronous Tantivy search.
/// Sync misses return `WouldBlock`.
///
/// A pin made by `new` serves one search, and releases its ranges when dropped. One made by
/// `shared` serves the searches of an index opened once: it keeps the ranges of the files it
/// retains while it lives, and those of the other files for the search that warmed them, in
/// `with_search_scope`.
#[derive(Clone)]
pub struct SearchPinDirectory {
    underlying: Arc<dyn Directory>,
    files: PinnedFiles,
    /// For a shared pin, the files whose ranges it keeps for all of its searches.
    retains: Option<fn(&Path) -> bool>,
}

impl SearchPinDirectory {
    /// Creates an empty range pin for one index search.
    pub fn new(underlying: Arc<dyn Directory>) -> Self {
        Self {
            underlying,
            files: PinnedFiles::default(),
            retains: None,
        }
    }

    /// Creates an empty range pin shared by the searches of an index, keeping the ranges of the
    /// files `retains` accepts for all of them.
    pub fn shared(underlying: Arc<dyn Directory>, retains: fn(&Path) -> bool) -> Self {
        Self {
            underlying,
            files: PinnedFiles::default(),
            retains: Some(retains),
        }
    }

    fn pinned_ranges(&self, path: &Path) -> PinnedRanges {
        match self.retains {
            Some(retains) if !retains(path) => PinnedRanges::Search {
                pin: Arc::as_ptr(&self.files.0) as usize,
                path: path.to_path_buf(),
            },
            _ => PinnedRanges::Kept(self.files.file_cache(path)),
        }
    }

    /// Inserts an externally fetched range during search warmup.
    pub fn insert(
        &self,
        path: impl Into<PathBuf>,
        range: Range<usize>,
        bytes: OwnedBytes,
    ) -> io::Result<()> {
        let path = path.into();
        self.pinned_ranges(&path).insert(range, bytes)
    }
}

/// Where the pinned ranges of a file are.
#[derive(Clone, Debug)]
enum PinnedRanges {
    /// Kept by the pin.
    Kept(FileRangeCache),
    /// Kept for the search running in the task, of a shared pin's file it does not retain.
    Search { pin: usize, path: PathBuf },
}

impl PinnedRanges {
    /// The ranges, unless they are a search's and no search runs in the task.
    fn cache(&self) -> Option<FileRangeCache> {
        match self {
            PinnedRanges::Kept(cache) => Some(cache.clone()),
            PinnedRanges::Search { pin, path } => SEARCH_SCOPE
                .try_with(|scope| scope.files(*pin).file_cache(path))
                .ok(),
        }
    }

    fn get(&self, range: Range<usize>) -> Option<OwnedBytes> {
        self.cache()?.get(range)
    }

    fn insert(&self, range: Range<usize>, bytes: OwnedBytes) -> io::Result<()> {
        match self.cache() {
            Some(cache) => cache.insert(range, bytes),
            None => Err(io::Error::other(
                "a shared search pin is warmed outside of a search scope",
            )),
        }
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
    ranges: PinnedRanges,
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
        self.ranges.get(range.clone()).ok_or_else(|| {
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
        let cache = self.ranges.cache();
        if let Some(bytes) = cache.as_ref().and_then(|cache| cache.get(range.clone())) {
            return Ok(bytes);
        }
        let bytes = self.underlying.read_bytes_async(range.clone()).await?;
        // Outside of a search scope, a shared pin has nowhere to keep a search's range.
        if let Some(cache) = cache {
            cache.insert(range, bytes.clone())?;
        }
        Ok(bytes)
    }
}

impl Directory for SearchPinDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        Ok(Arc::new(SearchPinFileHandle {
            underlying: self.underlying.get_file_handle(path)?,
            ranges: self.pinned_ranges(path),
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

    fn retains_kept(path: &Path) -> bool {
        path == Path::new("kept")
    }

    fn shared_pin() -> SearchPinDirectory {
        let directory = RamDirectory::default();
        for name in ["kept", "scoped"] {
            directory.atomic_write(Path::new(name), b"abcdef").unwrap();
        }
        SearchPinDirectory::shared(Arc::new(directory), retains_kept)
    }

    /// A shared pin keeps the ranges of the files it retains for every search, and those of
    /// the other files for the search that warmed them only.
    #[tokio::test]
    async fn test_shared_pin_keeps_search_ranges_for_their_search() {
        let pin = shared_pin();
        let kept = pin.open_read(Path::new("kept")).unwrap();
        let scoped = pin.open_read(Path::new("scoped")).unwrap();
        with_search_scope(async {
            kept.slice(0..2).read_bytes_async().await.unwrap();
            scoped.slice(2..4).read_bytes_async().await.unwrap();
            assert_eq!(kept.read_bytes_slice(0..2).unwrap().as_ref(), b"ab");
            assert_eq!(scoped.read_bytes_slice(2..4).unwrap().as_ref(), b"cd");
        })
        .await;

        // A later search reads the retained range, not the other search's.
        with_search_scope(async {
            assert_eq!(kept.read_bytes_slice(0..2).unwrap().as_ref(), b"ab");
            let error = scoped.read_bytes_slice(2..4).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
        })
        .await;

        // Outside of a search, a range of a file it does not retain is read but not kept.
        assert_eq!(kept.read_bytes_slice(0..2).unwrap().as_ref(), b"ab");
        let bytes = scoped.slice(2..4).read_bytes_async().await.unwrap();
        assert_eq!(bytes.as_ref(), b"cd");
        let error = scoped.read_bytes_slice(2..4).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
        assert!(
            pin.insert("scoped", 0..2, OwnedBytes::new(b"ab".to_vec()))
                .is_err()
        );
    }

    /// Concurrent searches through one shared pin do not see the ranges of each other.
    #[tokio::test]
    async fn test_shared_pin_separates_concurrent_searches() {
        let pin = shared_pin();
        let scoped = pin.open_read(Path::new("scoped")).unwrap();
        let (warmed, other) = tokio::sync::oneshot::channel();
        let first = with_search_scope(async {
            scoped.slice(0..2).read_bytes_async().await.unwrap();
            warmed.send(()).unwrap();
            tokio::task::yield_now().await;
            scoped.read_bytes_slice(0..2).map(|bytes| bytes.to_vec())
        });
        let second = with_search_scope(async {
            other.await.unwrap();
            scoped.read_bytes_slice(0..2).map(|bytes| bytes.to_vec())
        });
        let (first, second) = tokio::join!(first, second);
        assert_eq!(first.unwrap(), b"ab");
        assert_eq!(second.unwrap_err().kind(), io::ErrorKind::WouldBlock);
    }

    fn pinned(cache: &FileRangeCache) -> Vec<Range<usize>> {
        let ranges = cache.0.read().unwrap();
        ranges.values().map(|cached| cached.range.clone()).collect()
    }

    #[test]
    fn test_file_range_cache_cases() {
        let data = (0..32u8).collect::<Vec<_>>();
        let bytes = |range: Range<usize>| OwnedBytes::new(data[range].to_vec());
        let cache = FileRangeCache::default();
        cache.insert(4..8, bytes(4..8)).unwrap();
        cache.insert(10..14, bytes(10..14)).unwrap();

        // A contained range changes nothing.
        cache.insert(5..7, bytes(5..7)).unwrap();
        assert_eq!(pinned(&cache), vec![4..8, 10..14]);
        // A range with the same start replaces the shorter one.
        cache.insert(10..16, bytes(10..16)).unwrap();
        assert_eq!(pinned(&cache), vec![4..8, 10..16]);
        // A covering range replaces every range it contains.
        cache.insert(2..18, bytes(2..18)).unwrap();
        assert_eq!(pinned(&cache), vec![2..18]);

        // Reads inside one range, across adjacent ranges, and with a gap.
        cache.insert(18..20, bytes(18..20)).unwrap();
        cache.insert(22..24, bytes(22..24)).unwrap();
        assert_eq!(cache.get(3..9).unwrap().as_ref(), &data[3..9]);
        assert_eq!(cache.get(16..20).unwrap().as_ref(), &data[16..20]);
        assert!(cache.get(16..23).is_none());
        assert!(cache.get(0..4).is_none());
        assert!(cache.get(5..5).unwrap().is_empty());
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
