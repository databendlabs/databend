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

use std::collections::HashMap;
use std::fmt;
use std::io;
use std::ops::Range;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::Mutex;
use std::sync::Weak;
use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_metrics::storage::metrics_inc_block_inverted_index_read_bytes;
use databend_common_metrics::storage::metrics_inc_block_inverted_index_read_milliseconds;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_cache::InvertedIndexFullPayloadCache;
use databend_storages_common_cache::InvertedIndexLookupCache;
use databend_storages_common_cache::InvertedIndexMetaCache;
use databend_storages_common_cache::InvertedIndexPayloadCache;
use databend_storages_common_index::BundleExternalFiles;
use databend_storages_common_index::BundleFileRanges;
use databend_storages_common_index::FooterDirectory;
use databend_storages_common_index::INVERTED_INDEX_BUNDLE_INITIAL_FOOTER_READ_SIZE;
use databend_storages_common_index::InvertedIndexBundleFooter;
use databend_storages_common_index::InvertedIndexLookupBytes;
use databend_storages_common_index::InvertedIndexMeta;
use databend_storages_common_index::InvertedIndexPayloadBytes;
use databend_storages_common_index::SearchPinDirectory;
use databend_storages_common_index::inverted_index_meta_cache_key;
use opendal::Operator;
use tantivy::Directory;
use tantivy::HasLen;
use tantivy::directory::DirectoryLock;
use tantivy::directory::FileHandle;
use tantivy::directory::Lock;
use tantivy::directory::OwnedBytes;
use tantivy::directory::WatchCallback;
use tantivy::directory::WatchHandle;
use tantivy::directory::WritePtr;
use tantivy::directory::error::DeleteError;
use tantivy::directory::error::LockError;
use tantivy::directory::error::OpenReadError;
use tantivy::directory::error::OpenWriteError;

const CACHE_PAGE_SIZE: usize = 64 * 1024;
const MAX_MERGED_READ_SIZE: usize = 1024 * 1024;
const MAX_MERGED_PAGES: usize = MAX_MERGED_READ_SIZE / CACHE_PAGE_SIZE;
const MAX_FULL_LOOKUP_CACHE_SIZE: usize = MAX_MERGED_READ_SIZE;
const LOOKUP_FULL_CACHE_KEY_PREFIX: &str = "ii-lookup-full-v1:";
const LOOKUP_PAGE_CACHE_KEY_PREFIX: &str = "ii-lookup-page-v1:";
const PAYLOAD_CACHE_KEY_PREFIX: &str = "ii-payload-page-v1:";
const FULL_PAYLOAD_CACHE_KEY_PREFIX: &str = "ii-payload-full-v1:";

fn record_inverted_index_read(bytes: usize, elapsed: std::time::Duration) {
    metrics_inc_block_inverted_index_read_bytes(u64::try_from(bytes).unwrap_or(u64::MAX));
    metrics_inc_block_inverted_index_read_milliseconds(
        u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX),
    );
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RangeCachePolicy {
    LookupWhole,
    Pages(PageCacheKind),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PageCacheKind {
    Lookup,
    Payload,
}

fn component_name(path: &Path) -> &str {
    path.extension()
        .and_then(|extension| extension.to_str())
        .unwrap_or("file")
}

fn range_cache_policy(component: &str, file_len: u64) -> RangeCachePolicy {
    // Policy is a function of component and file length only, so a later read uses the same cache
    // as the tail prefill. Small `.fieldnorm` / `.fast` files live entirely in Lookup; large ones
    // are Payload pages. They are never stored in both caches.
    match component {
        "term" if file_len <= MAX_FULL_LOOKUP_CACHE_SIZE as u64 => RangeCachePolicy::LookupWhole,
        "term" => RangeCachePolicy::Pages(PageCacheKind::Lookup),
        "fieldnorm" | "fast" if file_len <= MAX_FULL_LOOKUP_CACHE_SIZE as u64 => {
            RangeCachePolicy::LookupWhole
        }
        _ => RangeCachePolicy::Pages(PageCacheKind::Payload),
    }
}

fn whole_lookup_cache_key(location: &str, component: &str, file_id: usize) -> String {
    format!("{LOOKUP_FULL_CACHE_KEY_PREFIX}{location}:{component}:{file_id}")
}

type FetchLock = tokio::sync::Mutex<()>;
static FETCH_LOCKS: LazyLock<Mutex<HashMap<String, Weak<FetchLock>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn fetch_lock(key: String) -> Arc<FetchLock> {
    let mut locks = FETCH_LOCKS
        .lock()
        .expect("inverted-index fetch lock map poisoned");
    if let Some(lock) = locks.get(&key).and_then(Weak::upgrade) {
        return lock;
    }
    locks.retain(|_, lock| lock.strong_count() > 0);
    let lock = Arc::new(FetchLock::new(()));
    locks.insert(key, Arc::downgrade(&lock));
    lock
}

fn page_fetch_lock(
    location: &str,
    file_id: usize,
    kind: PageCacheKind,
    page_no: usize,
) -> Arc<FetchLock> {
    let domain = match kind {
        PageCacheKind::Lookup => "lookup",
        PageCacheKind::Payload => "payload",
    };
    fetch_lock(format!("page:{location}:{domain}:{file_id}:{page_no}"))
}

fn full_payload_fetch_lock(location: &str, file_id: usize) -> Arc<FetchLock> {
    fetch_lock(format!("full-payload:{location}:{file_id}"))
}

fn footer_fetch_lock(location: &str) -> Arc<FetchLock> {
    fetch_lock(format!("footer:{location}"))
}

#[derive(Clone)]
struct RemoteBundleDirectory {
    operator: Operator,
    location: Arc<str>,
    file_ranges: BundleFileRanges,
    external_files: BundleExternalFiles,
    lookup_cache: Option<InvertedIndexLookupCache>,
    payload_cache: Option<InvertedIndexPayloadCache>,
    full_payload_cache: Option<InvertedIndexFullPayloadCache>,
}

impl RemoteBundleDirectory {
    /// Object holding `path`, its byte range inside that object, and a bundle-wide file id used
    /// by cache keys and fetch locks (inline files first, then external files).
    fn locate(&self, path: &Path) -> Option<(Arc<str>, Range<u64>, usize)> {
        let mut file_id = 0;
        for (candidate, range) in &self.file_ranges.files {
            if candidate == path {
                return Some((self.location.clone(), range.clone(), file_id));
            }
            file_id += 1;
        }
        for (candidate, external) in &self.external_files.files {
            if candidate == path {
                let object = Arc::from(format!("{}{}", self.location, external.suffix));
                return Some((object, 0..external.len, file_id));
            }
            file_id += 1;
        }
        None
    }
}

impl fmt::Debug for RemoteBundleDirectory {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("RemoteBundleDirectory")
            .field("location", &self.location)
            .field("files", &self.file_ranges.files.len())
            .finish()
    }
}

struct RemoteBundleFileHandle {
    operator: Operator,
    /// Bundle location; cache keys and fetch locks are scoped by it.
    location: Arc<str>,
    /// Object actually read: the bundle itself or a sibling object for an external file.
    object: Arc<str>,
    file_range: Range<u64>,
    file_len: usize,
    file_id: usize,
    cache_policy: RangeCachePolicy,
    lookup_cache: Option<InvertedIndexLookupCache>,
    payload_cache: Option<InvertedIndexPayloadCache>,
    full_payload_cache: Option<InvertedIndexFullPayloadCache>,
    path: PathBuf,
    component_name: String,
}

impl fmt::Debug for RemoteBundleFileHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("RemoteBundleFileHandle")
            .field("path", &self.path)
            .field("component_name", &self.component_name)
            .field("file_range", &self.file_range)
            .finish()
    }
}

impl HasLen for RemoteBundleFileHandle {
    fn len(&self) -> usize {
        self.file_len
    }
}

impl RemoteBundleFileHandle {
    fn page_range(&self, page_no: usize) -> io::Result<Range<usize>> {
        let start = page_no.checked_mul(CACHE_PAGE_SIZE).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "inverted-index page offset overflow",
            )
        })?;
        let end = start.saturating_add(CACHE_PAGE_SIZE).min(self.len());
        Ok(start..end)
    }

    fn whole_lookup_key(&self) -> String {
        whole_lookup_cache_key(&self.location, &self.component_name, self.file_id)
    }

    fn full_payload_key(&self) -> String {
        format!(
            "{FULL_PAYLOAD_CACHE_KEY_PREFIX}{}:{}:{}",
            self.location, self.component_name, self.file_id
        )
    }

    fn cached_full_payload(&self) -> Option<Bytes> {
        let cache = self.full_payload_cache.as_ref()?;
        let key = self.full_payload_key();
        let data = cache.get(&key)?.data.clone();
        if data.len() != self.len() {
            cache.evict(&key);
            return None;
        }
        Some(data)
    }

    fn page_key(&self, kind: PageCacheKind, page_no: usize) -> String {
        let prefix = match kind {
            PageCacheKind::Lookup => LOOKUP_PAGE_CACHE_KEY_PREFIX,
            PageCacheKind::Payload => PAYLOAD_CACHE_KEY_PREFIX,
        };
        format!(
            "{prefix}{}:{}:{}:{page_no}",
            self.location, self.component_name, self.file_id
        )
    }

    fn cached_page(&self, kind: PageCacheKind, page_no: usize) -> io::Result<Option<Bytes>> {
        let key = self.page_key(kind, page_no);
        let expected_len = self.page_range(page_no)?.len();
        let data = match kind {
            PageCacheKind::Lookup => self.lookup_cache.get(&key).map(|value| value.data.clone()),
            PageCacheKind::Payload => self.payload_cache.get(&key).map(|value| value.data.clone()),
        };
        let Some(data) = data else {
            return Ok(None);
        };
        if data.len() != expected_len {
            match kind {
                PageCacheKind::Lookup => {
                    self.lookup_cache.evict(&key);
                }
                PageCacheKind::Payload => {
                    self.payload_cache.evict(&key);
                }
            }
            return Ok(None);
        }
        Ok(Some(data))
    }

    fn insert_page(&self, kind: PageCacheKind, page_no: usize, data: Bytes) -> Bytes {
        let key = self.page_key(kind, page_no);
        match kind {
            PageCacheKind::Lookup => self
                .lookup_cache
                .insert(key, InvertedIndexLookupBytes::new(data))
                .data
                .clone(),
            PageCacheKind::Payload => self
                .payload_cache
                .insert(key, InvertedIndexPayloadBytes::new(data))
                .data
                .clone(),
        }
    }

    async fn fetch_range(&self, range: Range<usize>) -> io::Result<Bytes> {
        let logical_start = u64::try_from(range.start).map_err(io::Error::other)?;
        let logical_end = u64::try_from(range.end).map_err(io::Error::other)?;
        let absolute_start = self
            .file_range
            .start
            .checked_add(logical_start)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bundle range overflow"))?;
        let absolute_end = self
            .file_range
            .start
            .checked_add(logical_end)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bundle range overflow"))?;
        let start = Instant::now();
        let data = self
            .operator
            .read_with(self.object.as_ref())
            .range(absolute_start..absolute_end)
            .await
            .map_err(io::Error::other)?
            .to_bytes();
        if data.len() != range.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "object storage returned a short inverted-index range",
            ));
        }
        record_inverted_index_read(data.len(), start.elapsed());
        Ok(data)
    }

    async fn fetch_page_group(
        &self,
        kind: PageCacheKind,
        first_page: usize,
        last_page: usize,
    ) -> io::Result<Vec<(usize, Bytes)>> {
        let logical_start = self.page_range(first_page)?.start;
        let logical_end = self.page_range(last_page)?.end;
        let data = self.fetch_range(logical_start..logical_end).await?;

        let mut pages = Vec::with_capacity(last_page - first_page + 1);
        for page_no in first_page..=last_page {
            let page_range = self.page_range(page_no)?;
            let start = page_range.start - logical_start;
            let end = page_range.end - logical_start;
            // Copy each page so evicting one page does not retain the complete merged read.
            pages.push((
                page_no,
                self.insert_page(kind, page_no, Bytes::copy_from_slice(&data[start..end])),
            ));
        }
        Ok(pages)
    }

    fn is_full_postings_read(&self, range: &Range<usize>) -> bool {
        range.start == 0
            && range.end == self.len()
            && matches!(self.component_name.as_str(), "idx" | "pos")
    }

    async fn read_full_postings_file(&self) -> io::Result<OwnedBytes> {
        // The warmup planner explicitly requests the complete physical `.idx` / `.pos` through
        // the raw bundle directory, not Tantivy's footer-stripped field slices. Preserve one
        // full-read singleflight per component instead of translating it into page requests.
        let fetch_lock = full_payload_fetch_lock(&self.location, self.file_id);
        let _guard = fetch_lock.lock().await;
        if let Some(data) = self.cached_full_payload() {
            return Ok(OwnedBytes::new(data.to_vec()));
        }
        let data = self.fetch_range(0..self.len()).await?;
        // Full components are stored as one entry in a separate memory pool. Oversized files
        // are only pinned for this search; they must not evict hot payload pages.
        if let Some(cache) = &self.full_payload_cache {
            if data
                .len()
                .saturating_add(std::mem::size_of::<InvertedIndexPayloadBytes>())
                <= cache.bytes_capacity() as usize
            {
                cache.insert(
                    self.full_payload_key(),
                    InvertedIndexPayloadBytes::new(data.clone()),
                );
            }
        }
        Ok(OwnedBytes::new(Vec::<u8>::from(data)))
    }

    async fn read_cached_lookup_file(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        let key = self.whole_lookup_key();
        if let Some(value) = self.lookup_cache.get(&key)
            && value.data.len() == self.len()
        {
            return Ok(OwnedBytes::new(value.data.slice(range).to_vec()));
        }

        let fetch_lock = page_fetch_lock(&self.location, self.file_id, PageCacheKind::Lookup, 0);
        let _guard = fetch_lock.lock().await;
        // Recheck after waiting for another fetch. Only evict invalid entries under the lock,
        // so a stale lock-free lookup cannot remove a component just populated by another reader.
        if let Some(value) = self.lookup_cache.get(&key) {
            if value.data.len() == self.len() {
                return Ok(OwnedBytes::new(value.data.slice(range).to_vec()));
            }
            self.lookup_cache.evict(&key);
        }

        let data = self.fetch_range(0..self.len()).await?;
        self.lookup_cache
            .insert(key, InvertedIndexLookupBytes::new(data.clone()));
        Ok(OwnedBytes::new(data.slice(range).to_vec()))
    }

    /// Dispatch whole-file, full-payload, paged, and uncached reads before entering page logic.
    async fn read_range(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        if range.end > self.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "inverted-index range exceeds logical file length",
            ));
        }
        if range.is_empty() {
            return Ok(OwnedBytes::empty());
        }

        match self.cache_policy {
            RangeCachePolicy::LookupWhole if self.lookup_cache.is_some() => {
                return self.read_cached_lookup_file(range).await;
            }
            RangeCachePolicy::Pages(kind) => {
                if kind == PageCacheKind::Payload {
                    if let Some(data) = self.cached_full_payload() {
                        return Ok(OwnedBytes::new(data.slice(range).to_vec()));
                    }
                    if self.is_full_postings_read(&range)
                        && (self.payload_cache.is_some() || self.full_payload_cache.is_some())
                    {
                        return self.read_full_postings_file().await;
                    }
                }
                let pages_enabled = match kind {
                    PageCacheKind::Lookup => self.lookup_cache.is_some(),
                    PageCacheKind::Payload => self.payload_cache.is_some(),
                };
                if pages_enabled {
                    return self.read_pages(kind, range).await;
                }
            }
            RangeCachePolicy::LookupWhole => {}
        }

        self.fetch_range(range)
            .await
            .map(|data| OwnedBytes::new(data.to_vec()))
    }

    async fn read_pages(&self, kind: PageCacheKind, range: Range<usize>) -> io::Result<OwnedBytes> {
        let first_page = range.start / CACHE_PAGE_SIZE;
        let last_page = (range.end - 1) / CACHE_PAGE_SIZE;
        // Retain pages for this request even if the global cache evicts them before assembly.
        let mut request_pages = HashMap::new();
        for page_no in first_page..=last_page {
            if let Some(page) = self.cached_page(kind, page_no)? {
                request_pages.insert(page_no, page);
            }
        }
        self.fill_missing_pages(kind, first_page, last_page, &mut request_pages)
            .await?;

        let mut output = Vec::with_capacity(range.len());
        for page_no in first_page..=last_page {
            let page = request_pages.get(&page_no).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    "inverted-index page was not available for the current request",
                )
            })?;
            let page_range = self.page_range(page_no)?;
            let copy_start = range.start.max(page_range.start) - page_range.start;
            let copy_end = range.end.min(page_range.end) - page_range.start;
            output.extend_from_slice(&page[copy_start..copy_end]);
        }
        Ok(OwnedBytes::new(output))
    }

    async fn fill_missing_pages(
        &self,
        kind: PageCacheKind,
        first_page: usize,
        last_page: usize,
        request_pages: &mut HashMap<usize, Bytes>,
    ) -> io::Result<()> {
        let mut page_no = first_page;
        while page_no <= last_page {
            if request_pages.contains_key(&page_no) {
                page_no += 1;
                continue;
            }
            let missing_start = page_no;
            while page_no < last_page
                && page_no - missing_start + 1 < MAX_MERGED_PAGES
                && !request_pages.contains_key(&(page_no + 1))
            {
                page_no += 1;
            }
            self.fetch_missing_run(kind, missing_start, page_no, request_pages)
                .await?;
            page_no += 1;
        }
        Ok(())
    }

    async fn fetch_missing_run(
        &self,
        kind: PageCacheKind,
        first_page: usize,
        last_page: usize,
        request_pages: &mut HashMap<usize, Bytes>,
    ) -> io::Result<()> {
        // Always acquire locks in ascending order, including for overlapping requests.
        let mut _guards = Vec::with_capacity(last_page - first_page + 1);
        for page_no in first_page..=last_page {
            let fetch_lock = page_fetch_lock(&self.location, self.file_id, kind, page_no);
            _guards.push(fetch_lock.lock_owned().await);
        }

        // Another request may have filled some pages while we waited. Retain each hit, then
        // group the remaining holes without probing the global cache again during the scan.
        for page_no in first_page..=last_page {
            if let Some(page) = self.cached_page(kind, page_no)? {
                request_pages.insert(page_no, page);
            }
        }
        let mut page_no = first_page;
        while page_no <= last_page {
            if request_pages.contains_key(&page_no) {
                page_no += 1;
                continue;
            }
            let fetch_start = page_no;
            while page_no < last_page && !request_pages.contains_key(&(page_no + 1)) {
                page_no += 1;
            }
            request_pages.extend(self.fetch_page_group(kind, fetch_start, page_no).await?);
            page_no += 1;
        }
        Ok(())
    }
}

#[async_trait]
impl FileHandle for RemoteBundleFileHandle {
    fn read_bytes(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            format!(
                "remote inverted-index range {}..{} for {} must be warmed asynchronously",
                range.start,
                range.end,
                self.path.display()
            ),
        ))
    }

    async fn read_bytes_async(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        self.read_range(range).await
    }
}

impl Directory for RemoteBundleDirectory {
    fn get_file_handle(
        &self,
        path: &Path,
    ) -> std::result::Result<Arc<dyn FileHandle>, OpenReadError> {
        let (object, file_range, file_id) = self
            .locate(path)
            .ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))?;
        let file_len_u64 = file_range.end - file_range.start;
        let file_len = usize::try_from(file_len_u64).map_err(|error| {
            OpenReadError::wrap_io_error(io::Error::other(error), path.to_path_buf())
        })?;
        let component_name = component_name(path).to_owned();
        let cache_policy = range_cache_policy(&component_name, file_len_u64);
        Ok(Arc::new(RemoteBundleFileHandle {
            operator: self.operator.clone(),
            location: self.location.clone(),
            object,
            file_range,
            file_len,
            file_id,
            cache_policy,
            lookup_cache: self.lookup_cache.clone(),
            payload_cache: self.payload_cache.clone(),
            full_payload_cache: self.full_payload_cache.clone(),
            path: path.to_path_buf(),
            component_name,
        }))
    }

    fn atomic_read(&self, path: &Path) -> std::result::Result<Vec<u8>, OpenReadError> {
        Err(OpenReadError::wrap_io_error(
            io::Error::new(
                io::ErrorKind::WouldBlock,
                "remote atomic read must be served by the bundle footer",
            ),
            path.to_path_buf(),
        ))
    }

    fn exists(&self, path: &Path) -> std::result::Result<bool, OpenReadError> {
        Ok(self.file_ranges.contains(path) || self.external_files.contains(path))
    }

    fn delete(&self, path: &Path) -> std::result::Result<(), DeleteError> {
        Err(DeleteError::IoError {
            io_error: Arc::new(io::Error::new(io::ErrorKind::Unsupported, "read-only")),
            filepath: path.to_path_buf(),
        })
    }

    fn open_write(&self, path: &Path) -> std::result::Result<WritePtr, OpenWriteError> {
        Err(OpenWriteError::wrap_io_error(
            io::Error::new(io::ErrorKind::Unsupported, "read-only"),
            path.to_path_buf(),
        ))
    }

    fn atomic_write(&self, _path: &Path, _data: &[u8]) -> io::Result<()> {
        Err(io::Error::new(io::ErrorKind::Unsupported, "read-only"))
    }

    fn sync_directory(&self) -> io::Result<()> {
        Ok(())
    }

    fn watch(&self, _watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        Ok(WatchHandle::empty())
    }

    fn acquire_lock(&self, _lock: &Lock) -> std::result::Result<DirectoryLock, LockError> {
        Ok(DirectoryLock::from(Box::new(|| {})))
    }
}

pub(crate) struct BundleSearchDirectory {
    pub directory: FooterDirectory,
}

struct FooterCacheState {
    meta_cache: Option<InvertedIndexMetaCache>,
    lookup_cache: Option<InvertedIndexLookupCache>,
    meta_key: String,
    location: Arc<str>,
    object_size: u64,
}

impl FooterCacheState {
    fn new(
        location: &str,
        object_size: u64,
        meta_cache: Option<InvertedIndexMetaCache>,
        lookup_cache: Option<InvertedIndexLookupCache>,
    ) -> Self {
        Self {
            meta_cache,
            lookup_cache,
            meta_key: inverted_index_meta_cache_key(location),
            location: Arc::from(location),
            object_size,
        }
    }

    fn load(&self) -> Option<InvertedIndexBundleFooter> {
        let metadata = self.meta_cache.get(&self.meta_key)?;
        if metadata.object_size != self.object_size {
            self.meta_cache.evict(&self.meta_key);
            return None;
        }
        match InvertedIndexBundleFooter::open_footer_for_object(
            metadata.footer_bytes.as_ref(),
            self.object_size,
            None,
        ) {
            Ok(footer) => Some(footer),
            Err(_) => {
                self.meta_cache.evict(&self.meta_key);
                None
            }
        }
    }

    fn store(&self, footer_bytes: Bytes) {
        self.meta_cache.insert(
            self.meta_key.clone(),
            InvertedIndexMeta::new(self.object_size, footer_bytes),
        );
    }

    fn populate_small_lookup_cache(
        &self,
        tail: &Bytes,
        tail_start: u64,
        footer: &InvertedIndexBundleFooter,
    ) {
        // Only whole Lookup files that are fully covered by this tail are stored. `.idx` / `.pos`
        // / `.store` stay uncached even when the tail already contains them.
        if self.lookup_cache.is_none() {
            return;
        }
        for (file_id, (path, range)) in footer.file_ranges.files.iter().enumerate() {
            let Some(file_len) = range.end.checked_sub(range.start) else {
                continue;
            };
            let component = component_name(path);
            if range_cache_policy(component, file_len) != RangeCachePolicy::LookupWhole
                || range.start < tail_start
                || range.end > footer.footer_start
            {
                continue;
            }
            let Ok(start) = usize::try_from(range.start - tail_start) else {
                continue;
            };
            let Ok(end) = usize::try_from(range.end - tail_start) else {
                continue;
            };
            let Some(data) = tail.get(start..end) else {
                continue;
            };
            let key = whole_lookup_cache_key(&self.location, component, file_id);
            self.lookup_cache.insert(
                key,
                InvertedIndexLookupBytes::new(Bytes::copy_from_slice(data)),
            );
        }
    }

    async fn load_or_fetch(
        &self,
        operator: &Operator,
        location: &str,
    ) -> Result<InvertedIndexBundleFooter> {
        if let Some(footer) = self.load() {
            return Ok(footer);
        }
        if self.meta_cache.is_none() {
            return self.fetch_and_store(operator, location).await;
        }

        // Serialize cold recovery per immutable object. Recheck under the lock so waiters reuse
        // the first caller's complete cached footer instead of issuing another tail read.
        let fetch_lock = footer_fetch_lock(location);
        let _guard = fetch_lock.lock().await;
        if let Some(footer) = self.load() {
            return Ok(footer);
        }
        self.fetch_and_store(operator, location).await
    }

    async fn fetch_and_store(
        &self,
        operator: &Operator,
        location: &str,
    ) -> Result<InvertedIndexBundleFooter> {
        let initial_read_size = u64::try_from(INVERTED_INDEX_BUNDLE_INITIAL_FOOTER_READ_SIZE)
            .map_err(|_| ErrorCode::StorageOther("inverted-index footer read size is invalid"))?;
        let tail_start = self.object_size.saturating_sub(initial_read_size);
        let start = Instant::now();
        let tail = operator
            .read_with(location)
            .range(tail_start..self.object_size)
            .await?;
        record_inverted_index_read(tail.len(), start.elapsed());
        let tail_bytes = tail.to_bytes();
        let footer_start = InvertedIndexBundleFooter::footer_start_from_tail(
            tail_bytes.as_ref(),
            self.object_size,
            tail_start,
        )
        .map_err(|error| {
            ErrorCode::StorageOther(format!(
                "failed to inspect inverted-index footer for {location}: {error}"
            ))
        })?;

        let (footer_bytes, footer) = if footer_start >= tail_start {
            let (footer_bytes, footer) = InvertedIndexBundleFooter::open_footer_from_tail(
                tail_bytes.as_ref(),
                self.object_size,
                tail_start,
            )
            .map_err(|error| {
                ErrorCode::StorageOther(format!(
                    "failed to open inverted-index footer for {location}: {error}"
                ))
            })?;
            (Bytes::copy_from_slice(footer_bytes), footer)
        } else {
            let start = Instant::now();
            let footer_data = operator
                .read_with(location)
                .range(footer_start..self.object_size)
                .await?;
            record_inverted_index_read(footer_data.len(), start.elapsed());
            let footer_bytes = footer_data.to_bytes();
            let footer = InvertedIndexBundleFooter::open_footer_for_object(
                footer_bytes.as_ref(),
                self.object_size,
                Some(footer_start),
            )
            .map_err(|error| {
                ErrorCode::StorageOther(format!(
                    "failed to open inverted-index footer for {location}: {error}"
                ))
            })?;
            (footer_bytes, footer)
        };

        self.populate_small_lookup_cache(&tail_bytes, tail_start, &footer);
        self.store(footer_bytes);
        Ok(footer)
    }
}

pub(crate) async fn load_bundle_search_directory(
    operator: &Operator,
    location: &str,
    object_size: u64,
) -> Result<BundleSearchDirectory> {
    let cache_manager = CacheManager::instance();
    let lookup_cache = cache_manager.get_inverted_index_lookup_cache();
    let cache_state = FooterCacheState::new(
        location,
        object_size,
        cache_manager.get_inverted_index_meta_cache(),
        lookup_cache.clone(),
    );
    let footer = cache_state.load_or_fetch(operator, location).await?;

    let remote = RemoteBundleDirectory {
        operator: operator.clone(),
        location: Arc::from(location),
        file_ranges: footer.file_ranges.clone(),
        external_files: footer.external_files.clone(),
        lookup_cache,
        payload_cache: cache_manager.get_inverted_index_payload_cache(),
        full_payload_cache: cache_manager.get_inverted_index_full_payload_cache(),
    };
    let search_pin = SearchPinDirectory::new(Arc::new(remote));
    let directory = FooterDirectory::new(search_pin, footer);
    Ok(BundleSearchDirectory { directory })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use databend_storages_common_cache::HybridCache;
    use databend_storages_common_cache::InMemoryLruCache;
    use databend_storages_common_index::BundleOpenSlice;
    use opendal::raw::Access;
    use opendal::raw::Layer;
    use opendal::raw::LayeredAccess;
    use opendal::raw::OpList;
    use opendal::raw::OpRead;
    use opendal::raw::OpWrite;
    use opendal::raw::RpDelete;
    use opendal::raw::RpList;
    use opendal::raw::RpRead;
    use opendal::raw::RpWrite;
    use opendal::services::Memory;

    use super::*;

    #[derive(Clone, Debug, Default)]
    struct RecordingLayer {
        ranges: Arc<Mutex<Vec<Range<u64>>>>,
    }

    impl<A: Access> Layer<A> for RecordingLayer {
        type LayeredAccess = RecordingAccessor<A>;

        fn layer(&self, inner: A) -> Self::LayeredAccess {
            RecordingAccessor {
                inner,
                ranges: self.ranges.clone(),
            }
        }
    }

    #[derive(Clone, Debug)]
    struct RecordingAccessor<A: Access> {
        inner: A,
        ranges: Arc<Mutex<Vec<Range<u64>>>>,
    }

    impl<A: Access> LayeredAccess for RecordingAccessor<A> {
        type Inner = A;
        type Reader = A::Reader;
        type Writer = A::Writer;
        type Lister = A::Lister;
        type Deleter = A::Deleter;

        fn inner(&self) -> &Self::Inner {
            &self.inner
        }

        async fn read(&self, path: &str, args: OpRead) -> opendal::Result<(RpRead, Self::Reader)> {
            let range = args.range();
            if let Some(size) = range.size() {
                self.ranges
                    .lock()
                    .unwrap()
                    .push(range.offset()..range.offset() + size);
            }
            self.inner.read(path, args).await
        }

        async fn write(
            &self,
            path: &str,
            args: OpWrite,
        ) -> opendal::Result<(RpWrite, Self::Writer)> {
            self.inner.write(path, args).await
        }

        async fn list(&self, path: &str, args: OpList) -> opendal::Result<(RpList, Self::Lister)> {
            self.inner.list(path, args).await
        }

        async fn delete(&self) -> opendal::Result<(RpDelete, Self::Deleter)> {
            self.inner.delete().await
        }
    }

    fn payload_cache(capacity: usize) -> Option<InvertedIndexPayloadCache> {
        HybridCache::new(
            "payload".to_string(),
            Some(InMemoryLruCache::with_bytes_capacity(
                "memory_payload".to_string(),
                capacity,
            )),
            None,
        )
    }

    fn full_payload_cache(capacity: usize) -> Option<InvertedIndexFullPayloadCache> {
        (capacity > 0).then(|| {
            InMemoryLruCache::with_bytes_capacity("memory_full_payload".to_string(), capacity)
        })
    }

    async fn payload_file_handle_with_full_capacity(
        location: &str,
        data: Vec<u8>,
        full_capacity: usize,
    ) -> Result<(RemoteBundleFileHandle, Arc<Mutex<Vec<Range<u64>>>>)> {
        let layer = RecordingLayer::default();
        let ranges = layer.ranges.clone();
        let operator = Operator::new(Memory::default())?.layer(layer).finish();
        operator.write(location, data.clone()).await?;
        Ok((
            RemoteBundleFileHandle {
                operator,
                location: Arc::from(location),
                object: Arc::from(location),
                file_range: 0..u64::try_from(data.len()).unwrap(),
                file_len: data.len(),
                file_id: 0,
                cache_policy: RangeCachePolicy::Pages(PageCacheKind::Payload),
                lookup_cache: None,
                payload_cache: payload_cache(data.len() * 2),
                full_payload_cache: full_payload_cache(full_capacity),
                path: PathBuf::from("segment.idx"),
                component_name: "idx".to_owned(),
            },
            ranges,
        ))
    }

    async fn payload_file_handle(
        location: &str,
        data: Vec<u8>,
    ) -> Result<(RemoteBundleFileHandle, Arc<Mutex<Vec<Range<u64>>>>)> {
        let full_capacity = data.len() * 2;
        payload_file_handle_with_full_capacity(location, data, full_capacity).await
    }

    fn footer_fixture() -> (Vec<u8>, u64, InvertedIndexBundleFooter) {
        let object = InvertedIndexBundleFooter::build(
            [("segment.idx", b"postings".as_slice())],
            BTreeMap::new(),
            BTreeMap::from([(PathBuf::from("segment.idx"), vec![BundleOpenSlice {
                range: 0..4,
                bytes: Arc::from(b"post".as_slice()),
            }])]),
            b"managed".to_vec(),
            b"meta".to_vec(),
        )
        .unwrap();
        let object_size = u64::try_from(object.len()).unwrap();
        let footer = InvertedIndexBundleFooter::open(&object).unwrap();
        (object, object_size, footer)
    }

    fn cache_state(location: &str, object_size: u64) -> FooterCacheState {
        let meta_cache = HybridCache::new(
            "meta".to_string(),
            Some(InMemoryLruCache::with_items_capacity(
                "memory_meta".to_string(),
                10,
            )),
            None,
        );
        let lookup_cache = HybridCache::new(
            "lookup".to_string(),
            Some(InMemoryLruCache::with_bytes_capacity(
                "memory_lookup".to_string(),
                4 * MAX_FULL_LOOKUP_CACHE_SIZE,
            )),
            None,
        );
        FooterCacheState::new(location, object_size, meta_cache, lookup_cache)
    }

    fn persisted_footer(object: &[u8], footer: &InvertedIndexBundleFooter) -> Bytes {
        Bytes::copy_from_slice(&object[usize::try_from(footer.footer_start).unwrap()..])
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_page_reads_merge_misses_and_reuse_cache() -> Result<()> {
        let requested_len = (MAX_MERGED_PAGES * 2 + 1) * CACHE_PAGE_SIZE;
        let data = (0..requested_len + CACHE_PAGE_SIZE)
            .map(|index| index as u8)
            .collect::<Vec<_>>();
        for kind in [PageCacheKind::Lookup, PageCacheKind::Payload] {
            let (mut file, ranges) = payload_file_handle("merged-pages", data.clone()).await?;
            file.cache_policy = RangeCachePolicy::Pages(kind);
            if kind == PageCacheKind::Lookup {
                file.path = PathBuf::from("segment.term");
                file.component_name = "term".to_owned();
                file.lookup_cache = HybridCache::new(
                    "lookup".to_string(),
                    Some(InMemoryLruCache::with_bytes_capacity(
                        "memory_lookup".to_string(),
                        data.len() * 2,
                    )),
                    None,
                );
            }
            for _ in 0..2 {
                assert_eq!(
                    file.read_range(0..requested_len).await?.as_ref(),
                    &data[..requested_len]
                );
            }
            assert!(file.cached_page(kind, 0)?.is_some());
            assert!(file.cached_full_payload().is_none());
            if kind == PageCacheKind::Lookup {
                assert!(file.cached_page(PageCacheKind::Payload, 0)?.is_none());
            }
            assert_eq!(*ranges.lock().unwrap(), vec![
                0..MAX_MERGED_READ_SIZE as u64,
                MAX_MERGED_READ_SIZE as u64..(2 * MAX_MERGED_READ_SIZE) as u64,
                (2 * MAX_MERGED_READ_SIZE) as u64..requested_len as u64,
            ]);
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_missing_run_rechecks_pages_after_waiting_for_lock() -> Result<()> {
        let data = (0..6 * CACHE_PAGE_SIZE)
            .map(|index| index as u8)
            .collect::<Vec<_>>();
        let (file, ranges) = payload_file_handle("rechecked-page-holes", data.clone()).await?;
        let lock = page_fetch_lock(&file.location, file.file_id, PageCacheKind::Payload, 0);
        let guard = lock.lock().await;
        let range = 7..5 * CACHE_PAGE_SIZE - 11;
        let read = file.read_range(range.clone());
        tokio::pin!(read);
        // Force the initial cache scan to finish, then populate pages while the read waits.
        assert!(futures::poll!(read.as_mut()).is_pending());
        for page_no in [2, 4] {
            file.insert_page(
                PageCacheKind::Payload,
                page_no,
                Bytes::copy_from_slice(&data[file.page_range(page_no)?]),
            );
        }
        drop(guard);

        assert_eq!(read.await?.as_ref(), &data[range]);
        assert_eq!(*ranges.lock().unwrap(), vec![
            0..(2 * CACHE_PAGE_SIZE) as u64,
            (3 * CACHE_PAGE_SIZE) as u64..(4 * CACHE_PAGE_SIZE) as u64,
        ]);
        Ok(())
    }

    // Build actual Tantivy managed/composite files rather than manufacturing a 0..len request.
    // Local caches keep this regression independent of process-wide cache state.
    async fn expansion_bundle_fixture() -> Result<(
        RemoteBundleDirectory,
        InvertedIndexBundleFooter,
        Arc<Mutex<Vec<Range<u64>>>>,
    )> {
        use databend_storages_common_index::collect_index_open_slices;
        use tantivy::Index;
        use tantivy::IndexSettings;
        use tantivy::directory::RamDirectory;
        use tantivy::schema::FAST;
        use tantivy::schema::Schema;
        use tantivy::schema::TEXT;

        let mut schema = Schema::builder();
        let body = schema.add_text_field("body", TEXT);
        let title = schema.add_text_field("title", TEXT);
        let num = schema.add_u64_field("num", FAST);
        let raw = RamDirectory::default();
        let index = Index::create(raw.clone(), schema.build(), IndexSettings::default())?;
        let mut writer = index.writer_with_num_threads(1, 50_000_000)?;
        for row in 0..4000 {
            let body_text = if row % 2 == 0 {
                "alpha beta"
            } else {
                "omega gamma"
            };
            let title_text = if row % 3 == 0 { "needle" } else { "other" };
            writer.add_document(
                tantivy::doc!(body => body_text, title => title_text, num => row as u64),
            )?;
        }
        writer.commit()?;
        drop(writer);

        let open_slices = collect_index_open_slices(raw.clone())?;
        let mut paths = index
            .directory()
            .list_managed_files()
            .into_iter()
            .collect::<Vec<_>>();
        paths.sort_unstable();
        let files = paths
            .into_iter()
            .filter(|path| {
                path != Path::new(".managed.json")
                    && path != Path::new("meta.json")
                    && raw.exists(path).unwrap()
            })
            .map(|path| raw.atomic_read(&path).map(|bytes| (path, bytes)))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let data = InvertedIndexBundleFooter::build(
            files,
            BTreeMap::new(),
            open_slices,
            raw.atomic_read(Path::new(".managed.json"))?,
            raw.atomic_read(Path::new("meta.json"))?,
        )?;
        let footer = InvertedIndexBundleFooter::open(&data)?;
        let layer = RecordingLayer::default();
        let ranges = layer.ranges.clone();
        let operator = Operator::new(Memory::default())?.layer(layer).finish();
        let location = "real-expansion-bundle";
        operator.write(location, data).await?;
        Ok((
            RemoteBundleDirectory {
                operator,
                location: Arc::from(location),
                file_ranges: footer.file_ranges.clone(),
                external_files: footer.external_files.clone(),
                lookup_cache: None,
                payload_cache: payload_cache(16 * 1024 * 1024),
                full_payload_cache: full_payload_cache(16 * 1024 * 1024),
            },
            footer,
            ranges,
        ))
    }

    async fn execute_expansion_bundle_query(
        remote: &RemoteBundleDirectory,
        footer: &InvertedIndexBundleFooter,
        text: &str,
        fuzziness: Option<u8>,
        has_score: bool,
    ) -> Result<Vec<u32>> {
        use tantivy::Index;
        use tantivy::collector::DocSetCollector;
        use tantivy::collector::TopDocs;
        use tantivy::query::QueryParser;
        use tantivy::query_grammar::parse_query;

        let directory = FooterDirectory::new(
            SearchPinDirectory::new(Arc::new(remote.clone())),
            footer.clone(),
        );
        let index = Index::open(directory.clone())?;
        let schema = index.schema();
        let body = schema.get_field("body")?;
        let mut parser = QueryParser::new(schema.clone(), vec![body], index.tokenizers().clone());
        if let Some(distance) = fuzziness {
            parser.set_field_fuzzy(body, false, distance, true);
        }
        let query = parser.parse_query(text)?;
        let ast = parse_query(text).unwrap();
        let warmup = crate::io::read::InvertedIndexWarmupInfo::try_create(
            query.as_ref(),
            &ast,
            &schema,
            &[body],
            fuzziness,
        )?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        warmup
            .warm_searcher(&searcher, &directory, has_score)
            .await?;
        // Search is synchronous and must succeed using pinned data, without remote reads.
        let mut rows = if has_score {
            searcher
                .search(
                    query.as_ref(),
                    &TopDocs::with_limit(searcher.num_docs() as usize).order_by_score(),
                )?
                .into_iter()
                .map(|(score, address)| {
                    assert!(score.is_finite());
                    address.doc_id
                })
                .collect::<Vec<_>>()
        } else {
            searcher
                .search(query.as_ref(), &DocSetCollector)?
                .into_iter()
                .map(|address| address.doc_id)
                .collect::<Vec<_>>()
        };
        rows.sort_unstable();
        Ok(rows)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_real_expansion_warmups_cache_physical_components() -> Result<()> {
        let (fixture, footer, ranges) = expansion_bundle_fixture().await?;
        let idx = footer
            .file_ranges
            .files
            .iter()
            .find(|(path, _)| component_name(path) == "idx")
            .unwrap()
            .1
            .clone();
        let pos = footer
            .file_ranges
            .files
            .iter()
            .find(|(path, _)| component_name(path) == "pos")
            .unwrap()
            .1
            .clone();

        for (text, fuzziness, with_positions) in [
            ("body:\"alpha be\"*", None, true),
            ("body:alpa", Some(1), false),
            ("body:[alpha TO alpha]", None, false),
            // Full and exact terms on different fields share the same physical component.
            ("body:\"alpha be\"* OR title:needle", None, true),
            // Fast-field and scoring metadata may warm concurrently with full postings.
            ("body:\"alpha be\"* OR num:[1 TO 1]", None, true),
        ] {
            let mut remote = fixture.clone();
            remote.payload_cache = payload_cache(16 * 1024 * 1024);
            remote.full_payload_cache = full_payload_cache(16 * 1024 * 1024);
            let full_cache = remote.full_payload_cache.as_ref().unwrap();
            let page_cache = remote
                .payload_cache
                .as_ref()
                .unwrap()
                .in_memory_cache()
                .unwrap();
            ranges.lock().unwrap().clear();

            let rows =
                execute_expansion_bundle_query(&remote, &footer, text, fuzziness, true).await?;
            let expected = (0..4000u32)
                .filter(|row| {
                    row % 2 == 0
                        || (text.contains("title:") && row % 3 == 0)
                        || (text.contains("num:") && *row == 1)
                })
                .collect::<Vec<_>>();
            assert_eq!(rows, expected, "{text}");
            assert_eq!(
                full_cache.len(),
                if with_positions { 2 } else { 1 },
                "{text}"
            );
            assert_eq!(page_cache.len(), 0, "{text}");

            let payload_reads = || {
                let mut reads = ranges
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|range| {
                        (range.start < idx.end && range.end > idx.start)
                            || (range.start < pos.end && range.end > pos.start)
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                reads.sort_unstable_by_key(|range| range.start);
                reads
            };
            let first_reads = payload_reads();
            let mut expected_reads = vec![idx.clone()];
            if with_positions {
                expected_reads.push(pos.clone());
            }
            expected_reads.sort_unstable_by_key(|range| range.start);
            assert_eq!(first_reads, expected_reads, "{text}");

            // Reopening gives a new SearchPinDirectory, so this verifies cross-query full-cache
            // reuse rather than reuse of the first search's pin. No further payload GET is allowed.
            assert_eq!(
                execute_expansion_bundle_query(&remote, &footer, text, fuzziness, false).await?,
                expected
            );
            assert_eq!(payload_reads(), first_reads, "{text}");
            assert_eq!(
                execute_expansion_bundle_query(&remote, &footer, "body:alpha", None, false).await?,
                (0..4000u32).filter(|row| row % 2 == 0).collect::<Vec<_>>(),
            );
            assert_eq!(payload_reads(), first_reads, "partial query after {text}");
            assert_eq!(page_cache.len(), 0, "{text}");
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_unavailable_full_cache_preserves_pages_and_falls_back() -> Result<()> {
        let data = (0..3 * CACHE_PAGE_SIZE)
            .map(|index| index as u8)
            .collect::<Vec<_>>();
        for oversized in [false, true] {
            let capacity = if oversized {
                data.len() - 1
            } else {
                data.len() * 2
            };
            let (file, ranges) = payload_file_handle_with_full_capacity(
                "full-cache-fallback",
                data.clone(),
                capacity,
            )
            .await?;
            file.read_range(0..CACHE_PAGE_SIZE).await?;
            assert_eq!(file.read_range(0..data.len()).await?.as_ref(), data);
            if !oversized {
                assert!(file.cached_full_payload().is_some());
                file.full_payload_cache
                    .as_ref()
                    .unwrap()
                    .evict(&file.full_payload_key());
            }
            assert!(file.cached_full_payload().is_none());
            assert!(file.cached_page(PageCacheKind::Payload, 0)?.is_some());
            assert!(file.cached_page(PageCacheKind::Payload, 1)?.is_none());
            assert_eq!(
                file.read_range(0..CACHE_PAGE_SIZE).await?.as_ref(),
                &data[..CACHE_PAGE_SIZE]
            );
            let partial = CACHE_PAGE_SIZE + 7..CACHE_PAGE_SIZE + 31;
            assert_eq!(
                file.read_range(partial.clone()).await?.as_ref(),
                &data[partial]
            );
            assert_eq!(*ranges.lock().unwrap(), vec![
                0..CACHE_PAGE_SIZE as u64,
                0..data.len() as u64,
                CACHE_PAGE_SIZE as u64..(2 * CACHE_PAGE_SIZE) as u64,
            ]);
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_lookup_reads_replace_invalid_entry_and_share_fetch() -> Result<()> {
        let data = (0..CACHE_PAGE_SIZE + 23)
            .map(|index| index as u8)
            .collect::<Vec<_>>();
        let (mut file, ranges) = payload_file_handle("lookup-singleflight", data.clone()).await?;
        file.path = PathBuf::from("segment.term");
        file.component_name = "term".to_owned();
        file.cache_policy = RangeCachePolicy::LookupWhole;
        file.lookup_cache = HybridCache::new(
            "lookup".to_string(),
            Some(InMemoryLruCache::with_bytes_capacity(
                "memory_lookup".to_string(),
                data.len() * 2,
            )),
            None,
        );
        let key = file.whole_lookup_key();
        file.lookup_cache.insert(
            key.clone(),
            InvertedIndexLookupBytes::new(Bytes::from_static(b"invalid length")),
        );

        let first_range = 7..31;
        let second_range = CACHE_PAGE_SIZE + 5..CACHE_PAGE_SIZE + 12;
        let (first, second) = tokio::try_join!(
            file.read_range(first_range.clone()),
            file.read_range(second_range.clone()),
        )?;
        assert_eq!(first.as_ref(), &data[first_range]);
        assert_eq!(second.as_ref(), &data[second_range]);
        assert_eq!(file.lookup_cache.get(&key).unwrap().data, data);
        assert_eq!(*ranges.lock().unwrap(), vec![0..data.len() as u64]);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_overlapping_reads_share_page_fetches() -> Result<()> {
        let page_count = MAX_MERGED_PAGES + 8;
        let data = (0..page_count * CACHE_PAGE_SIZE)
            .map(|index| index as u8)
            .collect::<Vec<_>>();
        let (file, ranges) = payload_file_handle("overlapping-pages", data.clone()).await?;
        let left_end = MAX_MERGED_PAGES * CACHE_PAGE_SIZE;
        let right_start = 8 * CACHE_PAGE_SIZE;
        let right_end = page_count * CACHE_PAGE_SIZE;

        let (left, right) = tokio::try_join!(
            file.read_range(0..left_end),
            file.read_range(right_start..right_end),
        )?;

        assert_eq!(left.as_ref(), &data[..left_end]);
        assert_eq!(right.as_ref(), &data[right_start..right_end]);
        assert_eq!(*ranges.lock().unwrap(), vec![
            0..MAX_MERGED_READ_SIZE as u64,
            MAX_MERGED_READ_SIZE as u64..right_end as u64,
        ]);
        Ok(())
    }

    #[test]
    fn test_range_cache_policy() {
        assert_eq!(range_cache_policy("term", 1), RangeCachePolicy::LookupWhole);
        assert_eq!(
            range_cache_policy("term", MAX_FULL_LOOKUP_CACHE_SIZE as u64 + 1),
            RangeCachePolicy::Pages(PageCacheKind::Lookup)
        );
        assert_eq!(
            range_cache_policy("fieldnorm", 1),
            RangeCachePolicy::LookupWhole
        );
        assert_eq!(range_cache_policy("fast", 1), RangeCachePolicy::LookupWhole);
        assert_eq!(
            range_cache_policy("fast", MAX_FULL_LOOKUP_CACHE_SIZE as u64 + 1),
            RangeCachePolicy::Pages(PageCacheKind::Payload)
        );
        for component in ["idx", "pos", "store", "del"] {
            assert_eq!(
                range_cache_policy(component, 1),
                RangeCachePolicy::Pages(PageCacheKind::Payload)
            );
        }
    }

    #[test]
    fn test_footer_cache_validates_cached_entries() {
        let (object, object_size, footer) = footer_fixture();
        for (name, size, bytes, valid) in [
            (
                "valid",
                object_size,
                persisted_footer(&object, &footer),
                true,
            ),
            (
                "corrupt",
                object_size,
                Bytes::from_static(b"corrupt"),
                false,
            ),
            (
                "stale",
                object_size + 1,
                persisted_footer(&object, &footer),
                false,
            ),
        ] {
            let state = cache_state(name, size);
            state.meta_cache.insert(
                state.meta_key.clone(),
                InvertedIndexMeta::new(object_size, bytes),
            );
            if valid {
                let cached = state.load().unwrap();
                assert_eq!(cached.file_ranges, footer.file_ranges);
                assert_eq!(cached.open_slices, footer.open_slices);
                assert_eq!(cached.managed_json, footer.managed_json);
                assert_eq!(cached.meta_json, footer.meta_json);
            } else {
                assert!(state.load().is_none());
            }
            assert_eq!(
                state.meta_cache.contains_key(&state.meta_key),
                valid,
                "{name}"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_initial_tail_prefills_meta_and_small_lookup_files() -> Result<()> {
        let files = [
            ("segment.idx", b"postings".as_slice()),
            ("segment.store", b"store".as_slice()),
            ("segment.fast", b"fast".as_slice()),
            ("segment.fieldnorm", b"norms".as_slice()),
            ("segment.term", b"terms".as_slice()),
        ];
        let object = InvertedIndexBundleFooter::build(
            files,
            BTreeMap::new(),
            BTreeMap::new(),
            b"managed".to_vec(),
            b"meta".to_vec(),
        )?;
        let object_size = object.len() as u64;
        let layer = RecordingLayer::default();
        let ranges = layer.ranges.clone();
        let operator = Operator::new(Memory::default())?.layer(layer).finish();
        operator.write("index", object).await?;
        let state = cache_state("index", object_size);
        for _ in 0..2 {
            let footer = state.load_or_fetch(&operator, "index").await?;
            assert!(state.meta_cache.contains_key(&state.meta_key));
            for (path, bytes) in files {
                let file_id = footer
                    .file_ranges
                    .files
                    .keys()
                    .position(|p| p == Path::new(path))
                    .unwrap();
                let component = component_name(Path::new(path));
                let entry = state
                    .lookup_cache
                    .get(whole_lookup_cache_key("index", component, file_id));
                if matches!(component, "term" | "fast" | "fieldnorm") {
                    assert_eq!(entry.unwrap().data.as_ref(), bytes);
                } else {
                    assert!(entry.is_none());
                }
            }
        }
        assert_eq!(*ranges.lock().unwrap(), vec![0..object_size]);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_load_or_fetch_reads_large_footer_by_exact_range() -> Result<()> {
        let managed_json = vec![b'm'; INVERTED_INDEX_BUNDLE_INITIAL_FOOTER_READ_SIZE + 128];
        let object = InvertedIndexBundleFooter::build(
            [("segment.idx", b"postings".as_slice())],
            BTreeMap::new(),
            BTreeMap::new(),
            managed_json.clone(),
            b"meta".to_vec(),
        )?;
        let object_size = u64::try_from(object.len()).unwrap();
        let initial_tail_start =
            object_size - u64::try_from(INVERTED_INDEX_BUNDLE_INITIAL_FOOTER_READ_SIZE).unwrap();
        let expected = InvertedIndexBundleFooter::open(&object)?;
        assert!(expected.footer_start < initial_tail_start);
        let operator = Operator::new(Memory::default())?.finish();
        operator.write("index", object).await?;
        let state = cache_state("index", object_size);

        let footer = state.load_or_fetch(&operator, "index").await?;

        assert_eq!(footer.managed_json.as_ref(), managed_json);
        let metadata = state.meta_cache.get(&state.meta_key).unwrap();
        assert_eq!(
            metadata.footer_bytes.len(),
            usize::try_from(object_size - footer.footer_start).unwrap()
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_external_files_are_read_from_sibling_objects() -> Result<()> {
        let postings = (0..3 * CACHE_PAGE_SIZE)
            .map(|i| i as u8)
            .collect::<Vec<_>>();
        let object = InvertedIndexBundleFooter::build(
            [("segment.term", b"terms".as_slice())],
            BTreeMap::from([(
                PathBuf::from("segment.idx"),
                databend_storages_common_index::ExternalFile {
                    suffix: ".idx".to_string(),
                    len: postings.len() as u64,
                },
            )]),
            BTreeMap::new(),
            b"managed".to_vec(),
            b"meta".to_vec(),
        )?;
        let layer = RecordingLayer::default();
        let ranges = layer.ranges.clone();
        let operator = Operator::new(Memory::default())?.layer(layer).finish();
        operator.write("index", object.clone()).await?;
        operator.write("index.idx", postings.clone()).await?;
        let footer = InvertedIndexBundleFooter::open(&object)?;
        let remote = RemoteBundleDirectory {
            operator,
            location: Arc::from("index"),
            file_ranges: footer.file_ranges.clone(),
            external_files: footer.external_files.clone(),
            lookup_cache: None,
            payload_cache: payload_cache(postings.len() * 2),
            full_payload_cache: full_payload_cache(postings.len() * 2),
        };

        assert!(remote.exists(Path::new("segment.idx"))?);
        assert!(remote.exists(Path::new("segment.term"))?);
        let idx = remote.get_file_handle(Path::new("segment.idx"))?;
        assert_eq!(idx.len(), postings.len());
        let middle = CACHE_PAGE_SIZE + 10..2 * CACHE_PAGE_SIZE - 10;
        assert_eq!(
            idx.read_bytes_async(middle.clone()).await?.as_ref(),
            &postings[middle]
        );
        // The range is read from `index.idx` at its own offsets, not from the bundle object.
        assert_eq!(*ranges.lock().unwrap(), vec![
            CACHE_PAGE_SIZE as u64..(2 * CACHE_PAGE_SIZE) as u64
        ]);
        let term = remote.get_file_handle(Path::new("segment.term"))?;
        assert_eq!(term.read_bytes_async(0..5).await?.as_ref(), b"terms");
        Ok(())
    }
}
