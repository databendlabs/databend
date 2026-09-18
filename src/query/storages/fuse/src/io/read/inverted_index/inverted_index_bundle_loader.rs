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
use databend_storages_common_cache::InvertedIndexLookupCache;
use databend_storages_common_cache::InvertedIndexMetaCache;
use databend_storages_common_cache::InvertedIndexPayloadCache;
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
const MAX_FULL_LOOKUP_CACHE_SIZE: usize = MAX_MERGED_READ_SIZE;
const LOOKUP_FULL_CACHE_KEY_PREFIX: &str = "ii-lookup-full-v1:";
const LOOKUP_PAGE_CACHE_KEY_PREFIX: &str = "ii-lookup-page-v1:";
const PAYLOAD_CACHE_KEY_PREFIX: &str = "ii-payload-page-v1:";

fn record_inverted_index_read(bytes: usize, elapsed: std::time::Duration) {
    metrics_inc_block_inverted_index_read_bytes(u64::try_from(bytes).unwrap_or(u64::MAX));
    metrics_inc_block_inverted_index_read_milliseconds(
        u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX),
    );
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RangeCachePolicy {
    LookupWhole,
    LookupPages,
    PayloadPages,
}

fn component_name(path: &Path) -> &str {
    path.extension()
        .and_then(|extension| extension.to_str())
        .unwrap_or("file")
}

fn range_cache_policy(path: &Path, file_len: u64) -> RangeCachePolicy {
    // Policy is a function of component and file length only, so a later read uses the same cache
    // as the tail prefill. Small `.fieldnorm` / `.fast` files live entirely in Lookup; large ones
    // are Payload pages. They are never stored in both caches.
    match component_name(path) {
        "term" if file_len <= MAX_FULL_LOOKUP_CACHE_SIZE as u64 => RangeCachePolicy::LookupWhole,
        "term" => RangeCachePolicy::LookupPages,
        "fieldnorm" | "fast" if file_len <= MAX_FULL_LOOKUP_CACHE_SIZE as u64 => {
            RangeCachePolicy::LookupWhole
        }
        _ => RangeCachePolicy::PayloadPages,
    }
}

fn whole_lookup_cache_key(location: &str, path: &Path, file_id: usize) -> String {
    format!(
        "{LOOKUP_FULL_CACHE_KEY_PREFIX}{location}:{}:{file_id}",
        component_name(path)
    )
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
    policy: RangeCachePolicy,
    page_no: usize,
) -> Arc<FetchLock> {
    let domain = match policy {
        RangeCachePolicy::LookupWhole | RangeCachePolicy::LookupPages => "lookup",
        RangeCachePolicy::PayloadPages => "payload",
    };
    fetch_lock(format!("page:{location}:{domain}:{file_id}:{page_no}"))
}

fn footer_fetch_lock(location: &str) -> Arc<FetchLock> {
    fetch_lock(format!("footer:{location}"))
}

#[derive(Clone)]
struct RemoteBundleDirectory {
    operator: Operator,
    location: Arc<str>,
    file_ranges: BundleFileRanges,
    lookup_cache: Option<InvertedIndexLookupCache>,
    payload_cache: Option<InvertedIndexPayloadCache>,
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
    location: Arc<str>,
    file_range: Range<u64>,
    file_len: usize,
    file_id: usize,
    cache_policy: RangeCachePolicy,
    lookup_cache: Option<InvertedIndexLookupCache>,
    payload_cache: Option<InvertedIndexPayloadCache>,
    path: PathBuf,
}

impl fmt::Debug for RemoteBundleFileHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("RemoteBundleFileHandle")
            .field("path", &self.path)
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
        whole_lookup_cache_key(&self.location, &self.path, self.file_id)
    }

    fn page_key(&self, page_no: usize) -> String {
        let component = component_name(&self.path);
        match self.cache_policy {
            RangeCachePolicy::LookupPages => format!(
                "{LOOKUP_PAGE_CACHE_KEY_PREFIX}{}:{component}:{}:{page_no}",
                self.location, self.file_id
            ),
            RangeCachePolicy::PayloadPages => format!(
                "{PAYLOAD_CACHE_KEY_PREFIX}{}:{component}:{}:{page_no}",
                self.location, self.file_id
            ),
            RangeCachePolicy::LookupWhole => {
                unreachable!("whole lookup components do not use page cache keys")
            }
        }
    }

    fn cached_page(&self, page_no: usize) -> io::Result<Option<Bytes>> {
        let key = self.page_key(page_no);
        let expected_len = self.page_range(page_no)?.len();
        let data = match self.cache_policy {
            RangeCachePolicy::LookupPages => {
                self.lookup_cache.get(&key).map(|value| value.data.clone())
            }
            RangeCachePolicy::PayloadPages => {
                self.payload_cache.get(&key).map(|value| value.data.clone())
            }
            RangeCachePolicy::LookupWhole => {
                unreachable!("whole lookup components do not use page cache entries")
            }
        };
        let Some(data) = data else {
            return Ok(None);
        };
        if data.len() != expected_len {
            match self.cache_policy {
                RangeCachePolicy::LookupPages => {
                    self.lookup_cache.evict(&key);
                }
                RangeCachePolicy::PayloadPages => {
                    self.payload_cache.evict(&key);
                }
                RangeCachePolicy::LookupWhole => unreachable!(),
            }
            return Ok(None);
        }
        Ok(Some(data))
    }

    fn insert_page(&self, page_no: usize, data: Bytes) -> Bytes {
        let key = self.page_key(page_no);
        match self.cache_policy {
            RangeCachePolicy::LookupPages => self
                .lookup_cache
                .insert(key, InvertedIndexLookupBytes::new(data))
                .data
                .clone(),
            RangeCachePolicy::PayloadPages => self
                .payload_cache
                .insert(key, InvertedIndexPayloadBytes::new(data))
                .data
                .clone(),
            RangeCachePolicy::LookupWhole => {
                unreachable!("whole lookup components do not use page cache entries")
            }
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
            .read_with(self.location.as_ref())
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
                self.insert_page(page_no, Bytes::copy_from_slice(&data[start..end])),
            ));
        }
        Ok(pages)
    }

    async fn read_cached_lookup_file(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        let key = self.whole_lookup_key();
        let mut data = self.lookup_cache.get(&key).map(|value| value.data.clone());
        if data.as_ref().is_some_and(|data| data.len() != self.len()) {
            self.lookup_cache.evict(&key);
            data = None;
        }
        if data.is_none() {
            let fetch_lock = page_fetch_lock(&self.location, self.file_id, self.cache_policy, 0);
            let _guard = fetch_lock.lock().await;
            data = self.lookup_cache.get(&key).map(|value| value.data.clone());
            if data.as_ref().is_some_and(|data| data.len() != self.len()) {
                self.lookup_cache.evict(&key);
                data = None;
            }
            if data.is_none() {
                let fetched = self.fetch_range(0..self.len()).await?;
                data = Some(
                    self.lookup_cache
                        .insert(key, InvertedIndexLookupBytes::new(fetched))
                        .data
                        .clone(),
                );
            }
        }
        let data = data.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "inverted-index lookup component was not available after fetch",
            )
        })?;
        Ok(OwnedBytes::new(data.slice(range).to_vec()))
    }

    async fn read_cached_pages(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        if range.end > self.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "inverted-index range exceeds logical file length",
            ));
        }
        if range.is_empty() {
            return Ok(OwnedBytes::empty());
        }

        let cache_enabled = match self.cache_policy {
            RangeCachePolicy::LookupWhole | RangeCachePolicy::LookupPages => {
                self.lookup_cache.is_some()
            }
            RangeCachePolicy::PayloadPages => self.payload_cache.is_some(),
        };
        if !cache_enabled {
            return self
                .fetch_range(range)
                .await
                .map(|data| OwnedBytes::new(data.to_vec()));
        }
        if self.cache_policy == RangeCachePolicy::LookupWhole {
            return self.read_cached_lookup_file(range).await;
        }

        let first_page = range.start / CACHE_PAGE_SIZE;
        let last_page = (range.end - 1) / CACHE_PAGE_SIZE;
        let mut request_pages = HashMap::new();
        for page_no in first_page..=last_page {
            if let Some(page) = self.cached_page(page_no)? {
                request_pages.insert(page_no, page);
            }
        }
        if request_pages.len() != last_page - first_page + 1 {
            let mut page_no = first_page;
            while page_no <= last_page {
                if request_pages.contains_key(&page_no) {
                    page_no += 1;
                    continue;
                }
                if let Some(page) = self.cached_page(page_no)? {
                    request_pages.insert(page_no, page);
                    page_no += 1;
                    continue;
                }
                let fetch_lock =
                    page_fetch_lock(&self.location, self.file_id, self.cache_policy, page_no);
                let _guard = fetch_lock.lock().await;
                if let Some(page) = self.cached_page(page_no)? {
                    request_pages.insert(page_no, page);
                    page_no += 1;
                    continue;
                }
                let (fetched_page_no, page) = self
                    .fetch_page_group(page_no, page_no)
                    .await?
                    .pop()
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::NotFound,
                            "inverted-index page was not available after fetch",
                        )
                    })?;
                request_pages.insert(fetched_page_no, page);
                page_no += 1;
            }
        }

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
        self.read_cached_pages(range).await
    }
}

impl Directory for RemoteBundleDirectory {
    fn get_file_handle(
        &self,
        path: &Path,
    ) -> std::result::Result<Arc<dyn FileHandle>, OpenReadError> {
        let file_range = self
            .file_ranges
            .get(path)
            .ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))?;
        let file_len_u64 = file_range.end - file_range.start;
        let file_len = usize::try_from(file_len_u64).map_err(|error| {
            OpenReadError::wrap_io_error(io::Error::other(error), path.to_path_buf())
        })?;
        let file_id = self
            .file_ranges
            .files
            .keys()
            .position(|candidate| candidate == path)
            .ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))?;
        Ok(Arc::new(RemoteBundleFileHandle {
            operator: self.operator.clone(),
            location: self.location.clone(),
            file_range,
            file_len,
            file_id,
            cache_policy: range_cache_policy(path, file_len_u64),
            lookup_cache: self.lookup_cache.clone(),
            payload_cache: self.payload_cache.clone(),
            path: path.to_path_buf(),
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
        Ok(self.file_ranges.contains(path))
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
            if range_cache_policy(path, file_len) != RangeCachePolicy::LookupWhole
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
            let key = whole_lookup_cache_key(&self.location, path, file_id);
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
        lookup_cache,
        payload_cache: cache_manager.get_inverted_index_payload_cache(),
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
    use opendal::services::Memory;

    use super::*;

    fn footer_fixture() -> (Vec<u8>, u64, InvertedIndexBundleFooter) {
        let object = InvertedIndexBundleFooter::build(
            [("segment.idx", b"postings".as_slice())],
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

    #[test]
    fn test_range_cache_key_namespaces() {
        assert_eq!(LOOKUP_FULL_CACHE_KEY_PREFIX, "ii-lookup-full-v1:");
        assert_eq!(LOOKUP_PAGE_CACHE_KEY_PREFIX, "ii-lookup-page-v1:");
        assert_eq!(PAYLOAD_CACHE_KEY_PREFIX, "ii-payload-page-v1:");
    }

    #[test]
    fn test_range_cache_policy() {
        assert_eq!(
            range_cache_policy(Path::new("segment.term"), 1),
            RangeCachePolicy::LookupWhole
        );
        assert_eq!(
            range_cache_policy(
                Path::new("segment.term"),
                MAX_FULL_LOOKUP_CACHE_SIZE as u64 + 1
            ),
            RangeCachePolicy::LookupPages
        );
        assert_eq!(
            range_cache_policy(Path::new("segment.fieldnorm"), 1),
            RangeCachePolicy::LookupWhole
        );
        assert_eq!(
            range_cache_policy(Path::new("segment.fast"), 1),
            RangeCachePolicy::LookupWhole
        );
        assert_eq!(
            range_cache_policy(
                Path::new("segment.fast"),
                MAX_FULL_LOOKUP_CACHE_SIZE as u64 + 1
            ),
            RangeCachePolicy::PayloadPages
        );
        for path in ["segment.idx", "segment.pos", "segment.store", "segment.del"] {
            assert_eq!(
                range_cache_policy(Path::new(path), 1),
                RangeCachePolicy::PayloadPages
            );
        }
    }

    #[test]
    fn test_footer_cache_state_loads_complete_footer() {
        let (object, object_size, footer) = footer_fixture();
        let state = cache_state("index", object_size);
        state.store(persisted_footer(&object, &footer));

        let cached = state.load().unwrap();
        assert_eq!(cached.file_ranges, footer.file_ranges);
        assert_eq!(cached.open_slices, footer.open_slices);
        assert_eq!(cached.managed_json, footer.managed_json);
        assert_eq!(cached.meta_json, footer.meta_json);
        assert!(state.meta_cache.contains_key(&state.meta_key));
    }

    #[test]
    fn test_footer_tail_populates_complete_small_lookup_components() {
        let term_bytes = Bytes::from_static(b"small term dictionary");
        let fieldnorm_bytes = Bytes::from_static(b"small fieldnorm");
        let fast_bytes = Bytes::from_static(b"small fast fields");
        let store_bytes = Bytes::from_static(b"small document store");
        let object = InvertedIndexBundleFooter::build(
            [
                ("segment.idx", b"postings".as_slice()),
                ("segment.store", store_bytes.as_ref()),
                ("segment.fast", fast_bytes.as_ref()),
                ("segment.fieldnorm", fieldnorm_bytes.as_ref()),
                ("segment.term", term_bytes.as_ref()),
            ],
            BTreeMap::new(),
            b"managed".to_vec(),
            b"meta".to_vec(),
        )
        .unwrap();
        let object_size = u64::try_from(object.len()).unwrap();
        let footer = InvertedIndexBundleFooter::open(&object).unwrap();
        let state = cache_state("index", object_size);
        let tail = Bytes::from(object);

        state.populate_small_lookup_cache(&tail, 0, &footer);

        for (path, expected) in [
            ("segment.term", term_bytes),
            ("segment.fieldnorm", fieldnorm_bytes),
            ("segment.fast", fast_bytes),
        ] {
            let file_id = footer
                .file_ranges
                .files
                .keys()
                .position(|candidate| candidate == Path::new(path))
                .unwrap();
            let key = whole_lookup_cache_key("index", Path::new(path), file_id);
            assert_eq!(state.lookup_cache.get(&key).unwrap().data, expected);
        }

        for path in ["segment.idx", "segment.store"] {
            let file_id = footer
                .file_ranges
                .files
                .keys()
                .position(|candidate| candidate == Path::new(path))
                .unwrap();
            let key = whole_lookup_cache_key("index", Path::new(path), file_id);
            assert!(state.lookup_cache.get(&key).is_none());
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_load_or_fetch_populates_meta_and_small_lookup_from_initial_tail() -> Result<()> {
        let term_bytes = Bytes::from_static(b"small term dictionary");
        let object = InvertedIndexBundleFooter::build(
            [
                ("segment.idx", b"postings".as_slice()),
                ("segment.term", term_bytes.as_ref()),
            ],
            BTreeMap::new(),
            b"managed".to_vec(),
            b"meta".to_vec(),
        )?;
        let object_size = u64::try_from(object.len()).unwrap();
        let operator = Operator::new(Memory::default())?.finish();
        operator.write("index", object).await?;
        let state = cache_state("index", object_size);

        let footer = state.load_or_fetch(&operator, "index").await?;

        assert!(state.meta_cache.contains_key(&state.meta_key));
        let term_file_id = footer
            .file_ranges
            .files
            .keys()
            .position(|path| path == Path::new("segment.term"))
            .unwrap();
        let key = whole_lookup_cache_key("index", Path::new("segment.term"), term_file_id);
        assert_eq!(state.lookup_cache.get(&key).unwrap().data, term_bytes);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_load_or_fetch_reads_large_footer_by_exact_range() -> Result<()> {
        let managed_json = vec![b'm'; INVERTED_INDEX_BUNDLE_INITIAL_FOOTER_READ_SIZE + 128];
        let object = InvertedIndexBundleFooter::build(
            [("segment.idx", b"postings".as_slice())],
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

    #[test]
    fn test_footer_cache_state_invalidates_corrupt_footer() {
        let (_, object_size, _) = footer_fixture();
        let state = cache_state("index", object_size);
        state.meta_cache.insert(
            state.meta_key.clone(),
            InvertedIndexMeta::new(object_size, Bytes::from_static(b"corrupt")),
        );

        assert!(state.load().is_none());
        assert!(!state.meta_cache.contains_key(&state.meta_key));
    }

    #[test]
    fn test_footer_cache_state_invalidates_stale_object_size() {
        let (object, object_size, footer) = footer_fixture();
        let state = cache_state("index", object_size);
        state.store(persisted_footer(&object, &footer));
        let stale_state = FooterCacheState::new(
            "index",
            object_size + 1,
            state.meta_cache.clone(),
            state.lookup_cache.clone(),
        );

        assert!(stale_state.load().is_none());
        assert!(!state.meta_cache.contains_key(&state.meta_key));
    }
}
