// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Make assertions about IO operations to an [ObjectStore].
//!
//! When testing code that performs IO, you will often want to make assertions
//! about the number of reads and writes performed, the amount of data read or
//! written, and the number of disjoint periods where at least one IO is in-flight.
//!
//! This modules provides [`IOTracker`] which can be used to wrap any object store.
use std::fmt::{Display, Formatter};
use std::ops::Range;
#[cfg(feature = "test-util")]
use std::sync::atomic::AtomicU16;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    GetOptions, GetRange, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as OSResult, UploadPart,
};

use crate::object_store::WrappingObjectStore;

#[derive(Debug, Default, Clone)]
pub struct IOTracker(Arc<Mutex<IoStats>>);

impl IOTracker {
    /// Get IO statistics and reset the counters (incremental pattern).
    ///
    /// This returns the accumulated statistics since the last call and resets
    /// the internal counters to zero.
    pub fn incremental_stats(&self) -> IoStats {
        std::mem::take(&mut *self.0.lock().unwrap())
    }

    /// Get a snapshot of current IO statistics without resetting counters.
    ///
    /// This returns a clone of the current statistics without modifying the
    /// internal state. Use this when you need to check stats without resetting.
    pub fn stats(&self) -> IoStats {
        self.0.lock().unwrap().clone()
    }

    /// Record a read operation for tracking.
    ///
    /// This is used by readers that bypass the ObjectStore layer (like LocalObjectReader)
    /// to ensure their IO operations are still tracked.
    pub fn record_read(
        &self,
        #[allow(unused_variables)] method: &'static str,
        #[allow(unused_variables)] path: Path,
        num_bytes: u64,
        #[allow(unused_variables)] range: Option<Range<u64>>,
    ) {
        let mut stats = self.0.lock().unwrap();
        stats.read_iops += 1;
        stats.read_bytes += num_bytes;
        #[cfg(feature = "test-util")]
        stats.requests.push(IoRequestRecord {
            method,
            path,
            range,
        });
    }
}

impl WrappingObjectStore for IOTracker {
    fn wrap(&self, _store_prefix: &str, target: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        Arc::new(IoTrackingStore::new(target, self.0.clone()))
    }
}

#[derive(Debug, Default, Clone)]
pub struct IoStats {
    pub read_iops: u64,
    pub read_bytes: u64,
    pub write_iops: u64,
    pub written_bytes: u64,
    // This is only really meaningful in tests where there isn't any concurrent IO.
    #[cfg(feature = "test-util")]
    /// Number of disjoint periods where at least one IO is in-flight.
    pub num_stages: u64,
    #[cfg(feature = "test-util")]
    pub requests: Vec<IoRequestRecord>,
}

/// Assertions on IO statistics.
/// assert_io_eq!(io_stats, read_iops, 1);
/// assert_io_eq!(io_stats, write_iops, 0, "should be no writes");
/// assert_io_eq!(io_stats, num_hops, 1, "should be just {}", "one hop");
#[cfg(feature = "test-util")]
#[macro_export]
macro_rules! assert_io_eq {
    ($io_stats:expr, $field:ident, $expected:expr) => {
        assert_eq!(
            $io_stats.$field, $expected,
            "Expected {} to be {}, got {}. Requests: {:#?}",
            stringify!($field),
            $expected,
            $io_stats.$field,
            $io_stats.requests
        );
    };
    ($io_stats:expr, $field:ident, $expected:expr, $($arg:tt)+) => {
        assert_eq!(
            $io_stats.$field, $expected,
            "Expected {} to be {}, got {}. Requests: {:#?} {}",
            stringify!($field),
            $expected,
            $io_stats.$field,
            $io_stats.requests,
            format_args!($($arg)+)
        );
    };
}

#[cfg(feature = "test-util")]
#[macro_export]
macro_rules! assert_io_gt {
    ($io_stats:expr, $field:ident, $expected:expr) => {
        assert!(
            $io_stats.$field > $expected,
            "Expected {} to be > {}, got {}. Requests: {:#?}",
            stringify!($field),
            $expected,
            $io_stats.$field,
            $io_stats.requests
        );
    };
    ($io_stats:expr, $field:ident, $expected:expr, $($arg:tt)+) => {
        assert!(
            $io_stats.$field > $expected,
            "Expected {} to be > {}, got {}. Requests: {:#?} {}",
            stringify!($field),
            $expected,
            $io_stats.$field,
            $io_stats.requests,
            format_args!($($arg)+)
        );
    };
}

#[cfg(feature = "test-util")]
#[macro_export]
macro_rules! assert_io_lt {
    ($io_stats:expr, $field:ident, $expected:expr) => {
        assert!(
            $io_stats.$field < $expected,
            "Expected {} to be < {}, got {}. Requests: {:#?}",
            stringify!($field),
            $expected,
            $io_stats.$field,
            $io_stats.requests
        );
    };
    ($io_stats:expr, $field:ident, $expected:expr, $($arg:tt)+) => {
        assert!(
            $io_stats.$field < $expected,
            "Expected {} to be < {}, got {}. Requests: {:#?} {}",
            stringify!($field),
            $expected,
            $io_stats.$field,
            $io_stats.requests,
            format_args!($($arg)+)
        );
    };
}

// These fields are "dead code" because we just use them right now to display
// in test failure messages through Debug. (The lint ignores Debug impls.)
#[allow(dead_code)]
#[derive(Clone)]
pub struct IoRequestRecord {
    pub method: &'static str,
    pub path: Path,
    pub range: Option<Range<u64>>,
}

impl std::fmt::Debug for IoRequestRecord {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // For example: "put /path/to/file range: 0-100"
        write!(
            f,
            "IORequest(method={}, path=\"{}\"",
            self.method, self.path
        )?;
        if let Some(range) = &self.range {
            write!(f, ", range={:?}", range)?;
        }
        write!(f, ")")?;
        Ok(())
    }
}

impl Display for IoStats {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#?}", self)
    }
}

#[derive(Debug)]
pub struct IoTrackingStore {
    target: Arc<dyn ObjectStore>,
    stats: Arc<Mutex<IoStats>>,
    #[cfg(feature = "test-util")]
    active_requests: Arc<AtomicU16>,
}

impl Display for IoTrackingStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#?}", self)
    }
}

impl IoTrackingStore {
    pub fn new(target: Arc<dyn ObjectStore>, stats: Arc<Mutex<IoStats>>) -> Self {
        Self {
            target,
            stats,
            #[cfg(feature = "test-util")]
            active_requests: Arc::new(AtomicU16::new(0)),
        }
    }

    fn record_read(
        &self,
        method: &'static str,
        path: Path,
        num_bytes: u64,
        range: Option<Range<u64>>,
    ) {
        let mut stats = self.stats.lock().unwrap();
        stats.read_iops += 1;
        stats.read_bytes += num_bytes;
        #[cfg(feature = "test-util")]
        stats.requests.push(IoRequestRecord {
            method,
            path,
            range,
        });
        #[cfg(not(feature = "test-util"))]
        let _ = (method, path, range); // Suppress unused variable warnings
    }

    fn record_write(&self, method: &'static str, path: Path, num_bytes: u64) {
        let mut stats = self.stats.lock().unwrap();
        stats.write_iops += 1;
        stats.written_bytes += num_bytes;
        #[cfg(feature = "test-util")]
        stats.requests.push(IoRequestRecord {
            method,
            path,
            range: None,
        });
        #[cfg(not(feature = "test-util"))]
        let _ = (method, path); // Suppress unused variable warnings
    }

    #[cfg(feature = "test-util")]
    fn stage_guard(&self) -> StageGuard {
        StageGuard::new(self.active_requests.clone(), self.stats.clone())
    }

    #[cfg(not(feature = "test-util"))]
    fn stage_guard(&self) -> StageGuard {
        StageGuard
    }
}

#[async_trait::async_trait]
#[deny(clippy::missing_trait_methods)]
impl ObjectStore for IoTrackingStore {
    async fn put(&self, location: &Path, bytes: PutPayload) -> OSResult<PutResult> {
        let _guard = self.stage_guard();
        self.record_write("put", location.to_owned(), bytes.content_length() as u64);
        self.target.put(location, bytes).await
    }

    async fn put_opts(
        &self,
        location: &Path,
        bytes: PutPayload,
        opts: PutOptions,
    ) -> OSResult<PutResult> {
        let _guard = self.stage_guard();
        self.record_write(
            "put_opts",
            location.to_owned(),
            bytes.content_length() as u64,
        );
        self.target.put_opts(location, bytes, opts).await
    }

    async fn put_multipart(&self, location: &Path) -> OSResult<Box<dyn MultipartUpload>> {
        let _guard = self.stage_guard();
        let target = self.target.put_multipart(location).await?;
        Ok(Box::new(IoTrackingMultipartUpload {
            target,
            stats: self.stats.clone(),
            #[cfg(feature = "test-util")]
            path: location.to_owned(),
            #[cfg(feature = "test-util")]
            _guard,
        }))
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> OSResult<Box<dyn MultipartUpload>> {
        let _guard = self.stage_guard();
        let target = self.target.put_multipart_opts(location, opts).await?;
        Ok(Box::new(IoTrackingMultipartUpload {
            target,
            stats: self.stats.clone(),
            #[cfg(feature = "test-util")]
            path: location.to_owned(),
            #[cfg(feature = "test-util")]
            _guard,
        }))
    }

    async fn get(&self, location: &Path) -> OSResult<GetResult> {
        let _guard = self.stage_guard();
        let result = self.target.get(location).await;
        if let Ok(result) = &result {
            let num_bytes = result.range.end - result.range.start;
            self.record_read("get", location.to_owned(), num_bytes, None);
        }
        result
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> OSResult<GetResult> {
        let _guard = self.stage_guard();
        let range = match &options.range {
            Some(GetRange::Bounded(range)) => Some(range.clone()),
            _ => None, // TODO: fill in other options.
        };
        let result = self.target.get_opts(location, options).await;
        if let Ok(result) = &result {
            let num_bytes = result.range.end - result.range.start;

            self.record_read("get_opts", location.to_owned(), num_bytes, range);
        }
        result
    }

    async fn get_range(&self, location: &Path, range: Range<u64>) -> OSResult<Bytes> {
        let _guard = self.stage_guard();
        let result = self.target.get_range(location, range.clone()).await;
        if let Ok(result) = &result {
            self.record_read(
                "get_range",
                location.to_owned(),
                result.len() as u64,
                Some(range),
            );
        }
        result
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> OSResult<Vec<Bytes>> {
        let _guard = self.stage_guard();
        let result = self.target.get_ranges(location, ranges).await;
        if let Ok(result) = &result {
            self.record_read(
                "get_ranges",
                location.to_owned(),
                result.iter().map(|b| b.len() as u64).sum(),
                None,
            );
        }
        result
    }

    async fn head(&self, location: &Path) -> OSResult<ObjectMeta> {
        let _guard = self.stage_guard();
        self.record_read("head", location.to_owned(), 0, None);
        self.target.head(location).await
    }

    async fn delete(&self, location: &Path) -> OSResult<()> {
        let _guard = self.stage_guard();
        self.record_write("delete", location.to_owned(), 0);
        self.target.delete(location).await
    }

    fn delete_stream<'a>(
        &'a self,
        locations: BoxStream<'a, OSResult<Path>>,
    ) -> BoxStream<'a, OSResult<Path>> {
        self.target.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OSResult<ObjectMeta>> {
        let _guard = self.stage_guard();
        self.record_read("list", prefix.cloned().unwrap_or_default(), 0, None);
        self.target.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, OSResult<ObjectMeta>> {
        self.record_read(
            "list_with_offset",
            prefix.cloned().unwrap_or_default(),
            0,
            None,
        );
        self.target.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OSResult<ListResult> {
        let _guard = self.stage_guard();
        self.record_read(
            "list_with_delimiter",
            prefix.cloned().unwrap_or_default(),
            0,
            None,
        );
        self.target.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> OSResult<()> {
        let _guard = self.stage_guard();
        self.record_write("copy", from.to_owned(), 0);
        self.target.copy(from, to).await
    }

    async fn rename(&self, from: &Path, to: &Path) -> OSResult<()> {
        let _guard = self.stage_guard();
        self.record_write("rename", from.to_owned(), 0);
        self.target.rename(from, to).await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> OSResult<()> {
        let _guard = self.stage_guard();
        self.record_write("rename_if_not_exists", from.to_owned(), 0);
        self.target.rename_if_not_exists(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> OSResult<()> {
        let _guard = self.stage_guard();
        self.record_write("copy_if_not_exists", from.to_owned(), 0);
        self.target.copy_if_not_exists(from, to).await
    }
}

#[derive(Debug)]
struct IoTrackingMultipartUpload {
    target: Box<dyn MultipartUpload>,
    #[cfg(feature = "test-util")]
    path: Path,
    stats: Arc<Mutex<IoStats>>,
    #[cfg(feature = "test-util")]
    _guard: StageGuard,
}

#[async_trait::async_trait]
impl MultipartUpload for IoTrackingMultipartUpload {
    async fn abort(&mut self) -> OSResult<()> {
        self.target.abort().await
    }

    async fn complete(&mut self) -> OSResult<PutResult> {
        self.target.complete().await
    }

    fn put_part(&mut self, payload: PutPayload) -> UploadPart {
        {
            let mut stats = self.stats.lock().unwrap();
            stats.write_iops += 1;
            stats.written_bytes += payload.content_length() as u64;
            #[cfg(feature = "test-util")]
            stats.requests.push(IoRequestRecord {
                method: "put_part",
                path: self.path.to_owned(),
                range: None,
            });
        }
        self.target.put_part(payload)
    }
}

#[cfg(feature = "test-util")]
#[derive(Debug)]
struct StageGuard {
    active_requests: Arc<AtomicU16>,
    stats: Arc<Mutex<IoStats>>,
}

#[cfg(not(feature = "test-util"))]
struct StageGuard;

#[cfg(feature = "test-util")]
impl StageGuard {
    fn new(active_requests: Arc<AtomicU16>, stats: Arc<Mutex<IoStats>>) -> Self {
        active_requests.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Self {
            active_requests,
            stats,
        }
    }
}

#[cfg(feature = "test-util")]
impl Drop for StageGuard {
    fn drop(&mut self) {
        if self
            .active_requests
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst)
            == 1
        {
            let mut stats = self.stats.lock().unwrap();
            stats.num_stages += 1;
        }
    }
}
