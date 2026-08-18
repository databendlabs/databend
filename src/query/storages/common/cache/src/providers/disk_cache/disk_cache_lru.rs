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

use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use databend_common_config::DiskCacheKeyReloadPolicy;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_metrics::cache::metrics_inc_cache_miss_bytes;
use log::error;
use log::warn;
use parking_lot::RwLock;

use crate::CacheAccessor;
use crate::providers::disk_cache::DiskCache;

impl CacheAccessor for LruDiskCacheHolder {
    type V = Bytes;
    fn name(&self) -> &str {
        "LruDiskCacheHolder"
    }

    // TODO Change Arc<Bytes> to Bytes or Arc<Vec<u8>> ?
    fn get<Q: AsRef<str>>(&self, k: Q) -> Option<Arc<Bytes>> {
        let k = k.as_ref();
        {
            let mut cache = self.write();
            cache.get_cache_item_path_and_size(k)
        }
        .and_then(|(cache_file_path, file_size)| {
            match read_cache_content(cache_file_path, file_size as usize) {
                Ok(mut bytes) => {
                    if let Err(e) = validate_checksum(bytes.as_slice()) {
                        error!("disk cache item of key {k}, crc validation failure: {e}");
                        {
                            // remove the invalid cache, error of removal ignored
                            let r = {
                                let mut cache = self.write();
                                cache.remove(k)
                            };
                            if let Err(e) = r {
                                warn!("failed to remove invalid cache item of key {k}: {e}");
                            }
                        }
                        None
                    } else {
                        // trim the checksum bytes and return
                        let total_len = bytes.len();
                        let body_len = total_len - 4;
                        bytes.truncate(body_len);
                        let item = Arc::new(bytes.into());
                        Some(item)
                    }
                }
                Err(e) => {
                    // Failure of reading cache item is ignored,
                    // maybe it should be ignored by the caller?
                    error!("failed to read disk cache item of key {k}: {e}");
                    None
                }
            }
        })
    }

    fn get_sized<Q: AsRef<str>>(&self, k: Q, len: u64) -> Option<Arc<Self::V>> {
        let Some(cached_value) = self.get(k) else {
            metrics_inc_cache_miss_bytes(len, self.name());
            return None;
        };

        Some(cached_value)
    }

    fn insert(&self, key: String, value: Bytes) -> Arc<Bytes> {
        let crc = crc32fast::hash(value.as_ref());
        let crc_bytes = crc.to_le_bytes();
        let mut cache = self.write();
        if let Err(e) = cache.insert_bytes(&key, &[value.as_ref(), &crc_bytes]) {
            error!("put disk cache item failed {}", e);
        }
        Arc::new(value)
    }

    fn evict(&self, k: &str) -> bool {
        if let Err(e) = {
            let mut cache = self.write();
            cache.remove(k)
        } {
            error!("evict disk cache item failed {}", e);
            false
        } else {
            true
        }
    }

    fn contains_key(&self, k: &str) -> bool {
        let cache = self.read();
        cache.contains_key(k)
    }

    fn bytes_size(&self) -> u64 {
        let cache = self.read();
        cache.size()
    }

    fn items_capacity(&self) -> u64 {
        let cache = self.read();
        cache.items_capacity()
    }

    fn bytes_capacity(&self) -> u64 {
        let cache = self.read();
        cache.bytes_capacity()
    }

    fn len(&self) -> usize {
        let cache = self.read();
        cache.len()
    }

    fn clear(&self) {
        // Does nothing for disk cache
    }
}

/// The crc32 checksum is stored at the end of `bytes` and encoded as le u32.
// Although parquet page has built-in crc, but it is optional (and not generated in parquet2)
fn validate_checksum(bytes: &[u8]) -> Result<()> {
    let total_len = bytes.len();
    if total_len <= 4 {
        Err(ErrorCode::StorageOther(format!(
            "crc checksum validation failure: invalid file length {total_len}"
        )))
    } else {
        // total_len > 4 is ensured
        let crc_bytes: [u8; 4] = bytes[total_len - 4..].try_into().unwrap();
        let crc_provided = u32::from_le_bytes(crc_bytes);
        let crc_calculated = crc32fast::hash(&bytes[0..total_len - 4]);
        if crc_provided == crc_calculated {
            Ok(())
        } else {
            Err(ErrorCode::StorageOther(format!(
                "crc checksum validation failure: provided {crc_provided}, calculated {crc_calculated}"
            )))
        }
    }
}

pub type LruDiskCache = DiskCache;

/// One pending admission for the population worker.
pub(crate) struct CacheItem {
    pub(crate) key: String,
    pub(crate) value: Bytes,
    /// Whether `DiskCacheAccessor::insert` incremented the pending metric for
    /// this item before enqueueing it. Direct holder batches do not.
    pub(crate) track_pending_metric: bool,
}

/// Shared handle of the disk LRU cache, carrying a clone of the population
/// queue sender so that holders admit asynchronously without taking the lock.
#[derive(Clone)]
pub struct LruDiskCacheHolder {
    cache: Arc<RwLock<LruDiskCache>>,
    population_tx: crossbeam_channel::Sender<Vec<CacheItem>>,
}

impl std::ops::Deref for LruDiskCacheHolder {
    type Target = RwLock<LruDiskCache>;

    fn deref(&self) -> &Self::Target {
        &self.cache
    }
}

impl LruDiskCacheHolder {
    /// Best-effort asynchronous batch admission. One call — normally all
    /// chunks split from one fetched file segment — consumes one queue slot,
    /// regardless of the number of chunks. Existing keys are filtered under
    /// one read lock; the whole batch is silently dropped when the queue is
    /// full or the worker is gone.
    pub fn populate(&self, items: Vec<(String, Bytes)>) {
        let cache = self.cache.read();
        let batch = items
            .into_iter()
            .filter(|(key, _)| !cache.contains_key(key))
            .map(|(key, value)| CacheItem {
                key,
                value,
                track_pending_metric: false,
            })
            .collect::<Vec<_>>();
        drop(cache);

        if !batch.is_empty() {
            let _ = self.population_tx.try_send(batch);
        }
    }

    /// Batched `get`: the whole batch — LRU touches, file reads and crc
    /// checks — is served under a single lock acquisition, and corrupted
    /// entries are evicted on the spot. Results align with `keys` one to one.
    pub fn mget<Q: AsRef<str>>(&self, keys: &[Q]) -> Vec<Option<Arc<Bytes>>> {
        let mut cache = self.write();
        keys.iter()
            .map(|k| {
                let k = k.as_ref();
                let (cache_file_path, file_size) = cache.get_cache_item_path_and_size(k)?;
                match read_cache_content(cache_file_path, file_size as usize) {
                    Ok(mut bytes) => {
                        if let Err(e) = validate_checksum(bytes.as_slice()) {
                            error!("disk cache item of key {k}, crc validation failure: {e}");
                            // remove the invalid cache, error of removal ignored
                            if let Err(e) = cache.remove(k) {
                                warn!("failed to remove invalid cache item of key {k}: {e}");
                            }
                            None
                        } else {
                            // trim the checksum bytes and return
                            let total_len = bytes.len();
                            let body_len = total_len - 4;
                            bytes.truncate(body_len);
                            Some(Arc::new(bytes.into()))
                        }
                    }
                    Err(e) => {
                        // Failure of reading cache item is ignored,
                        // maybe it should be ignored by the caller?
                        error!("failed to read disk cache item of key {k}: {e}");
                        None
                    }
                }
            })
            .collect()
    }
}

pub struct LruDiskCacheBuilder;

impl LruDiskCacheBuilder {
    pub fn new_disk_cache(
        path: &PathBuf,
        disk_cache_bytes_size: usize,
        disk_cache_reload_policy: DiskCacheKeyReloadPolicy,
        sync_data: bool,
        population_tx: crossbeam_channel::Sender<Vec<CacheItem>>,
    ) -> Result<LruDiskCacheHolder> {
        let lru_disk_cache = DiskCache::new(
            path,
            disk_cache_bytes_size,
            disk_cache_reload_policy,
            sync_data,
        )
        .map_err(|e| ErrorCode::StorageOther(format!("create disk cache failed, {e}")))?;
        Ok(LruDiskCacheHolder {
            cache: Arc::new(RwLock::new(lru_disk_cache)),
            population_tx,
        })
    }
}

#[cfg(not(target_os = "linux"))]
pub fn read_cache_content(path: PathBuf, size: usize) -> std::io::Result<Vec<u8>> {
    use std::fs::File;
    use std::io::Read;

    let mut v = Vec::with_capacity(size);
    let mut file = File::open(path)?;
    file.read_to_end(&mut v)?;
    Ok(v)
}

#[cfg(target_os = "linux")]
pub fn read_cache_content(path: PathBuf, size: usize) -> std::io::Result<Vec<u8>> {
    linux_read::read_cache_content_with_syscall(path, size, linux_read::LibcRead)
}

#[cfg(target_os = "linux")]
mod linux_read {

    #[cfg(test)]
    use mockall::automock;

    use super::*;

    #[cfg_attr(test, automock)]
    pub(super) trait CallRead {
        unsafe fn read(
            &self,
            fd: i32,
            buf: *mut libc::c_void,
            count: libc::size_t,
        ) -> libc::ssize_t;
    }

    pub struct LibcRead;

    impl CallRead for LibcRead {
        unsafe fn read(
            &self,
            fd: i32,
            buf: *mut libc::c_void,
            count: libc::size_t,
        ) -> libc::ssize_t {
            unsafe { libc::read(fd, buf, count) }
        }
    }

    pub(super) fn read_cache_content_with_syscall<R: CallRead>(
        path: PathBuf,
        size: usize,
        read_syscall: R,
    ) -> std::io::Result<Vec<u8>> {
        use std::fs::OpenOptions;
        use std::os::unix::fs::OpenOptionsExt;
        use std::os::unix::prelude::AsRawFd;

        // Use O_NOATIME to prevent updating the file's access time (atime) during read operations.
        // We should not assume the filesystem is mounted with the 'noatime' option.
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOATIME)
            .open(&path)?;

        let mut vec = Vec::<u8>::with_capacity(size);
        let vec_ptr = vec.as_mut_ptr();
        let fd = file.as_raw_fd();
        let mut total_read = 0;

        while total_read < size {
            let result = unsafe {
                read_syscall.read(
                    fd,
                    vec_ptr.add(total_read) as *mut libc::c_void,
                    (size - total_read) as libc::size_t,
                )
            };

            match result {
                n if n > 0 => total_read += n as usize,
                0 => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        format!(
                            "Invalid cache item, expects {} bytes, got {}, path {:?}",
                            size, total_read, path
                        ),
                    ));
                }
                _ => {
                    let err = std::io::Error::last_os_error();
                    if err.raw_os_error() == Some(libc::EINTR) {
                        continue;
                    } else {
                        return Err(err);
                    }
                }
            }
        }

        // SAFETY:
        // - `vec` has sufficient capacity for `size` elements (ensured by prior allocation)
        // - All bytes in 0..size range are initialized (we've successfully read `size` bytes)
        unsafe { vec.set_len(size) };
        Ok(vec)
    }

    #[cfg(test)]
    mod tests_read_content {
        use std::fs::File;

        use mockall::Sequence;
        use mockall::predicate::*;
        use tempfile::TempDir;

        use super::*;

        struct TestFixture {
            temp_dir: TempDir,
        }

        impl TestFixture {
            pub fn new() -> TestFixture {
                TestFixture {
                    temp_dir: tempfile::Builder::new()
                        .prefix("read-cache-content-test")
                        .tempdir()
                        .unwrap(),
                }
            }

            pub fn create_test_file(&self, name: &str) -> PathBuf {
                let path = self.temp_dir.path().join(name);
                // We do NOT care about the content of the file being read in this suite;
                // those cases are covered by other unit tests (in the integration test suite).
                //
                // But we still need to prepare a file here, since `read_cache_content_with` will open it.
                File::create(&path).unwrap();
                path
            }
        }

        #[test]
        fn test_read_success_in_one_go() {
            let fixture = TestFixture::new();
            let path = fixture.create_test_file("test.txt");
            let size = 100;
            let mut mock = MockCallRead::new();

            mock.expect_read()
                .with(always(), always(), eq(100))
                .return_once(|_, _, _| 100);

            let result = read_cache_content_with_syscall(path, size, mock);
            assert!(result.is_ok());
            assert_eq!(result.unwrap().len(), size);
        }

        #[test]
        fn test_partial_reads() {
            let fixture = TestFixture::new();
            let path = fixture.create_test_file("test.txt");
            let size = 100;
            let mut mock = MockCallRead::new();
            let mut seq = Sequence::new();

            mock.expect_read()
                .times(1)
                .in_sequence(&mut seq)
                .returning(|_, _, _| 50);
            mock.expect_read()
                .times(1)
                .in_sequence(&mut seq)
                .returning(|_, _, _| 25);
            mock.expect_read()
                .times(1)
                .in_sequence(&mut seq)
                .returning(|_, _, _| 25);

            let result = read_cache_content_with_syscall(path, size, mock);
            assert!(result.is_ok());
            assert_eq!(result.unwrap().len(), size);
        }

        #[test]
        fn test_eintr_retry() {
            let fixture = TestFixture::new();
            let path = fixture.create_test_file("test.txt");
            let size = 100;
            let mut mock = MockCallRead::new();
            let mut seq = Sequence::new();

            mock.expect_read()
                .times(1)
                .in_sequence(&mut seq)
                .returning(|_, _, _| 80);

            mock.expect_read()
                .times(1)
                .in_sequence(&mut seq)
                .returning(|_, _, _| {
                    unsafe {
                        *libc::__errno_location() = libc::EINTR;
                    };
                    -1
                });

            mock.expect_read()
                .times(1)
                .in_sequence(&mut seq)
                .returning(|_, _, _| 20);

            let result = read_cache_content_with_syscall(path, size, mock);
            assert!(result.is_ok());
        }

        #[test]
        fn test_unexpected_eof() {
            let fixture = TestFixture::new();
            let path = fixture.create_test_file("test.txt");
            let size = 100;
            let mut mock = MockCallRead::new();
            let mut seq = Sequence::new();

            mock.expect_read()
                .times(1)
                .in_sequence(&mut seq)
                .return_once(|_, _, _| 50);

            // Simulating the size of cache item is lesser than expected
            mock.expect_read()
                .times(1)
                .in_sequence(&mut seq)
                .return_once(|_, _, _| 0);

            let result = read_cache_content_with_syscall(path, size, mock);
            assert!(result.is_err());
            assert_eq!(
                result.unwrap_err().kind(),
                std::io::ErrorKind::UnexpectedEof
            );
        }

        #[test]
        fn test_io_error() {
            let fixture = TestFixture::new();
            let path = fixture.create_test_file("test.txt");
            let size = 100;
            let mut mock = MockCallRead::new();

            let mut seq = Sequence::new();

            mock.expect_read()
                .times(1)
                .in_sequence(&mut seq)
                .return_once(|_, _, _| 50);

            mock.expect_read()
                .times(1)
                .in_sequence(&mut seq)
                .return_once(|_, _, _| {
                    unsafe {
                        *libc::__errno_location() = libc::EIO;
                    };
                    -1
                });

            let result = read_cache_content_with_syscall(path, size, mock);
            assert!(result.is_err());
            assert_eq!(result.unwrap_err().raw_os_error(), Some(libc::EIO));
        }
    }
}

#[cfg(test)]
mod tests_mget {
    use databend_common_config::DiskCacheKeyReloadPolicy;
    use tempfile::TempDir;

    use super::*;
    use crate::CacheAccessor;

    fn new_cache(bytes_capacity: usize) -> (TempDir, LruDiskCacheHolder) {
        let dir = TempDir::new().unwrap();
        // The population worker is irrelevant for holder-level tests; the
        // queue endpoint is dropped so `populate` becomes a silent no-op.
        let (population_tx, _population_rx) = crossbeam_channel::bounded(1);
        let holder = LruDiskCacheBuilder::new_disk_cache(
            &dir.path().to_path_buf(),
            bytes_capacity,
            DiskCacheKeyReloadPolicy::Reset,
            false,
            population_tx,
        )
        .unwrap();
        (dir, holder)
    }

    #[test]
    fn test_mget_results_align_with_keys() {
        let (_dir, cache) = new_cache(1 << 20);
        cache.insert("a".to_string(), Bytes::from_static(b"alpha"));
        cache.insert("c".to_string(), Bytes::from_static(b"charlie"));

        let results = cache.mget(&["a", "b", "c"]);
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].as_deref(), Some(&Bytes::from_static(b"alpha")));
        assert!(results[1].is_none());
        assert_eq!(results[2].as_deref(), Some(&Bytes::from_static(b"charlie")));
    }

    #[test]
    fn test_mget_touches_the_lru_order() {
        // Each 8-byte value is stored as 12 bytes (value + crc32); the
        // capacity holds exactly two entries.
        let (_dir, cache) = new_cache(24);
        cache.insert("a".to_string(), Bytes::from_static(b"aaaaaaaa"));
        cache.insert("b".to_string(), Bytes::from_static(b"bbbbbbbb"));

        // Touch `a`, making `b` the eviction candidate.
        assert!(cache.mget(&["a"])[0].is_some());
        cache.insert("c".to_string(), Bytes::from_static(b"cccccccc"));

        assert!(cache.contains_key("a"));
        assert!(!cache.contains_key("b"));
        assert!(cache.contains_key("c"));
    }

    #[test]
    fn test_mget_evicts_corrupted_entries() {
        let (_dir, cache) = new_cache(1 << 20);
        cache.insert("good".to_string(), Bytes::from_static(b"payload"));
        cache.insert("bad".to_string(), Bytes::from_static(b"payload"));

        // Corrupt the stored file in place, keeping its length intact so the
        // read succeeds and only the crc validation fails.
        let (path, size) = {
            let mut inner = cache.write();
            inner.get_cache_item_path_and_size("bad").unwrap()
        };
        std::fs::write(&path, vec![0xAB_u8; size as usize]).unwrap();

        let results = cache.mget(&["good", "bad"]);
        assert_eq!(results[0].as_deref(), Some(&Bytes::from_static(b"payload")));
        assert!(results[1].is_none());
        // The corrupted entry was evicted on the spot.
        assert!(!cache.contains_key("bad"));
        assert!(cache.contains_key("good"));
    }
}
