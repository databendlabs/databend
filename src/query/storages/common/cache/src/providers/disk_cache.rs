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

use std::fs;
use std::fs::File;
use std::hash::Hasher;
use std::io::IoSlice;
use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use databend_common_cache::Cache;
use databend_common_cache::Count;
use databend_common_cache::DefaultHashBuilder;
use databend_common_cache::FileSize;
use databend_common_cache::LruCache;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use log::error;
use log::warn;
use parking_lot::RwLock;
use siphasher::sip128;
use siphasher::sip128::Hasher128;

use crate::CacheAccessor;

pub struct DiskCache<C> {
    cache: C,
    root: PathBuf,
}

pub struct DiskCacheKey(String);

impl<S> From<S> for DiskCacheKey
where S: AsRef<str>
{
    // convert key string into hex string of SipHash 2-4 128 bit
    fn from(key: S) -> Self {
        let mut sip = sip128::SipHasher24::new();
        let key = key.as_ref();
        sip.write(key.as_bytes());
        let hash = sip.finish128();
        let hex_hash = hex::encode(hash.as_bytes());
        DiskCacheKey(hex_hash)
    }
}

impl From<&DiskCacheKey> for PathBuf {
    fn from(cache_key: &DiskCacheKey) -> Self {
        let prefix = &cache_key.0[0..3];
        let mut path_buf = PathBuf::from(prefix);
        path_buf.push(Path::new(&cache_key.0));
        path_buf
    }
}

impl<C> DiskCache<C>
where C: Cache<String, u64, DefaultHashBuilder, FileSize>
{
    /// Create an `DiskCache` with `hashbrown::hash_map::DefaultHashBuilder` that stores files in `path`,
    /// limited to `size` bytes.
    ///
    /// Existing files in `path` will be stored with their last-modified time from the filesystem
    /// used as the order for the recency of their use. Any files that are individually larger
    /// than `size` bytes will be removed.
    ///
    /// The cache is not observant of changes to files under `path` from external sources, it
    /// expects to have sole maintenance of the contents.
    pub fn new<T>(path: T, size: u64) -> self::result::Result<Self>
    where PathBuf: From<T> {
        DiskCache {
            cache: C::with_meter_and_hasher(size, FileSize, DefaultHashBuilder::default()),
            root: PathBuf::from(path),
        }
        .init()
    }
}

impl<C> DiskCache<C>
where C: Cache<String, u64, DefaultHashBuilder, FileSize>
{
    /// Return the current size of all the files in the cache.
    pub fn size(&self) -> u64 {
        self.cache.size()
    }

    /// Return the count of entries in the cache.
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    pub fn is_empty(&self) -> bool {
        self.cache.len() == 0
    }

    /// Return the maximum size of the cache.
    pub fn capacity(&self) -> u64 {
        self.cache.capacity()
    }

    /// Return the path in which the cache is stored.
    pub fn path(&self) -> &Path {
        self.root.as_path()
    }

    /// Return the path that `key` would be stored at.
    fn rel_to_abs_path<K: AsRef<Path>>(&self, rel_path: K) -> PathBuf {
        self.root.join(rel_path)
    }

    fn init(self) -> self::result::Result<Self> {
        // remove dir when init, ignore remove error
        if let Err(e) = fs::remove_dir_all(&self.root) {
            warn!("remove disk cache dir {:?} error {}", self.root, e);
        }
        fs::create_dir_all(&self.root)?;

        Ok(self)
    }

    /// Returns `true` if the disk cache can store a file of `size` bytes.
    pub fn can_store(&self, size: u64) -> bool {
        size <= self.cache.capacity()
    }

    fn cache_key(&self, key: &str) -> DiskCacheKey {
        DiskCacheKey::from(key)
    }

    fn abs_path_of_cache_key(&self, cache_key: &DiskCacheKey) -> PathBuf {
        let path = PathBuf::from(cache_key);
        self.rel_to_abs_path(path)
    }

    pub fn insert_bytes(&mut self, key: &str, bytes: &[&[u8]]) -> self::result::Result<()> {
        let bytes_len = bytes.iter().map(|x| x.len() as u64).sum::<u64>();
        // check if this chunk of bytes itself is too large
        if !self.can_store(bytes_len) {
            return Err(Error::FileTooLarge);
        }

        // check eviction
        while self.cache.size() + bytes_len > self.cache.capacity() {
            if let Some((rel_path, _)) = self.cache.pop_by_policy() {
                let cached_item_path = self.abs_path_of_cache_key(&DiskCacheKey(rel_path));
                fs::remove_file(&cached_item_path).unwrap_or_else(|e| {
                    error!(
                        "Error removing file from cache: `{:?}`: {}",
                        cached_item_path, e
                    )
                });
            }
        }
        debug_assert!(self.cache.size() <= self.cache.capacity());

        let cache_key = self.cache_key(key.as_ref());
        let path = self.abs_path_of_cache_key(&cache_key);
        if let Some(parent_path) = path.parent() {
            fs::create_dir_all(parent_path)?;
        }
        let mut f = File::create(&path)?;
        let mut bufs = Vec::with_capacity(bytes.len());
        for slick in bytes {
            bufs.push(IoSlice::new(slick));
        }
        f.write_all_vectored(&mut bufs)?;
        self.cache.put(cache_key.0, bytes_len);
        Ok(())
    }

    /// Return `true` if a file with path `key` is in the cache.
    pub fn contains_key(&self, key: &str) -> bool {
        let cache_key = self.cache_key(key);
        self.cache.contains(&cache_key.0)
    }

    pub fn get_cache_path(&mut self, key: &str) -> Option<PathBuf> {
        let cache_key = self.cache_key(key);
        self.cache
            .get(&cache_key.0)
            .map(|_| ()) // release the &mut self
            .map(|_| self.abs_path_of_cache_key(&cache_key))
    }

    /// Remove the given key from the cache.
    pub fn remove(&mut self, key: &str) -> Result<()> {
        let cache_key = self.cache_key(key);
        match self.cache.pop(&cache_key.0) {
            Some(_) => {
                let path = self.abs_path_of_cache_key(&cache_key);
                fs::remove_file(&path).map_err(|e| {
                    error!("Error removing file from cache: `{:?}`: {}", path, e);
                    Into::into(e)
                })
            }
            None => Ok(()),
        }
    }
}

pub mod result {
    use std::error::Error as StdError;
    use std::fmt;
    use std::io;

    /// Errors returned by this crate.
    #[derive(Debug)]
    pub enum Error {
        /// The file was too large to fit in the cache.
        FileTooLarge,
        /// The file was not in the cache.
        MalformedPath,
        /// An IO Error occurred.
        Io(io::Error),
    }

    impl fmt::Display for Error {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            match self {
                Error::FileTooLarge => write!(f, "File too large"),
                Error::MalformedPath => write!(f, "Malformed catch file path"),
                Error::Io(ref e) => write!(f, "{e}"),
            }
        }
    }

    impl StdError for Error {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            match self {
                Error::Io(ref e) => Some(e),
                _ => None,
            }
        }
    }

    impl From<io::Error> for Error {
        fn from(e: io::Error) -> Error {
            Error::Io(e)
        }
    }

    /// A convenience `Result` type
    pub type Result<T> = std::result::Result<T, Error>;
}

use result::*;

impl CacheAccessor<String, Bytes, databend_common_cache::DefaultHashBuilder, Count>
    for LruDiskCacheHolder
{
    fn get<Q: AsRef<str>>(&self, k: Q) -> Option<Arc<Bytes>> {
        let k = k.as_ref();
        {
            let mut cache = self.write();
            cache.get_cache_path(k)
        }
        .and_then(|cache_file_path| {
            // check disk cache
            let get_cache_content = || {
                let mut v = vec![];
                let mut file = File::open(cache_file_path)?;
                file.read_to_end(&mut v)?;
                Ok::<_, Box<dyn std::error::Error>>(v)
            };

            match get_cache_content() {
                Ok(mut bytes) => {
                    if let Err(e) = validate_checksum(bytes.as_slice()) {
                        error!("data cache, of key {k},  crc validation failure: {e}");
                        {
                            // remove the invalid cache, error of removal ignored
                            let r = {
                                let mut cache = self.write();
                                cache.remove(k)
                            };
                            if let Err(e) = r {
                                warn!("failed to remove invalid cache item, key {k}. {e}");
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
                    error!("get disk cache item failed, cache_key {k}. {e}");
                    None
                }
            }
        })
    }

    fn put(&self, key: String, value: Arc<Bytes>) {
        let crc = crc32fast::hash(value.as_ref());
        let crc_bytes = crc.to_le_bytes();
        let mut cache = self.write();
        if let Err(e) = cache.insert_bytes(&key, &[value.as_ref(), &crc_bytes]) {
            error!("put disk cache item failed {}", e);
        }
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

    fn size(&self) -> u64 {
        let cache = self.read();
        cache.size()
    }

    fn len(&self) -> usize {
        let cache = self.read();
        cache.len()
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
                "crc checksum validation failure, key : crc checksum not match, crc provided {crc_provided}, crc calculated {crc_calculated}"
            )))
        }
    }
}

pub type LruDiskCache = DiskCache<LruCache<String, u64, DefaultHashBuilder, FileSize>>;
pub type LruDiskCacheHolder = Arc<RwLock<LruDiskCache>>;

pub struct LruDiskCacheBuilder;
impl LruDiskCacheBuilder {
    pub fn new_disk_cache(
        path: &PathBuf,
        disk_cache_bytes_size: u64,
    ) -> Result<LruDiskCacheHolder> {
        let external_cache = DiskCache::new(path, disk_cache_bytes_size)
            .map_err(|e| ErrorCode::StorageOther(format!("create disk cache failed, {e}")))?;
        Ok(Arc::new(RwLock::new(external_cache)))
    }
}
