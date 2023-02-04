// Copyright 2023 Datafuse Labs.
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
use std::io::prelude::*;
use std::path::Path;
use std::path::PathBuf;

use siphasher::sip128;
use siphasher::sip128::Hasher128;
use tracing::error;
use walkdir::WalkDir;

use crate::Cache;
use crate::DefaultHashBuilder;
use crate::FileSize;
use crate::LruCache;

// TODO doc the disk cache path layout
// TODO checksum of cached data

/// Return an iterator of `(path, size)` of files under `path` sorted by ascending last-modified
/// time, such that the oldest modified file is returned first.
fn get_all_files<P: AsRef<Path>>(path: P) -> impl Iterator<Item = (PathBuf, u64)> {
    WalkDir::new(path.as_ref()).into_iter().filter_map(|e| {
        e.ok().and_then(|f| {
            // Only look at files
            if f.file_type().is_file() {
                f.metadata().ok().map(|m| (f.path().to_owned(), m.len()))
            } else {
                None
            }
        })
    })
}

/// An LRU cache of files on disk.
pub type LruDiskCache = DiskCache<LruCache<String, u64, DefaultHashBuilder, FileSize>>;

/// An basic disk cache of files on disk.
pub struct DiskCache<C> {
    cache: C,
    root: PathBuf,
}

// make it public for IT
pub struct CacheKey(String);

impl<S> From<S> for CacheKey
where S: AsRef<str>
{
    // convert key string into hex string of SipHash 2-4 128
    fn from(key: S) -> Self {
        let mut sip = sip128::SipHasher24::new();
        let key = key.as_ref();
        sip.write(key.as_bytes());
        let hash = sip.finish128();
        let hex_hash = hex::encode(hash.as_bytes());
        CacheKey(hex_hash)
    }
}

impl From<&CacheKey> for PathBuf {
    fn from(cache_key: &CacheKey) -> Self {
        let prefix = &cache_key.0[0..3];
        let mut path_buf = PathBuf::from(prefix);
        path_buf.push(Path::new(&cache_key.0));
        path_buf
    }
}

impl<C> DiskCache<C>
where C: Cache<String, u64, DefaultHashBuilder, FileSize>
{
    /// Create an `DiskCache` with `ritelinked::DefaultHashBuilder` that stores files in `path`,
    /// limited to `size` bytes.
    ///
    /// Existing files in `path` will be stored with their last-modified time from the filesystem
    /// used as the order for the recency of their use. Any files that are individually larger
    /// than `size` bytes will be removed.
    ///
    /// The cache is not observant of changes to files under `path` from external sources, it
    /// expects to have sole maintenance of the contents.
    pub fn new<T>(path: T, size: u64) -> Result<Self>
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

    /// Scan `self.root` for existing files and store them.
    fn init(mut self) -> Result<Self> {
        fs::create_dir_all(&self.root)?;
        for (file, size) in get_all_files(&self.root) {
            if !self.can_store(size) {
                fs::remove_file(file).unwrap_or_else(|e| {
                    error!(
                        "Error removing file `{}` which is too large for the cache ({} bytes)",
                        e, size
                    )
                });
            } else {
                while self.cache.size() + size > self.cache.capacity() {
                    let (rel_path, _) = self
                        .cache
                        .pop_by_policy()
                        .expect("Unexpectedly empty cache!");
                    let cache_item_path = self.cache_absolute_path(&CacheKey(rel_path));
                    fs::remove_file(&cache_item_path).unwrap_or_else(|e| {
                        error!(
                            "Error removing file from cache: `{:?}`: {}",
                            cache_item_path, e
                        )
                    });
                }
                let relative_path = file
                    .strip_prefix(&self.root)
                    .map_err(|_e| self::Error::MalformedPath)?;
                let cache_key = Self::recovery_from(relative_path);
                self.cache.put(cache_key, size);
            }
        }
        Ok(self)
    }

    /// Returns `true` if the disk cache can store a file of `size` bytes.
    pub fn can_store(&self, size: u64) -> bool {
        size <= self.cache.capacity()
    }

    fn recovery_from(relative_path: &Path) -> String {
        let key_string = match relative_path.file_name() {
            Some(file_name) => match file_name.to_str() {
                Some(str) => str.to_owned(),
                None => {
                    // relative_path is constructed by ourself, and shall be valid utf8 string
                    unreachable!()
                }
            },
            None => {
                // only called during init, and only path of files are passed in
                unreachable!()
            }
        };
        key_string
    }

    fn cache_key(&self, key: &str) -> CacheKey {
        CacheKey::from(key)
    }

    fn cache_absolute_path(&self, cache_key: &CacheKey) -> PathBuf {
        let path = PathBuf::from(cache_key);
        self.rel_to_abs_path(path)
    }

    /// Add a file with `bytes` as its contents to the cache at path `key`.
    pub fn insert_bytes(&mut self, key: &str, bytes: &[u8]) -> Result<()> {
        let item_len = bytes.len() as u64;
        // check if this chunk of bytes itself is too large
        if !self.can_store(item_len) {
            return Err(Error::FileTooLarge);
        }

        // check eviction
        if self.cache.size() + item_len > self.cache.capacity() {
            if let Some((rel_path, _)) = self.cache.pop_by_policy() {
                let cached_item_path = self.cache_absolute_path(&CacheKey(rel_path));
                fs::remove_file(&cached_item_path).unwrap_or_else(|e| {
                    error!(
                        "Error removing file from cache: `{:?}`: {}",
                        cached_item_path, e
                    )
                });
            }
        }

        let cache_key = self.cache_key(key.as_ref());
        let path = self.cache_absolute_path(&cache_key);
        if let Some(parent_path) = path.parent() {
            fs::create_dir_all(parent_path)?;
        }
        let mut f = File::create(&path)?;
        f.write_all(bytes)?;
        self.cache.put(cache_key.0, bytes.len() as u64);
        Ok(())
    }

    /// Return `true` if a file with path `key` is in the cache.
    pub fn contains_key(&self, key: &str) -> bool {
        let cache_key = self.cache_key(key);
        self.cache.contains(&cache_key.0)
    }

    /// Get an opened `File` for `key`, if one exists and can be opened. Updates the Cache state
    /// of the file if present. Avoid using this method if at all possible, prefer `.get`.
    pub fn get_file(&mut self, key: &str) -> Result<File> {
        let cache_key = self.cache_key(key);
        let path = self.cache_absolute_path(&cache_key);
        self.cache
            .get(&cache_key.0)
            .ok_or(Error::FileNotInCache)
            .and_then(|_len| File::open(path).map_err(Into::into))
    }

    /// Remove the given key from the cache.
    pub fn remove(&mut self, key: &str) -> Result<()> {
        let cache_key = self.cache_key(key);
        match self.cache.pop(&cache_key.0) {
            Some(_) => {
                let path = self.cache_absolute_path(&cache_key);
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
        FileNotInCache,
        /// The file was not in the cache.
        MalformedPath,
        /// An IO Error occurred.
        Io(io::Error),
    }

    impl fmt::Display for Error {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Error::FileTooLarge => write!(f, "File too large"),
                Error::FileNotInCache => write!(f, "File not in cache"),
                Error::MalformedPath => write!(f, "Malformed catch file path"),
                Error::Io(ref e) => write!(f, "{}", e),
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
