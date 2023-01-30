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

use std::boxed::Box;
use std::fs;
use std::fs::File;
use std::hash::BuildHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::io::prelude::*;
use std::path::Path;
use std::path::PathBuf;

use filetime::set_file_times;
use filetime::FileTime;
use tracing::error;
use walkdir::WalkDir;

use crate::Cache;
use crate::DefaultHashBuilder;
use crate::FileSize;
use crate::LruCache;

// TODO doc the disk cache path layout
// TODO extract new type CacheKey
// TODO checksum of cached data

/// Return an iterator of `(path, size)` of files under `path` sorted by ascending last-modified
/// time, such that the oldest modified file is returned first.
fn get_all_files<P: AsRef<Path>>(path: P) -> Box<dyn Iterator<Item = (PathBuf, u64)>> {
    let mut files: Vec<_> = WalkDir::new(path.as_ref())
        .into_iter()
        .filter_map(|e| {
            e.ok().and_then(|f| {
                // Only look at files
                if f.file_type().is_file() {
                    // Get the last-modified time, size, and the full path.
                    f.metadata().ok().and_then(|m| {
                        m.modified()
                            .ok()
                            .map(|mtime| (mtime, f.path().to_owned(), m.len()))
                    })
                } else {
                    None
                }
            })
        })
        .collect();
    // Sort by last-modified-time, so oldest file first.
    files.sort_by_key(|k| k.0);
    Box::new(files.into_iter().map(|(_mtime, path, size)| (path, size)))
}

/// An LRU cache of files on disk.
pub type LruDiskCache = DiskCache<LruCache<String, u64, DefaultHashBuilder, FileSize>>;

/// An basic disk cache of files on disk.
pub struct DiskCache<C, S: BuildHasher + Clone = DefaultHashBuilder>
where C: Cache<String, u64, S, FileSize>
{
    hash_builder: S,
    cache: C,
    root: PathBuf,
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
        let default_hash_builder = DefaultHashBuilder::new();
        DiskCache {
            hash_builder: default_hash_builder.clone(),
            cache: C::with_meter_and_hasher(size, FileSize, default_hash_builder),
            root: PathBuf::from(path),
        }
        .init()
    }
}

impl<C, S> DiskCache<C, S>
where
    C: Cache<String, u64, S, FileSize>,
    S: BuildHasher + Clone,
{
    /// Create an `DiskCache` with hasher that stores files in `path`, limited to `size` bytes.
    ///
    /// Existing files in `path` will be stored with their last-modified time from the filesystem
    /// used as the order for the recency of their use. Any files that are individually larger
    /// than `size` bytes will be removed.
    ///
    /// The cache is not observant of changes to files under `path` from external sources, it
    /// expects to have sole maintenance of the contents.
    pub fn new_with_hasher<T>(path: T, size: u64, hash_builder: S) -> Result<Self>
    where PathBuf: From<T> {
        DiskCache {
            hash_builder: hash_builder.clone(),
            cache: C::with_meter_and_hasher(size, FileSize, hash_builder),
            root: PathBuf::from(path),
        }
        .init()
    }

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
                    let remove_path = self.rel_to_abs_path(&rel_path);
                    // TODO: check that files are removable during `init`, so that this is only
                    // due to outside interference.
                    fs::remove_file(&remove_path).unwrap_or_else(|e| {
                        panic!("Error removing file from cache: `{:?}`: {}", remove_path, e)
                    });
                }
                let rel_path = file
                    .strip_prefix(&self.root)
                    .map_err(|_e| self::Error::MalformedPath)?;
                let cache_key = Self::recovery_from(rel_path);
                self.cache.put(cache_key, size);
            }
        }
        Ok(self)
    }

    /// Returns `true` if the disk cache can store a file of `size` bytes.
    pub fn can_store(&self, size: u64) -> bool {
        size <= self.cache.capacity()
    }

    pub fn recovery_from(str: &Path) -> String {
        let key_string = match str.as_os_str().to_str() {
            Some(str) => str.to_owned(),
            None => {
                unreachable!()
            }
        };
        key_string
    }

    pub fn cache_key<K>(&self, str: &K) -> String
    where K: Hash + Eq + ?Sized {
        // TODO we need a 128 bit digest
        let mut hash_state = self.hash_builder.build_hasher();
        str.hash(&mut hash_state);
        let digits = hash_state.finish();
        let hex_key = format!("{:x}", digits);
        hex_key
    }

    pub fn cache_path<K>(&self, str: &K) -> PathBuf
    where K: Hash + Eq + ?Sized {
        let hex_key = self.cache_key(str);
        let prefix = &hex_key[0..3];
        let mut path_buf = PathBuf::from(prefix);
        path_buf.push(Path::new(&hex_key));
        path_buf
    }

    /// Add a file with `bytes` as its contents to the cache at path `key`.
    pub fn insert_bytes<K: AsRef<str>>(&mut self, key: K, bytes: &[u8]) -> Result<()> {
        if !self.can_store(bytes.len() as u64) {
            return Err(Error::FileTooLarge);
        }

        // TODO combine these
        let cache_key = self.cache_key(key.as_ref());
        let rel_path = self.cache_path(key.as_ref());
        let path = self.rel_to_abs_path(rel_path);
        // TODO rm this panic, no nested dirs here
        fs::create_dir_all(path.parent().expect("Bad path?"))?;
        let mut f = File::create(&path)?;
        f.write_all(bytes)?;
        let size = bytes.len() as u64;
        if let Some(_replaced) = self.cache.put(cache_key, size) {
            // TODO remove the replaced item from disk
        }
        Ok(())
    }

    /// Return `true` if a file with path `key` is in the cache.
    pub fn contains_key<K: AsRef<str>>(&self, key: &K) -> bool
    where K: Hash + Eq + ?Sized {
        let cache_key = self.cache_key(key);
        self.cache.contains(&cache_key)
    }

    /// Get an opened `File` for `key`, if one exists and can be opened. Updates the Cache state
    /// of the file if present. Avoid using this method if at all possible, prefer `.get`.
    pub fn get_file<K>(&mut self, key: &K) -> Result<File>
    where K: Hash + Eq + ?Sized {
        let cache_key = self.cache_key(key);
        let rel_path = self.cache_path(key);
        let path = self.rel_to_abs_path(rel_path);
        self.cache
            .get(&cache_key)
            .ok_or(Error::FileNotInCache)
            .and_then(|_| {
                // TODO do we need to adjust the mtime, cross reboot?
                let t = FileTime::now();
                set_file_times(&path, t, t)?;
                File::open(path).map_err(Into::into)
            })
    }

    /// Remove the given key from the cache.
    pub fn remove<K>(&mut self, key: &K) -> Result<()>
    where K: Hash + Eq + ?Sized {
        let cache_key = self.cache_key(key);
        let rel_path = self.cache_path(key);
        match self.cache.pop(&cache_key) {
            Some(_) => {
                let path = self.rel_to_abs_path(rel_path);
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
