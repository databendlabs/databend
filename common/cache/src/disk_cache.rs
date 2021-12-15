// Copyright 2021 Datafuse Labs.
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
use std::ffi::OsStr;
use std::ffi::OsString;
use std::fs;
use std::fs::File;
use std::hash::BuildHasher;
use std::io;
use std::io::prelude::*;
use std::path::Path;
use std::path::PathBuf;

use common_tracing::tracing;
use filetime::set_file_times;
use filetime::FileTime;
use ritelinked::DefaultHashBuilder;
use walkdir::WalkDir;

use crate::Cache;
use crate::FileSize;
use crate::LruCache;

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
pub type LruDiskCache = DiskCache<LruCache<OsString, u64, DefaultHashBuilder, FileSize>>;

/// An basic disk cache of files on disk.
#[derive(Debug)]
pub struct DiskCache<C, S: BuildHasher + Clone = DefaultHashBuilder>
where C: Cache<OsString, u64, S, FileSize>
{
    #[allow(dead_code)]
    hash_builder: S,
    cache: C,
    root: PathBuf,
}

/// Trait objects can't be bounded by more than one non-builtin trait.
pub trait ReadSeek: Read + Seek + Send {}

impl<T: Read + Seek + Send> ReadSeek for T {}

enum AddFile<'a> {
    AbsPath(PathBuf),
    RelPath(&'a OsStr),
}

impl<C> DiskCache<C, DefaultHashBuilder>
where C: Cache<OsString, u64, DefaultHashBuilder, FileSize>
{
    /// Create an `DiskCache` with `ritelinked::DefaultHashBuilder` that stores files in `path`,
    /// limited to `size` bytes.
    ///
    /// Existing files in `path` will be stored with their last-modified time from the filesystem
    /// used as the order for the recency of their use. Any files that are individually larger
    /// than `size` bytes will be removed.
    ///
    /// The cache is not observant of changes to files under `path` from external sources, it
    /// expects to have sole maintence of the contents.
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
    C: Cache<OsString, u64, S, FileSize>,
    S: BuildHasher + Clone,
{
    /// Create an `DiskCache` with hasher that stores files in `path`, limited to `size` bytes.
    ///
    /// Existing files in `path` will be stored with their last-modified time from the filesystem
    /// used as the order for the recency of their use. Any files that are individually larger
    /// than `size` bytes will be removed.
    ///
    /// The cache is not observant of changes to files under `path` from external sources, it
    /// expects to have sole maintence of the contents.
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
                    tracing::error!(
                        "Error removing file `{}` which is too large for the cache ({} bytes)",
                        e,
                        size
                    )
                });
            } else {
                self.add_file(AddFile::AbsPath(file), size)
                    .unwrap_or_else(|e| tracing::error!("Error adding file: {}", e));
            }
        }
        Ok(self)
    }

    /// Returns `true` if the disk cache can store a file of `size` bytes.
    pub fn can_store(&self, size: u64) -> bool {
        size <= self.cache.capacity() as u64
    }

    /// Add the file at `path` of size `size` to the cache.
    fn add_file(&mut self, addfile_path: AddFile<'_>, size: u64) -> Result<()> {
        if !self.can_store(size) {
            return Err(Error::FileTooLarge);
        }
        let rel_path = match addfile_path {
            AddFile::AbsPath(ref p) => p.strip_prefix(&self.root).expect("Bad path?").as_os_str(),
            AddFile::RelPath(p) => p,
        };
        //TODO: ideally Cache::put would give us back the entries it had to remove.
        while self.cache.size() as u64 + size > self.cache.capacity() as u64 {
            let (rel_path, _) = self
                .cache
                .pop_by_policy()
                .expect("Unexpectedly empty cache!");
            let remove_path = self.rel_to_abs_path(rel_path);
            //TODO: check that files are removable during `init`, so that this is only
            // due to outside interference.
            fs::remove_file(&remove_path).unwrap_or_else(|e| {
                panic!("Error removing file from cache: `{:?}`: {}", remove_path, e)
            });
        }
        self.cache.put(rel_path.to_owned(), size);
        Ok(())
    }

    fn insert_by<K: AsRef<OsStr>, F: FnOnce(&Path) -> io::Result<()>>(
        &mut self,
        key: K,
        size: Option<u64>,
        by: F,
    ) -> Result<()> {
        if let Some(size) = size {
            if !self.can_store(size) {
                return Err(Error::FileTooLarge);
            }
        }
        let rel_path = key.as_ref();
        let path = self.rel_to_abs_path(rel_path);
        fs::create_dir_all(path.parent().expect("Bad path?"))?;
        by(&path)?;
        let size = size.unwrap_or(fs::metadata(path)?.len());
        self.add_file(AddFile::RelPath(rel_path), size)
            .map_err(|e| {
                tracing::error!(
                    "Failed to insert file `{}`: {}",
                    rel_path.to_string_lossy(),
                    e
                );
                fs::remove_file(&self.rel_to_abs_path(rel_path))
                    .expect("Failed to remove file we just created!");
                e
            })
    }

    /// Add a file by calling `with` with the open `File` corresponding to the cache at path `key`.
    pub fn insert_with<K: AsRef<OsStr>, F: FnOnce(File) -> io::Result<()>>(
        &mut self,
        key: K,
        with: F,
    ) -> Result<()> {
        self.insert_by(key, None, |path| with(File::create(&path)?))
    }

    /// Add a file with `bytes` as its contents to the cache at path `key`.
    pub fn insert_bytes<K: AsRef<OsStr>>(&mut self, key: K, bytes: &[u8]) -> Result<()> {
        self.insert_by(key, Some(bytes.len() as u64), |path| {
            let mut f = File::create(&path)?;
            f.write_all(bytes)?;
            Ok(())
        })
    }

    /// Add an existing file at `path` to the cache at path `key`.
    pub fn insert_file<K: AsRef<OsStr>, P: AsRef<OsStr>>(&mut self, key: K, path: P) -> Result<()> {
        let size = fs::metadata(path.as_ref())?.len();
        self.insert_by(key, Some(size), |new_path| {
            fs::rename(path.as_ref(), new_path).or_else(|_| {
                tracing::warn!("fs::rename failed, falling back to copy!");
                fs::copy(path.as_ref(), new_path)?;
                fs::remove_file(path.as_ref()).unwrap_or_else(|e| {
                    tracing::error!("Failed to remove original file in insert_file: {}", e)
                });
                Ok(())
            })
        })
    }

    /// Return `true` if a file with path `key` is in the cache.
    pub fn contains_key<K: AsRef<OsStr>>(&self, key: K) -> bool {
        self.cache.contains(key.as_ref())
    }

    /// Get an opened `File` for `key`, if one exists and can be opened. Updates the Cache state
    /// of the file if present. Avoid using this method if at all possible, prefer `.get`.
    pub fn get_file<K: AsRef<OsStr>>(&mut self, key: K) -> Result<File> {
        let rel_path = key.as_ref();
        let path = self.rel_to_abs_path(rel_path);
        self.cache
            .get(rel_path)
            .ok_or(Error::FileNotInCache)
            .and_then(|_| {
                let t = FileTime::now();
                set_file_times(&path, t, t)?;
                File::open(path).map_err(Into::into)
            })
    }

    /// Get an opened readable and seekable handle to the file at `key`, if one exists and can
    /// be opened. Updates the Cache state of the file if present.
    pub fn get<K: AsRef<OsStr>>(&mut self, key: K) -> Result<Box<dyn ReadSeek>> {
        self.get_file(key).map(|f| Box::new(f) as Box<dyn ReadSeek>)
    }

    /// Remove the given key from the cache.
    pub fn remove<K: AsRef<OsStr>>(&mut self, key: K) -> Result<()> {
        match self.cache.pop(key.as_ref()) {
            Some(_) => {
                let path = self.rel_to_abs_path(key.as_ref());
                fs::remove_file(&path).map_err(|e| {
                    tracing::error!("Error removing file from cache: `{:?}`: {}", path, e);
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
        /// An IO Error occurred.
        Io(io::Error),
    }

    impl fmt::Display for Error {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Error::FileTooLarge => write!(f, "File too large"),
                Error::FileNotInCache => write!(f, "File not in cache"),
                Error::Io(ref e) => write!(f, "{}", e),
            }
        }
    }

    impl StdError for Error {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            match self {
                Error::FileTooLarge => None,
                Error::FileNotInCache => None,
                Error::Io(ref e) => Some(e),
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

use common_exception::ErrorCode;
use result::*;

impl From<Error> for ErrorCode {
    fn from(error: Error) -> Self {
        match error {
            Error::FileNotInCache => {
                ErrorCode::DiskCacheFileNotInCache("disk cache error: file not in cache")
            }
            Error::FileTooLarge => {
                ErrorCode::DiskCacheFileTooLarge("disk cache error: file too large")
            }
            Error::Io(err) => {
                ErrorCode::DiskCacheIOError(format!("disk cache io error, cause: {}", err))
            }
        }
    }
}
