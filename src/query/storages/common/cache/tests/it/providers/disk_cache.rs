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
use std::io;
use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

use storages_common_cache::DiskCacheError;
use storages_common_cache::DiskCacheKey;
use storages_common_cache::DiskCacheResult;
use storages_common_cache::LruDiskCache as DiskCache;
use tempfile::TempDir;

struct TestFixture {
    /// Temp directory.
    pub tempdir: TempDir,
}

// helper trait to simplify the test case
trait InsertSingleSlice {
    fn insert_single_slice(&mut self, key: &str, bytes: &[u8]) -> DiskCacheResult<()>;
}

impl InsertSingleSlice for DiskCache {
    fn insert_single_slice(&mut self, key: &str, bytes: &[u8]) -> DiskCacheResult<()> {
        self.insert_bytes(key, &[bytes])
    }
}

fn create_file<T: AsRef<Path>, F: FnOnce(File) -> io::Result<()>>(
    dir: &Path,
    path: T,
    fill_contents: F,
) -> io::Result<PathBuf> {
    let b = dir.join(path);
    fs::create_dir_all(b.parent().unwrap())?;
    let f = fs::File::create(&b)?;
    fill_contents(f)?;
    b.canonicalize()
}

fn read_all<R: Read>(r: &mut R) -> io::Result<Vec<u8>> {
    let mut v = vec![];
    r.read_to_end(&mut v)?;
    Ok(v)
}

impl TestFixture {
    pub fn new() -> TestFixture {
        TestFixture {
            tempdir: tempfile::Builder::new()
                .prefix("lru-disk-cache-test")
                .tempdir()
                .unwrap(),
        }
    }

    pub fn tmp(&self) -> &Path {
        self.tempdir.path()
    }

    pub fn create_file<T: AsRef<Path>>(&self, path: T, size: usize) -> PathBuf {
        create_file(self.tempdir.path(), path, |mut f| {
            f.write_all(&vec![0; size])
        })
        .unwrap()
    }
}

#[test]
fn test_empty_dir() {
    let f = TestFixture::new();
    DiskCache::new(f.tmp(), 1024).unwrap();
}

#[test]
fn test_missing_root() {
    let f = TestFixture::new();
    DiskCache::new(f.tmp().join("not-here"), 1024).unwrap();
}

#[test]
fn test_some_existing_files() {
    let f = TestFixture::new();
    let items = 10;
    let sizes = (0..).take(items);
    let total_bytes: u64 = sizes.clone().sum();
    for i in sizes {
        let file_name = format!("file-{i}");
        let test_key = DiskCacheKey::from(file_name.as_str());
        let test_path = PathBuf::from(&test_key);
        f.create_file(test_path, i as usize);
    }

    let c = DiskCache::new(f.tmp(), total_bytes).unwrap();
    assert_eq!(c.size(), total_bytes);
    assert_eq!(c.len(), items);
}

#[test]
fn test_existing_file_too_large() {
    let f = TestFixture::new();
    let items_count = 10;
    let items_count_shall_be_kept = 10 - 2;
    let item_size = 10;
    let capacity = items_count_shall_be_kept * item_size;
    let sizes = (0..).take(items_count);
    for i in sizes {
        let file_name = format!("file-{i}");
        let test_key = DiskCacheKey::from(file_name.as_str());
        let test_path = PathBuf::from(&test_key);
        f.create_file(test_path, item_size);
    }
    let c = DiskCache::new(f.tmp(), capacity as u64).unwrap();

    assert_eq!(c.size(), capacity as u64);
    assert_eq!(c.len(), items_count_shall_be_kept);
    for i in (0..).take(items_count_shall_be_kept) {
        let file_name = format!("file-{i}");
        c.contains_key(file_name.as_str());
    }
}

#[test]
fn test_insert_bytes() {
    let f = TestFixture::new();
    let mut c = DiskCache::new(f.tmp(), 25).unwrap();
    c.insert_single_slice("a/b/c", &[0; 10]).unwrap();
    assert!(c.contains_key("a/b/c"));
    c.insert_single_slice("a/b/d", &[0; 10]).unwrap();
    assert_eq!(c.size(), 20);
    // Adding this third file should put the cache above the limit.
    c.insert_single_slice("x/y/z", &[0; 10]).unwrap();
    assert_eq!(c.size(), 20);
    // The least-recently-used file should have been removed.
    assert!(!c.contains_key("a/b/c"));

    let evicted_file_path = PathBuf::from(&DiskCacheKey::from("a/b/c"));
    assert!(!f.tmp().join(evicted_file_path).exists());
}

#[test]
fn test_insert_bytes_exact() {
    // Test that files adding up to exactly the size limit works.
    let f = TestFixture::new();
    let mut c = DiskCache::new(f.tmp(), 20).unwrap();
    c.insert_single_slice("file1", &[1; 10]).unwrap();
    c.insert_single_slice("file2", &[2; 10]).unwrap();
    assert_eq!(c.size(), 20);
    c.insert_single_slice("file3", &[3; 10]).unwrap();
    assert_eq!(c.size(), 20);
    assert!(!c.contains_key("file1"));
}

#[test]
fn test_add_get_lru() {
    let f = TestFixture::new();
    {
        let mut c = DiskCache::new(f.tmp(), 25).unwrap();
        c.insert_single_slice("file1", &[1; 10]).unwrap();
        c.insert_single_slice("file2", &[2; 10]).unwrap();
        // Get the file to bump its LRU status.
        assert_eq!(
            read_all(&mut File::open(c.get_cache_path("file1").unwrap()).unwrap()).unwrap(),
            vec![1u8; 10]
        );
        // Adding this third file should put the cache above the limit.
        c.insert_single_slice("file3", &[3; 10]).unwrap();
        assert_eq!(c.size(), 20);
        // The least-recently-used file should have been removed.
        assert!(!c.contains_key("file2"));
    }
}

#[test]
fn test_insert_bytes_too_large() {
    let f = TestFixture::new();
    let mut c = DiskCache::new(f.tmp(), 1).unwrap();
    match c.insert_single_slice("a/b/c", &[0; 2]) {
        Err(DiskCacheError::FileTooLarge) => {}
        x => panic!("Unexpected result: {x:?}"),
    }
}
