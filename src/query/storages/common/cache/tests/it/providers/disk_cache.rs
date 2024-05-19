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

use databend_storages_common_cache::DiskCacheError;
use databend_storages_common_cache::DiskCacheKey;
use databend_storages_common_cache::DiskCacheResult;
use databend_storages_common_cache::LruDiskCache as DiskCache;
use log::info;
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
}

#[test]
fn test_empty_dir() {
    let f = TestFixture::new();
    let fuzzy_reload_cache_keys = false;
    DiskCache::new(f.tmp(), 1024, fuzzy_reload_cache_keys).unwrap();
}

#[test]
fn test_missing_root() {
    let f = TestFixture::new();
    let fuzzy_reload_cache_keys = false;
    DiskCache::new(f.tmp().join("not-here"), 1024, fuzzy_reload_cache_keys).unwrap();
}

#[test]
fn test_fuzzy_restart_parallelism() {
    let _ = env_logger::builder().is_test(true).try_init();

    let f = TestFixture::new();
    let cache_root = f.tmp();

    // Create some cache entries
    let entry_count = 10;
    for i in 0..entry_count {
        let sub_dir = format!("dir{}", i % 10);
        let file_name = DiskCacheKey::from(format!("file{}.txt", i)).to_string();
        info!("Creating file {}", file_name);
        let file_path = cache_root.join(&sub_dir).join(&file_name);

        // Create parent directories recursively
        fs::create_dir_all(file_path.parent().unwrap()).unwrap();

        // Create a non-empty file
        let mut file = fs::File::create(file_path).unwrap();
        let content = format!("Content of file {}", i);
        file.write_all(content.as_bytes()).unwrap();
    }

    let cache = DiskCache::new(&cache_root.to_path_buf(), 1024 * 1024, true).unwrap();

    // Check if the keys are reloaded correctly
    for i in 0..entry_count {
        let file_name = format!("file{}.txt", i);
        assert!(cache.contains_key(&file_name));
    }
}

#[test]
fn test_reset_restart_parallelism() {
    let _ = env_logger::builder().is_test(true).try_init();

    let f = TestFixture::new();
    let cache_root = f.tmp();

    // Create some cache entries
    let entry_count = 100;
    for i in 0..entry_count {
        let sub_dir = format!("dir{}", i % 10);
        let file_name = DiskCacheKey::from(format!("file{}.txt", i)).to_string();
        let file_path = cache_root.join(&sub_dir).join(&file_name);

        // Create parent directories recursively
        fs::create_dir_all(file_path.parent().unwrap()).unwrap();

        // Create a non-empty file
        let mut file = fs::File::create(file_path).unwrap();
        let content = format!("Content of file {}", i);
        file.write_all(content.as_bytes()).unwrap();
    }

    // Check that all files within prefix directories are removed
    let prefix_dirs = fs::read_dir(cache_root).unwrap();
    let remaining_files = prefix_dirs
        .map(|entry| {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_dir() {
                fs::read_dir(path).unwrap().count()
            } else {
                0
            }
        })
        .sum::<usize>();
    assert_eq!(remaining_files, entry_count);

    let _cache = DiskCache::new(cache_root, 1024, false).unwrap();

    // Check that all files within prefix directories are removed
    let prefix_dirs = fs::read_dir(cache_root).unwrap();
    let remaining_files = prefix_dirs
        .map(|entry| {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_dir() {
                fs::read_dir(path).unwrap().count()
            } else {
                0
            }
        })
        .sum::<usize>();
    assert_eq!(
        remaining_files, 0,
        "Files within prefix directories are not completely removed"
    );
}

#[test]
fn test_insert_bytes() {
    let f = TestFixture::new();
    let fuzzy_reload_cache_keys = false;
    let mut c = DiskCache::new(f.tmp(), 25, fuzzy_reload_cache_keys).unwrap();
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
    let fuzzy_reload_cache_keys = false;
    let mut c = DiskCache::new(f.tmp(), 20, fuzzy_reload_cache_keys).unwrap();
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
        let fuzzy_reload_cache_keys = false;
        let mut c = DiskCache::new(f.tmp(), 25, fuzzy_reload_cache_keys).unwrap();
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
    let fuzzy_reload_cache_keys = false;
    let mut c = DiskCache::new(f.tmp(), 1, fuzzy_reload_cache_keys).unwrap();
    match c.insert_single_slice("a/b/c", &[0; 2]) {
        Err(DiskCacheError::FileTooLarge) => {}
        x => panic!("Unexpected result: {x:?}"),
    }
}

#[test]
fn test_evict_until_enough_space() {
    let f = TestFixture::new();
    let fuzzy_reload_cache_keys = false;
    let mut c = DiskCache::new(f.tmp(), 4, fuzzy_reload_cache_keys).unwrap();
    c.insert_single_slice("file1", &[1; 1]).unwrap();
    c.insert_single_slice("file2", &[2; 2]).unwrap();
    c.insert_single_slice("file3", &[3; 1]).unwrap();
    assert_eq!(c.size(), 4);

    // insert a single slice which size bigger than file1 and less than file1 + file2
    c.insert_single_slice("file4", &[3; 2]).unwrap();
    assert_eq!(c.size(), 3);
    // file1 and file2 MUST be evicted
    assert!(!c.contains_key("file1"));
    assert!(!c.contains_key("file2"));
    // file3 MUST be keeped
    assert!(c.contains_key("file3"));
}
