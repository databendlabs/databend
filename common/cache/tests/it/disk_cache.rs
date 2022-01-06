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

use std::fs;
use std::fs::File;
use std::io;
use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

use common_cache::DiskCacheError;
use common_cache::LruDiskCache;
use filetime::set_file_times;
use filetime::FileTime;
use tempfile::TempDir;

struct TestFixture {
    /// Temp directory.
    pub tempdir: TempDir,
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

/// Set the last modified time of `path` backwards by `seconds` seconds.
fn set_mtime_back<T: AsRef<Path>>(path: T, seconds: usize) {
    let m = fs::metadata(path.as_ref()).unwrap();
    let t = FileTime::from_last_modification_time(&m);
    let t = FileTime::from_unix_time(t.unix_seconds() - seconds as i64, t.nanoseconds());
    set_file_times(path, t, t).unwrap();
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
    LruDiskCache::new(f.tmp(), 1024).unwrap();
}

#[test]
fn test_missing_root() {
    let f = TestFixture::new();
    LruDiskCache::new(f.tmp().join("not-here"), 1024).unwrap();
}

#[test]
fn test_some_existing_files() {
    let f = TestFixture::new();
    f.create_file("file1", 10);
    f.create_file("file2", 10);
    let c = LruDiskCache::new(f.tmp(), 20).unwrap();
    assert_eq!(c.size(), 20);
    assert_eq!(c.len(), 2);
}

#[test]
fn test_existing_file_too_large() {
    let f = TestFixture::new();
    // Create files explicitly in the past.
    set_mtime_back(f.create_file("file1", 10), 10);
    set_mtime_back(f.create_file("file2", 10), 5);
    let c = LruDiskCache::new(f.tmp(), 15).unwrap();
    assert_eq!(c.size(), 10);
    assert_eq!(c.len(), 1);
    assert!(!c.contains_key("file1"));
    assert!(c.contains_key("file2"));
}

#[test]
fn test_existing_files_lru_mtime() {
    let f = TestFixture::new();
    // Create files explicitly in the past.
    set_mtime_back(f.create_file("file1", 10), 5);
    set_mtime_back(f.create_file("file2", 10), 10);
    let mut c = LruDiskCache::new(f.tmp(), 25).unwrap();
    assert_eq!(c.size(), 20);
    c.insert_bytes("file3", &[0; 10]).unwrap();
    assert_eq!(c.size(), 20);
    // The oldest file on disk should have been removed.
    assert!(!c.contains_key("file2"));
    assert!(c.contains_key("file1"));
}

#[test]
fn test_insert_bytes() {
    let f = TestFixture::new();
    let mut c = LruDiskCache::new(f.tmp(), 25).unwrap();
    c.insert_bytes("a/b/c", &[0; 10]).unwrap();
    assert!(c.contains_key("a/b/c"));
    c.insert_bytes("a/b/d", &[0; 10]).unwrap();
    assert_eq!(c.size(), 20);
    // Adding this third file should put the cache above the limit.
    c.insert_bytes("x/y/z", &[0; 10]).unwrap();
    assert_eq!(c.size(), 20);
    // The least-recently-used file should have been removed.
    assert!(!c.contains_key("a/b/c"));
    assert!(!f.tmp().join("a/b/c").exists());
}

#[test]
fn test_insert_bytes_exact() {
    // Test that files adding up to exactly the size limit works.
    let f = TestFixture::new();
    let mut c = LruDiskCache::new(f.tmp(), 20).unwrap();
    c.insert_bytes("file1", &[1; 10]).unwrap();
    c.insert_bytes("file2", &[2; 10]).unwrap();
    assert_eq!(c.size(), 20);
    c.insert_bytes("file3", &[3; 10]).unwrap();
    assert_eq!(c.size(), 20);
    assert!(!c.contains_key("file1"));
}

#[test]
fn test_add_get_lru() {
    let f = TestFixture::new();
    {
        let mut c = LruDiskCache::new(f.tmp(), 25).unwrap();
        c.insert_bytes("file1", &[1; 10]).unwrap();
        c.insert_bytes("file2", &[2; 10]).unwrap();
        // Get the file to bump its LRU status.
        assert_eq!(read_all(&mut c.get("file1").unwrap()).unwrap(), vec![
            1u8;
            10
        ]);
        // Adding this third file should put the cache above the limit.
        c.insert_bytes("file3", &[3; 10]).unwrap();
        assert_eq!(c.size(), 20);
        // The least-recently-used file should have been removed.
        assert!(!c.contains_key("file2"));
    }
    // Get rid of the cache, to test that the LRU persists on-disk as mtimes.
    // This is hacky, but mtime resolution on my mac with HFS+ is only 1 second, so we either
    // need to have a 1 second sleep in the test (boo) or adjust the mtimes back a bit so
    // that updating one file to the current time actually works to make it newer.
    set_mtime_back(f.tmp().join("file1"), 5);
    set_mtime_back(f.tmp().join("file3"), 5);
    {
        let mut c = LruDiskCache::new(f.tmp(), 25).unwrap();
        // Bump file1 again.
        c.get("file1").unwrap();
    }
    // Now check that the on-disk mtimes were updated and used.
    {
        let mut c = LruDiskCache::new(f.tmp(), 25).unwrap();
        assert!(c.contains_key("file1"));
        assert!(c.contains_key("file3"));
        assert_eq!(c.size(), 20);
        // Add another file to bump out the least-recently-used.
        c.insert_bytes("file4", &[4; 10]).unwrap();
        assert_eq!(c.size(), 20);
        assert!(!c.contains_key("file3"));
        assert!(c.contains_key("file1"));
    }
}

#[test]
fn test_insert_bytes_too_large() {
    let f = TestFixture::new();
    let mut c = LruDiskCache::new(f.tmp(), 1).unwrap();
    match c.insert_bytes("a/b/c", &[0; 2]) {
        Err(DiskCacheError::FileTooLarge) => {}
        x => panic!("Unexpected result: {:?}", x),
    }
}

#[test]
fn test_insert_file() {
    let f = TestFixture::new();
    let p1 = f.create_file("file1", 10);
    let p2 = f.create_file("file2", 10);
    let p3 = f.create_file("file3", 10);
    let mut c = LruDiskCache::new(f.tmp().join("cache"), 25).unwrap();
    c.insert_file("file1", &p1).unwrap();
    assert_eq!(c.len(), 1);
    c.insert_file("file2", &p2).unwrap();
    assert_eq!(c.len(), 2);
    // Get the file to bump its LRU status.
    assert_eq!(read_all(&mut c.get("file1").unwrap()).unwrap(), vec![
        0u8;
        10
    ]);
    // Adding this third file should put the cache above the limit.
    c.insert_file("file3", &p3).unwrap();
    assert_eq!(c.len(), 2);
    assert_eq!(c.size(), 20);
    // The least-recently-used file should have been removed.
    assert!(!c.contains_key("file2"));
    assert!(!p1.exists());
    assert!(!p2.exists());
    assert!(!p3.exists());
}

#[test]
fn test_remove() {
    let f = TestFixture::new();
    let p1 = f.create_file("file1", 10);
    let p2 = f.create_file("file2", 10);
    let p3 = f.create_file("file3", 10);
    let mut c = LruDiskCache::new(f.tmp().join("cache"), 25).unwrap();
    c.insert_file("file1", &p1).unwrap();
    c.insert_file("file2", &p2).unwrap();
    c.remove("file1").unwrap();
    c.insert_file("file3", &p3).unwrap();
    assert_eq!(c.len(), 2);
    assert_eq!(c.size(), 20);

    // file1 should have been removed.
    assert!(!c.contains_key("file1"));
    assert!(!f.tmp().join("cache").join("file1").exists());
    assert!(f.tmp().join("cache").join("file2").exists());
    assert!(f.tmp().join("cache").join("file3").exists());
    assert!(!p1.exists());
    assert!(!p2.exists());
    assert!(!p3.exists());

    let p4 = f.create_file("file1", 10);
    c.insert_file("file1", &p4).unwrap();
    assert_eq!(c.len(), 2);
    // file2 should have been removed.
    assert!(c.contains_key("file1"));
    assert!(!c.contains_key("file2"));
    assert!(!f.tmp().join("cache").join("file2").exists());
    assert!(!p4.exists());
}
