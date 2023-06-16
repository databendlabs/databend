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

use std::path::Path;

use storages_common_cache::RocksDbCache;
use tempfile::TempDir;

struct TestFixture {
    /// Temp directory.
    pub tempdir: TempDir,
}

impl TestFixture {
    pub fn new() -> TestFixture {
        TestFixture {
            tempdir: tempfile::Builder::new()
                .prefix("rocksdb-cache-test")
                .tempdir()
                .unwrap(),
        }
    }

    pub fn tmp(&self) -> &Path {
        self.tempdir.path()
    }
}

#[test]
fn test_put_get() {
    let f = TestFixture::new();
    let c = RocksDbCache::new(f.tmp().to_str().unwrap(), 2).unwrap();
    c.put("key", &"hello".as_bytes().to_vec()).unwrap();
    let value = c.get("key").unwrap().unwrap();
    assert_eq!("hello", &String::from_utf8(value).unwrap());
    c.put("now", &"world".as_bytes().to_vec()).unwrap();
    assert_eq!(
        "world",
        &String::from_utf8(c.get("now").unwrap().unwrap()).unwrap()
    );
    // after put `now`, `key` will be evicted
    assert_eq!(None, c.get("key").unwrap());
}
