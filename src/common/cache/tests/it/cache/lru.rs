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

use std::fmt::Debug;
use std::fmt::Formatter;

use databend_common_cache::Cache;
use databend_common_cache::LruCache;
use databend_common_cache::MemSized;

#[derive(Eq, PartialEq, Hash)]
struct TestKey(pub i32);

impl MemSized for TestKey {
    fn mem_bytes(&self) -> usize {
        0
    }
}

impl Debug for TestKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", &self.0)
    }
}

#[derive(Eq, PartialEq)]
struct TestValue(pub i32);

impl MemSized for TestValue {
    fn mem_bytes(&self) -> usize {
        1
    }
}

impl Debug for TestValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", &self.0)
    }
}

#[test]
fn test_put_and_get() {
    let mut cache = LruCache::with_items_capacity(2);
    cache.insert(TestKey(1), TestValue(10));
    cache.insert(TestKey(2), TestValue(20));
    assert_eq!(cache.get(&TestKey(1)), Some(&TestValue(10)));
    assert_eq!(cache.get(&TestKey(2)), Some(&TestValue(20)));
    assert_eq!(cache.len(), 2);
    assert_eq!(cache.bytes_size(), 2);
}

#[test]
fn test_put_update() {
    let mut cache = LruCache::with_items_capacity(1);
    cache.insert(TestKey(1), TestValue(10));
    cache.insert(TestKey(1), TestValue(19));
    assert_eq!(cache.get(&TestKey(1)), Some(&TestValue(19)));
    assert_eq!(cache.len(), 1);
}

#[test]
fn test_contains() {
    let mut cache = LruCache::with_items_capacity(1);
    cache.insert(TestKey(1), TestValue(10));
    assert!(cache.contains(&TestKey(1)));
}

#[test]
fn test_expire_lru() {
    let mut cache = LruCache::with_items_capacity(2);
    cache.insert(TestKey(1), TestValue(1));
    cache.insert(TestKey(2), TestValue(2));
    cache.insert(TestKey(3), TestValue(3));
    assert!(cache.get(&TestKey(1)).is_none());
    cache.insert(TestKey(2), TestValue(2));
    cache.insert(TestKey(4), TestValue(4));
    assert!(cache.get(&TestKey(3)).is_none());
}

#[test]
fn test_pop() {
    let mut cache = LruCache::with_items_capacity(2);
    cache.insert(TestKey(1), TestValue(10));
    cache.insert(TestKey(2), TestValue(20));
    assert_eq!(cache.len(), 2);
    let opt1 = cache.pop(&TestKey(1));
    assert!(opt1.is_some());
    assert_eq!(opt1.unwrap(), TestValue(10));
    assert!(cache.get(&TestKey(1)).is_none());
    assert_eq!(cache.len(), 1);
}

#[test]
fn test_debug() {
    let mut cache = LruCache::with_items_capacity(3);
    cache.insert(TestKey(1), TestValue(10));
    cache.insert(TestKey(2), TestValue(20));
    cache.insert(TestKey(3), TestValue(30));
    assert_eq!(format!("{:?}", cache), "{3: 30, 2: 20, 1: 10}");
    cache.insert(TestKey(2), TestValue(22));
    assert_eq!(format!("{:?}", cache), "{2: 22, 3: 30, 1: 10}");
    cache.insert(TestKey(6), TestValue(60));
    assert_eq!(format!("{:?}", cache), "{6: 60, 2: 22, 3: 30}");
    cache.get(&TestKey(3));
    assert_eq!(format!("{:?}", cache), "{3: 30, 6: 60, 2: 22}");
}

#[test]
fn test_remove() {
    let mut cache = LruCache::with_items_capacity(3);
    cache.insert(TestKey(1), TestValue(10));
    cache.insert(TestKey(2), TestValue(20));
    cache.insert(TestKey(3), TestValue(30));
    cache.insert(TestKey(4), TestValue(40));
    cache.insert(TestKey(5), TestValue(50));
    cache.pop(&TestKey(3));
    cache.pop(&TestKey(4));
    assert!(cache.get(&TestKey(3)).is_none());
    assert!(cache.get(&TestKey(4)).is_none());
    cache.insert(TestKey(6), TestValue(60));
    cache.insert(TestKey(7), TestValue(70));
    cache.insert(TestKey(8), TestValue(80));
    assert!(cache.get(&TestKey(5)).is_none());
    assert_eq!(cache.get(&TestKey(6)), Some(&TestValue(60)));
    assert_eq!(cache.get(&TestKey(7)), Some(&TestValue(70)));
    assert_eq!(cache.get(&TestKey(8)), Some(&TestValue(80)));
}

#[test]
fn test_clear() {
    let mut cache = LruCache::with_items_capacity(2);
    cache.insert(TestKey(1), TestValue(10));
    cache.insert(TestKey(2), TestValue(20));
    cache.clear();
    assert!(cache.get(&TestKey(1)).is_none());
    assert!(cache.get(&TestKey(2)).is_none());
    assert_eq!(format!("{:?}", cache), "{}");
}

#[test]
fn test_iter() {
    let mut cache = LruCache::with_items_capacity(3);
    cache.insert(TestKey(1), TestValue(10));
    cache.insert(TestKey(2), TestValue(20));
    cache.insert(TestKey(3), TestValue(30));
    cache.insert(TestKey(4), TestValue(40));
    cache.insert(TestKey(5), TestValue(50));
    assert_eq!(cache.iter().collect::<Vec<_>>(), [
        (&TestKey(3), &TestValue(30)),
        (&TestKey(4), &TestValue(40)),
        (&TestKey(5), &TestValue(50))
    ]);
    assert_eq!(cache.iter_mut().collect::<Vec<_>>(), [
        (&TestKey(3), &mut TestValue(30)),
        (&TestKey(4), &mut TestValue(40)),
        (&TestKey(5), &mut TestValue(50))
    ]);
    assert_eq!(cache.iter().rev().collect::<Vec<_>>(), [
        (&TestKey(5), &TestValue(50)),
        (&TestKey(4), &TestValue(40)),
        (&TestKey(3), &TestValue(30))
    ]);
    assert_eq!(cache.iter_mut().rev().collect::<Vec<_>>(), [
        (&TestKey(5), &mut TestValue(50)),
        (&TestKey(4), &mut TestValue(40)),
        (&TestKey(3), &mut TestValue(30))
    ]);
}

#[derive(Eq, PartialEq, Debug)]
struct TestBytesValue(pub Vec<u8>);

impl MemSized for TestBytesValue {
    fn mem_bytes(&self) -> usize {
        self.0.len()
    }
}

#[test]
fn test_metered_cache() {
    let mut cache = LruCache::with_bytes_capacity(5);
    cache.insert(TestKey(1), TestBytesValue(vec![1, 2]));
    assert_eq!(cache.bytes_size(), 2);
    cache.insert(TestKey(2), TestBytesValue(vec![3, 4]));
    cache.insert(TestKey(3), TestBytesValue(vec![5, 6]));
    assert_eq!(cache.bytes_size(), 4);
    assert!(!cache.contains(&TestKey(1)));
    cache.insert(TestKey(2), TestBytesValue(vec![7, 8]));
    cache.insert(TestKey(4), TestBytesValue(vec![9, 10]));
    assert_eq!(cache.bytes_size(), 4);
    assert!(!cache.contains(&TestKey(3)));
    assert_eq!(cache.get(&TestKey(2)), Some(&TestBytesValue(vec![7, 8])));
}

#[test]
fn test_metered_cache_reinsert_larger() {
    let mut cache = LruCache::with_bytes_capacity(5);
    cache.insert(TestKey(1), TestBytesValue(vec![1, 2]));
    cache.insert(TestKey(2), TestBytesValue(vec![3, 4]));
    assert_eq!(cache.bytes_size(), 4);
    cache.insert(TestKey(2), TestBytesValue(vec![5, 6, 7, 8]));
    assert_eq!(cache.bytes_size(), 4);
    assert!(!cache.contains(&TestKey(1)));
}

#[test]
fn test_metered_cache_oversize() {
    let mut cache = LruCache::with_bytes_capacity(2);
    cache.insert(TestKey(1), TestBytesValue(vec![1, 2]));
    cache.insert(TestKey(2), TestBytesValue(vec![3, 4, 5, 6]));
    assert_eq!(cache.bytes_size(), 0);
    assert!(!cache.contains(&TestKey(1)));
    assert!(!cache.contains(&TestKey(2)));
}
