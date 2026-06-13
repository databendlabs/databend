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

#![allow(clippy::arc_with_non_send_sync)]

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use bumpalo::Bump;
use databend_common_hashtable::HashMap;
use databend_common_hashtable::HashtableLike;
use databend_common_hashtable::ShortStringHashMap;
use databend_common_hashtable::StackHashMap;
use databend_common_hashtable::fast_memcmp;
use rand::Rng;
use rand::distributions::Alphanumeric;

macro_rules! simple_test {
    ($t: tt) => {
        static COUNT: AtomicUsize = AtomicUsize::new(0);
        #[derive(Debug)]
        struct U64(u64);
        impl U64 {
            fn new(x: u64) -> Self {
                COUNT.fetch_add(1, Ordering::Relaxed);
                Self(x)
            }
        }
        impl Drop for U64 {
            fn drop(&mut self) {
                COUNT.fetch_sub(1, Ordering::Relaxed);
            }
        }
        let mut sequence = vec![0u64; 1 << 12];
        sequence.fill_with(|| rand::thread_rng().gen_range(0..1 << 10));
        let mut standard = std::collections::HashMap::<u64, u64>::new();
        let mut hashtable = $t::<u64, U64>::new();
        for &s in sequence.iter() {
            match standard.get_mut(&s) {
                Some(x) => {
                    *x += 1;
                }
                None => {
                    standard.insert(s, 1);
                }
            }
        }
        for &s in sequence.iter() {
            match unsafe { hashtable.insert(s) } {
                Ok(x) => {
                    x.write(U64::new(1));
                }
                Err(x) => {
                    x.0 += 1;
                }
            }
        }
        assert_eq!(standard.len(), hashtable.len());
        let mut check = std::collections::HashSet::new();
        for e in hashtable.iter() {
            assert!(check.insert(e.key()));
            assert_eq!(standard[e.key()], e.get().0);
        }
        drop(hashtable);
        assert_eq!(COUNT.load(Ordering::Relaxed), 0);
    };
}

#[test]
fn test_hash_map() {
    simple_test!(HashMap);
}

#[test]
fn test_stack_hash_map() {
    simple_test!(StackHashMap);
}

#[test]
fn test_fast_memcmp() {
    let mut rng = rand::thread_rng();
    for size in 1..129 {
        let a: Vec<u8> = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(size)
            .collect();
        for _ in 0..1024 {
            // change a random byte in b and cmpare with a
            let mut b = a.clone();
            let idx = rng.gen_range(0..size);
            if b[idx] == u8::MAX {
                b[idx] = 1;
            } else {
                b[idx] += 1;
            }
            assert!(!fast_memcmp(a.as_slice(), b.as_slice()));
            b[idx] = a[idx];
            assert!(fast_memcmp(a.as_slice(), b.as_slice()));
        }
    }
}

#[test]
fn test_unsized_hash_map() {
    static COUNT: AtomicUsize = AtomicUsize::new(0);
    #[derive(Debug)]
    struct U64(u64);
    impl U64 {
        fn new(x: u64) -> Self {
            COUNT.fetch_add(1, Ordering::Relaxed);
            Self(x)
        }
    }
    impl Drop for U64 {
        fn drop(&mut self) {
            COUNT.fetch_sub(1, Ordering::Relaxed);
        }
    }
    let mut sequence = Vec::new();
    for _ in 0..1000 {
        let length = rand::thread_rng().gen_range(0..64);
        let mut array = vec![0u8; length];
        rand::thread_rng().fill(&mut array[..]);
        sequence.push(array);
    }
    let mut standard = std::collections::HashMap::<&[u8], u64>::new();
    for s in sequence.iter() {
        if let Some(x) = standard.get_mut(&s[..]) {
            *x += 1;
        } else {
            standard.insert(s, 1);
        }
    }
    let mut hashtable = ShortStringHashMap::<[u8], U64>::new(Arc::new(Bump::new()));
    for s in sequence.iter() {
        match unsafe { hashtable.insert_and_entry(s) } {
            Ok(mut e) => {
                e.write(U64::new(1u64));
            }
            Err(mut e) => {
                e.get_mut().0 += 1;
            }
        }
    }
    assert_eq!(standard.len(), hashtable.len());
    let mut check = std::collections::HashSet::new();
    for e in hashtable.iter() {
        assert!(check.insert(e.key()));
        assert_eq!(standard.get(e.key()).copied().unwrap(), e.get().0);
    }
    drop(hashtable);
    assert_eq!(COUNT.load(Ordering::Relaxed), 0);
}
