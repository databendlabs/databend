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

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use common_hashtable::HashMap;
use common_hashtable::StackHashMap;
use common_hashtable::TwolevelHashMap;
use rand::Rng;

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
fn test_twolevel_hash_map() {
    simple_test!(TwolevelHashMap);
}
