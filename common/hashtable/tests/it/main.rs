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
use std::sync::Arc;

use common_hashtable::HashMap;
use common_hashtable::HashMapKind;
use common_hashtable::HashTableGrower;
use common_hashtable::SingleLevelGrower;
use rand::Rng;

#[test]
fn test_hash_table_grower() {
    let mut grower = SingleLevelGrower::default();

    assert_eq!(grower.max_size(), 256);

    assert!(grower.overflow(129));
    assert!(!grower.overflow(128));

    assert_eq!(grower.place(1), 1);
    assert_eq!(grower.place(255), 255);
    assert_eq!(grower.place(256), 0);
    assert_eq!(grower.place(257), 1);

    assert_eq!(grower.next_place(1), 2);
    assert_eq!(grower.next_place(2), 3);
    assert_eq!(grower.next_place(254), 255);
    assert_eq!(grower.next_place(255), 0);

    grower.increase_size();
    assert_eq!(grower.max_size(), 1024);
}

#[test]
fn test_two_level_hash_table() {
    let mut hashtable = HashMapKind::<u64, u64>::create_hash_table();
    let mut inserted = true;
    let entity = hashtable.insert_key(&1u64, &mut inserted);
    if inserted {
        entity.set_value(2);
    }

    unsafe {
        hashtable.convert_to_two_level();
    }

    let is_two_level = match hashtable {
        HashMapKind::HashTable(_) => false,
        HashMapKind::TwoLevelHashTable(_) => true,
    };
    assert!(is_two_level);

    let entity = hashtable.insert_key(&1u64, &mut inserted);
    assert!(!inserted);

    assert_eq!(entity.get_value(), &2);
}

#[test]
fn test_hash_map_drop() {
    #[derive(Debug, Clone)]
    struct A {
        item: Arc<AtomicUsize>,
    }

    impl A {
        pub fn new(item: Arc<AtomicUsize>) -> Self {
            item.fetch_add(1, Ordering::Relaxed);
            Self { item }
        }
    }

    impl Drop for A {
        fn drop(&mut self) {
            self.item.fetch_sub(1, Ordering::Relaxed);
        }
    }

    let item = Arc::new(AtomicUsize::new(0));
    let two_level_item = Arc::new(AtomicUsize::new(0));

    {
        let mut table = HashMap::<u64, Vec<A>>::create();
        let mut two_level_table = HashMapKind::<u64, Vec<A>>::create_hash_table();
        let mut rng = rand::thread_rng();

        for _ in 0..1200 {
            let key = rng.gen::<u64>() % 300;

            let mut inserted = false;
            let entity = table.insert_key(&key, &mut inserted);
            if inserted {
                entity.set_value(vec![A::new(item.clone())]);
            } else {
                entity.get_mut_value().push(A::new(item.clone()));
            }

            inserted = false;
            let entity = two_level_table.insert_key(&key, &mut inserted);
            if inserted {
                entity.set_value(vec![A::new(item.clone())]);
            } else {
                entity.get_mut_value().push(A::new(item.clone()));
            }
        }
        unsafe {
            two_level_table.convert_to_two_level();
        }
    }
    let count = item.load(Ordering::Relaxed);
    let count2 = two_level_item.load(Ordering::Relaxed);
    assert_eq!(count, 0);
    assert_eq!(count2, 0);
}
