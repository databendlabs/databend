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

use std::collections::HashMap;
use std::sync::Arc;

use common_base::infallible::Mutex;
use common_base::mem_allocator::malloc_size;
use common_base::mem_allocator::MallocSizeOf;
use common_base::mem_allocator::MallocSizeOfExt;
use common_macros::MallocSizeOf;

#[test]
fn derive_vec() {
    #[derive(MallocSizeOf)]
    struct Trivia {
        v: Vec<u8>,
    }

    let t = Trivia { v: vec![0u8; 1024] };

    assert!(t.malloc_size_of() > 1000);
}

#[test]
fn derive_hashmap() {
    #[derive(MallocSizeOf, Default)]
    struct Trivia {
        hm: std::collections::HashMap<u64, Vec<u8>>,
    }

    let mut t = Trivia::default();

    t.hm.insert(1, vec![0u8; 2048]);

    assert!(t.malloc_size_of() > 2000);
}

#[test]
fn derive_ignore() {
    #[derive(MallocSizeOf, Default)]
    struct Trivia {
        hm: std::collections::HashMap<u64, Vec<u8>>,
        #[ignore_malloc_size_of = "I don't like vectors"]
        v: Vec<u8>,
    }

    let mut t = Trivia::default();

    t.hm.insert(1, vec![0u8; 2048]);
    t.v = vec![0u8; 1024];
    assert!(t.malloc_size_of() < 3000);
}

#[test]
fn derive_morecomplex() {
    #[derive(MallocSizeOf)]
    struct Trivia {
        hm: HashMap<u64, Vec<u8>>,
    }

    let mut t = Trivia { hm: HashMap::new() };

    t.hm.insert(1, vec![0u8; 2048]);
    t.hm.insert(2, vec![0u8; 2048]);
    t.hm.insert(3, vec![0u8; 2048]);
    t.hm.insert(4, vec![0u8; 2048]);

    println!("hashmap size: {}", t.malloc_size_of());

    assert!(t.malloc_size_of() > 8000);
}

#[test]
fn test_arc() {
    let val = Arc::new("test".to_string());
    let s = val.malloc_size_of();
    assert!(s > 4);
}

#[test]
fn test_dyn() {
    trait Augmented: MallocSizeOf {}
    impl Augmented for Vec<u8> {}
    let val: Arc<dyn Augmented> = Arc::new(vec![0u8; 1024]);
    assert!(malloc_size(&*val) > 1000);
}

#[test]
fn test_mutex() {
    let val = Mutex::new(vec![0u8; 1024]);
    assert!(malloc_size(&val) == 1024);
}
