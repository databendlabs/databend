// Copyright 2020 Parity Technologies
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::collections::HashMap;
use std::sync::Arc;

use common_infallible::Mutex;
use common_mem::malloc_size;
use common_mem::MallocSizeOf;
use common_mem::MallocSizeOfExt;
use common_mem_derive::*;

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
