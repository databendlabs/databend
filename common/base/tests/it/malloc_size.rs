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

#[allow(unused)]
use common_base::mem_allocator::*;

#[test]
fn test_large_vec() {
    use common_base::mem_allocator::new_malloc_size_ops;
    const N: usize = 128 * 1024 * 1024;
    let val = vec![1u8; N];
    let mut ops = new_malloc_size_ops();
    assert!(val.size_of(&mut ops) >= N);
    assert!(val.size_of(&mut ops) < 2 * N);
}

#[test]
fn btree_set() {
    let mut set = std::collections::BTreeSet::new();
    for t in 0..100 {
        set.insert(vec![t]);
    }
    // ~36 per value
    assert!(common_base::mem_allocator::malloc_size(&set) > 3000);
}

#[test]
fn special_malloc_size_of_0() {
    struct Data<P> {
        phantom: std::marker::PhantomData<P>,
    }

    common_base::malloc_size_of_is_0!(any: Data<P>);

    // MallocSizeOf is not implemented for [u8; 333]
    assert_eq!(
        common_base::mem_allocator::malloc_size(&Data::<[u8; 333]> {
            phantom: std::marker::PhantomData
        }),
        0
    );
}

#[test]
fn constant_size() {
    struct AlwaysTwo(Vec<u8>);

    impl MallocSizeOf for AlwaysTwo {
        fn size_of(&self, ops: &mut MallocSizeOfOps) -> usize {
            self.0.size_of(ops)
        }
        fn constant_size() -> Option<usize> {
            Some(2)
        }
    }

    assert_eq!(AlwaysTwo::constant_size(), Some(2));
    assert_eq!(std::cmp::Reverse::<u8>::constant_size(), Some(0));
    assert_eq!(std::cell::RefCell::<u8>::constant_size(), Some(0));
    assert_eq!(std::cell::Cell::<u8>::constant_size(), Some(0));
    // assert_eq!(Result::<(), ()>::constant_size(), Some(0));
    // assert_eq!(
    //     <(AlwaysTwo, (), [u8; 32], AlwaysTwo)>::constant_size(),
    //     Some(2 + 2)
    // );
    assert_eq!(Option::<u8>::constant_size(), Some(0));
    assert_eq!(<&String>::constant_size(), Some(0));

    assert_eq!(<String>::constant_size(), None);
    assert_eq!(std::borrow::Cow::<String>::constant_size(), None);
    // assert_eq!(Result::<(), String>::constant_size(), None);
    assert_eq!(Option::<AlwaysTwo>::constant_size(), None);
}
