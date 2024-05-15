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

use std::cell::RefCell;
use std::cmp::Ordering;
use std::fmt::Debug;
use std::rc::Rc;

use databend_common_base::containers::FixedHeap;
use rand::Rng;

#[test]
fn test_push_peek_pop() {
    let mut heap: FixedHeap<i32> = FixedHeap::new(16);
    assert_eq!(None, heap.peek());
    assert_eq!(heap.push(1), None);
    assert_eq!(Some(&1), heap.peek());
    assert_eq!(heap.push(3), None);
    assert_eq!(Some(&3), heap.peek());
    assert_eq!(heap.push(2), None);
    assert_eq!(Some(&3), heap.peek());

    assert_eq!(Some(3), heap.pop());
    assert_eq!(Some(&2), heap.peek());
    assert_eq!(Some(2), heap.pop());
    assert_eq!(Some(&1), heap.peek());
    assert_eq!(Some(1), heap.pop());
    assert_eq!(None, heap.peek());
    assert_eq!(None, heap.pop());
}

#[test]
fn test_add_last_swap_remove() {
    let mut heap: FixedHeap<i32> = FixedHeap::new(4);
    assert_eq!(heap.add_last(1), None);
    assert_eq!(heap.add_last(2), None);
    assert_eq!(heap.add_last(4), None);
    assert_eq!(heap.add_last(3), None);
    assert_eq!(heap.add_last(5), Some(5));

    assert_eq!(Some(1), heap.swap_remove(0));
    assert_eq!(Some(3), heap.swap_remove(0));
    assert_eq!(Some(4), heap.swap_remove(0));
    assert_eq!(Some(2), heap.swap_remove(0));
    assert_eq!(None, heap.swap_remove(0));
}

#[test]
fn test_push_full() {
    let mut heap: FixedHeap<i32> = FixedHeap::new(4);
    assert_eq!(heap.push(1), None);
    assert_eq!(heap.push(2), None);
    assert_eq!(heap.push(4), None);
    assert_eq!(heap.push(3), None);
    assert_eq!(heap.push(5), Some(1));

    assert_eq!(Some(5), heap.pop());
    assert_eq!(Some(4), heap.pop());
    assert_eq!(Some(3), heap.pop());
    assert_eq!(Some(2), heap.pop());
    assert_eq!(None, heap.pop());
}

#[test]
fn test_push_pop_equal() {
    let mut heap: FixedHeap<i32> = FixedHeap::new(4);
    assert_eq!(heap.push(7), None);
    assert_eq!(heap.push(7), None);
    assert_eq!(heap.push(7), None);

    assert_eq!(Some(7), heap.pop());
    assert_eq!(Some(7), heap.pop());
    assert_eq!(Some(7), heap.pop());
    assert_eq!(None, heap.pop());
}

#[test]
fn test_debug() {
    let mut heap: FixedHeap<i32> = FixedHeap::new(16);
    assert_eq!(heap.push(7), None);
    assert_eq!(heap.push(9), None);
    assert_eq!(heap.push(2), None);
    assert_eq!(heap.push(5), None);
    assert_eq!(heap.push(8), None);
    assert_eq!(heap.push(8), None);
    assert_eq!(heap.push(3), None);
    assert_eq!(
        format!("{:?}", heap),
        "FixedHeap { high: 7, data: [9, 8, 8, 5, 7, 2, 3] }"
    );
}

#[test]
fn test_len() {
    let mut heap: FixedHeap<i32> = FixedHeap::new(4);
    assert_eq!(0, heap.len());
    heap.push(2);
    heap.push(3);
    heap.push(6);
    assert_eq!(3, heap.len());
}

#[test]
fn test_cap() {
    let mut heap: FixedHeap<i32> = FixedHeap::new(4);
    assert_eq!(4, heap.cap());
    heap.push(2);
    heap.push(3);
    heap.push(6);
    assert_eq!(4, heap.cap());
}

#[test]
fn test_is_empty() {
    let mut heap: FixedHeap<i32> = FixedHeap::new(4);
    assert!(heap.is_empty());
    heap.push(2);
    heap.push(3);
    heap.push(6);
    assert!(!heap.is_empty());
}

#[test]
fn test_is_full() {
    let mut heap: FixedHeap<i32> = FixedHeap::new(4);
    assert!(!heap.is_full());
    heap.push(2);
    heap.push(3);
    heap.push(6);
    assert!(!heap.is_full());
    heap.push(1);
    heap.push(2);
    heap.push(3);
    heap.push(6);
    assert!(heap.is_full());
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct DropCounted<'a>(&'a RefCell<usize>);

impl<'a> Drop for DropCounted<'a> {
    fn drop(&mut self) {
        *self.0.borrow_mut() += 1;
    }
}

#[test]
fn test_drop() {
    let drops = RefCell::new(0usize);
    {
        let mut list: FixedHeap<DropCounted> = FixedHeap::new(16);
        for _ in 0..11 {
            list.push(DropCounted(&drops));
        }
        assert_eq!(*drops.borrow(), 0);

        // Pop and drop a few
        for _ in 0..4 {
            list.pop();
        }
        assert_eq!(*drops.borrow(), 4);

        // Let the rest drop
    }
    assert_eq!(*drops.borrow(), 11);
}

type Compapre<T> = Rc<dyn Fn(&T, &T) -> bool>;

struct CompareWrapper<T> {
    cmp: Compapre<T>,
    value: T,
}

impl<T: Debug> Debug for CompareWrapper<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self.value)
    }
}

impl<T> CompareWrapper<T> {
    fn take(self) -> T {
        self.value
    }
}

impl<T: PartialOrd> Ord for CompareWrapper<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        if (self.cmp)(&self.value, &other.value) {
            Ordering::Greater
        } else {
            Ordering::Less
        }
    }
}

impl<T: PartialOrd> PartialOrd for CompareWrapper<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: PartialEq> PartialEq for CompareWrapper<T> {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl<T: PartialEq> Eq for CompareWrapper<T> {}

#[test]
fn test_fuzz() {
    test_fuzz_impl(Rc::new(|a: &i8, b: &i8| a >= b));
    test_fuzz_impl(Rc::new(|a: &i8, b: &i8| a > b));
}

fn test_fuzz_impl(cmp: Compapre<i8>) {
    // Partial
    fuzz::<7>(10, 8, cmp.clone());
    fuzz::<7>(10, 9, cmp.clone());
    fuzz::<7>(10, 10, cmp.clone());
    fuzz::<7>(10, 11, cmp.clone());
    fuzz::<7>(10, 12, cmp.clone());
    fuzz::<7>(10, 13, cmp.clone());
    fuzz::<7>(10, 14, cmp.clone());
    fuzz::<7>(10, 15, cmp.clone());
    fuzz::<7>(10, 16, cmp.clone());

    // Full
    fuzz::<4>(10, 0, cmp.clone());
    fuzz::<4>(10, 1, cmp.clone());
    fuzz::<4>(10, 2, cmp.clone());
    fuzz::<4>(10, 3, cmp.clone());
    fuzz::<16>(10, 8, cmp.clone());
    fuzz::<16>(10, 9, cmp.clone());
    fuzz::<16>(10, 10, cmp.clone());
    fuzz::<16>(10, 11, cmp.clone());
    fuzz::<16>(10, 12, cmp.clone());
    fuzz::<16>(10, 13, cmp.clone());
    fuzz::<16>(10, 14, cmp.clone());
    fuzz::<16>(10, 15, cmp);
}

fn fuzz<const M: usize>(iters: usize, cap: usize, cmp: Compapre<i8>) {
    for _ in 0..iters {
        let mut heap: FixedHeap<CompareWrapper<i8>> = FixedHeap::new(cap);
        let mut array = [0i8; M];
        rand::thread_rng().fill(&mut array[..]);
        for element in array {
            heap.push(CompareWrapper {
                cmp: cmp.clone(),
                value: element,
            });
        }
        array.sort_by(|a, b| b.cmp(a));
        for &element in array.iter().take(cap) {
            assert_eq!(Some(element), heap.pop().map(CompareWrapper::take));
        }
        assert_eq!(None, heap.pop());
    }
}
