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
use std::fmt::Result;
use std::mem::MaybeUninit;

use crate::runtime::drop_guard;

/// A fixed-size heap structure inspired by https://github.com/rodolphito/fixed_heap
pub struct FixedHeap<T> {
    high: usize,
    data: Vec<MaybeUninit<T>>,
}

impl<T: Ord> FixedHeap<T> {
    /// Creates a new empty `FixedHeap`.
    ///
    /// # Examples
    ///
    /// ```
    /// let mut heap: FixedHeap<i32> = FixedHeap::new(16);
    /// ```
    pub fn new(cap: usize) -> Self {
        let mut data = Vec::with_capacity(cap);
        unsafe { data.set_len(cap) };
        Self { high: 0, data }
    }

    /// Returns a reference to the highest priority element.
    ///
    /// # Returns
    ///
    /// `None` if there are no elements in the heap.
    ///
    /// `Some(elem)` if there was an element. `elem` is higher priority than all other elements.
    ///
    /// # Examples
    ///
    /// ```
    /// let mut heap: FixedHeap<i32> = FixedHeap::new(16);
    /// assert_eq!(heap.add_last(1), None);
    /// assert_eq!(heap.peek(), Some(&1));
    /// ```
    #[inline(always)]
    pub fn peek(&self) -> Option<&T> {
        self.peek_at(0)
    }

    /// Returns a reference to the element at `index`.
    ///
    /// # Returns
    ///
    /// `None` if there is no element at `index`.
    ///
    /// `Some(elem)` if there was an element.
    ///
    /// # Examples
    ///
    /// ```
    /// let mut heap: FixedHeap<i32> = FixedHeap::new(16);
    /// assert_eq!(heap.add_last(1), None);
    /// assert_eq!(heap.add_last(2), None);
    /// assert_eq!(heap.peek_at(1), Some(&2));
    /// ```
    #[inline(always)]
    pub fn peek_at(&self, index: usize) -> Option<&T> {
        // # Safety
        // If `index` is below `high` then `data[index]` is initialized.
        if index < self.high {
            Some(unsafe { self.data.get_unchecked(index).assume_init_ref() })
        } else {
            None
        }
    }

    /// Tries to add `value` to the end, ignoring heap properties.
    ///
    /// Caution: this will not preserve the heap property of the structure
    ///
    /// # Returns
    ///
    /// `None` if there was spare capacity to accommodate `value`
    ///
    /// `Some(value)` if there was no spare capacity
    ///
    /// # Time Complexity
    ///
    /// O(1)
    ///
    /// # Examples
    ///
    /// ```
    /// let mut heap: FixedHeap<i32> = FixedHeap::new(16);
    /// assert_eq!(heap.add_last(1), None);
    /// assert_eq!(heap.add_last(2), Some(2));
    /// ```
    pub fn add_last(&mut self, value: T) -> Option<T> {
        if self.high == self.data.len() {
            // Not enough space to add it
            Some(value)
        } else {
            // There's enough space to add it
            // # Safety
            // `high` is guaranteed to be a valid index here because it is less than `N`
            unsafe {
                *self.data.get_unchecked_mut(self.high) = MaybeUninit::new(value);
            }
            self.high += 1;
            None
        }
    }

    /// Removes and returns the element at `index`, ignoring heap properties.
    /// Use `pop_at` instead to preserve heap properties.
    ///
    /// Caution: this will not preserve the heap property of the structure
    ///
    /// # Returns
    ///
    /// `None` if there's no element at `index`.
    ///
    /// `Some(elem)` if there was an element at `index`.
    ///
    /// # Time Complexity
    ///
    /// O(1)
    ///
    /// # Examples
    ///
    /// ```
    /// let mut heap: FixedHeap<i32> = FixedHeap::new(16);
    /// assert_eq!(heap.swap_remove(0), None); // []
    /// assert_eq!(heap.add_last(1), None); // [1]
    /// assert_eq!(heap.swap_remove(1), None); // [1]
    /// assert_eq!(heap.add_last(2), None); // [1, 2]
    /// assert_eq!(heap.swap_remove(0), Some(1)); // [2]
    /// assert_eq!(heap.swap_remove(0), Some(2)); // []
    /// ```
    pub fn swap_remove(&mut self, index: usize) -> Option<T> {
        if index < self.high {
            self.high -= 1;

            // # Safety
            // `data[index]` holds an initialized value because `index` is less than `high`
            // We just copy it because we're about to overwrite it anyways
            let removed_node = unsafe { self.data.get_unchecked(index).assume_init_read() };
            // We can also just copy the last element because the last index will now be treated as uninit
            unsafe {
                *self.data.get_unchecked_mut(index) =
                    MaybeUninit::new(self.data.get_unchecked(self.high).assume_init_read());
            }

            Some(removed_node)
        } else {
            None
        }
    }

    /// Tries to push `value` onto the heap, calling `comparer` with `state` to determine ordering.
    ///
    /// # Returns
    ///
    /// `None` if there was spare capacity to accommodate `value`
    ///
    /// `Some(elem)` if the lowest priority element `elem` had to be evicted to accommodate `value`.
    /// `elem` may be `value` if all the elements already present were higher priority than `value`.
    ///
    /// # Time Complexity
    ///
    /// If there was spare capacity, average time complexity O(1) and worst case O(log N)
    ///
    /// If the heap was full, average time complexity O(log N) and worst case O(N)
    /// It is recommended to avoid letting the heap reach capacity.
    ///
    /// # Examples
    ///
    /// ```
    /// let mut heap: FixedHeap<i32> = FixedHeap::new(16);
    /// heap.push(1);
    /// ```
    pub fn push(&mut self, value: T) -> Option<T> {
        let mut result = None;
        let mut node_index = self.high;
        let cap = self.data.len();
        if let Some(value) = self.add_last(value) {
            // There was no space for the value. Let's try to evict something.
            if self.data.is_empty() {
                // Trivial special case to avoid invalid array access
                return Some(value);
            } else if self.high == cap {
                // Slow path, replaces smallest element. Avoid if possible.
                // # Safety
                // All indexes are initialized because the heap is full
                let mut smallest_index = cap >> 1;
                for index in cap >> 1..cap {
                    let node = unsafe { self.data.get_unchecked(index).assume_init_ref() };
                    let smallest =
                        unsafe { self.data.get_unchecked(smallest_index).assume_init_ref() };
                    if smallest > node {
                        smallest_index = index;
                    }
                }
                let smallest = unsafe { self.data.get_unchecked(smallest_index).assume_init_ref() };
                if &value > smallest {
                    let replaced = std::mem::replace(
                        unsafe { self.data.get_unchecked_mut(smallest_index) },
                        MaybeUninit::new(value),
                    );
                    node_index = smallest_index;
                    result = Some(unsafe { replaced.assume_init() });
                } else {
                    return Some(value);
                }
            }
        }

        while node_index != 0 {
            let parent_index = (node_index - 1) >> 1;
            // # Safety
            // These indices are initialized because they are in `0..high`
            let node = unsafe { self.data.get_unchecked(node_index).assume_init_ref() };
            let parent = unsafe { self.data.get_unchecked(parent_index).assume_init_ref() };
            if node < parent {
                break;
            }

            unsafe {
                self.data.swap_unchecked(node_index, parent_index);
            }

            node_index = parent_index;
        }

        result
    }

    /// Removes and returns the highest priority element, calling `comparer` with `state` to determine ordering.
    ///
    /// # Returns
    ///
    /// `None` if there are no elements in the heap.
    ///
    /// `Some(elem)` if there was an element. `elem` is higher priority than all remaining elements.
    ///
    /// # Time Complexity
    ///
    /// Average time complexity O(1) and worst case O(log N)
    ///
    /// # Examples
    ///
    /// ```
    /// let mut heap: FixedHeap<i32> = FixedHeap::new(16);
    /// heap.push(1);
    /// assert_eq!(heap.pop(), Some(1));
    /// ```
    pub fn pop(&mut self) -> Option<T> {
        self.pop_at(0)
    }

    /// Removes and returns the element at index.
    ///
    /// # Returns
    ///
    /// `None` if there is no element at `index`.
    ///
    /// `Some(elem)` if there was an element.
    ///
    /// # Time Complexity
    ///
    /// Average time complexity O(1) and worst case O(log N)
    ///
    /// # Examples
    ///
    /// ```
    /// let mut heap: FixedHeap<i32> = FixedHeap::new(16);
    /// heap.push(1);
    /// heap.push(2);
    /// assert_eq!(heap.pop_at(1), Some(1));
    /// ```
    pub fn pop_at(&mut self, index: usize) -> Option<T> {
        if let Some(removed_node) = self.swap_remove(index) {
            let mut node_index: usize = index;
            loop {
                let lchild_index = (node_index << 1) + 1;
                let rchild_index = (node_index << 1) + 2;
                // # Safety
                // These indices are initialized because they are in `0..high`
                let node = unsafe { self.data.get_unchecked(node_index).assume_init_ref() };
                // Determine which child to sift upwards by comparing
                let swap = if rchild_index < self.high {
                    let lchild = unsafe { self.data.get_unchecked(lchild_index).assume_init_ref() };
                    let rchild = unsafe { self.data.get_unchecked(rchild_index).assume_init_ref() };
                    if lchild > rchild {
                        (lchild > node, lchild_index)
                    } else {
                        (rchild > node, rchild_index)
                    }
                } else if lchild_index < self.high {
                    let lchild = unsafe { self.data.get_unchecked(lchild_index).assume_init_ref() };
                    (lchild > node, lchild_index)
                } else {
                    (false, 0)
                };
                // Sift upwards if the `compared_index` is higher priority
                if let (true, compared_index) = swap {
                    unsafe {
                        self.data.swap_unchecked(node_index, compared_index);
                    }
                    node_index = compared_index;
                } else {
                    break;
                }
            }

            Some(removed_node)
        } else {
            None
        }
    }

    /// Provides immutable access to the backing array of the heap.
    #[inline(always)]
    pub fn as_slice(&self) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.data.as_ptr() as *const T, self.high) }
    }

    /// Returns the number of elements.
    ///
    /// # Examples
    ///
    /// ```
    /// let mut heap: FixedHeap<i32> = FixedHeap::new(16);
    /// heap.push(1);
    /// assert_eq!(heap.len(), 1);
    /// ```
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.high
    }

    /// Returns the capacity of the heap.
    ///
    /// # Examples
    ///
    /// ```
    /// let mut heap: FixedHeap<i32> = FixedHeap::new(16);
    /// assert_eq!(heap.cap(), 16);
    /// heap.push(1);
    /// assert_eq!(heap.cap(), 16);
    /// ```
    #[inline(always)]
    pub fn cap(&self) -> usize {
        self.data.len()
    }

    /// Returns true if there are no elements.
    ///
    /// # Examples
    ///
    /// ```
    /// let mut heap: FixedHeap<i32> = FixedHeap::new(16);
    /// assert!(heap.is_empty());
    /// heap.push(1);
    /// assert!(!heap.is_empty());
    /// ```
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.high == 0
    }

    /// Returns true if there is no free space left.
    ///
    /// # Examples
    ///
    /// ```
    /// let mut heap: FixedHeap<i32> = FixedHeap::new(1);
    /// assert!(!heap.is_full());
    /// heap.push(1);
    /// assert!(heap.is_full());
    /// ```
    #[inline(always)]
    pub fn is_full(&self) -> bool {
        self.high == self.data.len()
    }
}

unsafe impl<T: Sync> Sync for FixedHeap<T> {}
unsafe impl<T: Send> Send for FixedHeap<T> {}

impl<T> Drop for FixedHeap<T> {
    #[inline(always)]
    fn drop(&mut self) {
        drop_guard(move || {
            for i in 0..self.high {
                unsafe { self.data.get_unchecked_mut(i).assume_init_drop() };
            }
        })
    }
}

impl<T> Debug for FixedHeap<T>
where T: Debug + Ord
{
    fn fmt(&self, f: &mut Formatter) -> Result {
        f.debug_struct("FixedHeap")
            .field("high", &self.high)
            .field("data", &self.as_slice())
            .finish()
    }
}
