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

use std::alloc::Allocator;

use databend_common_base::runtime::drop_guard;

use super::table0::Entry;

type Ent<V> = Entry<[u8; 0], V>;

pub struct TableEmpty<V, A: Allocator + Clone> {
    pub(crate) has_zero: bool,
    pub(crate) slice: Box<[Entry<[u8; 0], V>; 1], A>,
    pub(crate) allocator: A,
}

impl<V, A: Allocator + Clone + Default> Default for TableEmpty<V, A> {
    fn default() -> Self {
        Self::new_in(A::default())
    }
}

impl<V, A: Allocator + Clone> TableEmpty<V, A> {
    pub fn new_in(allocator: A) -> Self {
        Self {
            has_zero: false,
            allocator: allocator.clone(),
            slice: unsafe { Box::<[Ent<V>; 1], A>::new_zeroed_in(allocator).assume_init() },
        }
    }

    pub fn capacity(&self) -> usize {
        1
    }

    pub fn len(&self) -> usize {
        usize::from(self.has_zero)
    }

    pub fn heap_bytes(&self) -> usize {
        std::mem::size_of::<Ent<V>>()
    }

    pub fn get(&self) -> Option<&Ent<V>> {
        if self.has_zero {
            Some(&self.slice[0])
        } else {
            None
        }
    }
    pub fn get_mut(&mut self) -> Option<&mut Ent<V>> {
        if self.has_zero {
            Some(&mut self.slice[0])
        } else {
            None
        }
    }
    /// # Safety
    ///
    /// The resulted `MaybeUninit` should be initialized immediately.
    pub fn insert(&mut self) -> Result<&mut Ent<V>, &mut Ent<V>> {
        if !self.has_zero {
            self.has_zero = true;
            Ok(&mut self.slice[0])
        } else {
            Err(&mut self.slice[0])
        }
    }

    pub fn iter(&self) -> TableEmptyIter<'_, V> {
        TableEmptyIter {
            slice: self.slice.as_ref(),
            i: usize::from(!self.has_zero),
        }
    }

    pub fn clear(&mut self) {
        unsafe {
            self.has_zero = false;
            let allocator = self.allocator.clone();
            self.slice = Box::<[Ent<V>; 1], A>::new_zeroed_in(allocator).assume_init();
        }
    }
}

impl<V, A: Allocator + Clone> Drop for TableEmpty<V, A> {
    fn drop(&mut self) {
        drop_guard(move || {
            if std::mem::needs_drop::<V>() && self.has_zero {
                unsafe {
                    self.slice[0].val.assume_init_drop();
                }
            }
        })
    }
}

pub struct TableEmptyIter<'a, V> {
    slice: &'a [Ent<V>; 1],
    i: usize,
}

impl<'a, V> Iterator for TableEmptyIter<'a, V> {
    type Item = &'a Ent<V>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.i > 0 {
            None
        } else {
            self.i += 1;
            Some(&self.slice[0])
        }
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = if self.i > 0 { 0 } else { 1 };
        (len, Some(len))
    }
}

pub struct TableEmptyIterMut<'a, V> {
    slice: &'a mut [Ent<V>; 1],
    i: usize,
}

impl<'a, V> Iterator for TableEmptyIterMut<'a, V> {
    type Item = &'a mut Ent<V>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.i > 0 {
            None
        } else {
            let res = unsafe { &mut *(self.slice.as_ptr().add(self.i) as *mut _) };
            self.i += 1;
            Some(res)
        }
    }
}
