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

use std::alloc::Allocator;

use super::table0::Entry;
use super::traits::Keyable;
use crate::container::Container;
use crate::container::StackContainer;

const SIZE: usize = 8192;

pub struct TailArray<K, V, A: Allocator> {
    allocator: A,
    pub(crate) datas: Vec<StackContainer<Entry<K, V>, SIZE, A>>,
    pub(crate) num_items: usize,
}

impl<K, V, A> TailArray<K, V, A>
where
    K: Keyable,
    A: Allocator + Clone,
{
    pub fn new(allocator: A) -> Self {
        Self {
            datas: vec![],
            num_items: 0,
            allocator,
        }
    }

    pub fn insert(&mut self, key: K) -> &mut Entry<K, V> {
        let pos = self.num_items % SIZE;
        if pos == 0 {
            let container = unsafe { StackContainer::new_zeroed(SIZE, self.allocator.clone()) };
            self.datas.push(container);
        }

        let tail = self.datas.last_mut().unwrap();
        unsafe { tail[pos].set_key(key) };

        self.num_items += 1;
        &mut tail[pos]
    }

    pub fn iter(&self) -> TailArrayIter<'_, K, V> {
        TailArrayIter {
            values: self.datas.iter().map(|v| v.as_ref()).collect(),
            num_items: self.num_items,
            i: 0,
        }
    }

    #[allow(dead_code)]
    pub fn iter_mut(&mut self) -> TailArrayIterMut<'_, K, V> {
        TailArrayIterMut {
            values: self.datas.iter_mut().map(|v| v.as_mut()).collect(),
            num_items: self.num_items,
            i: 0,
        }
    }
}

pub struct TailArrayIter<'a, K, V> {
    values: Vec<&'a [Entry<K, V>]>,
    num_items: usize,
    i: usize,
}

impl<'a, K, V> Iterator for TailArrayIter<'a, K, V> {
    type Item = &'a Entry<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.i >= self.num_items {
            None
        } else {
            let array = self.i / SIZE;
            let pos = self.i % SIZE;

            let v = self.values[array];
            let res = &v.as_ref()[pos];
            self.i += 1;
            Some(res)
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.num_items - self.i, Some(self.num_items - self.i))
    }
}

pub struct TailArrayIterMut<'a, K, V> {
    values: Vec<&'a mut [Entry<K, V>]>,
    num_items: usize,
    i: usize,
}

impl<'a, K, V> Iterator for TailArrayIterMut<'a, K, V>
where Self: 'a
{
    type Item = &'a mut Entry<K, V>  where Self: 'a ;

    fn next(&mut self) -> Option<Self::Item> {
        if self.i >= self.num_items {
            None
        } else {
            let array = self.i / SIZE;
            let pos = self.i % SIZE;

            let v = &mut self.values[array];
            let res = unsafe { &mut *(v.as_ptr().add(pos) as *mut _) };
            self.i += 1;
            Some(res)
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.num_items - self.i, Some(self.num_items - self.i))
    }
}
