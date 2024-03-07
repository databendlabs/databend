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

type Ent<V> = Entry<[u8; 2], V>;

pub struct Table1<V, A: Allocator + Clone> {
    pub(crate) data: Box<[Entry<[u8; 2], V>; 65536], A>,
    pub(crate) len: usize,
}

impl<V, A: Allocator + Clone> Table1<V, A> {
    pub fn new_in(allocator: A) -> Self {
        Self {
            data: unsafe {
                let mut res =
                    Box::<[Entry<[u8; 2], V>; 65536], A>::new_zeroed_in(allocator).assume_init();
                res[0].key.write([0xff, 0xff]);
                res
            },
            len: 0,
        }
    }
    pub fn capacity(&self) -> usize {
        65536
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn heap_bytes(&self) -> usize {
        std::mem::size_of::<[Entry<[u8; 2], V>; 65536]>()
    }

    pub fn get(&self, key: [u8; 2]) -> Option<&Ent<V>> {
        let e = &self.data[key[1] as usize * 256 + key[0] as usize];
        if unsafe { e.key.assume_init() } == key {
            Some(e)
        } else {
            None
        }
    }
    pub fn get_mut(&mut self, key: [u8; 2]) -> Option<&mut Ent<V>> {
        let e = &mut self.data[key[1] as usize * 256 + key[0] as usize];
        if unsafe { e.key.assume_init() } == key {
            Some(e)
        } else {
            None
        }
    }
    /// # Safety
    ///
    /// The resulted `MaybeUninit` should be initialized immediately.
    pub fn insert(&mut self, key: [u8; 2]) -> Result<&mut Ent<V>, &mut Ent<V>> {
        let e = &mut self.data[key[1] as usize * 256 + key[0] as usize];
        if unsafe { e.key.assume_init() } == key {
            Err(e)
        } else {
            self.len += 1;
            e.key.write(key);
            Ok(e)
        }
    }
    pub fn iter(&self) -> Table1Iter<'_, V> {
        Table1Iter {
            slice: self.data.as_ref(),
            i: 0,
        }
    }
    pub fn iter_mut(&mut self) -> Table1IterMut<'_, V> {
        Table1IterMut {
            slice: self.data.as_mut(),
            i: 0,
        }
    }
}

impl<V, A: Allocator + Clone> Drop for Table1<V, A> {
    fn drop(&mut self) {
        drop_guard(move || {
            if std::mem::needs_drop::<V>() {
                self.iter_mut().for_each(|e| unsafe {
                    e.val.assume_init_drop();
                });
            }
        })
    }
}

pub struct Table1Iter<'a, V> {
    slice: &'a [Entry<[u8; 2], V>; 65536],
    i: usize,
}

impl<'a, V> Iterator for Table1Iter<'a, V> {
    type Item = &'a Entry<[u8; 2], V>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.i < 65536
            && unsafe {
                u16::from_le_bytes(self.slice[self.i].key.assume_init()) as usize != self.i
            }
        {
            self.i += 1;
        }
        if self.i == 65536 {
            None
        } else {
            let res = unsafe { &*(self.slice.as_ptr().add(self.i) as *const _) };
            self.i += 1;
            Some(res)
        }
    }
}

pub struct Table1IterMut<'a, V> {
    slice: &'a mut [Entry<[u8; 2], V>; 65536],
    i: usize,
}

impl<'a, V> Iterator for Table1IterMut<'a, V> {
    type Item = &'a mut Entry<[u8; 2], V>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.i < 65536
            && unsafe {
                u16::from_le_bytes(self.slice[self.i].key.assume_init()) as usize != self.i
            }
        {
            self.i += 1;
        }
        if self.i == 65536 {
            None
        } else {
            let res = unsafe { &mut *(self.slice.as_ptr().add(self.i) as *mut _) };
            self.i += 1;
            Some(res)
        }
    }
}
