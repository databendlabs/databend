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
use std::mem::MaybeUninit;

pub(crate) static ALLKEYS: [[[u8; 2]; 256]; 256] = {
    let mut ans = [[[0u8; 2]; 256]; 256];
    let mut i = 0usize;
    while i < 256 {
        let mut j = 0usize;
        while j < 256 {
            ans[i][j] = [i as u8, j as u8];
            j += 1;
        }
        i += 1;
    }
    ans
};

pub(crate) struct Inner<V> {
    pub(crate) data: [[MaybeUninit<V>; 64]; 1024],
    pub(crate) bits: [u64; 1024],
}

pub struct Table1<V, A: Allocator + Clone> {
    pub(crate) inner: Box<Inner<V>, A>,
    pub(crate) len: usize,
}

impl<V, A: Allocator + Clone> Table1<V, A> {
    pub fn new_in(allocator: A) -> Self {
        Self {
            inner: unsafe { Box::<Inner<V>, A>::new_zeroed_in(allocator).assume_init() },
            len: 0,
        }
    }
    pub fn capacity(&self) -> usize {
        65536
    }
    pub fn len(&self) -> usize {
        self.len
    }
    pub fn get(&self, key: [u8; 2]) -> Option<&V> {
        let x = ((key[0] as usize) << 2) | (key[1] as usize >> 6);
        let y = key[1] & 63;
        let z = (self.inner.bits[x] & (1 << y)) != 0;
        if z {
            Some(unsafe { self.inner.data[x][y as usize].assume_init_ref().to_owned() })
        } else {
            None
        }
    }
    pub fn get_mut(&mut self, key: [u8; 2]) -> Option<&mut V> {
        let x = ((key[0] as usize) << 2) | (key[1] as usize >> 6);
        let y = key[1] & 63;
        let z = (self.inner.bits[x] & (1 << y)) != 0;
        if z {
            Some(unsafe { self.inner.data[x][y as usize].assume_init_mut() })
        } else {
            None
        }
    }
    /// # Safety
    ///
    /// The resulted `MaybeUninit` should be initialized immedidately.
    pub fn insert(&mut self, key: [u8; 2]) -> Result<&mut MaybeUninit<V>, &mut V> {
        let x = ((key[0] as usize) << 2) | (key[1] as usize >> 6);
        let y = key[1] & 63;
        let z = (self.inner.bits[x] & (1 << y)) != 0;
        if z {
            Err(unsafe { self.inner.data[x][y as usize].assume_init_mut() })
        } else {
            self.len += 1;
            self.inner.bits[x] |= 1 << y;
            Ok(&mut self.inner.data[x][y as usize])
        }
    }
    pub fn iter(&self) -> impl Iterator<Item = (&[u8; 2], &V)> + '_ {
        self.inner.data.iter().enumerate().flat_map(|(x, group)| {
            let mut bits = self.inner.bits[x];
            std::iter::from_fn(move || {
                let y = bits.trailing_zeros();
                if y == u64::BITS {
                    return None;
                }
                bits ^= 1 << y;
                let i = (x >> 2) as u8;
                let j = ((x & 3) << 6) as u8 | y as u8;
                let k = &ALLKEYS[i as usize][j as usize];
                let v = unsafe { group[y as usize].assume_init_ref() }.to_owned();
                Some((k, v))
            })
        })
    }
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&[u8; 2], &mut V)> + '_ {
        self.inner
            .data
            .iter_mut()
            .enumerate()
            .flat_map(|(x, group)| {
                let mut bits = self.inner.bits[x];
                std::iter::from_fn(move || {
                    let y = bits.trailing_zeros();
                    if y == u64::BITS {
                        return None;
                    }
                    bits ^= 1 << y;
                    let i = (x >> 2) as u8;
                    let j = ((x & 3) << 6) as u8 | y as u8;
                    let k = &ALLKEYS[i as usize][j as usize];
                    let v = unsafe { &mut *(group[y as usize].assume_init_mut() as *mut V) };
                    Some((k, v))
                })
            })
    }
}

impl<V, A: Allocator + Clone> Drop for Table1<V, A> {
    fn drop(&mut self) {
        if std::mem::needs_drop::<V>() {
            self.iter_mut().for_each(|(_, v)| unsafe {
                std::ptr::drop_in_place(v);
            });
        }
    }
}
