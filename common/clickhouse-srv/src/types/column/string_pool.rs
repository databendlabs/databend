// Copyright 2020 Datafuse Labs.
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

use std::io::Write;
use std::slice;

const AVG_STR_SIZE: usize = 80;

#[derive(Copy, Clone)]
struct StringPtr {
    chunk: usize,
    shift: usize,
    len: usize,
}

#[derive(Clone)]
pub(crate) struct StringPool {
    chunks: Vec<Vec<u8>>,
    pointers: Vec<StringPtr>,
    position: usize,
    capacity: usize,
}

pub(crate) struct StringIter<'a> {
    pool: &'a StringPool,
    index: usize,
}

impl<'a> Iterator for StringIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.pool.len() {
            let result = self.pool.get(self.index);
            self.index += 1;
            return Some(result);
        }

        None
    }
}

impl<T> From<Vec<T>> for StringPool
where T: AsRef<[u8]>
{
    fn from(source: Vec<T>) -> Self {
        let mut pool = StringPool::with_capacity(source.len());
        for s in source.iter() {
            let mut b = pool.allocate(s.as_ref().len());
            b.write_all(s.as_ref()).unwrap();
        }
        pool
    }
}

impl StringPool {
    pub(crate) fn with_capacity(capacity: usize) -> StringPool {
        StringPool {
            pointers: Vec::with_capacity(capacity),
            chunks: Vec::new(),
            position: 0,
            capacity,
        }
    }

    pub(crate) fn allocate(&mut self, size: usize) -> &mut [u8] {
        if self.free_space() < size || self.chunks.is_empty() {
            self.reserve(size);
            return self.allocate(size);
        }

        self.try_allocate(size).unwrap()
    }

    fn free_space(&self) -> usize {
        if let Some(buffer) = self.chunks.last() {
            return buffer.len() - self.position;
        }

        0
    }

    fn try_allocate(&mut self, size: usize) -> Option<&mut [u8]> {
        if !self.chunks.is_empty() {
            let chunk = self.chunks.len() - 1;

            let position = self.position;
            self.position += size;
            self.pointers.push(StringPtr {
                len: size,
                shift: position,
                chunk,
            });

            let buffer = &mut self.chunks[chunk];
            return Some(&mut buffer[position..position + size]);
        }

        None
    }

    fn reserve(&mut self, size: usize) {
        use std::cmp::max;
        self.position = 0;
        self.chunks
            .push(vec![0_u8; max(self.capacity * AVG_STR_SIZE, size)]);
    }

    #[inline(always)]
    pub(crate) fn get(&self, index: usize) -> &[u8] {
        let pointer = &self.pointers[index];
        unsafe { self.get_by_pointer(pointer) }
    }

    #[inline(always)]
    pub(crate) unsafe fn get_unchecked(&self, index: usize) -> &[u8] {
        let pointer = self.pointers.get_unchecked(index);
        self.get_by_pointer(pointer)
    }

    #[inline(always)]
    unsafe fn get_by_pointer(&self, pointer: &StringPtr) -> &[u8] {
        let chunk = &self.chunks.get_unchecked(pointer.chunk);

        let ptr = chunk.as_ptr().add(pointer.shift);
        slice::from_raw_parts(ptr, pointer.len)
    }

    #[inline(always)]
    pub(crate) fn len(&self) -> usize {
        self.pointers.len()
    }

    pub(crate) fn strings(&self) -> StringIter {
        StringIter {
            pool: self,
            index: 0,
        }
    }
}

#[cfg(test)]
mod test {
    use std::io::Write;

    use super::*;

    #[test]
    fn test_allocate() {
        let mut pool = StringPool::with_capacity(10);
        for i in 1..1000 {
            let buffer = pool.allocate(i);
            assert_eq!(buffer.len(), i);
            assert_eq!(buffer[0], 0);
            buffer[0] = 1
        }
    }

    #[test]
    fn test_get() {
        let mut pool = StringPool::with_capacity(10);

        for i in 0..1000 {
            let s = format!("text-{}", i);
            let mut buffer = pool.allocate(s.len());

            assert_eq!(buffer.len(), s.len());
            buffer.write_all(s.as_bytes()).unwrap();
        }

        for i in 0..1000 {
            let s = String::from_utf8(Vec::from(pool.get(i))).unwrap();
            assert_eq!(s, format!("text-{}", i));
        }
    }
}
