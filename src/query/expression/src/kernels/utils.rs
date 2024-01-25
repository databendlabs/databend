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

/// # Safety
///
/// * `ptr` must be [valid] for writes of `size_of::<T>()` bytes.
/// * The region of memory beginning at `val` with a size of `size_of::<T>()`
///   bytes must *not* overlap with the region of memory beginning at `ptr`
///   with the same size.
#[inline]
pub unsafe fn store_advance<T>(val: &T, ptr: &mut *mut u8) {
    unsafe {
        std::ptr::copy_nonoverlapping(val as *const T as *const u8, *ptr, std::mem::size_of::<T>());
        *ptr = ptr.add(std::mem::size_of::<T>())
    }
}

/// # Safety
///
/// * `ptr` must be [valid] for writes.
/// * `ptr` must be properly aligned.
#[inline]
pub unsafe fn store_advance_aligned<T>(val: T, ptr: &mut *mut T) {
    unsafe {
        std::ptr::write(*ptr, val);
        *ptr = ptr.add(1)
    }
}

/// # Safety
///
/// * `src` must be [valid] for reads of `count * size_of::<T>()` bytes.
/// * `ptr` must be [valid] for writes of `count * size_of::<T>()` bytes.
/// * Both `src` and `dst` must be properly aligned.
/// * The region of memory beginning at `val` with a size of `count * size_of::<T>()`
///   bytes must *not* overlap with the region of memory beginning at `ptr` with the
///   same size.
#[inline]
pub unsafe fn copy_advance_aligned<T>(src: *const T, ptr: &mut *mut T, count: usize) {
    unsafe {
        std::ptr::copy_nonoverlapping(src, *ptr, count);
        *ptr = ptr.add(count);
    }
}

/// # Safety
///
/// * `(ptr as usize - vec.as_ptr() as usize) / std::mem::size_of::<T>()` must be
///    less than or equal to the capacity of Vec.
#[inline]
pub unsafe fn set_vec_len_by_ptr<T>(vec: &mut Vec<T>, ptr: *const T) {
    unsafe {
        vec.set_len(ptr.offset_from(vec.as_ptr()) as usize);
    }
}

/// # Safety
/// # As: core::ptr::write
#[inline]
pub unsafe fn store<T: Copy>(val: T, ptr: *mut u8) {
    core::ptr::write(ptr as _, val)
}

/// Iterates over an arbitrarily aligned byte buffer
///
/// Yields an iterator of u64, and a remainder. The first byte in the buffer
/// will be the least significant byte in output u64
#[derive(Debug)]
pub struct BitChunks<'a> {
    buffer: &'a [u8],
    /// offset inside a byte, guaranteed to be between 0 and 7 (inclusive)
    bit_offset: usize,
    /// number of complete u64 chunks
    chunk_len: usize,
    /// number of remaining bits, guaranteed to be between 0 and 63 (inclusive)
    remainder_len: usize,
}

impl<'a> BitChunks<'a> {
    pub fn new(buffer: &'a [u8], offset: usize, len: usize) -> Self {
        assert!((offset + len + 7) / 8 <= buffer.len() * 8);

        let byte_offset = offset / 8;
        let bit_offset = offset % 8;

        // number of complete u64 chunks
        let chunk_len = len / 64;
        // number of remaining bits
        let remainder_len = len % 64;

        BitChunks::<'a> {
            buffer: &buffer[byte_offset..],
            bit_offset,
            chunk_len,
            remainder_len,
        }
    }
}

#[derive(Debug)]
pub struct BitChunkIterator {
    buffer: *const u64,
    bit_offset: usize,
    chunk_len: usize,
    index: usize,
}

impl<'a> BitChunks<'a> {
    /// Returns the number of remaining bits, guaranteed to be between 0 and 63 (inclusive)
    #[inline]
    pub const fn remainder_len(&self) -> usize {
        self.remainder_len
    }

    /// Returns an iterator over chunks of 64 bits represented as an u64
    #[inline]
    pub const fn iter(&self) -> BitChunkIterator {
        BitChunkIterator {
            buffer: self.buffer.as_ptr() as *const u64,
            bit_offset: self.bit_offset,
            chunk_len: self.chunk_len,
            index: 0,
        }
    }
}

impl Iterator for BitChunkIterator {
    type Item = u64;

    #[inline]
    fn next(&mut self) -> Option<u64> {
        let index = self.index;
        if index >= self.chunk_len {
            return None;
        }

        // bit-packed buffers are stored starting with the least-significant byte first
        // so when reading as u64 on a big-endian machine, the bytes need to be swapped
        let current = unsafe { std::ptr::read_unaligned(self.buffer.add(index)).to_le() };

        let bit_offset = self.bit_offset;

        let combined = if bit_offset == 0 {
            current
        } else {
            // the constructor ensures that bit_offset is in 0..8
            // that means we need to read at most one additional byte to fill in the high bits
            let next =
                unsafe { std::ptr::read_unaligned(self.buffer.add(index + 1) as *const u8) as u64 };

            (current >> bit_offset) | (next << (64 - bit_offset))
        };

        self.index = index + 1;

        Some(combined)
    }
}
