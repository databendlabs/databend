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

use std::intrinsics::assume;
use std::mem::MaybeUninit;
use std::ops::Deref;
use std::ops::DerefMut;

use super::table0::Entry;
use super::traits::Keyable;

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Hashed<K: Keyable> {
    hash: u64,
    key: K,
}

impl<K: Keyable> Hashed<K> {
    #[inline(always)]
    pub fn new(key: K) -> Self {
        Self {
            hash: K::hash(&key),
            key,
        }
    }
    #[inline(always)]
    pub fn key(self) -> K {
        self.key
    }
}

unsafe impl<K: Keyable> Keyable for Hashed<K> {
    #[inline(always)]
    fn is_zero(this: &MaybeUninit<Self>) -> bool {
        unsafe {
            let addr = std::ptr::addr_of!((*this.as_ptr()).key);
            K::is_zero(&*(addr as *const MaybeUninit<K>))
        }
    }

    #[inline(always)]
    fn equals_zero(this: &Self) -> bool {
        K::equals_zero(&this.key)
    }

    #[inline(always)]
    fn hash(&self) -> u64 {
        self.hash
    }
}

pub struct ZeroEntry<K, V>(pub Option<Entry<K, V>>);

impl<K, V> Deref for ZeroEntry<K, V> {
    type Target = Option<Entry<K, V>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<K, V> DerefMut for ZeroEntry<K, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<K, V> Drop for ZeroEntry<K, V> {
    fn drop(&mut self) {
        if let Some(e) = self.0.as_mut() {
            unsafe {
                e.val.assume_init_drop();
            }
        }
    }
}

#[inline(always)]
pub unsafe fn read_le(data: *const u8, len: usize) -> u64 {
    assume(0 < len && len <= 8);
    let s = 64 - 8 * len as isize;
    if data as usize & 2048 == 0 {
        (data as *const u64).read_unaligned() & (u64::MAX >> s)
    } else {
        (data.offset(len as isize - 8) as *const u64).read_unaligned() >> s
    }
}

#[cfg(all(target_arch = "x86_64", target_feature = "sse4.2"))]
#[inline]
pub fn fast_memcmp(a: &[u8], b: &[u8]) -> bool {
    unsafe { sse::memcmp_sse(a, b) }
}

#[cfg(not(all(any(target_arch = "x86_64"), target_feature = "sse4.2")))]
#[inline]
pub fn fast_memcmp(a: &[u8], b: &[u8]) -> bool {
    a == b
}

#[cfg(all(target_arch = "x86_64", target_feature = "sse4.2"))]
pub mod sse {
    use std::arch::x86_64::*;

    #[inline(always)]
    pub unsafe fn compare_sse2(a: *const u8, b: *const u8) -> bool {
        0xFFFF
            == _mm_movemask_epi8(_mm_cmpeq_epi8(
                _mm_loadu_si128(a as *const __m128i),
                _mm_loadu_si128(b as *const __m128i),
            ))
    }

    #[inline(always)]
    pub unsafe fn compare_sse2_x4(a: *const u8, b: *const u8) -> bool {
        0xFFFF
            == _mm_movemask_epi8(_mm_and_si128(
                _mm_and_si128(
                    _mm_cmpeq_epi8(
                        _mm_loadu_si128(a as *const __m128i),
                        _mm_loadu_si128(b as *const __m128i),
                    ),
                    _mm_cmpeq_epi8(
                        _mm_loadu_si128((a as *const __m128i).offset(1)),
                        _mm_loadu_si128((b as *const __m128i).offset(1)),
                    ),
                ),
                _mm_and_si128(
                    _mm_cmpeq_epi8(
                        _mm_loadu_si128((a as *const __m128i).offset(2)),
                        _mm_loadu_si128((b as *const __m128i).offset(2)),
                    ),
                    _mm_cmpeq_epi8(
                        _mm_loadu_si128((a as *const __m128i).offset(3)),
                        _mm_loadu_si128((b as *const __m128i).offset(3)),
                    ),
                ),
            ))
    }

    /// # Safety
    /// This is safe that we compare bytes via addr
    #[inline(always)]
    pub unsafe fn memcmp_sse(a: &[u8], b: &[u8]) -> bool {
        let mut size = a.len();
        if size != b.len() {
            return false;
        }

        let mut a = a.as_ptr();
        let mut b = b.as_ptr();

        if size <= 16 {
            if size >= 8 {
                return (a as *const u64).read_unaligned() == (b as *const u64).read_unaligned()
                    && (a.add(size - 8) as *const u64).read_unaligned()
                        == (b.add(size - 8) as *const u64).read_unaligned();
            } else if size >= 4 {
                return (a as *const u32).read_unaligned() == (b as *const u32).read_unaligned()
                    && (a.add(size - 4) as *const u32).read_unaligned()
                        == (b.add(size - 4) as *const u32).read_unaligned();
            } else if size >= 2 {
                return (a as *const u16).read_unaligned() == (b as *const u16).read_unaligned()
                    && (a.add(size - 2) as *const u16).read_unaligned()
                        == (b.add(size - 2) as *const u16).read_unaligned();
            } else if size >= 1 {
                return *a == *b;
            }
            return true;
        }

        while size >= 64 {
            if compare_sse2_x4(a, b) {
                a = a.add(64);
                b = b.add(64);

                size -= 64;
            } else {
                return false;
            }
        }

        match size / 16 {
            3 if !compare_sse2(a.add(32), b.add(32)) => false,
            2 if !compare_sse2(a.add(16), b.add(16)) => false,
            1 if !compare_sse2(a, b) => false,
            _ => compare_sse2(a.add(size - 16), b.add(size - 16)),
        }
    }
}
