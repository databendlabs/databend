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

// To avoid RUSTFLAGS="-C target-feature=+sse4.2" warning.
#![allow(unused_imports)]
use std::hash::BuildHasher;
use std::hash::Hasher;
use std::iter::TrustedLen;
use std::mem::MaybeUninit;
use std::num::NonZeroU64;

use ethnum::U256;
use ordered_float::OrderedFloat;

use crate::RowPtr;

/// # Safety
///
/// All functions must be implemented correctly.
pub unsafe trait Keyable: Sized + Copy + Eq {
    fn is_zero(this: &MaybeUninit<Self>) -> bool;

    fn equals_zero(this: &Self) -> bool;

    fn hash(&self) -> u64;
}

/// # Safety
///
/// There shouldn't be any uninitialized bytes or interior mutability in `Self`.
/// The implementation promises `from_bytes(as_bytes(self))` is `self`.
/// `as_bytes` should returns a slice that is valid for reads.
pub unsafe trait UnsizedKeyable {
    fn as_bytes(&self) -> &[u8];

    /// # Safety
    /// The argument should be a return value of `as_bytes` in the same implementation.
    unsafe fn from_bytes(bytes: &[u8]) -> &Self;
}

macro_rules! impl_key_for_primitive_types {
    ($t: ty) => {
        unsafe impl Keyable for $t {
            #[inline(always)]
            fn equals_zero(this: &Self) -> bool {
                *this == 0
            }

            #[inline(always)]
            fn is_zero(this: &MaybeUninit<Self>) -> bool {
                unsafe { this.assume_init() == 0 }
            }

            #[inline(always)]
            fn hash(&self) -> u64 {
                self.fast_hash()
            }
        }
    };
}

impl_key_for_primitive_types!(u8);
impl_key_for_primitive_types!(i8);
impl_key_for_primitive_types!(u16);
impl_key_for_primitive_types!(i16);
impl_key_for_primitive_types!(u32);
impl_key_for_primitive_types!(i32);
impl_key_for_primitive_types!(u64);
impl_key_for_primitive_types!(i64);
impl_key_for_primitive_types!(u128);
impl_key_for_primitive_types!(i128);

unsafe impl Keyable for U256 {
    #[inline(always)]
    fn equals_zero(this: &Self) -> bool {
        *this == U256::ZERO
    }

    #[inline(always)]
    fn is_zero(this: &MaybeUninit<Self>) -> bool {
        *unsafe { this.assume_init_ref() } == U256::ZERO
    }

    #[inline(always)]
    fn hash(&self) -> u64 {
        self.fast_hash()
    }
}

unsafe impl Keyable for OrderedFloat<f32> {
    #[inline(always)]
    fn is_zero(this: &MaybeUninit<Self>) -> bool {
        unsafe { this.assume_init() == 0.0 }
    }

    #[inline(always)]
    fn equals_zero(this: &Self) -> bool {
        *this == 0.0
    }

    #[inline(always)]
    fn hash(&self) -> u64 {
        self.fast_hash()
    }
}

unsafe impl Keyable for OrderedFloat<f64> {
    #[inline(always)]
    fn is_zero(this: &MaybeUninit<Self>) -> bool {
        unsafe { this.assume_init() == 0.0 }
    }

    #[inline(always)]
    fn equals_zero(this: &Self) -> bool {
        *this == 0.0
    }

    #[inline(always)]
    fn hash(&self) -> u64 {
        self.fast_hash()
    }
}

unsafe impl<const N: usize> Keyable for [u8; N] {
    #[inline(always)]
    fn is_zero(this: &MaybeUninit<Self>) -> bool {
        unsafe { this.assume_init() == [0; N] }
    }

    #[inline(always)]
    fn equals_zero(this: &Self) -> bool {
        *this == [0; N]
    }

    #[inline(always)]
    fn hash(&self) -> u64 {
        (self as &[u8]).fast_hash()
    }
}

unsafe impl UnsizedKeyable for [u8] {
    fn as_bytes(&self) -> &[u8] {
        self
    }

    unsafe fn from_bytes(bytes: &[u8]) -> &Self {
        bytes
    }
}

unsafe impl UnsizedKeyable for str {
    fn as_bytes(&self) -> &[u8] {
        self.as_bytes()
    }

    unsafe fn from_bytes(bytes: &[u8]) -> &Self {
        std::str::from_utf8_unchecked(bytes)
    }
}

pub trait FastHash {
    // Note: when using `_mm_crc32_u64`, the high 32 bits of the result is always 0.
    // But it's enough for our use case because hashtable's len will not exceed 2^32.
    // And for partitioned hashtable, we use the low 32 bits to get the bucket number.
    fn fast_hash(&self) -> u64;
}

macro_rules! impl_fast_hash_for_primitive_types {
    ($t: ty) => {
        impl FastHash for $t {
            #[inline(always)]
            fn fast_hash(&self) -> u64 {
                let mut hasher = *self as u64;
                hasher ^= hasher >> 33;
                hasher = hasher.wrapping_mul(0xff51afd7ed558ccd_u64);
                hasher ^= hasher >> 33;
                hasher = hasher.wrapping_mul(0xc4ceb9fe1a85ec53_u64);
                hasher ^= hasher >> 33;
                hasher
            }
        }
    };
}

impl_fast_hash_for_primitive_types!(u8);
impl_fast_hash_for_primitive_types!(i8);
impl_fast_hash_for_primitive_types!(u16);
impl_fast_hash_for_primitive_types!(i16);
impl_fast_hash_for_primitive_types!(u32);
impl_fast_hash_for_primitive_types!(i32);
impl_fast_hash_for_primitive_types!(u64);
impl_fast_hash_for_primitive_types!(i64);

impl FastHash for u128 {
    #[inline(always)]
    fn fast_hash(&self) -> u64 {
        cfg_if::cfg_if! {
            if #[cfg(target_feature = "sse4.2")] {
                use std::arch::x86_64::_mm_crc32_u64;
                let value = unsafe { _mm_crc32_u64(u64::MAX, *self as u64) };
                unsafe { _mm_crc32_u64(value, (*self >> 64) as u64) }
            } else {
                use std::hash::Hasher;
                let state = ahash::RandomState::with_seeds(SEEDS[0], SEEDS[1], SEEDS[2], SEEDS[3]);
                let mut hasher = state.build_hasher();
                hasher.write_u128(*self);
                hasher.finish()
            }
        }
    }
}

impl FastHash for i128 {
    #[inline(always)]
    fn fast_hash(&self) -> u64 {
        (*self as u128).fast_hash()
    }
}

impl FastHash for U256 {
    #[inline(always)]
    fn fast_hash(&self) -> u64 {
        cfg_if::cfg_if! {
            if #[cfg(target_feature = "sse4.2")] {
                use std::arch::x86_64::_mm_crc32_u64;
                let mut value = u64::MAX;
                for x in self.0 {
                    value = unsafe { _mm_crc32_u64(value, x as u64) };
                    value = unsafe { _mm_crc32_u64(value, (x >> 64) as u64) };
                }
                value
            } else {
                use std::hash::Hasher;
                let state = ahash::RandomState::with_seeds(SEEDS[0], SEEDS[1], SEEDS[2], SEEDS[3]);
                let mut hasher = state.build_hasher();
                for x in self.0 {
                    hasher.write_u128(x);
                }
                hasher.finish()
            }
        }
    }
}

impl FastHash for OrderedFloat<f32> {
    #[inline(always)]
    fn fast_hash(&self) -> u64 {
        if self.is_nan() {
            f32::NAN.to_bits().fast_hash()
        } else {
            self.to_bits().fast_hash()
        }
    }
}

impl FastHash for OrderedFloat<f64> {
    #[inline(always)]
    fn fast_hash(&self) -> u64 {
        if self.is_nan() {
            f64::NAN.to_bits().fast_hash()
        } else {
            self.to_bits().fast_hash()
        }
    }
}

// To avoid RUSTFLAGS="-C target-feature=+sse4.2" warning.
#[allow(dead_code)]
const SEEDS: [u64; 4] = [1, 1949, 2009, 9527];

impl FastHash for [u8] {
    #[inline(always)]
    fn fast_hash(&self) -> u64 {
        cfg_if::cfg_if! {
            if #[cfg(target_feature = "sse4.2")] {
                use crate::utils::read_le;
                use std::arch::x86_64::_mm_crc32_u64;
                let mut value = u64::MAX;
                for i in (0..self.len()).step_by(8) {
                    if i + 8 < self.len() {
                        unsafe {
                            let x = (&self[i] as *const u8 as *const u64).read_unaligned();
                            value = _mm_crc32_u64(value, x);
                        }
                    } else {
                        unsafe {
                            let x = read_le(&self[i] as *const u8, self.len() - i);
                            value = _mm_crc32_u64(value, x);
                        }
                    }
                }
                value
            } else {
                use std::hash::Hasher;
                let state = ahash::RandomState::with_seeds(SEEDS[0], SEEDS[1], SEEDS[2], SEEDS[3]);
                let mut hasher = state.build_hasher();
                hasher.write(self);
                hasher.finish()
            }
        }
    }
}

// trick for unsized_hashtable
impl<const N: usize> FastHash for ([u64; N], NonZeroU64) {
    #[inline(always)]
    fn fast_hash(&self) -> u64 {
        cfg_if::cfg_if! {
            if #[cfg(target_feature = "sse4.2")] {
                use std::arch::x86_64::_mm_crc32_u64;
                let mut value = u64::MAX;
                for x in self.0 {
                    value = unsafe { _mm_crc32_u64(value, x) };
                }
                value
            } else {
                 use std::hash::Hasher;
                let state = ahash::RandomState::with_seeds(SEEDS[0], SEEDS[1], SEEDS[2], SEEDS[3]);
                let mut hasher = state.build_hasher();
                for x in self.0 {
                    hasher.write_u64(x);
                }
                hasher.write_u64(self.1.get());
                hasher.finish()
            }
        }
    }
}

pub trait EntryRefLike: Copy {
    type KeyRef;
    type ValueRef;

    fn key(&self) -> Self::KeyRef;
    fn get(&self) -> Self::ValueRef;
}

pub trait EntryMutRefLike {
    type Key: ?Sized;
    type Value;

    fn key(&self) -> &Self::Key;
    fn get(&self) -> &Self::Value;
    fn get_mut(&mut self) -> &mut Self::Value;
    fn write(&mut self, value: Self::Value);
}

#[allow(clippy::len_without_is_empty)]
pub trait HashtableLike {
    type Key: ?Sized;
    type Value;

    type EntryRef<'a>: EntryRefLike<KeyRef = &'a Self::Key, ValueRef = &'a Self::Value>
    where
        Self: 'a,
        Self::Key: 'a,
        Self::Value: 'a;
    type EntryMutRef<'a>: EntryMutRefLike<Key = Self::Key, Value = Self::Value>
    where
        Self: 'a,
        Self::Key: 'a,
        Self::Value: 'a;

    type Iterator<'a>: Iterator<Item = Self::EntryRef<'a>> + TrustedLen
    where
        Self: 'a,
        Self::Key: 'a,
        Self::Value: 'a;
    type IteratorMut<'a>: Iterator<Item = Self::EntryMutRef<'a>>
    where
        Self: 'a,
        Self::Key: 'a,
        Self::Value: 'a;

    fn len(&self) -> usize;

    fn bytes_len(&self, without_arena: bool) -> usize;

    fn unsize_key_size(&self) -> Option<usize> {
        None
    }

    fn entry(&self, key_ref: &Self::Key) -> Option<Self::EntryRef<'_>>;
    fn entry_mut(&mut self, key_ref: &Self::Key) -> Option<Self::EntryMutRef<'_>>;

    fn get(&self, key_ref: &Self::Key) -> Option<&Self::Value>;
    fn get_mut(&mut self, key_ref: &Self::Key) -> Option<&mut Self::Value>;

    /// # Safety
    ///
    /// The uninitialized value of returned entry should be written immediately.
    unsafe fn insert(
        &mut self,
        key_ref: &Self::Key,
    ) -> Result<&mut MaybeUninit<Self::Value>, &mut Self::Value>;

    /// # Safety
    ///
    /// The uninitialized value of returned entry should be written immediately.
    unsafe fn insert_and_entry(
        &mut self,
        key_ref: &Self::Key,
    ) -> Result<Self::EntryMutRef<'_>, Self::EntryMutRef<'_>>;

    /// # Safety
    ///
    /// The uninitialized value of returned entry should be written immediately.
    unsafe fn insert_and_entry_with_hash(
        &mut self,
        key_ref: &Self::Key,
        hash: u64,
    ) -> Result<Self::EntryMutRef<'_>, Self::EntryMutRef<'_>>;

    fn iter(&self) -> Self::Iterator<'_>;

    fn clear(&mut self);
}

pub trait HashJoinHashtableLike {
    type Key: ?Sized;

    // Using hashes to probe hash table and converting them in-place to pointers for memory reuse.
    fn probe(&self, hashes: &mut [u64]);

    fn next_contains(&self, key: &Self::Key, ptr: u64) -> bool;

    fn next_probe(
        &self,
        key: &Self::Key,
        ptr: u64,
        vec_ptr: *mut RowPtr,
        occupied: usize,
        capacity: usize,
    ) -> (usize, u64);
}
