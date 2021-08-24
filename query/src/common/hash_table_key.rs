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


use crate::common::FastHash;

pub trait HashTableKeyable: FastHash + Eq + Sized + Copy {
    fn is_zero(&self) -> bool;
    fn eq_with_hash(&self, hash: u64, other_key: &Self, other_key_hash: u64) -> bool;
}

macro_rules! primitive_hasher_impl {
    ($primitive_type:ty) => {
        impl HashTableKeyable for $primitive_type {

            #[inline(always)]
            fn is_zero(&self) -> bool {
                *self == 0
            }

            #[inline(always)]
            fn eq_with_hash(&self, _hash: u64, other_key: &Self, _other_key_hash: u64) -> bool {
                self == other_key
            }
        }
    };
}

primitive_hasher_impl!(i8);
primitive_hasher_impl!(i16);
primitive_hasher_impl!(i32);
primitive_hasher_impl!(i64);
primitive_hasher_impl!(u8);
primitive_hasher_impl!(u16);
primitive_hasher_impl!(u32);
primitive_hasher_impl!(u64);
