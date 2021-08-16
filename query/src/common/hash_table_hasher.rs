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

use std::marker::PhantomData;

pub trait KeyHasher<Key> {
    fn hash(key: &Key) -> u64;
}

pub struct DefaultHasher<T> {
    /// Generics hold
    type_hold: PhantomData<T>,
}

macro_rules! primitive_hasher_impl {
    ($primitive_type:ty) => {
        impl KeyHasher<$primitive_type> for DefaultHasher<$primitive_type> {
            #[inline(always)]
            fn hash(key: &$primitive_type) -> u64 {
                let mut hash_value = *key as u64;
                hash_value ^= hash_value >> 33;
                hash_value = hash_value.wrapping_mul(0xff51afd7ed558ccd_u64);
                hash_value ^= hash_value >> 33;
                hash_value = hash_value.wrapping_mul(0xc4ceb9fe1a85ec53_u64);
                hash_value ^= hash_value >> 33;
                hash_value
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

#[test]
fn test_primitive_hasher() {
    println!("{:?}", DefaultHasher::<i8>::hash(&1_i8));
    println!("{:?}", DefaultHasher::<i8>::hash(&2_i8));
}
