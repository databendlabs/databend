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

pub trait HashTableEntity<Key>: Sized {
    fn is_zero_key(key: &Key) -> bool;

    unsafe fn is_zero(self: *mut Self) -> bool;
    unsafe fn key_equals(self: *mut Self, key: &Key, hash: u64) -> bool;
    unsafe fn set_key_and_hash(self: *mut Self, key: &Key, hash: u64);

    fn get_key<'a>(self: *mut Self) -> &'a Key;
    unsafe fn get_hash(self: *mut Self) -> u64;

    unsafe fn not_equals_key(self: *mut Self, other: *mut Self) -> bool;
}

pub struct DefaultHashTableEntity<Key: Sized, Value: Sized> {
    key: Key,
    value: Value,
    hash: u64,
}

impl<Key: Sized, Value: Sized> DefaultHashTableEntity<Key, Value> {
    #[inline(always)]
    pub fn set_value(self: *mut Self, value: Value) {
        unsafe {
            (*self).value = value;
        }
    }

    #[inline(always)]
    pub fn get_value<'a>(self: *mut Self) -> &'a Value {
        unsafe { &(*self).value }
    }
}

macro_rules! primitive_key_entity_impl {
    ($typ:ty) => {
        impl<Value: Sized> HashTableEntity<$typ> for DefaultHashTableEntity<$typ, Value> {
            fn is_zero_key(key: &$typ) -> bool {
                *key == 0
            }

            unsafe fn is_zero(self: *mut Self) -> bool {
                (*self).key == 0
            }

            unsafe fn key_equals(self: *mut Self, key: &$typ, _hash: u64) -> bool {
                (*self).key == *key
            }

            unsafe fn set_key_and_hash(self: *mut Self, key: &$typ, hash: u64) {
                (*self).key = *key;
                (*self).hash = hash;
            }

            fn get_key<'a>(self: *mut Self) -> &'a $typ {
                unsafe { &(*self).key }
            }

            unsafe fn get_hash(self: *mut Self) -> u64 {
                (*self).hash
            }

            unsafe fn not_equals_key(self: *mut Self, other: *mut Self) -> bool {
                !((*self).key == (*other).key)
            }
        }
    };
}

primitive_key_entity_impl!(i8);
primitive_key_entity_impl!(i16);
primitive_key_entity_impl!(i32);
primitive_key_entity_impl!(i64);
primitive_key_entity_impl!(u8);
primitive_key_entity_impl!(u16);
primitive_key_entity_impl!(u32);
primitive_key_entity_impl!(u64);
