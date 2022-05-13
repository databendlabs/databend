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

use crate::common::HashTableKeyable;

pub trait HashTableEntity<Key>: Sized {
    unsafe fn is_zero(self: *mut Self) -> bool;
    unsafe fn key_equals(self: *mut Self, key: &Key, hash: u64) -> bool;
    unsafe fn set_key_and_hash(self: *mut Self, key: &Key, hash: u64);

    fn get_key<'a>(self: *mut Self) -> &'a Key;
    unsafe fn get_hash(self: *mut Self) -> u64;

    unsafe fn not_equals_key(self: *mut Self, other: *mut Self) -> bool;
}

pub struct KeyValueEntity<Key, Value>
where
    Key: HashTableKeyable,
    Value: Sized + Copy,
{
    key: Key,
    value: Value,
    hash: u64,
}

impl<Key, Value> KeyValueEntity<Key, Value>
where
    Key: HashTableKeyable,
    Value: Sized + Copy,
{
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

impl<Key, Value> HashTableEntity<Key> for KeyValueEntity<Key, Value>
where
    Key: HashTableKeyable,
    Value: Sized + Copy,
{
    #[inline(always)]
    unsafe fn is_zero(self: *mut Self) -> bool {
        (*self).key.is_zero()
    }

    unsafe fn key_equals(self: *mut Self, key: &Key, hash: u64) -> bool {
        if Key::BEFORE_EQ_HASH && (*self).hash != hash {
            return false;
        }

        (*self).key == *key
    }

    unsafe fn set_key_and_hash(self: *mut Self, key: &Key, hash: u64) {
        (*self).hash = hash;
        (*self).key.set_key(key);
    }

    fn get_key<'a>(self: *mut Self) -> &'a Key {
        unsafe { &(*self).key }
    }

    unsafe fn get_hash(self: *mut Self) -> u64 {
        (*self).hash
    }

    unsafe fn not_equals_key(self: *mut Self, other: *mut Self) -> bool {
        if Key::BEFORE_EQ_HASH && (*self).hash != (*other).hash {
            return true;
        }

        !((*self).key == (*other).key)
    }
}
