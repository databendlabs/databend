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

use crate::common::HashTableEntity;
use crate::common::HashTableKeyable;
use crate::common::KeyValueEntity;

pub trait StateEntity<Key> {
    fn get_state_key<'a>(self: *mut Self) -> &'a Key;
    fn set_state_value(self: *mut Self, value: usize);
    fn get_state_value<'a>(self: *mut Self) -> &'a usize;
}

pub trait ShortFixedKeyable: Sized + Clone {
    fn lookup(&self) -> isize;
    fn is_zero_key(&self) -> bool;
}

pub struct ShortFixedKeysStateEntity<Key: ShortFixedKeyable> {
    pub key: Key,
    pub value: usize,
    pub fill: bool,
}

impl<Key: ShortFixedKeyable> StateEntity<Key> for ShortFixedKeysStateEntity<Key> {
    #[inline(always)]
    fn get_state_key<'a>(self: *mut Self) -> &'a Key {
        unsafe { &(*self).key }
    }

    #[inline(always)]
    fn set_state_value(self: *mut Self, value: usize) {
        unsafe { (*self).value = value }
    }

    #[inline(always)]
    fn get_state_value<'a>(self: *mut Self) -> &'a usize {
        unsafe { &(*self).value }
    }
}

impl<Key: HashTableKeyable> StateEntity<Key> for KeyValueEntity<Key, usize> {
    #[inline(always)]
    fn get_state_key<'a>(self: *mut Self) -> &'a Key {
        self.get_key()
    }

    #[inline(always)]
    fn set_state_value(self: *mut Self, value: usize) {
        self.set_value(value)
    }

    #[inline(always)]
    fn get_state_value<'a>(self: *mut Self) -> &'a usize {
        self.get_value()
    }
}

impl ShortFixedKeyable for u8 {
    #[inline(always)]
    fn lookup(&self) -> isize {
        *self as isize
    }

    #[inline(always)]
    fn is_zero_key(&self) -> bool {
        *self == 0
    }
}

impl ShortFixedKeyable for u16 {
    #[inline(always)]
    fn lookup(&self) -> isize {
        *self as isize
    }

    #[inline(always)]
    fn is_zero_key(&self) -> bool {
        *self == 0
    }
}
