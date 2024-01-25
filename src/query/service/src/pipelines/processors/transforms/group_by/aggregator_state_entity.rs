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

use databend_common_hashtable::HashtableEntry;
use databend_common_hashtable::HashtableKeyable;
use databend_common_hashtable::ShortStringHashtableEntryMutRef;
use databend_common_hashtable::ShortStringHashtableEntryRef;

pub trait StateEntityRef {
    type KeyRef: Copy;

    fn get_state_key(&self) -> Self::KeyRef;
    fn get_state_value(&self) -> usize;
}

pub trait StateEntityMutRef {
    type KeyRef: Copy;

    fn get_state_key(&self) -> Self::KeyRef;
    fn get_state_value(&self) -> usize;
    fn set_state_value(&mut self, value: usize);
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

impl<'a, Key: ShortFixedKeyable + Copy> StateEntityRef for &'a ShortFixedKeysStateEntity<Key> {
    type KeyRef = Key;

    #[inline(always)]
    fn get_state_key(&self) -> Key {
        self.key
    }

    #[inline(always)]
    fn get_state_value(&self) -> usize {
        self.value
    }
}

impl<'a, Key: ShortFixedKeyable + Copy> StateEntityMutRef
    for &'a mut ShortFixedKeysStateEntity<Key>
{
    type KeyRef = Key;

    #[inline(always)]
    fn get_state_key(&self) -> Key {
        self.key
    }

    #[inline(always)]
    fn get_state_value(&self) -> usize {
        self.value
    }

    #[inline(always)]
    fn set_state_value(&mut self, value: usize) {
        self.value = value;
    }
}

impl<'a, Key: HashtableKeyable> StateEntityRef for &'a HashtableEntry<Key, usize> {
    type KeyRef = Key;

    #[inline(always)]
    fn get_state_key(&self) -> Key {
        *self.key()
    }

    #[inline(always)]
    fn get_state_value(&self) -> usize {
        *self.get()
    }
}

impl<'a, Key: HashtableKeyable> StateEntityMutRef for &'a mut HashtableEntry<Key, usize> {
    type KeyRef = Key;

    #[inline(always)]
    fn get_state_key(&self) -> Key {
        *self.key()
    }

    #[inline(always)]
    fn get_state_value(&self) -> usize {
        *self.get()
    }

    #[inline(always)]
    fn set_state_value(&mut self, value: usize) {
        *self.get_mut() = value;
    }
}

impl<'a> StateEntityRef for ShortStringHashtableEntryRef<'a, [u8], usize> {
    type KeyRef = &'a [u8];

    #[inline(always)]
    fn get_state_key(&self) -> &'a [u8] {
        self.key()
    }

    #[inline(always)]
    fn get_state_value(&self) -> usize {
        *self.get()
    }
}

impl<'a> StateEntityMutRef for ShortStringHashtableEntryMutRef<'a, [u8], usize> {
    type KeyRef = &'a [u8];

    #[inline(always)]
    fn get_state_key(&self) -> &'a [u8] {
        self.key()
    }

    #[inline(always)]
    fn get_state_value(&self) -> usize {
        *self.get()
    }

    #[inline(always)]
    fn set_state_value(&mut self, value: usize) {
        *self.get_mut() = value;
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
