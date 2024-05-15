// Copyright 2020-2022 Jorge C. Leitão
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

use std::borrow::Borrow;
use std::fmt::Debug;
use std::fmt::{self};
use std::hash::BuildHasher;
use std::hash::BuildHasherDefault;
use std::hash::Hash;
use std::hash::Hasher;

use hashbrown::hash_map::RawEntryMut;
use hashbrown::HashMap;

use super::DictionaryKey;
use crate::arrow::array::indexable::AsIndexed;
use crate::arrow::array::indexable::Indexable;
use crate::arrow::array::Array;
use crate::arrow::array::MutableArray;
use crate::arrow::datatypes::DataType;
use crate::arrow::error::Error;
use crate::arrow::error::Result;

/// Hasher for pre-hashed values; similar to `hash_hasher` but with native endianness.
///
/// We know that we'll only use it for `u64` values, so we can avoid endian conversion.
///
/// Invariant: hash of a u64 value is always equal to itself.
#[derive(Copy, Clone, Default)]
pub struct PassthroughHasher(u64);

impl Hasher for PassthroughHasher {
    #[inline]
    fn write_u64(&mut self, value: u64) {
        self.0 = value;
    }

    fn write(&mut self, _: &[u8]) {
        unreachable!();
    }

    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }
}

#[derive(Clone)]
pub struct Hashed<K> {
    hash: u64,
    key: K,
}

#[inline]
fn ahash_hash<T: Hash + ?Sized>(value: &T) -> u64 {
    BuildHasherDefault::<ahash::AHasher>::default().hash_one(value)
}

impl<K> Hash for Hashed<K> {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.hash.hash(state)
    }
}

#[derive(Clone)]
pub struct ValueMap<K: DictionaryKey, M: MutableArray> {
    pub values: M,
    pub map: HashMap<Hashed<K>, (), BuildHasherDefault<PassthroughHasher>>, /* NB: *only* use insert_hashed_nocheck() and no other hashmap API */
}

impl<K: DictionaryKey, M: MutableArray> ValueMap<K, M> {
    pub fn try_empty(values: M) -> Result<Self> {
        if !values.is_empty() {
            return Err(Error::InvalidArgumentError(
                "initializing value map with non-empty values array".into(),
            ));
        }
        Ok(Self {
            values,
            map: HashMap::default(),
        })
    }

    pub fn from_values(values: M) -> Result<Self>
    where
        M: Indexable,
        M::Type: Eq + Hash,
    {
        let mut map = HashMap::<Hashed<K>, _, _>::with_capacity_and_hasher(
            values.len(),
            BuildHasherDefault::<PassthroughHasher>::default(),
        );
        for index in 0..values.len() {
            let key = K::try_from(index).map_err(|_| Error::Overflow)?;
            // safety: we only iterate within bounds
            let value = unsafe { values.value_unchecked_at(index) };
            let hash = ahash_hash(value.borrow());
            let entry = map.raw_entry_mut().from_hash(hash, |item| {
                // safety: invariant of the struct, it's always in bounds since we maintain it
                let stored_value = unsafe { values.value_unchecked_at(item.key.as_usize()) };
                stored_value.borrow() == value.borrow()
            });
            match entry {
                RawEntryMut::Occupied(_) => {
                    return Err(Error::InvalidArgumentError(
                        "duplicate value in dictionary values array".into(),
                    ));
                }
                RawEntryMut::Vacant(entry) => {
                    // NB: don't use .insert() here!
                    entry.insert_hashed_nocheck(hash, Hashed { hash, key }, ());
                }
            }
        }
        Ok(Self { values, map })
    }

    pub fn data_type(&self) -> &DataType {
        self.values.data_type()
    }

    pub fn into_values(self) -> M {
        self.values
    }

    pub fn take_into(&mut self) -> Box<dyn Array> {
        let arr = self.values.as_box();
        self.map.clear();
        arr
    }

    #[inline]
    pub fn values(&self) -> &M {
        &self.values
    }

    /// Try to insert a value and return its index (it may or may not get inserted).
    pub fn try_push_valid<V>(
        &mut self,
        value: V,
        mut push: impl FnMut(&mut M, V) -> Result<()>,
    ) -> Result<K>
    where
        M: Indexable,
        V: AsIndexed<M>,
        M::Type: Eq + Hash,
    {
        let hash = ahash_hash(value.as_indexed());
        let entry = self.map.raw_entry_mut().from_hash(hash, |item| {
            // safety: we've already checked (the inverse) when we pushed it, so it should be ok?
            let index = unsafe { item.key.as_usize() };
            // safety: invariant of the struct, it's always in bounds since we maintain it
            let stored_value = unsafe { self.values.value_unchecked_at(index) };
            stored_value.borrow() == value.as_indexed()
        });
        let result = match entry {
            RawEntryMut::Occupied(entry) => entry.key().key,
            RawEntryMut::Vacant(entry) => {
                let index = self.values.len();
                let key = K::try_from(index).map_err(|_| Error::Overflow)?;
                entry.insert_hashed_nocheck(hash, Hashed { hash, key }, ()); // NB: don't use .insert() here!
                push(&mut self.values, value)?;
                debug_assert_eq!(self.values.len(), index + 1);
                key
            }
        };
        Ok(result)
    }

    pub fn shrink_to_fit(&mut self) {
        self.values.shrink_to_fit();
    }
}

impl<K: DictionaryKey, M: MutableArray> Debug for ValueMap<K, M> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.values.fmt(f)
    }
}
