// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Map collection type for Iceberg

use std::collections::HashMap;
use std::hash::Hash;

use super::Literal;

/// Map is a collection of key-value pairs with a key type and a value type.
/// It used in Literal::Map, to make it hashable, the order of key-value pairs is stored in a separate vector
/// so that we can hash the map in a deterministic way. But it also means that the order of key-value pairs is matter
/// for the hash value.
///
/// When converting to Arrow (e.g., for Iceberg), the map should be represented using
/// the default field name "key_value".
///
/// Example:
///
/// ```text
/// let key_value_field = Field::new(
///     DEFAULT_MAP_FIELD_NAME,
///     arrow_schema::DataType::Struct(vec![
///         Arc::new(key_field.clone()),
///         Arc::new(value_field.clone()),
///     ].into()),
///     false
/// );
/// '''
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Map {
    index: HashMap<Literal, usize>,
    pair: Vec<(Literal, Option<Literal>)>,
}

impl Map {
    /// Creates a new empty map.
    pub fn new() -> Self {
        Self {
            index: HashMap::new(),
            pair: Vec::new(),
        }
    }

    /// Return the number of key-value pairs in the map.
    pub fn len(&self) -> usize {
        self.pair.len()
    }

    /// Returns true if the map contains no elements.
    pub fn is_empty(&self) -> bool {
        self.pair.is_empty()
    }

    /// Inserts a key-value pair into the map.
    /// If the map did not have this key present, None is returned.
    /// If the map did have this key present, the value is updated, and the old value is returned.
    pub fn insert(&mut self, key: Literal, value: Option<Literal>) -> Option<Option<Literal>> {
        if let Some(index) = self.index.get(&key) {
            let old_value = std::mem::replace(&mut self.pair[*index].1, value);
            Some(old_value)
        } else {
            self.pair.push((key.clone(), value));
            self.index.insert(key, self.pair.len() - 1);
            None
        }
    }

    /// Returns a reference to the value corresponding to the key.
    /// If the key is not present in the map, None is returned.
    pub fn get(&self, key: &Literal) -> Option<&Option<Literal>> {
        self.index.get(key).map(|index| &self.pair[*index].1)
    }

    /// The order of map is matter, so this method used to compare two maps has same key-value pairs without considering the order.
    pub fn has_same_content(&self, other: &Map) -> bool {
        if self.len() != other.len() {
            return false;
        }

        for (key, value) in &self.pair {
            match other.get(key) {
                Some(other_value) if value == other_value => (),
                _ => return false,
            }
        }

        true
    }
}

impl Default for Map {
    fn default() -> Self {
        Self::new()
    }
}

impl Hash for Map {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        for (key, value) in &self.pair {
            key.hash(state);
            value.hash(state);
        }
    }
}

impl FromIterator<(Literal, Option<Literal>)> for Map {
    fn from_iter<T: IntoIterator<Item = (Literal, Option<Literal>)>>(iter: T) -> Self {
        let mut map = Map::new();
        for (key, value) in iter {
            map.insert(key, value);
        }
        map
    }
}

impl IntoIterator for Map {
    type Item = (Literal, Option<Literal>);
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.pair.into_iter()
    }
}

impl<const N: usize> From<[(Literal, Option<Literal>); N]> for Map {
    fn from(value: [(Literal, Option<Literal>); N]) -> Self {
        value.iter().cloned().collect()
    }
}
