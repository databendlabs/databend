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

//! Struct type for Iceberg values

use std::ops::Index;

use super::Literal;

/// The partition struct stores the tuple of partition values for each file.
/// Its type is derived from the partition fields of the partition spec used to write the manifest file.
/// In v2, the partition struct's field ids must match the ids from the partition spec.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Struct {
    /// Vector to store the field values
    fields: Vec<Option<Literal>>,
}

impl Struct {
    /// Create a empty struct.
    pub fn empty() -> Self {
        Self { fields: Vec::new() }
    }

    /// Create a iterator to read the field in order of field_value.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = Option<&Literal>> {
        self.fields.iter().map(|field| field.as_ref())
    }

    /// returns true if the field at position `index` is null
    pub fn is_null_at_index(&self, index: usize) -> bool {
        self.fields[index].is_none()
    }

    /// Return fields in the struct.
    pub fn fields(&self) -> &[Option<Literal>] {
        &self.fields
    }
}

impl Index<usize> for Struct {
    type Output = Option<Literal>;

    fn index(&self, idx: usize) -> &Self::Output {
        &self.fields[idx]
    }
}

impl IntoIterator for Struct {
    type Item = Option<Literal>;

    type IntoIter = std::vec::IntoIter<Option<Literal>>;

    fn into_iter(self) -> Self::IntoIter {
        self.fields.into_iter()
    }
}

impl FromIterator<Option<Literal>> for Struct {
    fn from_iter<I: IntoIterator<Item = Option<Literal>>>(iter: I) -> Self {
        Struct {
            fields: iter.into_iter().collect(),
        }
    }
}
