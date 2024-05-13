// Copyright [2021] [Jorge C Leitao]
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

#[cfg(feature = "serde_types")]
use serde::Deserialize;
#[cfg(feature = "serde_types")]
use serde::Serialize;

use crate::schema::types::ParquetType;
use crate::schema::types::PrimitiveType;

/// A descriptor of a parquet column. It contains the necessary information to deserialize
/// a parquet column.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde_types", derive(Deserialize, Serialize))]
pub struct Descriptor {
    /// The [`PrimitiveType`] of this column
    pub primitive_type: PrimitiveType,

    /// The maximum definition level
    pub max_def_level: i16,

    /// The maximum repetition level
    pub max_rep_level: i16,
}

/// A descriptor for leaf-level primitive columns.
/// This encapsulates information such as definition and repetition levels and is used to
/// re-assemble nested data.
#[derive(Debug, PartialEq, Clone)]
#[cfg_attr(feature = "serde_types", derive(Deserialize, Serialize))]
pub struct ColumnDescriptor {
    /// The descriptor this columns' leaf.
    pub descriptor: Descriptor,

    /// The path of this column. For instance, "a.b.c.d".
    pub path_in_schema: Vec<String>,

    /// The [`ParquetType`] this descriptor is a leaf of
    pub base_type: ParquetType,
}

impl ColumnDescriptor {
    /// Creates new descriptor for leaf-level column.
    pub fn new(
        descriptor: Descriptor,
        path_in_schema: Vec<String>,
        base_type: ParquetType,
    ) -> Self {
        Self {
            descriptor,
            path_in_schema,
            base_type,
        }
    }
}
