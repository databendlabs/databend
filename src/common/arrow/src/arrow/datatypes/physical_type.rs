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

#[cfg(feature = "serde_types")]
use serde_derive::Deserialize;
#[cfg(feature = "serde_types")]
use serde_derive::Serialize;

pub use crate::arrow::types::PrimitiveType;

/// The set of physical types: unique in-memory representations of an Arrow array.
/// A physical type has a one-to-many relationship with a [`crate::datatypes::DataType`] and
/// a one-to-one mapping to each struct in this crate that implements [`crate::array::Array`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde_types", derive(Serialize, Deserialize))]
pub enum PhysicalType {
    /// A Null with no allocation.
    Null,
    /// A boolean represented as a single bit.
    Boolean,
    /// An array where each slot has a known compile-time size.
    Primitive(PrimitiveType),
    /// Opaque binary data of variable length.
    Binary,
    /// Opaque binary data of fixed size.
    FixedSizeBinary,
    /// Opaque binary data of variable length and 64-bit offsets.
    LargeBinary,
    /// A variable-length string in Unicode with UTF-8 encoding.
    Utf8,
    /// A variable-length string in Unicode with UFT-8 encoding and 64-bit offsets.
    LargeUtf8,
    /// A list of some data type with variable length.
    List,
    /// A list of some data type with fixed length.
    FixedSizeList,
    /// A list of some data type with variable length and 64-bit offsets.
    LargeList,
    /// A nested type that contains an arbitrary number of fields.
    Struct,
    /// A nested type that represents slots of differing types.
    Union,
    /// A nested type.
    Map,
    /// A dictionary encoded array by `IntegerType`.
    Dictionary(IntegerType),
}

impl PhysicalType {
    /// Whether this physical type equals [`PhysicalType::Primitive`] of type `primitive`.
    pub fn eq_primitive(&self, primitive: PrimitiveType) -> bool {
        if let Self::Primitive(o) = self {
            o == &primitive
        } else {
            false
        }
    }
}

/// the set of valid indices types of a dictionary-encoded Array.
/// Each type corresponds to a variant of [`crate::array::DictionaryArray`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde_types", derive(Serialize, Deserialize))]
pub enum IntegerType {
    /// A signed 8-bit integer.
    Int8,
    /// A signed 16-bit integer.
    Int16,
    /// A signed 32-bit integer.
    Int32,
    /// A signed 64-bit integer.
    Int64,
    /// An unsigned 8-bit integer.
    UInt8,
    /// An unsigned 16-bit integer.
    UInt16,
    /// An unsigned 32-bit integer.
    UInt32,
    /// An unsigned 64-bit integer.
    UInt64,
}
