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

#[derive(
    serde::Serialize, serde::Deserialize, Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord,
)]
pub enum TypeID {
    Null,
    Nullable,
    Boolean,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Int8,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,

    String,

    /// A 32-bit date representing the elapsed time since UNIX epoch (1970-01-01)
    /// in days (32 bits), it's physical type is Int32
    Date,

    /// A 64-bit timestamp representing the elapsed time since UNIX epoch (1970-01-01)
    /// in microseconds, it's physical type is Int64
    /// store UTC timestamp
    Timestamp,

    /// Interval represents the time interval, e.g. the elapsed time between two date or timestamp.
    /// Underneath Interval is stored as int64, so it supports negative values.
    Interval,

    Array,
    Struct,

    /// Variant is a tagged universal type, which can store values of any other type,
    /// including Object and Array, up to a maximum size of 16 MB.
    Variant,
    VariantArray,
    VariantObject,
}

impl TypeID {
    #[inline]
    pub fn null_at(&self) -> bool {
        matches!(self, TypeID::Nullable)
    }
}

impl std::fmt::Display for TypeID {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            TypeID::VariantArray => write!(f, "Array"),
            TypeID::VariantObject => write!(f, "Object"),
            _ => write!(f, "{:?}", self),
        }
    }
}
