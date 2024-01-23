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

use std::sync::Arc;

use enum_as_inner::EnumAsInner;

use crate::VariantValue;

/// A specific value of a data type.
#[derive(serde::Serialize, Debug, serde::Deserialize, Clone, PartialEq, EnumAsInner)]
pub enum DataValue {
    /// Base type.
    Null,
    Boolean(bool),
    Int64(i64),
    UInt64(u64),
    Float64(f64),
    String(Vec<u8>),

    // Container struct.
    Array(Vec<DataValue>),
    Struct(Vec<DataValue>),

    // Custom type.
    Variant(VariantValue),
    Geometry(Vec<u8>),
}

impl Eq for DataValue {}

pub type DataValueRef = Arc<DataValue>;

#[allow(clippy::derived_hash_with_manual_eq)]
impl std::hash::Hash for DataValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            DataValue::Null => {}
            DataValue::Boolean(v) => v.hash(state),
            DataValue::UInt64(v) => v.hash(state),
            DataValue::Int64(v) => v.hash(state),
            DataValue::Float64(v) => v.to_bits().hash(state),
            DataValue::String(v) => v.hash(state),
            DataValue::Array(v) => v.hash(state),
            DataValue::Struct(v) => v.hash(state),
            DataValue::Variant(v) => v.hash(state),
            DataValue::Geometry(v) => v.hash(state),
        }
    }
}
