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

// Borrow from apache/arrow/rust/datafusion/src/functions.rs
// See notice.md

use std::convert::From;
use std::convert::TryFrom;

use common_exception::ErrorCode;
use common_exception::Result;
use ordered_float::OrderedFloat;
use serde::Deserialize;
use serde::Serialize;

use crate::DataValue;

/// Enumeration of types that can be used in a GROUP BY expression
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Clone)]
pub enum DataGroupValue {
    #[serde(with = "OrderedFloatDef")]
    Float64(OrderedFloat<f64>),
    UInt64(u64),
    Int64(i64),
    String(Vec<u8>),
    Boolean(bool),
}

impl TryFrom<&DataValue> for DataGroupValue {
    type Error = ErrorCode;

    fn try_from(value: &DataValue) -> Result<Self> {
        Ok(match value {
            DataValue::Float64(v) => DataGroupValue::Float64(OrderedFloat::from(*v)),
            DataValue::Boolean(v) => DataGroupValue::Boolean(*v),
            DataValue::Int64(v) => DataGroupValue::Int64(*v),
            DataValue::UInt64(v) => DataGroupValue::UInt64(*v),
            DataValue::String(v) => DataGroupValue::String(v.clone()),

            v => {
                return Err(ErrorCode::BadDataValueType(format!(
                    "Cannot convert a DataValue ({:?}) into DataGroupValue",
                    v.data_type()
                )));
            }
        })
    }
}

impl From<&DataGroupValue> for DataValue {
    fn from(group_by_scalar: &DataGroupValue) -> Self {
        match group_by_scalar {
            DataGroupValue::Float64(v) => DataValue::Float64((*v).into()),
            DataGroupValue::Boolean(v) => DataValue::Boolean(*v),
            DataGroupValue::Int64(v) => DataValue::Int64(*v),
            DataGroupValue::UInt64(v) => DataValue::UInt64(*v),
            DataGroupValue::String(v) => DataValue::String(v.to_vec()),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "OrderedFloat")]
struct OrderedFloatDef<T>(pub T);

impl From<OrderedFloatDef<f64>> for OrderedFloat<f64> {
    fn from(def: OrderedFloatDef<f64>) -> Self {
        OrderedFloat::from(def.0)
    }
}
