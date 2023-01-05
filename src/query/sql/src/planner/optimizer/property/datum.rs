// Copyright 2022 Datafuse Labs.
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

use std::fmt::Display;
use std::fmt::Formatter;

use common_datavalues::DataValue;
use common_exception::ErrorCode;
use common_exception::Result;
use ordered_float::OrderedFloat;

pub type F64 = OrderedFloat<f64>;

/// Datum is the struct to represent a single value in optimizer.
#[derive(Debug, Clone)]
pub enum Datum {
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(F64),
    Bytes(Vec<u8>),
}

impl Datum {
    pub fn from_data_value(data_value: &DataValue) -> Option<Self> {
        match data_value {
            DataValue::Boolean(v) => Some(Datum::Bool(*v)),
            DataValue::Int64(v) => Some(Datum::Int(*v)),
            DataValue::UInt64(v) => Some(Datum::UInt(*v)),
            DataValue::Float64(v) => Some(Datum::Float(F64::from(*v))),
            DataValue::String(v) => Some(Datum::Bytes(v.clone())),
            _ => None,
        }
    }
}

impl Display for Datum {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Datum::Bool(v) => write!(f, "{}", v),
            Datum::Int(v) => write!(f, "{}", v),
            Datum::UInt(v) => write!(f, "{}", v),
            Datum::Float(v) => write!(f, "{}", v),
            Datum::Bytes(v) => {
                let s = String::from_utf8_lossy(v);
                write!(f, "{}", s)
            }
        }
    }
}

impl Datum {
    pub fn compare(&self, other: &Self) -> Result<std::cmp::Ordering> {
        match (self, other) {
            (Datum::Bool(l), Datum::Bool(r)) => Ok(l.cmp(r)),

            (Datum::Int(l), Datum::Int(r)) => Ok(l.cmp(r)),
            (Datum::Int(_), Datum::UInt(r)) => {
                Ok(Datum::Int(i64::try_from(*r)?).compare(self)?.reverse())
            }
            (Datum::Int(l), Datum::Float(_)) => Datum::Float(F64::from(*l as f64)).compare(other),

            (Datum::UInt(l), Datum::UInt(r)) => Ok(l.cmp(r)),
            (Datum::UInt(_), Datum::Int(_)) => Ok(other.compare(self)?.reverse()),
            (Datum::UInt(l), Datum::Float(_)) => Datum::Float(F64::from(*l as f64)).compare(other),

            (Datum::Float(l), Datum::Float(r)) => Ok(l.cmp(r)),
            (Datum::Float(_), Datum::Int(_)) => Ok(other.compare(self)?.reverse()),
            (Datum::Float(_), Datum::UInt(_)) => Ok(other.compare(self)?.reverse()),

            (Datum::Bytes(l), Datum::Bytes(r)) => Ok(l.cmp(r)),

            _ => Err(ErrorCode::Internal(format!(
                "Cannot compare between different kinds of datum: {:?}, {:?}",
                self, other
            ))),
        }
    }
}
