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

use std::fmt::Display;
use std::fmt::Formatter;

use databend_common_base::base::OrderedFloat;
use databend_common_exception::ErrorCode;

pub type F64 = OrderedFloat<f64>;

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Datum {
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(F64),
    Bytes(Vec<u8>),
}

impl Datum {
    pub fn is_bytes(&self) -> bool {
        matches!(self, Datum::Bytes(_))
    }

    pub fn cast_float(self) -> Self {
        match self {
            Datum::Int(v) => Datum::Float(F64::from(v as f64)),
            Datum::UInt(v) => Datum::Float(F64::from(v as f64)),
            _ => self,
        }
    }

    pub fn as_double(&self) -> Result<f64, ErrorCode> {
        match self {
            Datum::Bool(v) => Ok(*v as u8 as f64),
            Datum::Int(v) => Ok(*v as f64),
            Datum::UInt(v) => Ok(*v as f64),
            Datum::Float(v) => Ok(v.into_inner()),
            _ => Err(ErrorCode::IllegalDataType(format!(
                "Cannot convert {:?} to double",
                self
            ))),
        }
    }

    pub fn to_string(&self) -> Result<String, ErrorCode> {
        match self {
            Datum::Bool(v) => Ok(v.to_string()),
            Datum::Int(v) => Ok(v.to_string()),
            Datum::UInt(v) => Ok(v.to_string()),
            Datum::Float(v) => Ok(v.to_string()),
            Datum::Bytes(v) => Ok(String::from_utf8_lossy(v).to_string()),
        }
    }

    pub fn max(a: Option<Datum>, b: Option<Datum>) -> Option<Datum> {
        match (a, b) {
            (Some(x), Some(y)) if Self::type_comparable(&x, &y) => {
                if Self::compare(&x, &y).unwrap() == std::cmp::Ordering::Greater {
                    Some(x)
                } else {
                    Some(y)
                }
            }
            (Some(x), None) | (None, Some(x)) => Some(x),
            _ => None,
        }
    }

    pub fn min(a: Option<Datum>, b: Option<Datum>) -> Option<Datum> {
        match (a, b) {
            (Some(x), Some(y)) if Self::type_comparable(&x, &y) => {
                if Self::compare(&x, &y).unwrap() == std::cmp::Ordering::Less {
                    Some(x)
                } else {
                    Some(y)
                }
            }
            (Some(x), None) | (None, Some(x)) => Some(x),
            _ => None,
        }
    }
}

impl Display for Datum {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
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
    pub fn type_comparable(&self, other: &Datum) -> bool {
        self.is_numeric() && other.is_numeric()
            || matches!(
                (self, other),
                (Datum::Bool(_), Datum::Bool(_)) | (Datum::Bytes(_), Datum::Bytes(_))
            )
    }

    pub fn is_numeric(&self) -> bool {
        matches!(self, Datum::Int(_) | Datum::UInt(_) | Datum::Float(_))
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            Datum::Bool(_) => "Boolean",
            Datum::Int(_) => "Integer",
            Datum::UInt(_) => "Unsigned Integer",
            Datum::Float(_) => "Float",
            Datum::Bytes(_) => "String",
        }
    }

    pub fn compare(&self, other: &Self) -> Result<std::cmp::Ordering, ErrorCode> {
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
