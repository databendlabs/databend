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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatumKind {
    Int,
    UInt,
    Float,
    Bytes,
}

#[derive(
    serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash,
)]
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

    pub fn kind(&self) -> Option<DatumKind> {
        match self {
            Datum::Int(_) => Some(DatumKind::Int),
            Datum::UInt(_) => Some(DatumKind::UInt),
            Datum::Float(_) => Some(DatumKind::Float),
            Datum::Bytes(_) => Some(DatumKind::Bytes),
            Datum::Bool(_) => None,
        }
    }

    pub fn normalize_to_kind(&self, kind: DatumKind) -> Option<Datum> {
        match kind {
            DatumKind::Int => self.as_i64().map(Datum::Int),
            DatumKind::UInt => self.as_u64().map(Datum::UInt),
            DatumKind::Float => self.as_double().ok().map(F64::from).map(Datum::Float),
            DatumKind::Bytes => match self {
                Datum::Bytes(value) => Some(Datum::Bytes(value.clone())),
                _ => None,
            },
        }
    }

    pub fn lower_bound_to_kind(&self, kind: DatumKind) -> Option<Datum> {
        match kind {
            DatumKind::Int => self.lower_bound_as_i64().map(Datum::Int),
            DatumKind::UInt => self.lower_bound_as_u64().map(Datum::UInt),
            _ => self.normalize_to_kind(kind),
        }
    }

    pub fn upper_bound_to_kind(&self, kind: DatumKind) -> Option<Datum> {
        match kind {
            DatumKind::Int => self.upper_bound_as_i64().map(Datum::Int),
            DatumKind::UInt => self.upper_bound_as_u64().map(Datum::UInt),
            _ => self.normalize_to_kind(kind),
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Datum::Int(value) => Some(*value),
            Datum::UInt(value) => i64::try_from(*value).ok(),
            Datum::Float(value) => {
                let value = value.into_inner();
                (value.is_finite()
                    && value.fract() == 0.0
                    && value >= i64::MIN as f64
                    && value <= i64::MAX as f64)
                    .then_some(value as i64)
            }
            _ => None,
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Datum::UInt(value) => Some(*value),
            Datum::Int(value) => u64::try_from(*value).ok(),
            Datum::Float(value) => {
                let value = value.into_inner();
                (value.is_finite()
                    && value.fract() == 0.0
                    && value >= 0.0
                    && value <= u64::MAX as f64)
                    .then_some(value as u64)
            }
            _ => None,
        }
    }

    fn lower_bound_as_i64(&self) -> Option<i64> {
        match self {
            Datum::Float(value) => float_to_i64_bound(value.into_inner(), f64::ceil),
            _ => self.as_i64(),
        }
    }

    fn upper_bound_as_i64(&self) -> Option<i64> {
        match self {
            Datum::Float(value) => float_to_i64_bound(value.into_inner(), f64::floor),
            _ => self.as_i64(),
        }
    }

    fn lower_bound_as_u64(&self) -> Option<u64> {
        match self {
            Datum::Float(value) => float_to_u64_bound(value.into_inner(), f64::ceil),
            _ => self.as_u64(),
        }
    }

    fn upper_bound_as_u64(&self) -> Option<u64> {
        match self {
            Datum::Float(value) => float_to_u64_bound(value.into_inner(), f64::floor),
            _ => self.as_u64(),
        }
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

fn float_to_i64_bound(value: f64, round: fn(f64) -> f64) -> Option<i64> {
    let value = round(value);
    (value.is_finite() && value >= i64::MIN as f64 && value <= i64::MAX as f64)
        .then_some(value as i64)
}

fn float_to_u64_bound(value: f64, round: fn(f64) -> f64) -> Option<u64> {
    let value = round(value);
    (value.is_finite() && value >= 0.0 && value <= u64::MAX as f64).then_some(value as u64)
}
