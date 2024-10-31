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
use databend_common_exception::Result;
use databend_common_expression::types::number::NumberScalar;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::Scalar;

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
    pub fn from_scalar(data_value: Scalar) -> Option<Self> {
        match data_value {
            Scalar::Boolean(v) => Some(Datum::Bool(v)),
            Scalar::Number(NumberScalar::Int8(v)) => Some(Datum::Int(v as i64)),
            Scalar::Number(NumberScalar::Int16(v)) => Some(Datum::Int(v as i64)),
            Scalar::Number(NumberScalar::Int32(v)) | Scalar::Date(v) => Some(Datum::Int(v as i64)),
            Scalar::Number(NumberScalar::Int64(v)) | Scalar::Timestamp(v) => Some(Datum::Int(v)),
            Scalar::Number(NumberScalar::UInt8(v)) => Some(Datum::UInt(v as u64)),
            Scalar::Number(NumberScalar::UInt16(v)) => Some(Datum::UInt(v as u64)),
            Scalar::Number(NumberScalar::UInt32(v)) => Some(Datum::UInt(v as u64)),
            Scalar::Number(NumberScalar::UInt64(v)) => Some(Datum::UInt(v)),
            Scalar::Number(NumberScalar::Float32(v)) => {
                Some(Datum::Float(F64::from(f32::from(v) as f64)))
            }
            Scalar::Decimal(v) => Some(Datum::Float(F64::from(v.to_float64()))),
            Scalar::Number(NumberScalar::Float64(v)) => Some(Datum::Float(v)),
            Scalar::Binary(v) => Some(Datum::Bytes(v)),
            Scalar::String(v) => Some(Datum::Bytes(v.as_bytes().to_vec())),
            _ => None,
        }
    }

    pub fn to_scalar(&self, data_type: &DataType) -> Result<Option<Scalar>> {
        let scalar = match self {
            Datum::Bool(v) => Some(Scalar::Boolean(*v)),
            Datum::Int(v) => match data_type {
                DataType::Number(NumberDataType::Int8) => {
                    Some(Scalar::Number(NumberScalar::Int8(*v as i8)))
                }
                DataType::Number(NumberDataType::Int16) => {
                    Some(Scalar::Number(NumberScalar::Int16(*v as i16)))
                }
                DataType::Number(NumberDataType::Int32) => {
                    Some(Scalar::Number(NumberScalar::Int32(*v as i32)))
                }
                DataType::Number(NumberDataType::Int64) => {
                    Some(Scalar::Number(NumberScalar::Int64(*v)))
                }
                DataType::Number(NumberDataType::UInt8) => {
                    if *v > 0 {
                        Some(Scalar::Number(NumberScalar::UInt8(*v as u8)))
                    } else {
                        None
                    }
                }
                DataType::Number(NumberDataType::UInt16) => {
                    if *v > 0 {
                        Some(Scalar::Number(NumberScalar::UInt16(*v as u16)))
                    } else {
                        None
                    }
                }
                DataType::Number(NumberDataType::UInt32) => {
                    if *v > 0 {
                        Some(Scalar::Number(NumberScalar::UInt32(*v as u32)))
                    } else {
                        None
                    }
                }
                DataType::Number(NumberDataType::UInt64) => {
                    if *v > 0 {
                        Some(Scalar::Number(NumberScalar::UInt64(*v as u64)))
                    } else {
                        None
                    }
                }
                _ => None,
            },
            Datum::UInt(v) => match data_type {
                DataType::Number(NumberDataType::Int8) => {
                    if *v <= i8::MAX as u64 {
                        Some(Scalar::Number(NumberScalar::Int8(*v as i8)))
                    } else {
                        None
                    }
                }
                DataType::Number(NumberDataType::Int16) => {
                    if *v <= i16::MAX as u64 {
                        Some(Scalar::Number(NumberScalar::Int16(*v as i16)))
                    } else {
                        None
                    }
                }
                DataType::Number(NumberDataType::Int32) => {
                    if *v <= i32::MAX as u64 {
                        Some(Scalar::Number(NumberScalar::Int32(*v as i32)))
                    } else {
                        None
                    }
                }
                DataType::Number(NumberDataType::Int64) => {
                    if *v <= i64::MAX as u64 {
                        Some(Scalar::Number(NumberScalar::Int64(*v as i64)))
                    } else {
                        None
                    }
                }
                DataType::Number(NumberDataType::UInt8) => {
                    Some(Scalar::Number(NumberScalar::UInt8(*v as u8)))
                }
                DataType::Number(NumberDataType::UInt16) => {
                    Some(Scalar::Number(NumberScalar::UInt16(*v as u16)))
                }
                DataType::Number(NumberDataType::UInt32) => {
                    Some(Scalar::Number(NumberScalar::UInt32(*v as u32)))
                }
                DataType::Number(NumberDataType::UInt64) => {
                    Some(Scalar::Number(NumberScalar::UInt64(*v)))
                }
                _ => None,
            },
            Datum::Float(v) => match data_type {
                DataType::Number(NumberDataType::Float32) => {
                    if v.into_inner() <= f32::MAX as f64 {
                        Some(Scalar::Number(NumberScalar::Float32(OrderedFloat::from(
                            v.into_inner() as f32,
                        ))))
                    } else {
                        None
                    }
                }
                DataType::Number(NumberDataType::Float64) => {
                    Some(Scalar::Number(NumberScalar::Float64(*v)))
                }
                _ => None,
            },
            Datum::Bytes(v) => match data_type {
                DataType::String => {
                    let s = String::from_utf8(v.clone())?;
                    Some(Scalar::String(s))
                }
                DataType::Binary => Some(Scalar::Binary(v.clone())),
                _ => None,
            },
        };

        Ok(scalar)
    }

    pub fn is_bytes(&self) -> bool {
        matches!(self, Datum::Bytes(_))
    }

    pub fn to_double(&self) -> Result<f64> {
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

    pub fn to_string(&self) -> Result<String> {
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

    pub fn sub(x: &Datum, y: &Datum) -> Option<Datum> {
        match (x, y) {
            (Datum::Int(x), Datum::Int(y)) => Some(Datum::Int(x - y)),
            (Datum::UInt(x), Datum::UInt(y)) => Some(Datum::UInt(x - y)),
            (Datum::Float(x), Datum::Float(y)) => {
                Some(Datum::Float(F64::from(x.into_inner() - y.into_inner())))
            }
            _ => None,
            // (Datum::Bytes(x), Datum::Bytes(y)) => {
            //     // There are 128 characters in ASCII code and 128^4 = 268435456 < 2^32 < 128^5.
            //     if x.is_empty() || y.is_empty() || x.len() > 4 || y.len() > 4 {
            //         return None;
            //     }
            //     let mut min_value: u32 = 0;
            //     let mut max_value: u32 = 0;
            //     while x.len() != y.len() {
            //         if y.len() < x.len() {
            //             y.push(0);
            //         } else {
            //             x.push(0);
            //         }
            //     }
            //     for idx in 0..min.len() {
            //         min_value = min_value * 128 + y[idx] as u32;
            //         max_value = max_value * 128 + x[idx] as u32;
            //     }
            // }
        }
    }

    pub fn add(&self, other: &Datum) -> Option<Datum> {
        match (self, other) {
            (Datum::Int(x), Datum::Int(y)) => Some(Datum::Int(x + y)),
            (Datum::UInt(x), Datum::UInt(y)) => Some(Datum::UInt(x + y)),
            (Datum::Float(x), Datum::Float(y)) => {
                Some(Datum::Float(F64::from(x.into_inner() + y.into_inner())))
            }
            _ => None,
        }
    }

    pub fn div(x: &Datum, y: &Datum) -> Option<Datum> {
        match (x, y) {
            (Datum::Int(x), Datum::Int(y)) => {
                if *y == 0 {
                    return None;
                }
                Some(Datum::Int(x / y))
            }
            (Datum::UInt(x), Datum::UInt(y)) => {
                if *y == 0 {
                    return None;
                }
                Some(Datum::UInt(x / y))
            }
            (Datum::Float(x), Datum::Float(y)) => {
                if y.into_inner() == 0.0 {
                    return None;
                }
                Some(Datum::Float(F64::from(x.into_inner() / y.into_inner())))
            }
            _ => None,
        }
    }

    pub fn build_range_info(
        min: Datum,
        max: Datum,
        num_segments: usize,
        data_type: &DataType,
    ) -> Result<Option<Vec<(Scalar, Scalar)>>> {
        let mut result = Vec::with_capacity(num_segments);
        let num_segments_datum = match min {
            Datum::Int(_) => Datum::Int(num_segments as i64),
            Datum::UInt(_) => Datum::UInt(num_segments as u64),
            Datum::Float(_) => Datum::Float(OrderedFloat::from(num_segments as f64)),
            _ => return Ok(None),
        };
        if let Some(range) = Self::sub(&max, &min)
            && let Some(step) = Self::div(&range, &num_segments_datum)
        {
            let mut start = min;
            for _ in 0..num_segments {
                let end = Self::add(&start, &step).unwrap();
                if let Some(start) = start.to_scalar(data_type)?
                    && let Some(end) = end.to_scalar(data_type)?
                {
                    result.push((start, end));
                } else {
                    return Ok(None);
                }
                start = end;
            }
        }
        Ok(Some(result))
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
        matches!(
            (self, other),
            (Datum::Bool(_), Datum::Bool(_))
                | (Datum::Bytes(_), Datum::Bytes(_))
                | (Datum::Int(_), Datum::UInt(_))
                | (Datum::Int(_), Datum::Int(_))
                | (Datum::Int(_), Datum::Float(_))
                | (Datum::UInt(_), Datum::Int(_))
                | (Datum::UInt(_), Datum::UInt(_))
                | (Datum::UInt(_), Datum::Float(_))
                | (Datum::Float(_), Datum::Float(_))
                | (Datum::Float(_), Datum::Int(_))
                | (Datum::Float(_), Datum::UInt(_))
        )
    }

    pub fn is_numeric(&self) -> bool {
        matches!(self, Datum::Int(_) | Datum::UInt(_) | Datum::Float(_))
    }

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
