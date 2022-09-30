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

use std::cmp::Ordering;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::io::Write;

use ordered_float::OrderedFloat;

use super::constants::*;
use super::error::Error;

#[derive(Clone, Debug)]
pub enum Number {
    Int64(i64),
    UInt64(u64),
    Float64(f64),
}

impl Number {
    #[inline]
    pub fn compact_encode<W: Write>(&self, mut writer: W) -> Result<usize, Error> {
        match self {
            Self::Int64(v) => {
                if *v == 0 {
                    writer.write_all(&[NUMBER_ZERO])?;
                    return Ok(1);
                }
                writer.write_all(&[NUMBER_INT])?;
                if *v >= i8::MIN.into() && *v <= i8::MAX.into() {
                    writer.write_all(&(*v as i8).to_be_bytes())?;
                    Ok(2)
                } else if *v >= i16::MIN.into() && *v <= i16::MAX.into() {
                    writer.write_all(&(*v as i16).to_be_bytes())?;
                    Ok(3)
                } else if *v >= i32::MIN.into() && *v <= i32::MAX.into() {
                    writer.write_all(&(*v as i32).to_be_bytes())?;
                    Ok(5)
                } else {
                    writer.write_all(&v.to_be_bytes())?;
                    Ok(9)
                }
            }
            Self::UInt64(v) => {
                if *v == 0 {
                    writer.write_all(&[NUMBER_ZERO])?;
                    return Ok(1);
                }
                writer.write_all(&[NUMBER_UINT])?;
                if *v <= u8::MAX.into() {
                    writer.write_all(&(*v as u8).to_be_bytes())?;
                    Ok(2)
                } else if *v <= u16::MAX.into() {
                    writer.write_all(&(*v as u16).to_be_bytes())?;
                    Ok(3)
                } else if *v <= u32::MAX.into() {
                    writer.write_all(&(*v as u32).to_be_bytes())?;
                    Ok(5)
                } else {
                    writer.write_all(&v.to_be_bytes())?;
                    Ok(9)
                }
            }
            Self::Float64(v) => {
                if v.is_nan() {
                    writer.write_all(&[NUMBER_NAN])?;
                    return Ok(1);
                } else if v.is_infinite() {
                    if v.is_sign_negative() {
                        writer.write_all(&[NUMBER_NEG_INF])?;
                    } else {
                        writer.write_all(&[NUMBER_INF])?;
                    }
                    return Ok(1);
                }
                writer.write_all(&[NUMBER_FLOAT])?;
                writer.write_all(&v.to_be_bytes())?;
                Ok(9)
            }
        }
    }

    #[inline]
    pub fn decode(bytes: &[u8]) -> Number {
        let mut len = bytes.len();
        assert!(len > 0);
        len -= 1;

        let ty = bytes[0];
        match ty {
            NUMBER_ZERO => Number::UInt64(0),
            NUMBER_NAN => Number::Float64(f64::NAN),
            NUMBER_INF => Number::Float64(f64::INFINITY),
            NUMBER_NEG_INF => Number::Float64(f64::NEG_INFINITY),
            NUMBER_INT => match len {
                1 => Number::Int64(i8::from_be_bytes(bytes[1..].try_into().unwrap()) as i64),
                2 => Number::Int64(i16::from_be_bytes(bytes[1..].try_into().unwrap()) as i64),
                4 => Number::Int64(i32::from_be_bytes(bytes[1..].try_into().unwrap()) as i64),
                8 => Number::Int64(i64::from_be_bytes(bytes[1..].try_into().unwrap())),
                _ => unreachable!(),
            },
            NUMBER_UINT => match len {
                1 => Number::UInt64(u8::from_be_bytes(bytes[1..].try_into().unwrap()) as u64),
                2 => Number::UInt64(u16::from_be_bytes(bytes[1..].try_into().unwrap()) as u64),
                4 => Number::UInt64(u32::from_be_bytes(bytes[1..].try_into().unwrap()) as u64),
                8 => Number::UInt64(u64::from_be_bytes(bytes[1..].try_into().unwrap())),
                _ => unreachable!(),
            },
            NUMBER_FLOAT => Number::Float64(f64::from_be_bytes(bytes[1..].try_into().unwrap())),
            _ => unreachable!(),
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Number::Int64(v) => Some(*v),
            Number::UInt64(v) => {
                if *v <= i64::MAX.try_into().unwrap() {
                    Some(*v as i64)
                } else {
                    None
                }
            }
            Number::Float64(_) => None,
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Number::Int64(v) => {
                if *v >= 0 {
                    Some(*v as u64)
                } else {
                    None
                }
            }
            Number::UInt64(v) => Some(*v),
            Number::Float64(_) => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Number::Int64(v) => Some(*v as f64),
            Number::UInt64(v) => Some(*v as f64),
            Number::Float64(v) => Some(*v),
        }
    }
}

impl Default for Number {
    #[inline]
    fn default() -> Self {
        Number::UInt64(0)
    }
}

impl PartialEq for Number {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl PartialEq<&Number> for Number {
    #[inline]
    fn eq(&self, other: &&Number) -> bool {
        self.eq(*other)
    }
}

impl PartialEq<Number> for &Number {
    #[inline]
    fn eq(&self, other: &Number) -> bool {
        (*self).eq(other)
    }
}

impl Eq for Number {}

impl PartialOrd for Number {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialOrd<&Number> for Number {
    #[inline]
    fn partial_cmp(&self, other: &&Number) -> Option<Ordering> {
        self.partial_cmp(*other)
    }
}

impl PartialOrd<Number> for &Number {
    #[inline]
    fn partial_cmp(&self, other: &Number) -> Option<Ordering> {
        (*self).partial_cmp(other)
    }
}

impl Ord for Number {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Number::Int64(l), Number::Int64(r)) => l.cmp(r),
            (Number::UInt64(l), Number::UInt64(r)) => l.cmp(r),
            (Number::Int64(l), Number::UInt64(r)) => {
                if *l < 0 {
                    Ordering::Less
                } else {
                    (*l as u64).cmp(r)
                }
            }
            (Number::UInt64(l), Number::Int64(r)) => {
                if *r < 0 {
                    Ordering::Greater
                } else {
                    l.cmp(&(*r as u64))
                }
            }
            (_, _) => {
                let l = OrderedFloat(self.as_f64().unwrap());
                let r = OrderedFloat(other.as_f64().unwrap());
                l.cmp(&r)
            }
        }
    }
}

impl Display for Number {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Number::Int64(v) => write!(f, "{}", v),
            Number::UInt64(v) => write!(f, "{}", v),
            Number::Float64(v) => write!(f, "{}", v),
        }
    }
}
