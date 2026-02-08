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

use std::convert::TryFrom;

use databend_common_statistics::Datum;
use databend_common_statistics::Histogram;
pub use databend_common_statistics::Ndv;

use crate::Domain;
use crate::FunctionContext;
use crate::FunctionDomain;
use crate::Scalar;
use crate::types::AnyType;
use crate::types::DataType;
use crate::types::NumberDataType;
use crate::types::NumberDomain;
use crate::types::decimal::Decimal;
use crate::types::decimal::DecimalDomain;
use crate::types::decimal::DecimalSize;
use crate::types::i256;
use crate::types::number::F32;
use crate::types::number::F64;
use crate::types::number::NumberScalar;

pub trait ScalarFunctionStat: Send + Sync + 'static {
    fn stat_eval(
        &self,
        ctx: &FunctionContext,
        args: StatArgs<'_>,
    ) -> Result<Option<ReturnStat>, String>;
}

#[derive(Clone, Copy)]
pub enum DeriveStat {
    Nullary(fn(cardinality: f64, ctx: &FunctionContext) -> Result<Option<ReturnStat>, String>),
    Unary(fn(StatUnaryArg, ctx: &FunctionContext) -> Result<Option<ReturnStat>, String>),
    Binary(fn(StatBinaryArg, ctx: &FunctionContext) -> Result<Option<ReturnStat>, String>),
    Other(fn(StatArgs, ctx: &FunctionContext) -> Result<Option<ReturnStat>, String>),
}

impl ScalarFunctionStat for DeriveStat {
    fn stat_eval(
        &self,
        ctx: &FunctionContext,
        stat_args: StatArgs<'_>,
    ) -> Result<Option<ReturnStat>, String> {
        match self {
            DeriveStat::Nullary(func) => {
                assert!(stat_args.args.is_empty());
                func(stat_args.cardinality, ctx)
            }
            DeriveStat::Unary(func) => func(
                StatUnaryArg {
                    cardinality: stat_args.cardinality,
                    args: stat_args.args.as_array().unwrap(),
                },
                ctx,
            ),
            DeriveStat::Binary(func) => func(
                StatBinaryArg {
                    cardinality: stat_args.cardinality,
                    args: stat_args.args.as_array().unwrap(),
                },
                ctx,
            ),
            DeriveStat::Other(func) => func(stat_args, ctx),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ArgStat<'a> {
    pub domain: Domain,
    pub ndv: Ndv,
    pub null_count: u64,
    pub histogram: Option<&'a Histogram>,
}

#[derive(Debug, Clone)]
pub struct StatUnaryArg<'a> {
    pub cardinality: f64,
    pub args: &'a [ArgStat<'a>; 1],
}

#[derive(Debug, Clone)]
pub struct StatBinaryArg<'a> {
    pub cardinality: f64,
    pub args: &'a [ArgStat<'a>; 2],
}

#[derive(Debug, Clone)]
pub struct StatArgs<'a> {
    pub cardinality: f64,
    pub args: &'a [ArgStat<'a>],
}

#[derive(Debug)]
pub struct ReturnStat {
    pub domain: Domain,
    pub ndv: Ndv,
    pub null_count: u64,
    pub histogram: Option<Histogram>,
}

impl Scalar {
    pub fn to_datum(self) -> Option<Datum> {
        match self {
            Scalar::Boolean(v) => Some(Datum::Bool(v)),
            Scalar::Number(NumberScalar::Int8(v)) => Some(Datum::Int(v as i64)),
            Scalar::Number(NumberScalar::Int16(v)) => Some(Datum::Int(v as i64)),
            Scalar::Number(NumberScalar::Int32(v)) | Scalar::Date(v) => Some(Datum::Int(v as i64)),
            Scalar::Number(NumberScalar::Int64(v)) | Scalar::Timestamp(v) => Some(Datum::Int(v)),
            Scalar::Number(NumberScalar::UInt8(v)) => Some(Datum::UInt(v as u64)),
            Scalar::Number(NumberScalar::UInt16(v)) => Some(Datum::UInt(v as u64)),
            Scalar::Number(NumberScalar::UInt32(v)) => Some(Datum::UInt(v as u64)),
            Scalar::Number(NumberScalar::UInt64(v)) => Some(Datum::UInt(v)),
            Scalar::Number(NumberScalar::Float32(v)) => Some(Datum::Float(F64::from(v.0 as f64))),
            Scalar::Number(NumberScalar::Float64(v)) => Some(Datum::Float(v)),
            Scalar::Decimal(v) => Some(Datum::Float(F64::from(v.to_float64()))),
            Scalar::Binary(v) => Some(Datum::Bytes(v)),
            Scalar::String(v) => Some(Datum::Bytes(v.as_bytes().to_vec())),
            _ => None,
        }
    }
}

impl Domain {
    pub fn from_datum(
        data_type: &DataType,
        min: Datum,
        max: Datum,
        has_null: bool,
    ) -> Result<Domain, String> {
        let mut domain = Domain::full(data_type);
        match &mut domain {
            Domain::Number(domain) => match domain {
                NumberDomain::UInt8(domain) => {
                    domain.min = datum_to_u8(&min)?;
                    domain.max = datum_to_u8(&max)?;
                }
                NumberDomain::UInt16(domain) => {
                    domain.min = datum_to_u16(&min)?;
                    domain.max = datum_to_u16(&max)?;
                }
                NumberDomain::UInt32(domain) => {
                    domain.min = datum_to_u32(&min)?;
                    domain.max = datum_to_u32(&max)?;
                }
                NumberDomain::UInt64(domain) => {
                    domain.min = datum_to_u64(&min)?;
                    domain.max = datum_to_u64(&max)?;
                }
                NumberDomain::Int8(domain) => {
                    domain.min = datum_to_i8(&min)?;
                    domain.max = datum_to_i8(&max)?;
                }
                NumberDomain::Int16(domain) => {
                    domain.min = datum_to_i16(&min)?;
                    domain.max = datum_to_i16(&max)?;
                }
                NumberDomain::Int32(domain) => {
                    domain.min = datum_to_i32(&min)?;
                    domain.max = datum_to_i32(&max)?;
                }
                NumberDomain::Int64(domain) => {
                    domain.min = datum_to_i64(&min)?;
                    domain.max = datum_to_i64(&max)?;
                }
                NumberDomain::Float32(domain) => {
                    domain.min = datum_to_f32(&min)?;
                    domain.max = datum_to_f32(&max)?;
                }
                NumberDomain::Float64(domain) => {
                    domain.min = datum_to_f64(&min)?;
                    domain.max = datum_to_f64(&max)?;
                }
            },
            Domain::Decimal(domain) => match domain {
                DecimalDomain::Decimal64(domain, size) => {
                    domain.min = datum_to_decimal::<i64>(&min, *size)?;
                    domain.max = datum_to_decimal::<i64>(&max, *size)?;
                }
                DecimalDomain::Decimal128(domain, size) => {
                    domain.min = datum_to_decimal::<i128>(&min, *size)?;
                    domain.max = datum_to_decimal::<i128>(&max, *size)?;
                }
                DecimalDomain::Decimal256(domain, size) => {
                    domain.min = datum_to_decimal::<i256>(&min, *size)?;
                    domain.max = datum_to_decimal::<i256>(&max, *size)?;
                }
            },
            Domain::Boolean(domain) => {
                domain.has_false = !datum_to_bool(&min)?;
                domain.has_true = datum_to_bool(&max)?;
            }
            Domain::String(domain) => {
                domain.min = datum_to_string(&min)?;
                domain.max = Some(datum_to_string(&max)?);
            }
            Domain::Timestamp(domain) => {
                domain.min = datum_to_i64(&min)?;
                domain.max = datum_to_i64(&max)?;
            }
            Domain::Date(domain) => {
                domain.min = datum_to_i32(&min)?;
                domain.max = datum_to_i32(&max)?;
            }
            Domain::Nullable(nullable_domain) => {
                nullable_domain.has_null = has_null;
                nullable_domain.value = Some(Box::new(Domain::from_datum(
                    &data_type.remove_nullable(),
                    min.clone(),
                    max.clone(),
                    false,
                )?));
            }
            Domain::Undefined => {}
            Domain::TimestampTz(_) => {
                return Err(
                    "Statistics conversion for TIMESTAMP WITH TIME ZONE is not supported"
                        .to_string(),
                );
            }
            Domain::Interval(_) => {
                return Err("Statistics conversion for INTERVAL is not supported".to_string());
            }
            Domain::Array(_) | Domain::Map(_) | Domain::Tuple(_) => {
                return Err(format!(
                    "Unsupported data type {:?} for statistics conversion",
                    data_type
                ));
            }
        }

        Ok(domain)
    }
}

fn datum_to_bool(value: &Datum) -> Result<bool, String> {
    match value {
        Datum::Bool(v) => Ok(*v),
        _ => Err(type_mismatch("BOOLEAN", value)),
    }
}

fn datum_to_string(value: &Datum) -> Result<String, String> {
    match value {
        Datum::Bytes(v) => String::from_utf8(v.clone()).map_err(|e| e.to_string()),
        _ => Err(type_mismatch("STRING", value)),
    }
}

fn datum_to_u8(value: &Datum) -> Result<u8, String> {
    match value {
        Datum::UInt(v) => u8::try_from(*v).map_err(|e| format!("UInt8 out of range: {e}")),
        Datum::Float(v) => Ok(v.0 as _),
        _ => Err(type_mismatch("UInt8", value)),
    }
}

fn datum_to_u16(value: &Datum) -> Result<u16, String> {
    match value {
        Datum::UInt(v) => u16::try_from(*v).map_err(|e| format!("UInt16 out of range: {e}")),
        Datum::Float(v) => Ok(v.0 as _),
        _ => Err(type_mismatch("UInt16", value)),
    }
}

fn datum_to_u32(value: &Datum) -> Result<u32, String> {
    match value {
        Datum::UInt(v) => u32::try_from(*v).map_err(|e| format!("UInt32 out of range: {e}")),
        Datum::Float(v) => Ok(v.0 as _),
        _ => Err(type_mismatch("UInt32", value)),
    }
}

fn datum_to_u64(value: &Datum) -> Result<u64, String> {
    match value {
        Datum::UInt(v) => Ok(*v),
        Datum::Float(v) => Ok(v.0 as _),
        _ => Err(type_mismatch("UInt64", value)),
    }
}

fn datum_to_i8(value: &Datum) -> Result<i8, String> {
    match value {
        Datum::Int(v) => i8::try_from(*v).map_err(|e| format!("Int8 out of range: {e}")),
        Datum::Float(v) => Ok(v.0 as _),
        _ => Err(type_mismatch("Int8", value)),
    }
}

fn datum_to_i16(value: &Datum) -> Result<i16, String> {
    match value {
        Datum::Int(v) => i16::try_from(*v).map_err(|e| format!("Int16 out of range: {e}")),
        Datum::Float(v) => Ok(v.0 as _),
        _ => Err(type_mismatch("Int16", value)),
    }
}

fn datum_to_i32(value: &Datum) -> Result<i32, String> {
    match value {
        Datum::Int(v) => i32::try_from(*v).map_err(|e| format!("Int32 out of range: {e}")),
        Datum::Float(v) => Ok(v.0 as _),
        _ => Err(type_mismatch("Int32", value)),
    }
}

fn datum_to_i64(value: &Datum) -> Result<i64, String> {
    match value {
        Datum::Int(v) => Ok(*v),
        Datum::Float(v) => Ok(v.0 as _),
        _ => Err(type_mismatch("Int64", value)),
    }
}

fn datum_to_f32(value: &Datum) -> Result<F32, String> {
    let float = match value {
        Datum::Float(v) => v.into_inner(),
        _ => return Err(type_mismatch("Float32", value)),
    };
    Ok(F32::from(float as f32))
}

fn datum_to_f64(value: &Datum) -> Result<F64, String> {
    let float = match value {
        Datum::Float(v) => v.into_inner(),
        Datum::Int(v) => *v as f64,
        Datum::UInt(v) => *v as f64,
        _ => return Err(type_mismatch("Float64", value)),
    };
    Ok(F64::from(float))
}

fn datum_to_decimal<T: Decimal>(value: &Datum, size: DecimalSize) -> Result<T, String> {
    let numeric = match value {
        Datum::Float(v) => v.0,
        _ => return Err(type_mismatch("Decimal", value)),
    };
    let scaled = numeric * 10_f64.powi(size.scale() as i32);
    if !scaled.is_finite() {
        return Err(format!("Decimal scaling overflow for value {numeric}"));
    }
    let decimal = T::from_float(scaled);
    let min_allowed = T::min_for_precision(size.precision());
    let max_allowed = T::max_for_precision(size.precision());
    if decimal < min_allowed || decimal > max_allowed {
        return Err(format!(
            "Decimal value {} is out of range for size {:?}",
            numeric, size
        ));
    }
    Ok(decimal)
}

fn type_mismatch(expected: &str, actual: &Datum) -> String {
    format!("Cannot convert {actual:?} to {}", expected)
}
