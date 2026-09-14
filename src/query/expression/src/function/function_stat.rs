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

use databend_common_exception::ErrorCode;
use databend_common_statistics::Datum;
use databend_common_statistics::StatBounds;

pub use super::stat_distribution::ReturnStat;
pub use super::stat_distribution::StatArgs;
use super::stat_distribution::StatBinaryArg;
use super::stat_distribution::StatCardinality;
use super::stat_distribution::StatUnaryArg;
use crate::Domain;
use crate::FunctionContext;
use crate::FunctionDomain;
use crate::Scalar;
use crate::ScalarRef;
use crate::types::AnyType;
use crate::types::ArgType;
use crate::types::DataType;
use crate::types::NumberDomain;
use crate::types::boolean::BooleanDomain;
use crate::types::decimal::Decimal;
use crate::types::decimal::DecimalDomain;
use crate::types::decimal::DecimalSize;
use crate::types::i256;
use crate::types::nullable::NullableDomain;
use crate::types::number::F32;
use crate::types::number::F64;
use crate::types::number::NumberScalar;
use crate::types::number::SimpleDomain;
use crate::types::string::StringDomain;

macro_rules! scalar_to_datum {
    ($value:expr, $scalar:ident, $binary_to_bytes:expr, $string_to_bytes:expr) => {
        match $value {
            $scalar::Boolean(v) => Some(Datum::Bool(v)),
            $scalar::Number(NumberScalar::Int8(v)) => Some(Datum::Int(v as i64)),
            $scalar::Number(NumberScalar::Int16(v)) => Some(Datum::Int(v as i64)),
            $scalar::Number(NumberScalar::Int32(v)) | $scalar::Date(v) => {
                Some(Datum::Int(v as i64))
            }
            $scalar::Number(NumberScalar::Int64(v)) | $scalar::Timestamp(v) => Some(Datum::Int(v)),
            $scalar::TimestampTz(v) => Some(Datum::Int(v.timestamp())),
            $scalar::Number(NumberScalar::UInt8(v)) => Some(Datum::UInt(v as u64)),
            $scalar::Number(NumberScalar::UInt16(v)) => Some(Datum::UInt(v as u64)),
            $scalar::Number(NumberScalar::UInt32(v)) => Some(Datum::UInt(v as u64)),
            $scalar::Number(NumberScalar::UInt64(v)) => Some(Datum::UInt(v)),
            $scalar::Number(NumberScalar::Float32(v)) => Some(Datum::Float(F64::from(v.0 as f64))),
            $scalar::Number(NumberScalar::Float64(v)) => Some(Datum::Float(v)),
            $scalar::Decimal(v) => Some(Datum::Float(F64::from(v.to_float64()))),
            $scalar::Binary(v) => Some(Datum::Bytes($binary_to_bytes(v))),
            $scalar::String(v) => Some(Datum::Bytes($string_to_bytes(v))),
            _ => None,
        }
    };
}

pub trait ScalarFunctionStat: Send + Sync + 'static {
    fn stat_eval(
        &self,
        ctx: &FunctionContext,
        args: StatArgs<'_>,
    ) -> Result<Option<ReturnStat>, String>;
}

#[derive(Clone, Copy)]
pub enum DeriveStat {
    Nullary(fn(StatCardinality, ctx: &FunctionContext) -> Result<Option<ReturnStat>, String>),
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

impl Scalar {
    pub fn to_datum(self) -> Option<Datum> {
        scalar_to_datum!(self, Scalar, |v| v, |v: String| v.into_bytes())
    }
}

impl ScalarRef<'_> {
    pub fn to_datum(self) -> Option<Datum> {
        scalar_to_datum!(self, ScalarRef, |v: &[u8]| v.to_vec(), |v: &str| v
            .as_bytes()
            .to_vec())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum DomainStatBounds {
    /// The domain has a finite, non-NULL closed interval.
    Bounds(StatBounds),
    /// The domain contains NULL and no non-NULL values.
    AllNull,
    /// The domain cannot be represented by finite scalar statistics bounds.
    Unsupported,
}

impl Domain {
    pub fn stat_bounds(&self) -> DomainStatBounds {
        match self {
            Domain::Number(NumberDomain::UInt8(SimpleDomain { min, max })) => {
                DomainStatBounds::Bounds(StatBounds::UInt {
                    min: *min as u64,
                    max: *max as u64,
                })
            }
            Domain::Number(NumberDomain::UInt16(SimpleDomain { min, max })) => {
                DomainStatBounds::Bounds(StatBounds::UInt {
                    min: *min as u64,
                    max: *max as u64,
                })
            }
            Domain::Number(NumberDomain::UInt32(SimpleDomain { min, max })) => {
                DomainStatBounds::Bounds(StatBounds::UInt {
                    min: *min as u64,
                    max: *max as u64,
                })
            }
            Domain::Number(NumberDomain::UInt64(SimpleDomain { min, max })) => {
                DomainStatBounds::Bounds(StatBounds::UInt {
                    min: *min,
                    max: *max,
                })
            }
            Domain::Number(NumberDomain::Int8(SimpleDomain { min, max })) => {
                DomainStatBounds::Bounds(StatBounds::Int {
                    min: *min as i64,
                    max: *max as i64,
                })
            }
            Domain::Number(NumberDomain::Int16(SimpleDomain { min, max })) => {
                DomainStatBounds::Bounds(StatBounds::Int {
                    min: *min as i64,
                    max: *max as i64,
                })
            }
            Domain::Number(NumberDomain::Int32(SimpleDomain { min, max })) => {
                DomainStatBounds::Bounds(StatBounds::Int {
                    min: *min as i64,
                    max: *max as i64,
                })
            }
            Domain::Number(NumberDomain::Int64(SimpleDomain { min, max })) => {
                DomainStatBounds::Bounds(StatBounds::Int {
                    min: *min,
                    max: *max,
                })
            }
            Domain::Number(NumberDomain::Float32(SimpleDomain { min, max })) => {
                DomainStatBounds::Bounds(StatBounds::Float {
                    min: F64::from(min.into_inner() as f64),
                    max: F64::from(max.into_inner() as f64),
                })
            }
            Domain::Number(NumberDomain::Float64(SimpleDomain { min, max })) => {
                DomainStatBounds::Bounds(StatBounds::Float {
                    min: *min,
                    max: *max,
                })
            }
            Domain::Decimal(DecimalDomain::Decimal64(SimpleDomain { min, max }, size)) => {
                DomainStatBounds::Bounds(StatBounds::Float {
                    min: F64::from(min.to_float64(size.scale())),
                    max: F64::from(max.to_float64(size.scale())),
                })
            }
            Domain::Decimal(DecimalDomain::Decimal128(SimpleDomain { min, max }, size)) => {
                DomainStatBounds::Bounds(StatBounds::Float {
                    min: F64::from(min.to_float64(size.scale())),
                    max: F64::from(max.to_float64(size.scale())),
                })
            }
            Domain::Decimal(DecimalDomain::Decimal256(SimpleDomain { min, max }, size)) => {
                DomainStatBounds::Bounds(StatBounds::Float {
                    min: F64::from(min.to_float64(size.scale())),
                    max: F64::from(max.to_float64(size.scale())),
                })
            }
            Domain::Boolean(BooleanDomain {
                has_false: false,
                has_true: false,
            }) => DomainStatBounds::Unsupported,
            Domain::Boolean(domain) => DomainStatBounds::Bounds(StatBounds::Bool {
                min: !domain.has_false,
                max: domain.has_true,
            }),
            Domain::String(StringDomain {
                min,
                max: Some(max),
            }) => DomainStatBounds::Bounds(StatBounds::Bytes {
                min: min.as_bytes().to_vec(),
                max: max.as_bytes().to_vec(),
            }),
            Domain::Timestamp(SimpleDomain { min, max }) => {
                DomainStatBounds::Bounds(StatBounds::Int {
                    min: *min,
                    max: *max,
                })
            }
            Domain::TimestampTz(SimpleDomain { min, max }) => {
                DomainStatBounds::Bounds(StatBounds::Int {
                    min: min.timestamp(),
                    max: max.timestamp(),
                })
            }
            Domain::Date(SimpleDomain { min, max }) => DomainStatBounds::Bounds(StatBounds::Int {
                min: *min as i64,
                max: *max as i64,
            }),
            Domain::Nullable(NullableDomain {
                has_null: true,
                value: None,
            }) => DomainStatBounds::AllNull,
            Domain::Nullable(domain) => domain
                .value
                .as_deref()
                .map_or(DomainStatBounds::Unsupported, Domain::stat_bounds),
            _ => DomainStatBounds::Unsupported,
        }
    }

    pub fn from_bounds(
        data_type: &DataType,
        bounds: StatBounds,
        has_null: bool,
    ) -> Result<Domain, String> {
        if data_type.has_generic() {
            return Err(format!(
                "Statistics conversion requires concrete data type, got {data_type:?}"
            ));
        }
        if let DataType::Nullable(inner) = data_type {
            return Ok(Domain::Nullable(NullableDomain {
                has_null,
                value: Some(Box::new(Domain::from_bounds(inner, bounds, false)?)),
            }));
        }
        let mut domain = Domain::full(data_type);
        match (&mut domain, bounds) {
            (Domain::Number(NumberDomain::UInt8(domain)), StatBounds::UInt { min, max }) => {
                domain.min = u8::try_from(min).map_err(|e| format!("UInt8 out of range: {e}"))?;
                domain.max = u8::try_from(max).map_err(|e| format!("UInt8 out of range: {e}"))?;
            }
            (Domain::Number(NumberDomain::UInt16(domain)), StatBounds::UInt { min, max }) => {
                domain.min = u16::try_from(min).map_err(|e| format!("UInt16 out of range: {e}"))?;
                domain.max = u16::try_from(max).map_err(|e| format!("UInt16 out of range: {e}"))?;
            }
            (Domain::Number(NumberDomain::UInt32(domain)), StatBounds::UInt { min, max }) => {
                domain.min = u32::try_from(min).map_err(|e| format!("UInt32 out of range: {e}"))?;
                domain.max = u32::try_from(max).map_err(|e| format!("UInt32 out of range: {e}"))?;
            }
            (Domain::Number(NumberDomain::UInt64(domain)), StatBounds::UInt { min, max }) => {
                domain.min = min;
                domain.max = max;
            }
            (Domain::Number(NumberDomain::Int8(domain)), StatBounds::Int { min, max }) => {
                domain.min = i8::try_from(min).map_err(|e| format!("Int8 out of range: {e}"))?;
                domain.max = i8::try_from(max).map_err(|e| format!("Int8 out of range: {e}"))?;
            }
            (Domain::Number(NumberDomain::Int16(domain)), StatBounds::Int { min, max }) => {
                domain.min = i16::try_from(min).map_err(|e| format!("Int16 out of range: {e}"))?;
                domain.max = i16::try_from(max).map_err(|e| format!("Int16 out of range: {e}"))?;
            }
            (Domain::Number(NumberDomain::Int32(domain)), StatBounds::Int { min, max }) => {
                domain.min = i32::try_from(min).map_err(|e| format!("Int32 out of range: {e}"))?;
                domain.max = i32::try_from(max).map_err(|e| format!("Int32 out of range: {e}"))?;
            }
            (Domain::Number(NumberDomain::Int64(domain)), StatBounds::Int { min, max }) => {
                domain.min = min;
                domain.max = max;
            }
            (Domain::Number(NumberDomain::Float32(domain)), StatBounds::Float { min, max }) => {
                domain.min = F32::from(min.into_inner() as f32);
                domain.max = F32::from(max.into_inner() as f32);
            }
            (Domain::Number(NumberDomain::Float64(domain)), StatBounds::Float { min, max }) => {
                domain.min = min;
                domain.max = max;
            }
            (
                Domain::Decimal(DecimalDomain::Decimal64(domain, size)),
                StatBounds::Float { min, max },
            ) => {
                domain.min = f64_to_decimal::<i64>(min.into_inner(), *size)?;
                domain.max = f64_to_decimal::<i64>(max.into_inner(), *size)?;
            }
            (
                Domain::Decimal(DecimalDomain::Decimal128(domain, size)),
                StatBounds::Float { min, max },
            ) => {
                domain.min = f64_to_decimal::<i128>(min.into_inner(), *size)?;
                domain.max = f64_to_decimal::<i128>(max.into_inner(), *size)?;
            }
            (
                Domain::Decimal(DecimalDomain::Decimal256(domain, size)),
                StatBounds::Float { min, max },
            ) => {
                domain.min = f64_to_decimal::<i256>(min.into_inner(), *size)?;
                domain.max = f64_to_decimal::<i256>(max.into_inner(), *size)?;
            }
            (Domain::Boolean(domain), StatBounds::Bool { min, max }) => {
                domain.has_false = !min;
                domain.has_true = max;
            }
            (Domain::String(domain), StatBounds::Bytes { min, max }) => {
                domain.min = String::from_utf8(min).map_err(|e| e.to_string())?;
                domain.max = Some(String::from_utf8(max).map_err(|e| e.to_string())?);
            }
            (Domain::Timestamp(domain), StatBounds::Int { min, max }) => {
                domain.min = min;
                domain.max = max;
            }
            (Domain::Date(domain), StatBounds::Int { min, max }) => {
                domain.min = i32::try_from(min).map_err(|e| format!("Date out of range: {e}"))?;
                domain.max = i32::try_from(max).map_err(|e| format!("Date out of range: {e}"))?;
            }
            (Domain::Nullable(_), bounds) => {
                return Err(format!(
                    "statistics bounds {bounds:?} cannot be converted to data type {data_type:?}"
                ));
            }
            (Domain::Undefined, _) => {}
            (Domain::TimestampTz(_), _) => {
                return Err(
                    "Statistics conversion for TIMESTAMP WITH TIME ZONE is not supported"
                        .to_string(),
                );
            }
            (Domain::Interval(_), _) => {
                return Err("Statistics conversion for INTERVAL is not supported".to_string());
            }
            (Domain::Array(_) | Domain::Map(_) | Domain::Tuple(_), _) => {
                return Err(format!(
                    "Unsupported data type {:?} for statistics conversion",
                    data_type
                ));
            }
            (domain, bounds) => {
                return Err(format!(
                    "statistics bounds {bounds:?} do not match domain {domain:?} for data type {data_type:?}"
                ));
            }
        }

        Ok(domain)
    }
}

fn f64_to_decimal<T: Decimal>(numeric: f64, size: DecimalSize) -> Result<T, String> {
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

impl DataType {
    pub fn full_stat_bounds(&self) -> Result<StatBounds, ErrorCode> {
        let domain = match self {
            DataType::Nullable(inner) => return inner.full_stat_bounds(),
            DataType::Boolean
            | DataType::Number(_)
            | DataType::Decimal(_)
            | DataType::Timestamp
            | DataType::TimestampTz
            | DataType::Date => Domain::full(self),
            data_type => {
                return Err(ErrorCode::InvalidArgument(format!(
                    "cannot construct finite statistics bounds for data type {data_type}"
                )));
            }
        };

        match domain.stat_bounds() {
            DomainStatBounds::Bounds(bounds) => Ok(bounds),
            DomainStatBounds::AllNull | DomainStatBounds::Unsupported => {
                Err(ErrorCode::InvalidArgument(format!(
                    "cannot construct finite statistics bounds for data type {self}"
                )))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_common_column::types::timestamp_tz;
    use databend_common_statistics::StatBounds;

    use super::*;
    use crate::types::NumberDataType;

    #[test]
    fn timestamp_tz_to_datum_uses_timestamp_micros() {
        let value = timestamp_tz::new(1_234_567, 8 * 3600);

        assert_eq!(
            Scalar::TimestampTz(value).to_datum(),
            Some(Datum::Int(1_234_567))
        );
        assert_eq!(
            ScalarRef::TimestampTz(value).to_datum(),
            Some(Datum::Int(1_234_567))
        );
    }

    #[test]
    fn stat_bounds_from_domain_preserve_finite_non_null_range() {
        let domain = Domain::Nullable(crate::types::nullable::NullableDomain {
            has_null: true,
            value: Some(Box::new(Domain::Number(NumberDomain::Int32(
                SimpleDomain { min: -2, max: 3 },
            )))),
        });

        assert_eq!(
            domain.stat_bounds(),
            DomainStatBounds::Bounds(StatBounds::Int { min: -2, max: 3 })
        );

        let unbounded_string = Domain::String(StringDomain {
            min: "a".to_string(),
            max: None,
        });
        assert_eq!(
            unbounded_string.stat_bounds(),
            DomainStatBounds::Unsupported
        );

        let all_null = Domain::Nullable(crate::types::nullable::NullableDomain {
            has_null: true,
            value: None,
        });
        assert_eq!(all_null.stat_bounds(), DomainStatBounds::AllNull);

        let empty_nullable = Domain::Nullable(crate::types::nullable::NullableDomain {
            has_null: false,
            value: None,
        });
        assert_eq!(empty_nullable.stat_bounds(), DomainStatBounds::Unsupported);
    }

    #[test]
    fn decimal_domain_stat_bounds_apply_scale_without_scalar_conversion() {
        let domain = Domain::Decimal(DecimalDomain::Decimal64(
            SimpleDomain { min: 123, max: 456 },
            DecimalSize::new_unchecked(5, 2),
        ));

        assert_eq!(
            domain.stat_bounds(),
            DomainStatBounds::Bounds(StatBounds::Float {
                min: F64::from(1.23),
                max: F64::from(4.56),
            })
        );
    }

    #[test]
    fn stat_bounds_from_data_type_use_full_representable_range() {
        let data_type = DataType::Number(NumberDataType::UInt16).wrap_nullable();
        let bounds = data_type.full_stat_bounds().unwrap();

        assert_eq!(bounds, StatBounds::UInt {
            min: u16::MIN as u64,
            max: u16::MAX as u64
        });
        assert!(DataType::String.full_stat_bounds().is_err());
    }
}
