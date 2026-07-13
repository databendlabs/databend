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

use databend_common_column::types::months_days_micros;
use databend_common_column::types::timestamp_tz;
use databend_common_exception::ErrorCode;
use enum_as_inner::EnumAsInner;

use crate::FunctionContext;
use crate::Scalar;
use crate::types::AccessType;
use crate::types::AnyType;
use crate::types::ArgType;
use crate::types::BooleanType;
use crate::types::DataType;
use crate::types::DateType;
use crate::types::DecimalDataType;
use crate::types::DecimalType;
use crate::types::IntervalType;
use crate::types::NumberDataType;
use crate::types::NumberType;
use crate::types::StringType;
use crate::types::TimestampType;
use crate::types::boolean::BooleanDomain;
use crate::types::decimal::Decimal;
use crate::types::decimal::DecimalDomain;
use crate::types::decimal::DecimalScalar;
use crate::types::i256;
use crate::types::nullable::NullableDomain;
use crate::types::number::F32;
use crate::types::number::F64;
use crate::types::number::NumberDomain;
use crate::types::number::NumberScalar;
use crate::types::number::SimpleDomain;
use crate::types::string::StringDomain;
use crate::types::timestamp_tz::TimestampTzType;
use crate::with_decimal_mapped_type;
use crate::with_decimal_type;
use crate::with_number_type;

/// Returns the argument for which a function is monotonically increasing under the given
/// argument domains and function context (e.g. the session time zone). `None` means
/// monotonicity cannot be proven for that range.
pub type MonotonicityCheck = fn(&FunctionContext, &[Domain]) -> Option<usize>;

#[derive(Debug, Clone)]
pub struct FunctionProperty {
    pub non_deterministic: bool,
    pub kind: FunctionKind,
    // strictly increasing or strictly decreasing, like y = x + 1
    // y = x ^ 2 is not monotonicity, but it's only monotonicity in [-x, 0] and [0, +x]
    // only works for function with 1-sized arg now
    pub monotonicity: bool,
    // will be monotonicity if arg is one of `monotonicity_by_type`
    pub monotonicity_by_type: Vec<DataType>,
    // Range-sensitive monotonicity for functions with constant or constrained arguments.
    // This is consumed by index pruning and does not change scalar evaluation semantics.
    pub monotonicity_check: Option<MonotonicityCheck>,
}

impl FunctionProperty {
    pub fn non_deterministic(mut self) -> Self {
        self.non_deterministic = true;
        self
    }

    pub fn monotonicity(mut self) -> Self {
        self.monotonicity = true;
        self
    }

    pub fn monotonicity_type(mut self, data_type: DataType) -> Self {
        self.monotonicity_by_type.push(data_type);
        self
    }

    pub fn monotonicity_check(mut self, check: MonotonicityCheck) -> Self {
        self.monotonicity_check = Some(check);
        self
    }

    pub fn kind(mut self, kind: FunctionKind) -> Self {
        self.kind = kind;
        self
    }
}

impl Default for FunctionProperty {
    fn default() -> Self {
        FunctionProperty {
            non_deterministic: false,
            monotonicity: false,
            monotonicity_by_type: vec![],
            monotonicity_check: None,
            kind: FunctionKind::Scalar,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FunctionKind {
    Scalar,
    SRF,
}

/// Describe the behavior of a function to eliminate the runtime
/// evaluation of the function if possible.
#[derive(Debug, Clone, PartialEq, EnumAsInner)]
pub enum FunctionDomain<T: AccessType> {
    /// The function may return error.
    MayThrow,
    /// The function must not return error, and the return value can be
    /// any valid value the type can represent.
    Full,
    /// The function must not return error, and have further information
    /// about the range of the output value.
    Domain(T::Domain),
}

/// The range of the possible values that a scalar or the scalars in a
/// column can take. We can assume the values outside the range are not
/// possible, but we cannot assume the values inside the range must exist.
#[derive(Debug, Clone, PartialEq, EnumAsInner)]
pub enum Domain {
    Number(NumberDomain),
    Decimal(DecimalDomain),
    Boolean(BooleanDomain),
    String(StringDomain),
    Timestamp(SimpleDomain<i64>),
    TimestampTz(SimpleDomain<timestamp_tz>),
    Date(SimpleDomain<i32>),
    Interval(SimpleDomain<months_days_micros>),
    Nullable(NullableDomain<AnyType>),
    /// `Array(None)` means that the array is empty, thus there is no inner domain information.
    Array(Option<Box<Domain>>),
    /// `Map(None)` means that the map is empty, thus there is no inner domain information.
    Map(Option<Box<Domain>>),
    Tuple(Vec<Domain>),
    /// For certain types, like `Variant`, the domain is useless therefore is not defined.
    Undefined,
}

/// Type-erased exact extrema observed in a set of values.
///
/// The typed payloads are shared with [`Domain`], but this is not a domain: its
/// bounds are actual non-NULL values observed in a column. The boolean in each
/// variant records whether NULL also occurred in the same set.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum MinMax {
    Number(NumberDomain, bool),
    Decimal(DecimalDomain, bool),
    Boolean(BooleanDomain, bool),
    String(SimpleDomain<String>, bool),
    Timestamp(SimpleDomain<i64>, bool),
    TimestampTz(SimpleDomain<timestamp_tz>, bool),
    Date(SimpleDomain<i32>, bool),
    Interval(SimpleDomain<months_days_micros>, bool),
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ColumnMinMax {
    Empty,
    AllNull,
    Values(MinMax),
}

impl ColumnMinMax {
    pub fn merge(&mut self, other: &Self) -> Result<(), ErrorCode> {
        match (self, other) {
            (_, ColumnMinMax::Empty) => Ok(()),
            (this @ ColumnMinMax::Empty, other) => {
                *this = other.clone();
                Ok(())
            }
            (ColumnMinMax::AllNull, ColumnMinMax::AllNull) => Ok(()),
            (this @ ColumnMinMax::AllNull, ColumnMinMax::Values(min_max)) => {
                let mut min_max = min_max.clone();
                min_max.set_has_null();
                *this = ColumnMinMax::Values(min_max);
                Ok(())
            }
            (ColumnMinMax::Values(min_max), ColumnMinMax::AllNull) => {
                min_max.set_has_null();
                Ok(())
            }
            (ColumnMinMax::Values(lhs), ColumnMinMax::Values(rhs)) => lhs.merge(rhs),
        }
    }

    pub fn into_option(self) -> Option<MinMax> {
        match self {
            ColumnMinMax::Values(min_max) => Some(min_max),
            ColumnMinMax::Empty | ColumnMinMax::AllNull => None,
        }
    }
}

impl MinMax {
    pub fn has_null(&self) -> bool {
        match self {
            MinMax::Number(_, has_null)
            | MinMax::Decimal(_, has_null)
            | MinMax::Boolean(_, has_null)
            | MinMax::String(_, has_null)
            | MinMax::Timestamp(_, has_null)
            | MinMax::TimestampTz(_, has_null)
            | MinMax::Date(_, has_null)
            | MinMax::Interval(_, has_null) => *has_null,
        }
    }

    fn set_has_null(&mut self) {
        match self {
            MinMax::Number(_, value)
            | MinMax::Decimal(_, value)
            | MinMax::Boolean(_, value)
            | MinMax::String(_, value)
            | MinMax::Timestamp(_, value)
            | MinMax::TimestampTz(_, value)
            | MinMax::Date(_, value)
            | MinMax::Interval(_, value) => *value = true,
        }
    }

    pub fn with_null(mut self) -> Self {
        self.set_has_null();
        self
    }

    pub fn merge(&mut self, other: &Self) -> Result<(), ErrorCode> {
        match (self, other) {
            (MinMax::Number(lhs, lhs_has_null), MinMax::Number(rhs, rhs_has_null)) => {
                lhs.merge(rhs)?;
                *lhs_has_null |= *rhs_has_null;
                Ok(())
            }
            (MinMax::Decimal(lhs, lhs_has_null), MinMax::Decimal(rhs, rhs_has_null)) => {
                lhs.merge(rhs)?;
                *lhs_has_null |= *rhs_has_null;
                Ok(())
            }
            (MinMax::Boolean(lhs, lhs_has_null), MinMax::Boolean(rhs, rhs_has_null)) => {
                lhs.merge(rhs);
                *lhs_has_null |= *rhs_has_null;
                Ok(())
            }
            (MinMax::String(lhs, lhs_has_null), MinMax::String(rhs, rhs_has_null)) => {
                lhs.merge(rhs);
                *lhs_has_null |= *rhs_has_null;
                Ok(())
            }
            (MinMax::Timestamp(lhs, lhs_has_null), MinMax::Timestamp(rhs, rhs_has_null)) => {
                lhs.merge(rhs);
                *lhs_has_null |= *rhs_has_null;
                Ok(())
            }
            (MinMax::TimestampTz(lhs, lhs_has_null), MinMax::TimestampTz(rhs, rhs_has_null)) => {
                lhs.merge(rhs);
                *lhs_has_null |= *rhs_has_null;
                Ok(())
            }
            (MinMax::Date(lhs, lhs_has_null), MinMax::Date(rhs, rhs_has_null)) => {
                lhs.merge(rhs);
                *lhs_has_null |= *rhs_has_null;
                Ok(())
            }
            (MinMax::Interval(lhs, lhs_has_null), MinMax::Interval(rhs, rhs_has_null)) => {
                lhs.merge(rhs);
                *lhs_has_null |= *rhs_has_null;
                Ok(())
            }
            (lhs, rhs) => Err(ErrorCode::InvalidArgument(format!(
                "cannot merge min/max values {lhs:?} and {rhs:?}"
            ))),
        }
    }

    pub fn scalars(&self) -> (Scalar, Scalar) {
        with_number_type!(|NUM| match self {
            MinMax::Number(NumberDomain::NUM(values), _) => (
                Scalar::Number(NumberScalar::NUM(values.min)),
                Scalar::Number(NumberScalar::NUM(values.max)),
            ),
            MinMax::Decimal(decimal, _) => with_decimal_type!(|DECIMAL| match decimal {
                DecimalDomain::DECIMAL(values, size) => (
                    Scalar::Decimal(DecimalScalar::DECIMAL(values.min, *size)),
                    Scalar::Decimal(DecimalScalar::DECIMAL(values.max, *size)),
                ),
            }),
            MinMax::Boolean(values, _) => match (values.has_false, values.has_true) {
                (true, true) => (Scalar::Boolean(false), Scalar::Boolean(true)),
                (true, false) => (Scalar::Boolean(false), Scalar::Boolean(false)),
                (false, true) => (Scalar::Boolean(true), Scalar::Boolean(true)),
                (false, false) => unreachable!("MinMax cannot contain an empty boolean range"),
            },
            MinMax::String(values, _) => (
                Scalar::String(values.min.clone()),
                Scalar::String(values.max.clone()),
            ),
            MinMax::Timestamp(values, _) =>
                (Scalar::Timestamp(values.min), Scalar::Timestamp(values.max),),
            MinMax::TimestampTz(values, _) => (
                Scalar::TimestampTz(values.min),
                Scalar::TimestampTz(values.max),
            ),
            MinMax::Date(values, _) => {
                (Scalar::Date(values.min), Scalar::Date(values.max))
            }
            MinMax::Interval(values, _) =>
                (Scalar::Interval(values.min), Scalar::Interval(values.max),),
        })
    }
}

impl<T: AccessType> FunctionDomain<T> {
    pub fn map<U: AccessType>(self, f: impl Fn(T::Domain) -> U::Domain) -> FunctionDomain<U> {
        match self {
            FunctionDomain::MayThrow => FunctionDomain::MayThrow,
            FunctionDomain::Full => FunctionDomain::Full,
            FunctionDomain::Domain(domain) => FunctionDomain::Domain(f(domain)),
        }
    }
}

impl<T: ArgType> FunctionDomain<T> {
    /// Return the range of the output value.
    ///
    /// Return `None` if the function may return error.
    pub fn normalize(self) -> Option<T::Domain> {
        match self {
            FunctionDomain::MayThrow => None,
            FunctionDomain::Full => Some(T::full_domain()),
            FunctionDomain::Domain(domain) => Some(domain),
        }
    }
}

impl Domain {
    pub fn check_data_type(&self, data_type: &DataType) -> Result<(), String> {
        if self.matches_data_type(data_type) {
            Ok(())
        } else {
            Err(format!(
                "domain does not match data type: domain {self:?}, data type {data_type:?}"
            ))
        }
    }

    pub fn finite_cardinality_upper(&self) -> Option<u128> {
        match self {
            Domain::Boolean(domain) => {
                Some((domain.has_false as u8 + domain.has_true as u8).into())
            }
            Domain::Number(domain) => domain.finite_cardinality_upper(),
            Domain::Nullable(NullableDomain { value: None, .. }) => Some(0),
            Domain::Nullable(NullableDomain {
                value: Some(box Domain::Boolean(domain)),
                ..
            }) => Some((domain.has_false as u8 + domain.has_true as u8).into()),
            Domain::Nullable(NullableDomain {
                value: Some(box Domain::Number(domain)),
                ..
            }) => domain.finite_cardinality_upper(),
            Domain::Nullable(NullableDomain {
                value: Some(box Domain::Nullable(_)),
                ..
            }) => unreachable!(),
            _ => None,
        }
    }

    pub fn matches_data_type(&self, data_type: &DataType) -> bool {
        match data_type {
            DataType::Nullable(inner_type) => match self {
                Domain::Nullable(nullable_domain) => nullable_domain
                    .value
                    .as_deref()
                    .is_none_or(|domain| domain.matches_data_type(inner_type)),
                _ => false,
            },
            DataType::Null => matches!(self, Domain::Nullable(NullableDomain { value: None, .. })),
            data_type => self.matches_non_nullable_data_type(data_type),
        }
    }

    fn matches_non_nullable_data_type(&self, data_type: &DataType) -> bool {
        match (self, data_type) {
            (Domain::Number(domain), DataType::Number(num_type)) => {
                with_number_type!(|TYPE| match domain {
                    NumberDomain::TYPE(_) => NumberDataType::TYPE,
                }) == *num_type
            }
            (Domain::Decimal(domain), DataType::Decimal(size)) => domain.decimal_size() == *size,
            (Domain::Boolean(_), DataType::Boolean)
            | (Domain::String(_), DataType::String)
            | (Domain::Timestamp(_), DataType::Timestamp)
            | (Domain::TimestampTz(_), DataType::TimestampTz)
            | (Domain::Date(_), DataType::Date)
            | (Domain::Interval(_), DataType::Interval) => true,
            (Domain::Array(None), DataType::EmptyArray | DataType::Array(_)) => true,
            (Domain::Array(Some(domain)), DataType::Array(data_type)) => {
                domain.matches_data_type(data_type)
            }
            (Domain::Map(None), DataType::EmptyMap | DataType::Map(_)) => true,
            (Domain::Map(Some(domain)), DataType::Map(data_type)) => {
                domain.matches_data_type(data_type)
            }
            (Domain::Tuple(domains), DataType::Tuple(data_types)) => {
                domains.len() == data_types.len()
                    && domains
                        .iter()
                        .zip(data_types)
                        .all(|(domain, data_type)| domain.matches_data_type(data_type))
            }
            (Domain::Undefined, DataType::Binary)
            | (Domain::Undefined, DataType::Bitmap)
            | (Domain::Undefined, DataType::Variant)
            | (Domain::Undefined, DataType::Geometry)
            | (Domain::Undefined, DataType::Geography)
            | (Domain::Undefined, DataType::Vector(_))
            | (Domain::Undefined, DataType::Opaque(_)) => true,
            _ => false,
        }
    }

    pub fn full(data_type: &DataType) -> Self {
        match data_type {
            DataType::Boolean => Domain::Boolean(BooleanType::full_domain()),
            DataType::String => Domain::String(StringType::full_domain()),
            DataType::Number(NumberDataType::UInt8) => {
                Domain::Number(NumberDomain::UInt8(NumberType::<u8>::full_domain()))
            }
            DataType::Number(NumberDataType::UInt16) => {
                Domain::Number(NumberDomain::UInt16(NumberType::<u16>::full_domain()))
            }
            DataType::Number(NumberDataType::UInt32) => {
                Domain::Number(NumberDomain::UInt32(NumberType::<u32>::full_domain()))
            }
            DataType::Number(NumberDataType::UInt64) => {
                Domain::Number(NumberDomain::UInt64(NumberType::<u64>::full_domain()))
            }
            DataType::Number(NumberDataType::Int8) => {
                Domain::Number(NumberDomain::Int8(NumberType::<i8>::full_domain()))
            }
            DataType::Number(NumberDataType::Int16) => {
                Domain::Number(NumberDomain::Int16(NumberType::<i16>::full_domain()))
            }
            DataType::Number(NumberDataType::Int32) => {
                Domain::Number(NumberDomain::Int32(NumberType::<i32>::full_domain()))
            }
            DataType::Number(NumberDataType::Int64) => {
                Domain::Number(NumberDomain::Int64(NumberType::<i64>::full_domain()))
            }
            DataType::Number(NumberDataType::Float32) => {
                Domain::Number(NumberDomain::Float32(NumberType::<F32>::full_domain()))
            }
            DataType::Number(NumberDataType::Float64) => {
                Domain::Number(NumberDomain::Float64(NumberType::<F64>::full_domain()))
            }
            DataType::Decimal(size) => {
                with_decimal_mapped_type!(|DECIMAL| match DecimalDataType::from(*size) {
                    DecimalDataType::DECIMAL(size) =>
                        DECIMAL::upcast_domain(DecimalType::<DECIMAL>::full_domain(&size), size),
                })
            }
            DataType::Timestamp => Domain::Timestamp(TimestampType::full_domain()),
            DataType::TimestampTz => Domain::TimestampTz(TimestampTzType::full_domain()),
            DataType::Date => Domain::Date(DateType::full_domain()),
            DataType::Interval => Domain::Interval(IntervalType::full_domain()),
            DataType::Null => Domain::Nullable(NullableDomain {
                has_null: true,
                value: None,
            }),
            DataType::Nullable(ty) => Domain::Nullable(NullableDomain {
                has_null: true,
                value: Some(Box::new(Domain::full(ty))),
            }),
            DataType::Tuple(fields_ty) => {
                Domain::Tuple(fields_ty.iter().map(Domain::full).collect())
            }
            DataType::EmptyArray => Domain::Array(None),
            DataType::Array(ty) => Domain::Array(Some(Box::new(Domain::full(ty)))),
            DataType::EmptyMap => Domain::Map(None),
            DataType::Map(ty) => Domain::Map(Some(Box::new(Domain::full(ty)))),
            DataType::Binary
            | DataType::Bitmap
            | DataType::Variant
            | DataType::Geometry
            | DataType::Geography
            | DataType::Vector(_)
            | DataType::Opaque(_) => Domain::Undefined,
            DataType::Generic(_) | DataType::StageLocation => unreachable!(),
        }
    }

    pub fn merge(&self, other: &Domain) -> Domain {
        match (self, other) {
            (Domain::Number(this), Domain::Number(other)) => {
                let mut merged = *this;
                merged
                    .merge(other)
                    .unwrap_or_else(|_| unreachable!("unable to merge {this:?} with {other:?}"));
                Domain::Number(merged)
            }
            (Domain::Decimal(this), Domain::Decimal(other)) => {
                let mut merged = *this;
                merged
                    .merge(other)
                    .unwrap_or_else(|_| unreachable!("unable to merge {this:?} with {other:?}"));
                Domain::Decimal(merged)
            }
            (Domain::Boolean(this), Domain::Boolean(other)) => {
                let mut merged = *this;
                merged.merge(other);
                Domain::Boolean(merged)
            }
            (Domain::String(this), Domain::String(other)) => Domain::String(StringDomain {
                min: this.min.as_str().min(&other.min).to_string(),
                max: this
                    .max
                    .as_ref()
                    .zip(other.max.as_ref())
                    .map(|(self_max, other_max)| self_max.max(other_max).to_string()),
            }),
            (Domain::Timestamp(this), Domain::Timestamp(other)) => {
                let mut merged = *this;
                merged.merge(other);
                Domain::Timestamp(merged)
            }
            (Domain::Date(this), Domain::Date(other)) => {
                let mut merged = *this;
                merged.merge(other);
                Domain::Date(merged)
            }
            (Domain::Interval(this), Domain::Interval(other)) => {
                let mut merged = *this;
                merged.merge(other);
                Domain::Interval(merged)
            }
            (
                Domain::Nullable(NullableDomain {
                    has_null: true,
                    value: None,
                }),
                Domain::Nullable(NullableDomain {
                    has_null: true,
                    value: None,
                }),
            ) => Domain::Nullable(NullableDomain {
                has_null: true,
                value: None,
            }),
            (
                Domain::Nullable(NullableDomain {
                    has_null: _,
                    value: Some(self_value),
                }),
                Domain::Nullable(NullableDomain {
                    has_null: true,
                    value: None,
                }),
            ) => Domain::Nullable(NullableDomain {
                has_null: true,
                value: Some(self_value.clone()),
            }),
            (
                Domain::Nullable(NullableDomain {
                    has_null: true,
                    value: None,
                }),
                Domain::Nullable(NullableDomain {
                    has_null: _,
                    value: Some(other_value),
                }),
            ) => Domain::Nullable(NullableDomain {
                has_null: true,
                value: Some(other_value.clone()),
            }),
            (
                Domain::Nullable(NullableDomain {
                    has_null: self_has_null,
                    value: Some(self_value),
                }),
                Domain::Nullable(NullableDomain {
                    has_null: other_has_null,
                    value: Some(other_value),
                }),
            ) => Domain::Nullable(NullableDomain {
                has_null: *self_has_null || *other_has_null,
                value: Some(Box::new(self_value.merge(other_value))),
            }),
            (Domain::Array(None), Domain::Array(None)) => Domain::Array(None),
            (Domain::Array(Some(_)), Domain::Array(None)) => self.clone(),
            (Domain::Array(None), Domain::Array(Some(_))) => other.clone(),
            (Domain::Array(Some(self_arr)), Domain::Array(Some(other_arr))) => {
                Domain::Array(Some(Box::new(self_arr.merge(other_arr))))
            }
            (Domain::Map(None), Domain::Map(None)) => Domain::Map(None),
            (Domain::Map(Some(_)), Domain::Map(None)) => self.clone(),
            (Domain::Map(None), Domain::Map(Some(_))) => other.clone(),
            (Domain::Map(Some(self_arr)), Domain::Map(Some(other_arr))) => {
                Domain::Map(Some(Box::new(self_arr.merge(other_arr))))
            }
            (Domain::Tuple(self_tup), Domain::Tuple(other_tup)) => Domain::Tuple(
                self_tup
                    .iter()
                    .zip(other_tup.iter())
                    .map(|(self_tup, other_tup)| self_tup.merge(other_tup))
                    .collect(),
            ),
            (Domain::Undefined, Domain::Undefined) => Domain::Undefined,
            (this, other) => unreachable!("unable to merge {this:?} with {other:?}"),
        }
    }

    pub fn as_singleton(&self) -> Option<Scalar> {
        match self {
            Domain::Number(NumberDomain::Int8(SimpleDomain { min, max })) if min == max => {
                Some(Scalar::Number(NumberScalar::Int8(*min)))
            }
            Domain::Number(NumberDomain::Int16(SimpleDomain { min, max })) if min == max => {
                Some(Scalar::Number(NumberScalar::Int16(*min)))
            }
            Domain::Number(NumberDomain::Int32(SimpleDomain { min, max })) if min == max => {
                Some(Scalar::Number(NumberScalar::Int32(*min)))
            }
            Domain::Number(NumberDomain::Int64(SimpleDomain { min, max })) if min == max => {
                Some(Scalar::Number(NumberScalar::Int64(*min)))
            }
            Domain::Number(NumberDomain::UInt8(SimpleDomain { min, max })) if min == max => {
                Some(Scalar::Number(NumberScalar::UInt8(*min)))
            }
            Domain::Number(NumberDomain::UInt16(SimpleDomain { min, max })) if min == max => {
                Some(Scalar::Number(NumberScalar::UInt16(*min)))
            }
            Domain::Number(NumberDomain::UInt32(SimpleDomain { min, max })) if min == max => {
                Some(Scalar::Number(NumberScalar::UInt32(*min)))
            }
            Domain::Number(NumberDomain::UInt64(SimpleDomain { min, max })) if min == max => {
                Some(Scalar::Number(NumberScalar::UInt64(*min)))
            }
            Domain::Number(NumberDomain::Float32(SimpleDomain { min, max }))
                if min == max && !min.is_nan() =>
            {
                Some(Scalar::Number(NumberScalar::Float32(*min)))
            }
            Domain::Number(NumberDomain::Float64(SimpleDomain { min, max }))
                if min == max && !min.is_nan() =>
            {
                Some(Scalar::Number(NumberScalar::Float64(*min)))
            }
            Domain::Decimal(DecimalDomain::Decimal128(SimpleDomain { min, max }, sz))
                if min == max =>
            {
                Some(Scalar::Decimal(DecimalScalar::Decimal128(*min, *sz)))
            }
            Domain::Decimal(DecimalDomain::Decimal256(SimpleDomain { min, max }, sz))
                if min == max =>
            {
                Some(Scalar::Decimal(DecimalScalar::Decimal256(*min, *sz)))
            }
            Domain::Boolean(BooleanDomain {
                has_false: true,
                has_true: false,
            }) => Some(Scalar::Boolean(false)),
            Domain::Boolean(BooleanDomain {
                has_false: false,
                has_true: true,
            }) => Some(Scalar::Boolean(true)),
            Domain::String(StringDomain { min, max }) if Some(min) == max.as_ref() => {
                Some(Scalar::String(min.clone()))
            }
            Domain::Timestamp(SimpleDomain { min, max }) if min == max => {
                Some(Scalar::Timestamp(*min))
            }
            Domain::TimestampTz(SimpleDomain { min, max }) if min == max => {
                Some(Scalar::TimestampTz(*min))
            }
            Domain::Date(SimpleDomain { min, max }) if min == max => Some(Scalar::Date(*min)),
            Domain::Interval(SimpleDomain { min, max }) if min == max => {
                Some(Scalar::Interval(*min))
            }
            Domain::Nullable(NullableDomain {
                has_null: true,
                value: None,
            }) => Some(Scalar::Null),
            Domain::Nullable(NullableDomain {
                has_null: false,
                value: Some(value),
            }) => value.as_singleton(),
            Domain::Tuple(fields) => Some(Scalar::Tuple(
                fields
                    .iter()
                    .map(|field| field.as_singleton())
                    .collect::<Option<Vec<_>>>()?,
            )),
            _ => None,
        }
    }
}

pub trait SimpleDomainCmp {
    fn domain_eq(&self, other: &Self) -> FunctionDomain<BooleanType>;
    fn domain_noteq(&self, other: &Self) -> FunctionDomain<BooleanType>;
    fn domain_gt(&self, other: &Self) -> FunctionDomain<BooleanType>;
    fn domain_gte(&self, other: &Self) -> FunctionDomain<BooleanType>;
    fn domain_lt(&self, other: &Self) -> FunctionDomain<BooleanType>;
    fn domain_lte(&self, other: &Self) -> FunctionDomain<BooleanType>;
    fn domain_contains(&self, other: &Self) -> FunctionDomain<BooleanType>;
}

const ALL_TRUE_DOMAIN: BooleanDomain = BooleanDomain {
    has_true: true,
    has_false: false,
};

const ALL_FALSE_DOMAIN: BooleanDomain = BooleanDomain {
    has_true: false,
    has_false: true,
};

impl<T: Ord> SimpleDomainCmp for SimpleDomain<T> {
    fn domain_eq(&self, other: &Self) -> FunctionDomain<BooleanType> {
        if self.min > other.max || self.max < other.min {
            FunctionDomain::Domain(ALL_FALSE_DOMAIN)
        } else if self.min == self.max && other.min == other.max && self.min == other.min {
            FunctionDomain::Domain(ALL_TRUE_DOMAIN)
        } else {
            FunctionDomain::Full
        }
    }

    fn domain_noteq(&self, other: &Self) -> FunctionDomain<BooleanType> {
        if self.min > other.max || self.max < other.min {
            FunctionDomain::Domain(ALL_TRUE_DOMAIN)
        } else if self.min == self.max && other.min == other.max && self.min == other.min {
            FunctionDomain::Domain(ALL_FALSE_DOMAIN)
        } else {
            FunctionDomain::Full
        }
    }

    fn domain_gt(&self, other: &Self) -> FunctionDomain<BooleanType> {
        if self.min > other.max {
            FunctionDomain::Domain(ALL_TRUE_DOMAIN)
        } else if self.max <= other.min {
            FunctionDomain::Domain(ALL_FALSE_DOMAIN)
        } else {
            FunctionDomain::Full
        }
    }

    fn domain_gte(&self, other: &Self) -> FunctionDomain<BooleanType> {
        if self.min >= other.max {
            FunctionDomain::Domain(ALL_TRUE_DOMAIN)
        } else if self.max < other.min {
            FunctionDomain::Domain(ALL_FALSE_DOMAIN)
        } else {
            FunctionDomain::Full
        }
    }

    fn domain_lt(&self, other: &Self) -> FunctionDomain<BooleanType> {
        if self.max < other.min {
            FunctionDomain::Domain(ALL_TRUE_DOMAIN)
        } else if self.min >= other.max {
            FunctionDomain::Domain(ALL_FALSE_DOMAIN)
        } else {
            FunctionDomain::Full
        }
    }

    fn domain_lte(&self, other: &Self) -> FunctionDomain<BooleanType> {
        if self.max <= other.min {
            FunctionDomain::Domain(ALL_TRUE_DOMAIN)
        } else if self.min > other.max {
            FunctionDomain::Domain(ALL_FALSE_DOMAIN)
        } else {
            FunctionDomain::Full
        }
    }

    fn domain_contains(&self, other: &Self) -> FunctionDomain<BooleanType> {
        if self.min > other.max || self.max < other.min {
            FunctionDomain::Domain(ALL_FALSE_DOMAIN)
        } else {
            FunctionDomain::Full
        }
    }
}

impl SimpleDomainCmp for StringDomain {
    fn domain_eq(&self, other: &Self) -> FunctionDomain<BooleanType> {
        let (d1, d2) = unify_string(self, other);
        d1.domain_eq(&d2)
    }

    fn domain_noteq(&self, other: &Self) -> FunctionDomain<BooleanType> {
        let (d1, d2) = unify_string(self, other);
        d1.domain_noteq(&d2)
    }

    fn domain_gt(&self, other: &Self) -> FunctionDomain<BooleanType> {
        let (d1, d2) = unify_string(self, other);
        d1.domain_gt(&d2)
    }

    fn domain_gte(&self, other: &Self) -> FunctionDomain<BooleanType> {
        let (d1, d2) = unify_string(self, other);
        d1.domain_gte(&d2)
    }

    fn domain_lt(&self, other: &Self) -> FunctionDomain<BooleanType> {
        let (d1, d2) = unify_string(self, other);
        d1.domain_lt(&d2)
    }

    fn domain_lte(&self, other: &Self) -> FunctionDomain<BooleanType> {
        let (d1, d2) = unify_string(self, other);
        d1.domain_lte(&d2)
    }

    fn domain_contains(&self, other: &Self) -> FunctionDomain<BooleanType> {
        let (d1, d2) = unify_string(self, other);
        d1.domain_contains(&d2)
    }
}

pub fn unify_string(
    lhs: &StringDomain,
    rhs: &StringDomain,
) -> (SimpleDomain<String>, SimpleDomain<String>) {
    let mut max = lhs.min.as_str().max(&rhs.min);
    if let Some(lhs_max) = &lhs.max {
        max = max.max(lhs_max);
    }
    if let Some(rhs_max) = &rhs.max {
        max = max.max(rhs_max);
    }

    let mut max = max.to_string();
    max.push('\0');

    (
        SimpleDomain {
            min: lhs.min.clone(),
            max: lhs.max.clone().unwrap_or_else(|| max.clone()),
        },
        SimpleDomain {
            min: rhs.min.clone(),
            max: rhs.max.clone().unwrap_or_else(|| max.clone()),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_domain_equality_for_singletons() {
        let singleton = SimpleDomain { min: 7, max: 7 };

        assert_eq!(
            singleton.domain_eq(&singleton),
            FunctionDomain::Domain(ALL_TRUE_DOMAIN)
        );
        assert_eq!(
            singleton.domain_noteq(&singleton),
            FunctionDomain::Domain(ALL_FALSE_DOMAIN)
        );
    }

    #[test]
    fn test_domain_equality_keeps_existing_range_results() {
        let lhs = SimpleDomain { min: 1, max: 3 };
        let overlapping = SimpleDomain { min: 2, max: 4 };
        let disjoint = SimpleDomain { min: 5, max: 7 };

        assert_eq!(lhs.domain_eq(&overlapping), FunctionDomain::Full);
        assert_eq!(lhs.domain_noteq(&overlapping), FunctionDomain::Full);
        assert_eq!(
            lhs.domain_eq(&disjoint),
            FunctionDomain::Domain(ALL_FALSE_DOMAIN)
        );
        assert_eq!(
            lhs.domain_noteq(&disjoint),
            FunctionDomain::Domain(ALL_TRUE_DOMAIN)
        );
    }

    #[test]
    fn test_string_domain_equality_for_singletons_and_unbounded_ranges() {
        let singleton = StringDomain {
            min: "databend".to_string(),
            max: Some("databend".to_string()),
        };
        let unbounded = StringDomain {
            min: "".to_string(),
            max: None,
        };

        assert_eq!(
            singleton.domain_eq(&singleton),
            FunctionDomain::Domain(ALL_TRUE_DOMAIN)
        );
        assert_eq!(
            singleton.domain_noteq(&singleton),
            FunctionDomain::Domain(ALL_FALSE_DOMAIN)
        );
        assert_eq!(unbounded.domain_eq(&unbounded), FunctionDomain::Full);
        assert_eq!(unbounded.domain_noteq(&unbounded), FunctionDomain::Full);
    }
}
