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

use enum_as_inner::EnumAsInner;

use crate::types::boolean::BooleanDomain;
use crate::types::nullable::NullableDomain;
use crate::types::number::overflow_cast;
use crate::types::number::Number;
use crate::types::number::NumberDomain;
use crate::types::string::StringDomain;
use crate::types::timestamp::TimestampDomain;
use crate::types::AnyType;
use crate::with_number_type;
use crate::Scalar;

#[derive(Debug, Clone, Default)]
pub struct FunctionProperty {
    pub commutative: bool,
}

impl FunctionProperty {
    pub fn commutative(mut self, commutative: bool) -> Self {
        self.commutative = commutative;
        self
    }
}

#[derive(Debug, Clone, PartialEq, EnumAsInner)]
pub enum Domain {
    Int8(NumberDomain<i8>),
    Int16(NumberDomain<i16>),
    Int32(NumberDomain<i32>),
    Int64(NumberDomain<i64>),
    UInt8(NumberDomain<u8>),
    UInt16(NumberDomain<u16>),
    UInt32(NumberDomain<u32>),
    UInt64(NumberDomain<u64>),
    Float32(NumberDomain<f32>),
    Float64(NumberDomain<f64>),
    Boolean(BooleanDomain),
    String(StringDomain),
    Timestamp(TimestampDomain),
    Nullable(NullableDomain<AnyType>),
    Array(Option<Box<Domain>>),
    Tuple(Vec<Domain>),
    Undefined,
}

impl Domain {
    pub fn merge(&self, other: &Domain) -> Domain {
        match (self, other) {
            (Domain::Boolean(this), Domain::Boolean(other)) => Domain::Boolean(BooleanDomain {
                has_false: this.has_false || other.has_false,
                has_true: this.has_true || other.has_true,
            }),
            (Domain::String(this), Domain::String(other)) => Domain::String(StringDomain {
                min: this.min.as_slice().min(&other.min).to_vec(),
                max: this
                    .max
                    .as_ref()
                    .zip(other.max.as_ref())
                    .map(|(self_max, other_max)| self_max.max(other_max).to_vec()),
            }),
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
            (Domain::Tuple(self_tup), Domain::Tuple(other_tup)) => Domain::Tuple(
                self_tup
                    .iter()
                    .zip(other_tup.iter())
                    .map(|(self_tup, other_tup)| self_tup.merge(other_tup))
                    .collect(),
            ),
            (this, other) => {
                with_number_type!(|TYPE| match (this, other) {
                    (Domain::TYPE(this), Domain::TYPE(other)) => Domain::TYPE(NumberDomain {
                        min: this.min.min(other.min),
                        max: this.max.max(other.max),
                    }),
                    _ => unreachable!("unable to merge {this:?} with {other:?}"),
                })
            }
        }
    }

    pub fn as_singleton(&self) -> Option<Scalar> {
        match self {
            Domain::Int8(NumberDomain { min, max }) if min == max => Some(Scalar::Int8(*min)),
            Domain::Int16(NumberDomain { min, max }) if min == max => Some(Scalar::Int16(*min)),
            Domain::Int32(NumberDomain { min, max }) if min == max => Some(Scalar::Int32(*min)),
            Domain::Int64(NumberDomain { min, max }) if min == max => Some(Scalar::Int64(*min)),
            Domain::UInt8(NumberDomain { min, max }) if min == max => Some(Scalar::UInt8(*min)),
            Domain::UInt16(NumberDomain { min, max }) if min == max => Some(Scalar::UInt16(*min)),
            Domain::UInt32(NumberDomain { min, max }) if min == max => Some(Scalar::UInt32(*min)),
            Domain::UInt64(NumberDomain { min, max }) if min == max => Some(Scalar::UInt64(*min)),
            Domain::Float32(NumberDomain { min, max }) if min == max => Some(Scalar::Float32(*min)),
            Domain::Float64(NumberDomain { min, max }) if min == max => Some(Scalar::Float64(*min)),
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
            Domain::Nullable(NullableDomain {
                has_null: true,
                value: None,
            }) => Some(Scalar::Null),
            Domain::Nullable(NullableDomain {
                has_null: false,
                value: Some(value),
            }) => value.as_singleton(),
            Domain::Array(None) => Some(Scalar::EmptyArray),
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

impl<T: Number> NumberDomain<T> {
    /// Returns the saturating cast domain and a flag denoting whether overflow happened.
    pub fn overflow_cast<U: Number>(&self) -> (NumberDomain<U>, bool) {
        let (min, min_overflowing) = overflow_cast::<T, U>(self.min);
        let (max, max_overflowing) = overflow_cast::<T, U>(self.max);
        (
            NumberDomain { min, max },
            min_overflowing || max_overflowing,
        )
    }
}
