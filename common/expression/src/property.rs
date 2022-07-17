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

use crate::types::AnyType;
use crate::types::ArgType;
use crate::types::BooleanType;
use crate::types::DataType;
use crate::types::GenericMap;
use crate::types::NumberType;
use crate::types::StringType;
use crate::types::ValueType;

#[derive(Clone, Default)]
pub struct FunctionProperty {
    pub commutative: bool,
}

impl FunctionProperty {
    pub fn commutative(mut self, commutative: bool) -> Self {
        self.commutative = commutative;
        self
    }
}

#[derive(Debug, Clone, EnumAsInner)]
pub enum Domain {
    Int(IntDomain),
    UInt(UIntDomain),
    Boolean(BooleanDomain),
    String(StringDomain),
    Nullable(NullableDomain<AnyType>),
    Array(Option<Box<Domain>>),
    Tuple(Vec<Domain>),
}

#[derive(Debug, Clone)]
pub struct IntDomain {
    pub min: i64,
    pub max: i64,
}

#[derive(Debug, Clone)]
pub struct UIntDomain {
    pub min: u64,
    pub max: u64,
}

#[derive(Debug, Clone)]
pub struct BooleanDomain {
    pub has_false: bool,
    pub has_true: bool,
}

#[derive(Debug, Clone)]
pub struct StringDomain {
    pub min: Vec<u8>,
    pub max: Option<Vec<u8>>,
}

pub struct NullableDomain<T: ValueType> {
    pub has_null: bool,
    pub value: Option<Box<T::Domain>>,
}

impl<T: ValueType> Clone for NullableDomain<T> {
    fn clone(&self) -> Self {
        NullableDomain {
            has_null: self.has_null,
            value: self.value.clone(),
        }
    }
}

impl<T: ValueType> std::fmt::Debug for NullableDomain<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NullableDomain")
            .field("has_null", &self.has_null)
            .field("value", &self.value)
            .finish()
    }
}

impl Domain {
    pub fn full(ty: &DataType, generics: &GenericMap) -> Self {
        match ty {
            DataType::Null => Domain::Nullable(NullableDomain {
                has_null: true,
                value: None,
            }),
            DataType::EmptyArray => Domain::Array(None),
            DataType::Int8 => Domain::Int(NumberType::<i8>::full_domain(generics)),
            DataType::Int16 => Domain::Int(NumberType::<i16>::full_domain(generics)),
            DataType::UInt8 => Domain::UInt(NumberType::<u8>::full_domain(generics)),
            DataType::UInt16 => Domain::UInt(NumberType::<u16>::full_domain(generics)),
            DataType::Boolean => Domain::Boolean(BooleanType::full_domain(generics)),
            DataType::String => Domain::String(StringType::full_domain(generics)),
            DataType::Nullable(ty) => Domain::Nullable(NullableDomain {
                has_null: true,
                value: Some(Box::new(Domain::full(ty, generics))),
            }),
            DataType::Tuple(tys) => {
                Domain::Tuple(tys.iter().map(|ty| Domain::full(ty, generics)).collect())
            }
            DataType::Array(ty) => Domain::Array(Some(Box::new(Domain::full(ty, generics)))),
            DataType::Generic(idx) => Domain::full(&generics[*idx], generics),
        }
    }

    pub fn merge(&self, other: &Domain) -> Domain {
        match (self, other) {
            (Domain::Int(self_int), Domain::Int(other_int)) => Domain::Int(IntDomain {
                min: self_int.min.min(other_int.min),
                max: self_int.max.max(other_int.max),
            }),
            (Domain::UInt(self_uint), Domain::UInt(other_uint)) => Domain::UInt(UIntDomain {
                min: self_uint.min.min(other_uint.min),
                max: self_uint.max.max(other_uint.max),
            }),
            (Domain::Boolean(self_bool), Domain::Boolean(other_bool)) => {
                Domain::Boolean(BooleanDomain {
                    has_false: self_bool.has_false || other_bool.has_false,
                    has_true: self_bool.has_true || other_bool.has_true,
                })
            }
            (Domain::String(self_str), Domain::String(other_str)) => Domain::String(StringDomain {
                min: self_str.min.as_slice().min(&other_str.min).to_vec(),
                max: self_str
                    .max
                    .as_ref()
                    .zip(other_str.max.as_ref())
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
            (a, b) => unreachable!("unable to merge {:?} with {:?}", a, b),
        }
    }
}
