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

// DO NOT EDIT.
// This crate keeps some legacy codes for compatibility

use std::cmp::Ordering;

use enum_as_inner::EnumAsInner;
use serde::Deserialize;
use serde::Serialize;

use crate::types::decimal::DecimalScalar;
use crate::types::number::NumberScalar;
use crate::Scalar;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, EnumAsInner)]
pub enum SimpleScalar {
    Null,
    Number(NumberScalar),
    Decimal(DecimalScalar),
    Timestamp(i64),
    Date(i32),
    Boolean(bool),
    // For compat reason, we keep this attribute which treat string/binary into string
    #[serde(alias = "String", alias = "Binary")]
    String(Vec<u8>),
}

impl From<SimpleScalar> for Scalar {
    fn from(value: SimpleScalar) -> Self {
        match value {
            SimpleScalar::Null => Scalar::Null,
            SimpleScalar::Number(num_scalar) => Scalar::Number(num_scalar),
            SimpleScalar::Decimal(dec_scalar) => Scalar::Decimal(dec_scalar),
            SimpleScalar::Timestamp(ts) => Scalar::Timestamp(ts),
            SimpleScalar::Date(date) => Scalar::Date(date),
            SimpleScalar::String(s) => Scalar::String(s),
            SimpleScalar::Boolean(v) => Scalar::Boolean(v),
        }
    }
}

impl From<Scalar> for SimpleScalar {
    fn from(value: Scalar) -> Self {
        match value {
            Scalar::Null => SimpleScalar::Null,
            Scalar::Number(num_scalar) => SimpleScalar::Number(num_scalar),
            Scalar::Decimal(dec_scalar) => SimpleScalar::Decimal(dec_scalar),
            Scalar::Timestamp(ts) => SimpleScalar::Timestamp(ts),
            Scalar::Date(date) => SimpleScalar::Date(date),
            Scalar::String(string) => SimpleScalar::String(string),
            Scalar::Boolean(v) => SimpleScalar::Boolean(v),
            other => unreachable!(
                "Cannot convert non-simple scalar to simple scalar {:?}",
                other
            ),
        }
    }
}

impl PartialOrd for SimpleScalar {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (SimpleScalar::Null, SimpleScalar::Null) => Some(Ordering::Equal),
            (SimpleScalar::Number(n1), SimpleScalar::Number(n2)) => n1.partial_cmp(n2),
            (SimpleScalar::Decimal(d1), SimpleScalar::Decimal(d2)) => d1.partial_cmp(d2),
            (SimpleScalar::String(s1), SimpleScalar::String(s2)) => s1.partial_cmp(s2),
            (SimpleScalar::Timestamp(t1), SimpleScalar::Timestamp(t2)) => t1.partial_cmp(t2),
            (SimpleScalar::Date(d1), SimpleScalar::Date(d2)) => d1.partial_cmp(d2),
            (SimpleScalar::Boolean(d1), SimpleScalar::Boolean(d2)) => d1.partial_cmp(d2),
            _ => Some(Ordering::Equal),
        }
    }
}
