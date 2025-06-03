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
// This crate keeps some Index codes for compatibility, it's locked by bincode of meta's v3 version

use databend_common_column::types::months_days_micros;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use enum_as_inner::EnumAsInner;
use serde::Deserialize;
use serde::Serialize;

use crate::types::decimal::DecimalScalar;
use crate::types::number::NumberScalar;
use crate::Scalar;

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, EnumAsInner)]
pub enum IndexScalar {
    Null,
    Number(NumberScalar),
    Decimal(DecimalScalar),
    Timestamp(i64),
    Date(i32),
    Interval(months_days_micros),
    Boolean(bool),
    // For compat reason, we keep this attribute which treat string/binary into string
    #[serde(alias = "String", alias = "Binary")]
    String(Vec<u8>),
    Tuple(Vec<IndexScalar>),
    BinaryV2(Vec<u8>),
    Variant(Vec<u8>),
}

impl TryFrom<IndexScalar> for Scalar {
    type Error = ErrorCode;

    fn try_from(value: IndexScalar) -> Result<Self> {
        Ok(match value {
            IndexScalar::Null => Scalar::Null,
            IndexScalar::Number(num_scalar) => Scalar::Number(num_scalar),
            IndexScalar::Decimal(dec_scalar) => Scalar::Decimal(dec_scalar),
            IndexScalar::Timestamp(ts) => Scalar::Timestamp(ts),
            IndexScalar::Date(date) => Scalar::Date(date),
            IndexScalar::Interval(interval) => Scalar::Interval(interval),
            IndexScalar::Boolean(b) => Scalar::Boolean(b),
            IndexScalar::String(s) => Scalar::String(String::from_utf8(s).map_err(|e| {
                ErrorCode::InvalidUtf8String(format!("invalid utf8 data for string type: {}", e))
            })?),
            IndexScalar::BinaryV2(s) => Scalar::Binary(s),
            IndexScalar::Variant(s) => Scalar::Variant(s),
            IndexScalar::Tuple(tuple) => Scalar::Tuple(
                tuple
                    .into_iter()
                    .map(|c| c.try_into())
                    .collect::<Result<_>>()?,
            ),
        })
    }
}

impl TryFrom<Scalar> for IndexScalar {
    type Error = ErrorCode;

    fn try_from(value: Scalar) -> Result<Self> {
        Ok(match value {
            Scalar::Null => IndexScalar::Null,
            Scalar::Number(num_scalar) => IndexScalar::Number(num_scalar),
            Scalar::Decimal(dec_scalar) => IndexScalar::Decimal(dec_scalar),
            Scalar::Timestamp(ts) => IndexScalar::Timestamp(ts),
            Scalar::Date(date) => IndexScalar::Date(date),
            Scalar::Interval(interval) => IndexScalar::Interval(interval),
            Scalar::Boolean(b) => IndexScalar::Boolean(b),
            Scalar::String(string) => IndexScalar::String(string.as_bytes().to_vec()),
            Scalar::Binary(s) => IndexScalar::BinaryV2(s),
            Scalar::Tuple(tuple) => IndexScalar::Tuple(
                tuple
                    .into_iter()
                    .map(|c| c.try_into())
                    .collect::<Result<_>>()?,
            ),
            Scalar::Array(_)
            | Scalar::Map(_)
            // we only support variant read only
            | Scalar::Variant(_)
            | Scalar::Bitmap(_)
            | Scalar::Geometry(_)
            | Scalar::Geography(_)
            | Scalar::Vector(_)
            | Scalar::EmptyArray
            | Scalar::EmptyMap => return Err(ErrorCode::Unimplemented("Unsupported scalar type")),
        })
    }
}
