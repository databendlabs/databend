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

//! Key handling for `GROUP BY key ORDER BY COUNT(*) DESC LIMIT n`.
//!
//! [`exact`] holds the per-worker Misra-Gries summary with exact counts used
//! by the adaptive executor. When the summary cannot prove exact counts but
//! can prove a complete candidate set, the executor rescans the input through
//! the scalar function [`IS_TOPK_COUNT_CANDIDATE`], which keeps the rows of
//! the candidate keys and every NULL row, so the NULL group stays exact.

pub mod exact;

use std::borrow::Borrow;
use std::hash::Hash;
use std::io::Write;

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::simple_type::SimpleType;
use databend_common_expression::types::simple_type::SimpleValueType;

/// Scalar function `is_topk_count_candidate(candidates, key)`.
pub const IS_TOPK_COUNT_CANDIDATE: &str = "is_topk_count_candidate";

/// A key type the summary can hash without allocating per row.
pub(crate) trait TopKCountKey: AccessType {
    type Key: Hash
        + Eq
        + Clone
        + Send
        + Sync
        + BorshSerialize
        + BorshDeserialize
        + Borrow<Self::Lookup>
        + 'static;
    type Lookup: ?Sized + Hash + Eq;

    fn lookup<'a>(value: &'a Self::ScalarRef<'_>) -> &'a Self::Lookup;

    fn to_key(value: Self::ScalarRef<'_>) -> Self::Key;

    fn into_scalar(
        key: Self::Key,
        data_type: &databend_common_expression::types::DataType,
    ) -> databend_common_expression::Scalar;
}

impl<T> TopKCountKey for SimpleValueType<T>
where
    T: SimpleType,
    T::Scalar: Hash + Sync + BorshSerialize + BorshDeserialize,
{
    type Key = T::Scalar;
    type Lookup = T::Scalar;

    fn lookup(value: &T::Scalar) -> &T::Scalar {
        value
    }

    fn to_key(value: T::Scalar) -> T::Scalar {
        value
    }

    fn into_scalar(
        key: Self::Key,
        data_type: &databend_common_expression::types::DataType,
    ) -> databend_common_expression::Scalar {
        T::upcast_scalar(key, data_type)
    }
}

impl TopKCountKey for StringType {
    type Key = String;
    type Lookup = str;

    fn lookup<'a>(value: &'a &str) -> &'a str {
        value
    }

    fn to_key(value: &str) -> String {
        value.to_string()
    }

    fn into_scalar(
        key: String,
        _data_type: &databend_common_expression::types::DataType,
    ) -> databend_common_expression::Scalar {
        databend_common_expression::Scalar::String(key)
    }
}

/// Calls `$body` with `$key` bound to the [`TopKCountKey`] type of a
/// non-nullable data type, or evaluates `$otherwise` for unsupported types.
///
/// Keys whose equality differs from their bytes, such as floats, are not
/// supported.
macro_rules! with_topk_count_key_type {
    ($data_type:expr, | $key:ident | $body:expr, $otherwise:expr) => {{
        use databend_common_expression::types::DataType as __DataType;
        use databend_common_expression::types::NumberDataType as __NumberDataType;
        match $data_type {
            __DataType::Number(number) => {
                databend_common_expression::with_integer_mapped_type!(|NUM| match number {
                    __NumberDataType::NUM => {
                        type $key = databend_common_expression::types::NumberType<NUM>;
                        $body
                    }
                    _ => $otherwise,
                })
            }
            __DataType::String => {
                type $key = databend_common_expression::types::StringType;
                $body
            }
            __DataType::Date => {
                type $key = databend_common_expression::types::DateType;
                $body
            }
            __DataType::Timestamp => {
                type $key = databend_common_expression::types::TimestampType;
                $body
            }
            _ => $otherwise,
        }
    }};
}

pub(crate) use with_topk_count_key_type;

/// Returns true if the summary supports keys of `data_type`.
pub fn is_topk_count_key_type(data_type: &databend_common_expression::types::DataType) -> bool {
    with_topk_count_key_type!(data_type.remove_nullable(), |_K| true, false)
}

/// The capacity used for a `LIMIT n` query: `max(4096, 64 * n)`.
pub fn topk_count_capacity(limit: usize) -> usize {
    limit.saturating_mul(64).max(4096)
}

/// Encodes a proven candidate set for [`IS_TOPK_COUNT_CANDIDATE`]; `None` keeps every key.
pub(crate) fn encode_candidates<K: BorshSerialize>(
    candidates: &Option<Vec<K>>,
    writer: &mut impl Write,
) -> std::io::Result<()> {
    candidates.serialize(writer)
}

/// Decodes the output of [`encode_candidates`].
pub(crate) fn decode_candidates<K: BorshDeserialize>(
    mut data: &[u8],
) -> std::io::Result<Option<Vec<K>>> {
    Option::<Vec<K>>::deserialize_reader(&mut data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_candidates() {
        for candidates in [
            None,
            Some(vec![]),
            Some(vec!["a".to_string(), "b".to_string()]),
        ] {
            let mut data = vec![];
            encode_candidates(&candidates, &mut data).unwrap();
            assert_eq!(decode_candidates::<String>(&data).unwrap(), candidates);
        }
    }
}
