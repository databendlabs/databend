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

//! The rules of automatic casts that hold across functions.
//!
//! Type checking casts an argument to the type a function overload takes when a rule allows it.
//! The rules here are shared by many functions, and planner and storage code rely on the casts
//! they allow; which rules a function uses is decided where the function is registered.
//! [`decimal_auto_cast`] is the rule for decimals, whose sizes a list of type pairs cannot hold.

use crate::types::ALL_INTEGER_TYPES;
use crate::types::ALL_NUMERICS_TYPES;
use crate::types::DataType;
use crate::types::Decimal;
use crate::types::NumberDataType;
use crate::types::i256;

/// Whether a decimal rule casts `src` to `dest` automatically, for any function: a string or an
/// integer to a decimal that holds it, a decimal to a wider decimal, and a decimal to Float64.
pub fn decimal_auto_cast(src: &DataType, dest: &DataType) -> bool {
    match (src, dest) {
        (DataType::String, DataType::Decimal(_)) => true,
        (DataType::Decimal(x), DataType::Decimal(y)) => {
            x.scale() <= y.scale()
                && (x.leading_digits() <= y.leading_digits()
                    || y.precision() == i256::MAX_PRECISION)
        }
        (DataType::Number(n), DataType::Decimal(d)) if !n.is_float() => {
            let properties = n.get_decimal_properties().unwrap();
            properties.scale() <= d.scale()
                && properties.precision() - properties.scale() <= d.leading_digits()
        }
        // Only to Float64, otherwise `sqrt(1234.56789)` would have the signature
        // `sqrt(1234.56789::Float32)`.
        (DataType::Decimal(_), DataType::Number(n)) => n.is_float64(),
        _ => false,
    }
}

/// Whether an automatic cast from `src` to `dest` is a `TRY_CAST`: a Variant extracts a value
/// with [`cast_from_variant_rules`], and a value of another JSON type becomes NULL.
pub fn is_auto_try_cast(src: &DataType, dest: &DataType) -> bool {
    src.remove_nullable() == DataType::Variant
        && dest.is_nullable()
        && is_variant_auto_cast_target(dest)
}

/// The types that a Variant extracts automatically, as NULL for a value of another JSON type.
const VARIANT_AUTO_CAST_TARGETS: &[DataType] = &[
    DataType::Boolean,
    DataType::Date,
    DataType::Timestamp,
    DataType::String,
    DataType::Number(NumberDataType::UInt8),
    DataType::Number(NumberDataType::UInt16),
    DataType::Number(NumberDataType::UInt32),
    DataType::Number(NumberDataType::UInt64),
    DataType::Number(NumberDataType::Int8),
    DataType::Number(NumberDataType::Int16),
    DataType::Number(NumberDataType::Int32),
    DataType::Number(NumberDataType::Int64),
    DataType::Number(NumberDataType::Float32),
    DataType::Number(NumberDataType::Float64),
];

/// Whether [`cast_from_variant_rules`] cast a Variant to `dest`, nullable or not.
pub fn is_variant_auto_cast_target(dest: &DataType) -> bool {
    VARIANT_AUTO_CAST_TARGETS.contains(&dest.remove_nullable())
}

/// The cast rules for any situation, including comparison functions, joins, etc. Among numbers,
/// they are the lossless casts of [`NumberDataType::can_lossless_cast_to`], and the lossy casts
/// `UInt64 -> Int64`, `UInt64 -> Float64` and `Int64 -> Float64`. Through the lossy casts,
/// comparing a `UInt64` with an `Int64` casts to `Int64`, unlike the lossless common types of
/// [`crate::conversion::common_super_type_with_conversion`].
pub fn general_cast_rules() -> Vec<(DataType, DataType)> {
    let mut rules = vec![
        (DataType::String, DataType::Binary),
        (DataType::String, DataType::Timestamp),
        (DataType::String, DataType::TimestampTz),
        (DataType::String, DataType::Date),
        (DataType::String, DataType::Interval),
        (DataType::String, DataType::Boolean),
        (DataType::Date, DataType::Timestamp),
    ];
    let lossless = ALL_NUMERICS_TYPES.iter().flat_map(|src| {
        ALL_NUMERICS_TYPES
            .iter()
            .filter(move |dest| src != *dest && src.can_lossless_cast_to(**dest))
            .map(|dest| (*src, *dest))
    });
    let lossy = [
        (NumberDataType::UInt64, NumberDataType::Int64),
        (NumberDataType::UInt64, NumberDataType::Float64),
        (NumberDataType::Int64, NumberDataType::Float64),
    ];
    rules.extend(
        lossless
            .chain(lossy)
            .map(|(src, dest)| (DataType::Number(src), DataType::Number(dest))),
    );
    rules
}

/// The rules for automatic casting from string to other types. For example, they are
/// used to allow `add_hours('2023-01-01 00:00:00', '1')`.
pub fn cast_from_string_rules() -> Vec<(DataType, DataType)> {
    [
        NumberDataType::Int64,
        NumberDataType::UInt64,
        NumberDataType::Float64,
        NumberDataType::Float32,
    ]
    .into_iter()
    .map(|num| (DataType::String, DataType::Number(num)))
    .collect()
}

/// A Variant, nullable or not, extracts a nullable value of [`VARIANT_AUTO_CAST_TARGETS`].
pub fn cast_from_variant_rules() -> Vec<(DataType, DataType)> {
    [DataType::Variant, DataType::Variant.wrap_nullable()]
        .into_iter()
        .flat_map(|src| {
            VARIANT_AUTO_CAST_TARGETS
                .iter()
                .map(move |dest| (src.clone(), dest.wrap_nullable()))
        })
        .collect()
}

/// A signed integer casts to UInt64.
pub fn cast_int_to_uint64_rules() -> Vec<(DataType, DataType)> {
    ALL_INTEGER_TYPES
        .iter()
        .filter(|num| num.is_signed())
        .map(|num| {
            (
                DataType::Number(*num),
                DataType::Number(NumberDataType::UInt64),
            )
        })
        .collect()
}

/// A number casts to Boolean.
pub fn cast_from_number_rules() -> Vec<(DataType, DataType)> {
    ALL_NUMERICS_TYPES
        .iter()
        .map(|num| (DataType::Number(*num), DataType::Boolean))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_variant_auto_cast_targets() {
        let mut targets = vec![
            DataType::Boolean,
            DataType::Date,
            DataType::Timestamp,
            DataType::String,
        ];
        targets.extend(ALL_NUMERICS_TYPES.iter().map(|num| DataType::Number(*num)));
        for target in &targets {
            assert!(is_variant_auto_cast_target(target), "{target}");
            assert!(
                is_variant_auto_cast_target(&target.wrap_nullable()),
                "{target}"
            );
        }
        for other in [
            DataType::Binary,
            DataType::Decimal(Default::default()),
            DataType::TimestampTz,
            DataType::Interval,
            DataType::Variant,
            DataType::Array(Box::new(DataType::String)),
        ] {
            assert!(!is_variant_auto_cast_target(&other), "{other}");
        }
    }
}
