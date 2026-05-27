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

use crate::type_check::common_super_type;
use crate::types::DataType;
use crate::types::Decimal;
use crate::types::DecimalSize;
use crate::types::NumberDataType;
use crate::types::i256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConversionClass {
    /// Same logical type, no conversion needed.
    Identity,
    /// Conversion preserves distinct source values in the target type.
    LosslessInjective,
    /// Conversion is deterministic but can lose information or merge values.
    Lossy,
    /// Conversion semantics depend on runtime contents, for example String -> Number.
    ValueDependent,
    /// Conversion is represented by TRY_CAST or may turn failures into NULL.
    TryOnly,
    /// No supported conversion is known.
    Unsupported,
}

impl ConversionClass {
    pub fn is_lossless_injective(&self) -> bool {
        matches!(self, Self::Identity | Self::LosslessInjective)
    }

    pub fn is_safe_for_equality_inference(&self) -> bool {
        self.is_lossless_injective()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommonTypeConversion {
    pub common_type: DataType,
    pub left: ConversionClass,
    pub right: ConversionClass,
}

impl CommonTypeConversion {
    pub fn is_safe_for_equality_inference(&self) -> bool {
        is_type_safe_for_equality_inference(&self.common_type)
            && self.left.is_safe_for_equality_inference()
            && self.right.is_safe_for_equality_inference()
    }
}

impl From<&DataType> for DataType {
    fn from(value: &DataType) -> Self {
        value.clone()
    }
}

pub fn classify_conversion(src: &DataType, dest: &DataType) -> ConversionClass {
    if src == dest {
        return ConversionClass::Identity;
    }

    match (src, dest) {
        (DataType::Null, _) => ConversionClass::LosslessInjective,
        (DataType::Nullable(_), DataType::Null) => ConversionClass::Lossy,
        (_, DataType::Null) => ConversionClass::Unsupported,

        (DataType::Nullable(src), DataType::Nullable(dest)) => classify_conversion(src, dest),
        (DataType::Nullable(_), _) => ConversionClass::Unsupported,
        (src, DataType::Nullable(dest)) => match classify_conversion(src, dest) {
            ConversionClass::Identity => ConversionClass::LosslessInjective,
            class => class,
        },

        (DataType::EmptyArray, DataType::Array(_)) => ConversionClass::LosslessInjective,
        (DataType::EmptyMap, DataType::Map(_)) => ConversionClass::LosslessInjective,

        (DataType::Array(src), DataType::Array(dest))
        | (DataType::Map(src), DataType::Map(dest)) => classify_conversion(src, dest),
        (DataType::Tuple(src), DataType::Tuple(dest)) if src.len() == dest.len() => {
            combine_conversion_classes(
                src.iter()
                    .zip(dest)
                    .map(|(src, dest)| classify_conversion(src, dest)),
            )
        }

        (DataType::Number(src), DataType::Number(dest)) => classify_number_conversion(*src, *dest),
        (DataType::Number(src), DataType::Decimal(dest)) => {
            if let Some(src) = src.get_decimal_properties() {
                match classify_decimal_conversion(src, *dest) {
                    ConversionClass::Identity => ConversionClass::LosslessInjective,
                    class => class,
                }
            } else {
                ConversionClass::Lossy
            }
        }
        (DataType::Decimal(src), DataType::Decimal(dest)) => {
            classify_decimal_conversion(*src, *dest)
        }
        (DataType::Decimal(_), DataType::Number(dest)) if dest.is_float() => ConversionClass::Lossy,
        (DataType::Decimal(_), DataType::Number(_)) => ConversionClass::Lossy,

        (DataType::Boolean, DataType::String | DataType::Number(_) | DataType::Decimal(_)) => {
            ConversionClass::LosslessInjective
        }
        (DataType::Number(_), DataType::String) | (DataType::Decimal(_), DataType::String) => {
            ConversionClass::LosslessInjective
        }

        (DataType::Date, DataType::Timestamp) => ConversionClass::LosslessInjective,
        (DataType::Timestamp, DataType::Date) => ConversionClass::Lossy,

        (DataType::String, dest) if is_value_dependent_string_target(dest) => {
            ConversionClass::ValueDependent
        }
        (DataType::Variant, dest) if is_variant_try_cast_target(dest) => ConversionClass::TryOnly,

        _ => ConversionClass::Unsupported,
    }
}

pub fn common_super_type_with_conversion(
    left: impl Into<DataType>,
    right: impl Into<DataType>,
) -> Option<CommonTypeConversion> {
    let left = left.into();
    let right = right.into();
    let common_type = common_type_for_conversion(left.clone(), right.clone())?;
    let left_class = classify_conversion(&left, &common_type);
    let right_class = classify_conversion(&right, &common_type);

    if matches!(left_class, ConversionClass::Unsupported)
        || matches!(right_class, ConversionClass::Unsupported)
    {
        return None;
    }

    Some(CommonTypeConversion {
        common_type,
        left: left_class,
        right: right_class,
    })
}

fn common_type_for_conversion(left: DataType, right: DataType) -> Option<DataType> {
    match (left, right) {
        (DataType::Null, DataType::Null) => Some(DataType::Null),
        (DataType::Null, ty @ DataType::Nullable(_))
        | (ty @ DataType::Nullable(_), DataType::Null) => Some(ty),
        (DataType::Null, ty) | (ty, DataType::Null) => Some(DataType::Nullable(Box::new(ty))),

        (DataType::Nullable(left), DataType::Nullable(right)) => {
            Some(common_type_for_conversion(*left, *right)?.wrap_nullable())
        }
        (DataType::Nullable(left), right) => {
            Some(common_type_for_conversion(*left, right)?.wrap_nullable())
        }
        (left, DataType::Nullable(right)) => {
            Some(common_type_for_conversion(left, *right)?.wrap_nullable())
        }

        (DataType::EmptyArray, ty @ DataType::Array(_))
        | (ty @ DataType::Array(_), DataType::EmptyArray) => Some(ty),
        (DataType::Array(left), DataType::Array(right)) => Some(DataType::Array(Box::new(
            common_type_for_conversion(*left, *right)?,
        ))),

        (DataType::EmptyMap, ty @ DataType::Map(_))
        | (ty @ DataType::Map(_), DataType::EmptyMap) => Some(ty),
        (DataType::Map(left), DataType::Map(right)) => Some(DataType::Map(Box::new(
            common_type_for_conversion(*left, *right)?,
        ))),

        (DataType::Tuple(left), DataType::Tuple(right)) if left.len() == right.len() => {
            let fields = left
                .into_iter()
                .zip(right)
                .map(|(left, right)| common_type_for_conversion(left, right))
                .collect::<Option<Vec<_>>>()?;
            Some(DataType::Tuple(fields))
        }

        (DataType::Number(left), DataType::Number(right)) => Some(number_common_type(left, right)),

        (left, right) => {
            if let Some(common_type) = common_super_type(left.clone(), right.clone(), &[]) {
                return Some(common_type);
            }

            match (left, right) {
                (DataType::Date, DataType::Timestamp) | (DataType::Timestamp, DataType::Date) => {
                    Some(DataType::Timestamp)
                }

                (DataType::String, ty) if is_value_dependent_string_target(&ty) => Some(ty),
                (ty, DataType::String) if is_value_dependent_string_target(&ty) => Some(ty),

                (DataType::Variant, ty) if is_variant_try_cast_target(&ty) => {
                    Some(ty.wrap_nullable())
                }
                (ty, DataType::Variant) if is_variant_try_cast_target(&ty) => {
                    Some(ty.wrap_nullable())
                }

                _ => None,
            }
        }
    }
}

fn classify_number_conversion(src: NumberDataType, dest: NumberDataType) -> ConversionClass {
    if src == dest {
        return ConversionClass::Identity;
    }

    match (src.is_integer(), dest.is_integer()) {
        (true, true) if src.can_lossless_cast_to(dest) => ConversionClass::LosslessInjective,
        (true, true) => ConversionClass::Lossy,
        (false, false) if src.can_lossless_cast_to(dest) => ConversionClass::LosslessInjective,
        (false, false) => ConversionClass::Lossy,
        (true, false) | (false, true) => ConversionClass::Lossy,
    }
}

fn classify_decimal_conversion(src: DecimalSize, dest: DecimalSize) -> ConversionClass {
    if src == dest {
        return ConversionClass::Identity;
    }

    if src.scale() <= dest.scale() && src.leading_digits() <= dest.leading_digits() {
        ConversionClass::LosslessInjective
    } else {
        ConversionClass::Lossy
    }
}

fn number_common_type(left: NumberDataType, right: NumberDataType) -> DataType {
    if left == right {
        return DataType::Number(left);
    }

    if left.is_integer() && right.is_integer() {
        if left.can_lossless_cast_to(right) {
            return DataType::Number(right);
        }
        if right.can_lossless_cast_to(left) {
            return DataType::Number(left);
        }

        let left = left.get_decimal_properties().unwrap();
        let right = right.get_decimal_properties().unwrap();
        return DataType::Decimal(decimal_common_size(left, right));
    }

    if left.is_float() && right.is_float() {
        if left.can_lossless_cast_to(right) {
            return DataType::Number(right);
        }
        return DataType::Number(left);
    }

    if left.is_float() {
        DataType::Number(left)
    } else {
        DataType::Number(right)
    }
}

fn decimal_common_size(left: DecimalSize, right: DecimalSize) -> DecimalSize {
    let scale = left.scale().max(right.scale());
    let precision = scale + left.leading_digits().max(right.leading_digits());
    let precision =
        if left.precision() <= i128::MAX_PRECISION && right.precision() <= i128::MAX_PRECISION {
            precision.min(i128::MAX_PRECISION)
        } else {
            precision.min(i256::MAX_PRECISION)
        };

    DecimalSize::new_unchecked(precision, scale)
}

fn combine_conversion_classes(
    classes: impl IntoIterator<Item = ConversionClass>,
) -> ConversionClass {
    let mut result = ConversionClass::Identity;
    for class in classes {
        result = match (result, class) {
            (ConversionClass::Unsupported, _) | (_, ConversionClass::Unsupported) => {
                ConversionClass::Unsupported
            }
            (ConversionClass::TryOnly, _) | (_, ConversionClass::TryOnly) => {
                ConversionClass::TryOnly
            }
            (ConversionClass::ValueDependent, _) | (_, ConversionClass::ValueDependent) => {
                ConversionClass::ValueDependent
            }
            (ConversionClass::Lossy, _) | (_, ConversionClass::Lossy) => ConversionClass::Lossy,
            (ConversionClass::LosslessInjective, _) | (_, ConversionClass::LosslessInjective) => {
                ConversionClass::LosslessInjective
            }
            (ConversionClass::Identity, ConversionClass::Identity) => ConversionClass::Identity,
        };
    }
    result
}

fn is_value_dependent_string_target(ty: &DataType) -> bool {
    match ty {
        DataType::Nullable(ty) => is_value_dependent_string_target(ty),
        DataType::Number(_)
        | DataType::Decimal(_)
        | DataType::Boolean
        | DataType::Date
        | DataType::Timestamp
        | DataType::TimestampTz
        | DataType::Interval => true,
        _ => false,
    }
}

fn is_variant_try_cast_target(ty: &DataType) -> bool {
    match ty {
        DataType::Nullable(ty) => is_variant_try_cast_target(ty),
        DataType::Boolean
        | DataType::Date
        | DataType::Timestamp
        | DataType::String
        | DataType::Number(_) => true,
        _ => false,
    }
}

fn is_type_safe_for_equality_inference(ty: &DataType) -> bool {
    !matches!(
        ty.remove_nullable(),
        DataType::Map(_)
            | DataType::EmptyMap
            | DataType::Binary
            | DataType::Geometry
            | DataType::Geography
            | DataType::Vector(_)
            | DataType::Opaque(_)
            | DataType::Generic(_)
            | DataType::StageLocation
    )
}
