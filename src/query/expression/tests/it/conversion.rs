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

use ConversionClass::*;
use DataType::*;
use NumberDataType::*;
use databend_common_expression::conversion::ConversionClass;
use databend_common_expression::conversion::classify_conversion;
use databend_common_expression::conversion::common_super_type_with_conversion;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DecimalSize;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::VectorDataType;
use pretty_assertions::assert_eq;

macro_rules! assert_conversion_matrix {
    ($($name:literal => $ty:expr),+ $(,)?) => {{
        let types = vec![$(($name, $ty)),+];
        for (left_name, left) in &types {
            for (right_name, right) in &types {
                assert_conversion_pair_is_consistent(left_name, left, right_name, right);
            }
        }
    }};
}

#[test]
fn test_identity_for_all_data_type_variants() {
    for (name, ty) in all_data_type_variants() {
        assert_eq!(
            classify_conversion(&ty, &ty),
            Identity,
            "identity conversion for {name}: {ty}"
        );

        let common = common_super_type_with_conversion(&ty, &ty)
            .unwrap_or_else(|| panic!("missing common type for identity {name}: {ty}"));
        assert_eq!(common.common_type, ty, "common type for {name}: {ty}");
        assert_eq!(common.left, Identity, "left class for {name}: {ty}");
        assert_eq!(common.right, Identity, "right class for {name}: {ty}");
    }
}

#[test]
fn test_conversion_matrix_covers_representative_type_pairs() {
    assert_conversion_matrix! {
        "null" => Null,
        "empty_array" => EmptyArray,
        "empty_map" => EmptyMap,
        "boolean" => Boolean,
        "binary" => Binary,
        "string" => String,
        "timestamp" => Timestamp,
        "timestamp_tz" => TimestampTz,
        "date" => Date,
        "bitmap" => Bitmap,
        "variant" => Variant,
        "geometry" => Geometry,
        "interval" => Interval,
        "geography" => Geography,
        "uint8" => number(UInt8),
        "uint16" => number(UInt16),
        "uint32" => number(UInt32),
        "uint64" => number(UInt64),
        "int8" => number(Int8),
        "int16" => number(Int16),
        "int32" => number(Int32),
        "int64" => number(Int64),
        "float32" => number(Float32),
        "float64" => number(Float64),
        "decimal_i64" => decimal(9, 2),
        "decimal_i128" => decimal(38, 4),
        "decimal_i256" => decimal(76, 6),
        "nullable_int64" => nullable(number(Int64)),
        "nullable_string" => nullable(String),
        "array_int64" => array(number(Int64)),
        "array_string" => array(String),
        "map_string_int64" => map(String, number(Int64)),
        "tuple_string_int64" => tuple(vec![String, number(Int64)]),
        "vector_int8" => Vector(VectorDataType::Int8(3)),
        "vector_float32" => Vector(VectorDataType::Float32(3)),
        "opaque" => Opaque(1),
        "generic" => Generic(1),
        "stage_location" => StageLocation,
    };
}

#[test]
fn test_null_and_nullable_conversions() {
    assert_class(Null, String, LosslessInjective);
    assert_class(String, Null, Unsupported);
    assert_class(Null, nullable(String), LosslessInjective);
    assert_class(nullable(String), Null, Lossy);
    assert_class(String, nullable(String), LosslessInjective);
    assert_class(nullable(String), String, Unsupported);

    assert_class(
        nullable(number(Int32)),
        nullable(number(Int64)),
        LosslessInjective,
    );
    assert_class(nullable(number(Int64)), nullable(number(Int32)), Lossy);
    assert_class(nullable(String), nullable(number(Int64)), ValueDependent);
    assert_class(nullable(number(Int32)), number(Int64), Unsupported);

    assert_common(
        Null,
        String,
        nullable(String),
        LosslessInjective,
        LosslessInjective,
    );
    assert_common(
        nullable(number(Int64)),
        number(UInt64),
        nullable(decimal(20, 0)),
        LosslessInjective,
        LosslessInjective,
    );
    assert_common(
        nullable(Null),
        String,
        nullable(String),
        LosslessInjective,
        LosslessInjective,
    );
    assert_common(
        nullable(Null),
        nullable(String),
        nullable(String),
        LosslessInjective,
        Identity,
    );
    assert_common(
        nullable(Variant),
        String,
        nullable(String),
        TryOnly,
        LosslessInjective,
    );
}

#[test]
fn test_number_conversion_classes_cover_all_number_types() {
    for number_ty in all_number_types() {
        assert_class(number(number_ty), number(number_ty), Identity);
    }

    let cases = [
        (UInt8, UInt16, LosslessInjective),
        (UInt16, UInt8, Lossy),
        (UInt8, Int16, LosslessInjective),
        (UInt32, Int64, LosslessInjective),
        (UInt64, Int64, Lossy),
        (Int8, Int16, LosslessInjective),
        (Int64, Int32, Lossy),
        (Int16, UInt16, Lossy),
        (Float32, Float64, LosslessInjective),
        (Float64, Float32, Lossy),
        (Int32, Float64, Lossy),
        (Float32, Int64, Lossy),
    ];

    for (src, dest, class) in cases {
        assert_class(number(src), number(dest), class);
    }
}

#[test]
fn test_number_common_types() {
    assert_common(
        number(UInt8),
        number(UInt16),
        number(UInt16),
        LosslessInjective,
        Identity,
    );
    assert_common(
        number(UInt8),
        number(Int16),
        number(Int16),
        LosslessInjective,
        Identity,
    );
    assert_common(
        number(Int8),
        number(UInt8),
        decimal(3, 0),
        LosslessInjective,
        LosslessInjective,
    );
    assert_common(
        number(Int64),
        number(UInt64),
        decimal(20, 0),
        LosslessInjective,
        LosslessInjective,
    );
    assert_common(
        number(UInt64),
        number(Int64),
        decimal(20, 0),
        LosslessInjective,
        LosslessInjective,
    );
    assert_common(
        number(Float32),
        number(Float64),
        number(Float64),
        LosslessInjective,
        Identity,
    );
    assert_common(
        number(Int64),
        number(Float64),
        number(Float64),
        Lossy,
        Identity,
    );
    assert_common(
        number(Float32),
        number(Int64),
        number(Float32),
        Identity,
        Lossy,
    );
}

#[test]
fn test_decimal_conversion_classes() {
    assert_class(decimal(10, 2), decimal(10, 2), Identity);
    assert_class(decimal(10, 2), decimal(12, 2), LosslessInjective);
    assert_class(decimal(10, 2), decimal(12, 4), LosslessInjective);
    assert_class(decimal(10, 2), decimal(10, 4), Lossy);
    assert_class(decimal(10, 4), decimal(10, 2), Lossy);

    assert_class(number(Int16), decimal(5, 0), LosslessInjective);
    assert_class(number(UInt64), decimal(20, 0), LosslessInjective);
    assert_class(number(Int64), decimal(18, 0), Lossy);
    assert_class(number(Float64), decimal(20, 5), Lossy);
    assert_class(decimal(10, 2), number(Int64), Lossy);
    assert_class(decimal(10, 2), number(Float64), Lossy);
}

#[test]
fn test_decimal_common_types() {
    assert_common(
        decimal(10, 2),
        decimal(12, 4),
        decimal(12, 4),
        LosslessInjective,
        Identity,
    );
    assert_common(
        decimal(12, 4),
        decimal(10, 2),
        decimal(12, 4),
        Identity,
        LosslessInjective,
    );
    assert_common(
        decimal(38, 2),
        decimal(76, 4),
        decimal(76, 4),
        LosslessInjective,
        Identity,
    );
    assert_common(
        number(Int32),
        decimal(12, 2),
        decimal(12, 2),
        LosslessInjective,
        Identity,
    );
    assert_common(
        decimal(10, 2),
        number(Float64),
        number(Float64),
        Lossy,
        Identity,
    );
}

#[test]
fn test_temporal_string_and_variant_conversions() {
    assert_class(Boolean, String, LosslessInjective);
    assert_class(Boolean, number(Int64), LosslessInjective);
    assert_class(Boolean, decimal(1, 0), LosslessInjective);
    assert_class(number(Int64), String, LosslessInjective);
    assert_class(decimal(18, 3), String, LosslessInjective);

    assert_class(Date, Timestamp, LosslessInjective);
    assert_class(Timestamp, Date, Lossy);
    assert_class(Timestamp, TimestampTz, Unsupported);

    for dest in [
        Boolean,
        Date,
        Timestamp,
        TimestampTz,
        Interval,
        number(Int64),
        number(Float64),
        decimal(18, 3),
        nullable(number(Int64)),
    ] {
        assert_class(String, dest, ValueDependent);
    }

    assert_class(String, Variant, Unsupported);
    assert_class(Variant, decimal(18, 3), Unsupported);

    for dest in [
        Boolean,
        Date,
        Timestamp,
        String,
        number(Int64),
        nullable(String),
    ] {
        assert_class(Variant, dest, TryOnly);
    }
}

#[test]
fn test_temporal_string_and_variant_common_types() {
    assert_common(Date, Timestamp, Timestamp, LosslessInjective, Identity);
    assert_common(Timestamp, Date, Timestamp, Identity, LosslessInjective);
    assert_common(String, Boolean, Boolean, ValueDependent, Identity);
    assert_common(Boolean, String, Boolean, Identity, ValueDependent);
    assert_common(
        String,
        number(Int64),
        decimal(38, 5),
        ValueDependent,
        LosslessInjective,
    );
    assert_common(
        number(Float64),
        String,
        number(Float64),
        Identity,
        ValueDependent,
    );
    assert_common(
        Variant,
        String,
        nullable(String),
        TryOnly,
        LosslessInjective,
    );
    assert_common(
        String,
        Variant,
        nullable(String),
        LosslessInjective,
        TryOnly,
    );
    assert_common(
        number(Int64),
        Variant,
        nullable(number(Int64)),
        LosslessInjective,
        TryOnly,
    );
    assert_no_common(Variant, decimal(18, 3));
}

#[test]
fn test_array_map_and_tuple_conversion_classes() {
    assert_class(EmptyArray, array(number(Int64)), LosslessInjective);
    assert_class(EmptyMap, map(String, number(Int64)), LosslessInjective);
    assert_class(
        array(number(Int32)),
        array(number(Int64)),
        LosslessInjective,
    );
    assert_class(array(number(Int64)), array(number(Int32)), Lossy);
    assert_class(array(String), array(Date), ValueDependent);
    assert_class(array(String), array(Binary), Unsupported);

    assert_class(
        map(String, number(Int32)),
        map(String, number(Int64)),
        LosslessInjective,
    );
    assert_class(
        map(String, number(Int64)),
        map(String, number(Int32)),
        Lossy,
    );
    assert_class(
        map(String, number(Int64)),
        map(Date, number(Int64)),
        ValueDependent,
    );
    assert_class(
        map(String, number(Int64)),
        map(Binary, number(Int64)),
        Unsupported,
    );

    assert_class(
        tuple(vec![number(Int32), String]),
        tuple(vec![number(Int64), Date]),
        ValueDependent,
    );
    assert_class(
        tuple(vec![number(Int64), number(Int32)]),
        tuple(vec![number(Int32), number(Int64)]),
        Lossy,
    );
    assert_class(
        tuple(vec![Variant, number(Int32)]),
        tuple(vec![String, number(Int64)]),
        TryOnly,
    );
    assert_class(
        tuple(vec![number(Int32), String]),
        tuple(vec![number(Int64)]),
        Unsupported,
    );
    assert_class(
        tuple(vec![number(Int64), String]),
        tuple(vec![number(Int32), Binary]),
        Unsupported,
    );
}

#[test]
fn test_array_map_and_tuple_common_types() {
    assert_common(
        array(number(Int64)),
        array(number(UInt64)),
        array(decimal(20, 0)),
        LosslessInjective,
        LosslessInjective,
    );
    assert_common(
        map(String, number(Int32)),
        map(String, number(Int64)),
        map(String, number(Int64)),
        LosslessInjective,
        Identity,
    );
    assert_common(
        tuple(vec![String, number(Int64)]),
        tuple(vec![Date, number(UInt64)]),
        tuple(vec![Date, decimal(20, 0)]),
        ValueDependent,
        LosslessInjective,
    );

    assert_no_common(array(String), array(Binary));
    assert_no_common(tuple(vec![String, number(Int64)]), tuple(vec![String]));
}

#[test]
fn test_representative_unsupported_conversions() {
    let cases = [
        (Binary, String),
        (Bitmap, String),
        (Geometry, Geography),
        (
            Vector(VectorDataType::Int8(3)),
            Vector(VectorDataType::Float32(3)),
        ),
        (Opaque(1), Opaque(2)),
        (Generic(1), Generic(2)),
        (StageLocation, String),
    ];

    for (src, dest) in cases {
        assert_class(src, dest, Unsupported);
    }
}

fn assert_class(src: DataType, dest: DataType, expected: ConversionClass) {
    assert_eq!(
        classify_conversion(&src, &dest),
        expected,
        "{src} -> {dest}"
    );
}

fn assert_common(
    left: DataType,
    right: DataType,
    common_type: DataType,
    left_class: ConversionClass,
    right_class: ConversionClass,
) {
    let common = common_super_type_with_conversion(&left, &right)
        .unwrap_or_else(|| panic!("missing common conversion for {left} and {right}"));
    assert_eq!(
        common.common_type, common_type,
        "{left} / {right} common type"
    );
    assert_eq!(common.left, left_class, "{left} / {right} left class");
    assert_eq!(common.right, right_class, "{left} / {right} right class");
}

fn assert_no_common(left: DataType, right: DataType) {
    assert_eq!(
        common_super_type_with_conversion(&left, &right),
        None,
        "{left} / {right}"
    );
}

fn assert_conversion_pair_is_consistent(
    left_name: &str,
    left: &DataType,
    right_name: &str,
    right: &DataType,
) {
    let conversion_class = classify_conversion(left, right);
    if left == right {
        assert_eq!(
            conversion_class, Identity,
            "{left_name} should be identity-compatible with itself"
        );
    }

    if conversion_class == Identity {
        assert_eq!(
            left, right,
            "only equal logical types should classify as identity: {left_name} -> {right_name}"
        );
    }

    match (
        common_super_type_with_conversion(left, right),
        common_super_type_with_conversion(right, left),
    ) {
        (Some(forward), Some(reverse)) => {
            assert_eq!(
                forward.common_type, reverse.common_type,
                "common type should be symmetric for {left_name} and {right_name}"
            );
            assert_eq!(
                forward.left,
                classify_conversion(left, &forward.common_type),
                "left classification should match common type for {left_name} / {right_name}"
            );
            assert_eq!(
                forward.right,
                classify_conversion(right, &forward.common_type),
                "right classification should match common type for {left_name} / {right_name}"
            );
            assert_eq!(
                reverse.left, forward.right,
                "reverse left class should mirror forward right for {left_name} / {right_name}"
            );
            assert_eq!(
                reverse.right, forward.left,
                "reverse right class should mirror forward left for {left_name} / {right_name}"
            );
        }
        (None, None) => {}
        (forward, reverse) => {
            panic!(
                "common conversion should be symmetric for {left_name} and {right_name}: forward={forward:?}, reverse={reverse:?}"
            );
        }
    }
}

fn number(ty: NumberDataType) -> DataType {
    Number(ty)
}

fn decimal(precision: u8, scale: u8) -> DataType {
    Decimal(DecimalSize::new_unchecked(precision, scale))
}

fn nullable(ty: DataType) -> DataType {
    Nullable(Box::new(ty))
}

fn array(ty: DataType) -> DataType {
    Array(Box::new(ty))
}

fn map(key: DataType, value: DataType) -> DataType {
    Map(Box::new(tuple(vec![key, value])))
}

fn tuple(fields: Vec<DataType>) -> DataType {
    Tuple(fields)
}

fn all_number_types() -> [NumberDataType; 10] {
    [
        UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64,
    ]
}

fn all_data_type_variants() -> Vec<(std::string::String, DataType)> {
    let mut types = vec![
        ("null".to_string(), Null),
        ("empty_array".to_string(), EmptyArray),
        ("empty_map".to_string(), EmptyMap),
        ("boolean".to_string(), Boolean),
        ("binary".to_string(), Binary),
        ("string".to_string(), String),
        ("timestamp".to_string(), Timestamp),
        ("timestamp_tz".to_string(), TimestampTz),
        ("date".to_string(), Date),
        ("bitmap".to_string(), Bitmap),
        ("variant".to_string(), Variant),
        ("geometry".to_string(), Geometry),
        ("interval".to_string(), Interval),
        ("geography".to_string(), Geography),
        ("vector_int8".to_string(), Vector(VectorDataType::Int8(3))),
        (
            "vector_float32".to_string(),
            Vector(VectorDataType::Float32(3)),
        ),
        ("opaque".to_string(), Opaque(1)),
        ("generic".to_string(), Generic(1)),
        ("stage_location".to_string(), StageLocation),
        ("decimal_i64".to_string(), decimal(9, 2)),
        ("decimal_i128".to_string(), decimal(38, 4)),
        ("decimal_i256".to_string(), decimal(76, 6)),
        ("nullable".to_string(), nullable(number(Int64))),
        ("array".to_string(), array(number(Int64))),
        ("map".to_string(), map(String, number(Int64))),
        ("tuple".to_string(), tuple(vec![String, number(Int64)])),
    ];

    for number_ty in all_number_types() {
        types.push((format!("{number_ty:?}"), number(number_ty)));
    }

    types
}
