// Copyright 2026 Datafuse Labs.
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

use databend_common_column::bitmap::Bitmap;
use databend_common_expression::Column;
use databend_common_expression::ColumnMinMax;
use databend_common_expression::Domain;
use databend_common_expression::FromData;
use databend_common_expression::MinMax;
use databend_common_expression::Scalar;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::ArrayColumn;
use databend_common_expression::types::BinaryType;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::SimpleDomain;
use databend_common_expression::types::StringType;
use databend_common_expression::types::nullable::NullableColumn;
use databend_common_expression::types::nullable::NullableDomain;
use databend_common_expression::types::number::NumberDomain;

#[test]
fn test_nullable_column_min_max_excludes_null_placeholders() {
    let column = NullableColumn::new_column(
        Int32Type::from_data(vec![99, 4, 1]),
        Bitmap::from([false, true, true]),
    );

    let min_max = column.min_max().unwrap().into_option().unwrap();
    assert!(min_max.has_null());
    assert_eq!(min_max.scalars().0, 1i32.into());
    assert_eq!(min_max.scalars().1, 4i32.into());
}

#[test]
fn test_nullable_column_min_max_filters_without_materializing() {
    let boolean = BooleanType::from_opt_data(vec![None, Some(true), Some(true)]);
    let boolean = boolean.min_max().unwrap().into_option().unwrap();
    assert_eq!(
        boolean.scalars(),
        (Scalar::Boolean(true), Scalar::Boolean(true))
    );
    assert!(boolean.has_null());

    let boolean = BooleanType::from_opt_data(vec![Some(false), None, Some(true)]);
    let boolean = boolean.min_max().unwrap().into_option().unwrap();
    assert_eq!(
        boolean.scalars(),
        (Scalar::Boolean(false), Scalar::Boolean(true))
    );
    assert!(boolean.has_null());

    let string = StringType::from_opt_data(vec![Some("z"), None, Some("a")]);
    let string = string.min_max().unwrap().into_option().unwrap();
    assert_eq!(
        string.scalars(),
        (
            Scalar::String("a".to_string()),
            Scalar::String("z".to_string())
        )
    );
    assert!(string.has_null());
}

#[test]
fn test_nullable_array_domain_excludes_values_from_null_rows() {
    let array = Column::Array(Box::new(ArrayColumn::<AnyType>::new(
        Int32Type::from_data(vec![99, 100, 1, 4]),
        vec![0_u64, 2, 4].into(),
    )));
    let nullable = NullableColumn::new_column(array, Bitmap::from([false, true]));

    assert_eq!(
        nullable.domain(),
        Domain::Nullable(NullableDomain {
            has_null: true,
            value: Some(Box::new(Domain::Array(Some(Box::new(Domain::Number(
                NumberDomain::Int32(SimpleDomain { min: 1, max: 4 }),
            )))))),
        })
    );
}

#[test]
fn test_nullable_domain_preserves_legacy_empty_inner_boundaries() {
    let int32_type = DataType::Number(NumberDataType::Int32);
    let full_int32 = Domain::full(&int32_type);

    let all_null = NullableColumn::new_column(
        Int32Type::from_data(vec![9, 1]),
        Bitmap::from([false, false]),
    );
    assert_eq!(
        all_null.domain(),
        Domain::Nullable(NullableDomain {
            has_null: true,
            value: Some(Box::new(full_int32.clone())),
        })
    );

    let values_only_in_null_row = NullableColumn::new_column(
        Column::Array(Box::new(ArrayColumn::<AnyType>::new(
            Int32Type::from_data(vec![9, 1]),
            vec![0_u64, 2, 2].into(),
        ))),
        Bitmap::from([false, true]),
    );
    assert_eq!(
        values_only_in_null_row.domain(),
        Domain::Nullable(NullableDomain {
            has_null: true,
            value: Some(Box::new(Domain::Array(Some(Box::new(full_int32))))),
        })
    );
}

#[test]
fn test_nullable_tuple_domain_uses_only_parent_visible_rows() {
    let column = NullableColumn::new_column(
        Column::Tuple(vec![NullableColumn::new_column(
            Int32Type::from_data(vec![99, 1, 4]),
            Bitmap::from([false, true, false]),
        )]),
        Bitmap::from([false, true, true]),
    );

    assert_eq!(
        column.domain(),
        Domain::Nullable(NullableDomain {
            has_null: true,
            value: Some(Box::new(Domain::Tuple(vec![Domain::Nullable(
                NullableDomain {
                    has_null: true,
                    value: Some(Box::new(Domain::Number(NumberDomain::Int32(
                        SimpleDomain { min: 1, max: 1 },
                    )))),
                },
            )]))),
        })
    );
}

#[test]
fn test_nullable_sliced_array_domain_uses_underlying_offsets() {
    let array = Column::Array(Box::new(
        ArrayColumn::<AnyType>::new(
            Int32Type::from_data(vec![99, 98, 4, 1, 97]),
            vec![0_u64, 2, 4, 5].into(),
        )
        .slice(1..3),
    ));
    let column = NullableColumn::new_column(array, Bitmap::from([true, false]));

    assert_eq!(
        column.domain(),
        Domain::Nullable(NullableDomain {
            has_null: true,
            value: Some(Box::new(Domain::Array(Some(Box::new(Domain::Number(
                NumberDomain::Int32(SimpleDomain { min: 1, max: 4 }),
            )))))),
        })
    );
}

#[test]
fn test_empty_tuple_domain_remains_full() {
    let column = Column::Tuple(vec![Column::Array(Box::new(ArrayColumn::<AnyType>::new(
        Int32Type::from_data(vec![]),
        vec![0_u64].into(),
    )))]);

    assert_eq!(column.domain(), Domain::full(&column.data_type()));
}

#[test]
fn test_empty_and_all_null_min_max_are_distinct_outer_states() {
    assert_eq!(
        Int32Type::from_data(vec![]).min_max().unwrap(),
        ColumnMinMax::Empty
    );

    let all_null = NullableColumn::new_column(
        Int32Type::from_data(vec![7, 8]),
        Bitmap::from([false, false]),
    )
    .min_max()
    .unwrap();
    assert_eq!(all_null, ColumnMinMax::AllNull);

    let values = Int32Type::from_data(vec![3, 5])
        .min_max()
        .unwrap()
        .into_option()
        .unwrap();
    assert!(!values.has_null());
    assert_eq!(values.scalars().0, 3i32.into());
    assert_eq!(values.scalars().1, 5i32.into());

    assert!(matches!(
        values,
        MinMax::Number(NumberDomain::Int32(_), false)
    ));
}

#[test]
fn test_min_max_rejects_unsupported_type() {
    let binary = BinaryType::from_data(vec![b"a".as_slice(), b"b".as_slice()]);
    assert!(binary.min_max().is_err());
}

#[test]
fn test_min_max_merge_and_serde_reuse_typed_range() {
    let mut lhs = MinMax::Number(NumberDomain::Int32(SimpleDomain { min: 3, max: 5 }), false);
    let rhs = MinMax::Number(NumberDomain::Int32(SimpleDomain { min: 1, max: 4 }), true);

    lhs.merge(&rhs).unwrap();
    assert_eq!(lhs.scalars(), (1i32.into(), 5i32.into()));
    assert!(lhs.has_null());

    let encoded = serde_json::to_vec(&lhs).unwrap();
    let decoded: MinMax = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(decoded, lhs);
}

#[test]
fn test_min_max_merge_rejects_incompatible_other() {
    let mut lhs = MinMax::Number(NumberDomain::Int32(SimpleDomain { min: 3, max: 5 }), false);
    let original = lhs.clone();
    let rhs = MinMax::Number(NumberDomain::Int64(SimpleDomain { min: 1, max: 4 }), true);

    assert!(lhs.merge(&rhs).is_err());
    assert_eq!(lhs, original);
}

#[test]
fn test_column_min_max_merge_preserves_all_null_state() {
    let mut merged = ColumnMinMax::Empty;
    merged.merge(&ColumnMinMax::AllNull).unwrap();
    merged
        .merge(&ColumnMinMax::Values(MinMax::Number(
            NumberDomain::Int32(SimpleDomain { min: 3, max: 5 }),
            false,
        )))
        .unwrap();

    let min_max = merged.into_option().unwrap();
    assert!(min_max.has_null());
    assert_eq!(min_max.scalars(), (3i32.into(), 5i32.into()));
}
