use std::io::Write;

use databend_common_exception::Result;
use databend_common_expression::FromData;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::DateType;
use databend_common_expression::types::F32;
use databend_common_expression::types::F64;
use databend_common_expression::types::Float32Type;
use databend_common_expression::types::Float64Type;
use databend_common_expression::types::NullableColumn;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::UInt64Type;
use goldenfile::Mint;

use super::support::AggregationSimulator;
use super::support::assert_v2_direct_matches_serialized;
use super::support::eval_aggregate;
use super::support::eval_v2_aggr;
use super::support::simulate_two_groups_group_by;
use super::support::write_aggregate_expr_case;

fn run_count_distinct_cases(file: &mut impl Write, simulator: impl AggregationSimulator) {
    let columns = [
        (
            "a",
            databend_common_expression::types::number::Int64Type::from_data(vec![4i64, 3, 2, 1])
                .into(),
        ),
        (
            "x_null",
            databend_common_expression::types::number::UInt64Type::from_data_with_validity(
                vec![1u64, 2, 3, 4],
                vec![true, true, false, false],
            )
            .into(),
        ),
        (
            "all_null",
            databend_common_expression::types::number::UInt64Type::from_data_with_validity(
                vec![1u64, 2, 3, 4],
                vec![false, false, false, false],
            )
            .into(),
        ),
        (
            "s",
            StringType::from_data(vec!["abc", "def", "abc", "xyz"]).into(),
        ),
        (
            "s_null",
            StringType::from_data_with_validity(vec!["a", "", "c", "d"], vec![
                true, false, true, true,
            ])
            .into(),
        ),
        ("date_col", DateType::from_data(vec![1, 2, 1, 3]).into()),
        ("ts", TimestampType::from_data(vec![10, 20, 10, 30]).into()),
        (
            "json",
            StringType::from_data(vec![r#"{"k":1}"#, r#"{"k":2}"#, r#"{"k":1}"#, r#"null"#]).into(),
        ),
    ];
    let columns = columns.as_slice();

    write_aggregate_expr_case(file, "count_distinct(null)", columns, simulator, vec![]);
    write_aggregate_expr_case(
        file,
        "count_distinct(null,null)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(file, "count_distinct(1)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "count_distinct(a)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "count_distinct(s)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "count_distinct(date_col)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "count_distinct(ts)", columns, simulator, vec![]);
    write_aggregate_expr_case(
        file,
        "count_distinct(parse_json(json))",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(file, "count_distinct(x_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "count_distinct(x_null,a)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "count_distinct(all_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(
        file,
        "count_distinct(all_null,s)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(file, "count_distinct(s_null,s)", columns, simulator, vec![]);
}

#[test]
fn test_count_distinct() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("count_distinct.txt").unwrap();
    run_count_distinct_cases(file, eval_aggregate);
}

#[test]
fn test_count_distinct_group_by() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("count_distinct_group_by.txt").unwrap();
    run_count_distinct_cases(file, simulate_two_groups_group_by);
}

#[test]
fn test_v2_count_distinct_suffix_names_are_case_insensitive() -> Result<()> {
    let values = NullableColumn::new_column(
        UInt64Type::from_data(vec![10, 20, 10, 40, 20]),
        Bitmap::from([true, false, true, true, true]),
    );
    let entries = [values.into()];

    assert_v2_direct_matches_serialized("COUNT_DISTINCT", &entries, 5)
}

#[test]
fn test_count_distinct_nullable_string_serialized() -> Result<()> {
    let values = StringType::from_data_with_validity(
        vec!["", "duplicate", "ignored", "duplicate", "other", ""],
        vec![true, true, false, true, true, true],
    );
    let entries = [values.into()];
    for serialized in [false, true] {
        let (result, _) = eval_v2_aggr("count_distinct", &entries, 6, serialized)?;
        assert_eq!(result, UInt64Type::from_data(vec![3]));
    }
    Ok(())
}

#[test]
fn test_count_distinct_float_equality_serialized() -> Result<()> {
    let columns = [
        Float32Type::from_data(vec![
            F32::from(-0.0f32),
            F32::from(0.0f32),
            F32::from(f32::from_bits(0x7fc00001)),
            F32::from(f32::from_bits(0xffc00002)),
            F32::from(1.0f32),
            F32::from(1.0f32),
        ]),
        Float64Type::from_data(vec![
            F64::from(-0.0f64),
            F64::from(0.0f64),
            F64::from(f64::from_bits(0x7ff8000000000001)),
            F64::from(f64::from_bits(0xfff8000000000002)),
            F64::from(1.0f64),
            F64::from(1.0f64),
        ]),
    ];
    for column in columns {
        for column in [
            column.clone(),
            NullableColumn::new_column(
                column,
                Bitmap::from([true, true, true, true, false, false]),
            ),
        ] {
            let expected = if matches!(column, databend_common_expression::Column::Nullable(_)) {
                2
            } else {
                3
            };
            for serialized in [false, true] {
                let (result, _) =
                    eval_v2_aggr("count_distinct", &[column.clone().into()], 6, serialized)?;
                assert_eq!(result, UInt64Type::from_data(vec![expected]));
            }
        }
    }
    Ok(())
}

#[test]
fn test_count_distinct_merges_v1_states() -> Result<()> {
    use std::hash::Hasher;

    use databend_common_expression::BlockEntry;
    use databend_common_expression::ColumnBuilder;
    use databend_common_expression::Scalar;
    use databend_common_expression::ScalarRef;
    use databend_common_expression::aggregate_function::*;
    use databend_common_expression::types::BinaryType;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::DecimalScalar;
    use databend_common_expression::types::DecimalSize;
    use databend_common_expression::types::NumberScalar;
    use databend_common_functions::aggregates::AGGR_REGISTRY;
    use siphasher::sip128::Hasher128;
    use siphasher::sip128::SipHasher24;

    let decimal = DecimalSize::new_unchecked(15, 2);
    for (old, new, native) in [
        (
            vec![Scalar::Number(NumberScalar::Int64(1))],
            vec![Scalar::Number(NumberScalar::Int64(2))],
            true,
        ),
        (
            vec![Scalar::Number(NumberScalar::Float64(F64::from(1.0)))],
            vec![Scalar::Number(NumberScalar::Float64(F64::from(2.0)))],
            true,
        ),
        (vec![Scalar::Date(1)], vec![Scalar::Date(2)], true),
        (vec![Scalar::Timestamp(1)], vec![Scalar::Timestamp(2)], true),
        (
            vec![Scalar::String("old".into())],
            vec![Scalar::String("new".into())],
            false,
        ),
        (
            vec![Scalar::Boolean(false)],
            vec![Scalar::Boolean(true)],
            false,
        ),
        (
            vec![Scalar::Decimal(DecimalScalar::Decimal64(1, decimal))],
            vec![Scalar::Decimal(DecimalScalar::Decimal64(2, decimal))],
            false,
        ),
        (
            vec![Scalar::Date(1), Scalar::String("a".into())],
            vec![Scalar::Date(1), Scalar::String("b".into())],
            false,
        ),
    ] {
        let args_type = old
            .iter()
            .map(|value| value.as_ref().infer_data_type())
            .collect::<Vec<_>>();
        let function = AGGR_REGISTRY.resolve(RawAggregateCall {
            name: "count",
            params: &[],
            args_type: &args_type,
            distinct: true,
            order_by: &[],
        })?;
        // Construct the v1 wire representation independently of the new serializer.
        let (legacy, field_type) = if native {
            let mut builder = ColumnBuilder::with_capacity(&args_type[0], 1);
            builder.push(old[0].as_ref());
            (
                Scalar::Array(builder.build()),
                DataType::Array(Box::new(args_type[0].clone())),
            )
        } else if let [Scalar::String(value)] = old.as_slice() {
            let mut hasher = SipHasher24::new();
            hasher.write(value.as_bytes());
            let hash: u128 = hasher.finish128().into();
            let mut bytes = vec![1]; // v1 uvarint set length
            bytes.extend_from_slice(&hash.to_le_bytes());
            (Scalar::Binary(bytes), DataType::Binary)
        } else {
            let bytes = borsh::to_vec(&old)?;
            (
                Scalar::Array(BinaryType::from_data(vec![bytes])),
                DataType::Array(Box::new(DataType::Binary)),
            )
        };
        assert_eq!(
            function.state_data_type(),
            DataType::Tuple(vec![field_type.clone()])
        );
        let owner = AggregateStateOwner::new(vec![function.clone()])?;
        let state = BlockEntry::new_const_column(field_type.clone(), legacy, 1);
        for _ in 0..2 {
            function.merge_serialized(MergeSerializedInput {
                states: owner.state_set(0),
                state: &state,
                filter: None,
            })?;
        }
        let mut builders = [ColumnBuilder::with_capacity(&field_type, 1)];
        function.serialize(SerializeInput {
            states: owner.state_set(0),
            builders: &mut builders,
        })?;
        let [builder] = builders;
        assert_eq!(builder.build_scalar().as_ref(), state.index(0).unwrap());
        let entries = new
            .into_iter()
            .zip(args_type)
            .map(|(value, ty)| BlockEntry::new_const_column(ty, value, 2))
            .collect::<Vec<_>>();
        function.accumulate(AccumulateInput {
            state: owner.state(0),
            columns: entries.as_slice().into(),
            validity: None,
        })?;
        let mut builder = ColumnBuilder::with_capacity(
            &DataType::Number(databend_common_expression::types::NumberDataType::UInt64),
            1,
        );
        function.merge_result(MergeResultInput {
            state: owner.state(0),
            builder: &mut builder,
        })?;
        assert_eq!(
            builder.build_scalar().as_ref(),
            ScalarRef::Number(NumberScalar::UInt64(2))
        );
    }
    Ok(())
}
