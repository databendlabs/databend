use std::io::Write;

use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::FromData;
use databend_common_expression::Symbol;
use databend_common_expression::aggregate_function::AggregateBoundOrderByItem;
use databend_common_expression::aggregate_function::AggregateBoundOrderBySource;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::ArrayColumn;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::Buffer;
use databend_common_expression::types::DataType;
use databend_common_expression::types::UInt64Type;
use goldenfile::Mint;

use super::aggregate_case_fixtures as fixtures;
use super::aggregate_case_support::eval_aggregate;
use super::aggregate_function_v2_support::eval_v2_aggr;
use super::aggregate_simulation_support::AggregationSimulator;
use super::aggregate_simulation_support::simulate_two_groups_group_by;
use super::aggregate_simulation_support::write_aggregate_expr_case;

fn run_array_agg_cases(file: &mut impl Write, simulator: impl AggregationSimulator) {
    let mut columns: Vec<(&str, databend_common_expression::BlockEntry)> = vec![
        (
            "a",
            databend_common_expression::types::number::Int64Type::from_data(vec![4i64, 3, 2, 1])
                .into(),
        ),
        (
            "const_int",
            databend_common_expression::BlockEntry::new_const_column_arg::<
                databend_common_expression::types::Int32Type,
            >(5, 4),
        ),
        (
            "const_int_null",
            databend_common_expression::BlockEntry::new_const_column_arg::<
                databend_common_expression::types::NullableType<
                    databend_common_expression::types::Int32Type,
                >,
            >(None, 4),
        ),
        (
            "b",
            databend_common_expression::types::number::UInt64Type::from_data(vec![1u64, 2, 3, 4])
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
            "date_col",
            databend_common_expression::types::DateType::from_data(vec![1, 2, 1, 3]).into(),
        ),
        (
            "dt",
            databend_common_expression::types::TimestampType::from_data(vec![1i64, 0, 2, 3]).into(),
        ),
        (
            "event1",
            databend_common_expression::types::BooleanType::from_data(vec![
                true, false, false, false,
            ])
            .into(),
        ),
        (
            "dec",
            databend_common_expression::types::Decimal64Type::from_opt_data_with_size(
                vec![Some(110), Some(220), None, Some(330)],
                Some(databend_common_expression::types::DecimalSize::new_unchecked(15, 2)),
            )
            .into(),
        ),
        (
            "s",
            databend_common_expression::types::StringType::from_data(vec![
                "abc", "def", "opq", "xyz",
            ])
            .into(),
        ),
        (
            "s_null",
            databend_common_expression::types::StringType::from_data_with_validity(
                vec!["abc", "", "opq", "xyz"],
                vec![true, false, true, true],
            )
            .into(),
        ),
        (
            "json",
            databend_common_expression::types::StringType::from_data(vec![
                r#"{"k1":"v1","k2":"v2"}"#,
                r#"[1,2,3,"abc"]"#,
                r#"99999"#,
                r#""xyz""#,
            ])
            .into(),
        ),
    ];
    columns.push(("bitmap", fixtures::bitmap_column().into()));
    columns.extend(fixtures::geometry_columns());
    let columns = columns.as_slice();

    write_aggregate_expr_case(file, "array_agg(1)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "array_agg(const_int)", columns, simulator, vec![]);
    write_aggregate_expr_case(
        file,
        "array_agg(const_int_null)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(file, "array_agg('a')", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "array_agg(NULL)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "array_agg([])", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "array_agg({})", columns, simulator, vec![]);
    write_aggregate_expr_case(
        file,
        "array_agg(if(event1, [], NULL))",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "array_agg(if(event1, {}, NULL))",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(file, "array_agg(a)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "array_agg_state(x_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "array_agg(a)", columns, simulator, vec![
        AggregateBoundOrderByItem {
            index: Symbol::new(0),
            source: AggregateBoundOrderBySource::Argument { index: 0 },
            data_type: columns[0].1.data_type(),
            nulls_first: false,
            asc: true,
        },
    ]);
    write_aggregate_expr_case(file, "array_agg(b)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "array_agg(x_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "array_agg(all_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "list(a)", columns, simulator, vec![
        AggregateBoundOrderByItem {
            index: Symbol::new(0),
            source: AggregateBoundOrderBySource::Argument { index: 0 },
            data_type: columns[0].1.data_type(),
            nulls_first: false,
            asc: true,
        },
    ]);
    write_aggregate_expr_case(file, "array_agg(date_col)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "array_agg(dt)", columns, simulator, vec![]);
    write_aggregate_expr_case(
        file,
        "array_agg(to_interval('1 day'))",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(file, "array_agg(event1)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "array_agg(dec)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "array_agg(s)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "array_agg(s_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "array_agg(to_binary(s))", columns, simulator, vec![]);
    write_aggregate_expr_case(
        file,
        "array_agg(to_binary(s_null))",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(file, "array_agg(bitmap)", columns, simulator, vec![]);
    write_aggregate_expr_case(
        file,
        "array_agg(parse_json(json))",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "array_agg(to_geometry(point))",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "array_agg(to_geography(point_4326))",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(file, "array_agg([b, b])", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "array_agg(map([s], [a]))", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "array_agg((a, s))", columns, simulator, vec![]);
}

#[test]
fn test_array_agg() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("array_agg.txt").unwrap();
    run_array_agg_cases(file, eval_aggregate);
}

#[test]
fn test_array_agg_group_by() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("array_agg_group_by.txt").unwrap();
    run_array_agg_cases(file, simulate_two_groups_group_by);
}

#[test]
fn test_v2_array_agg_if_applies_predicate() -> Result<()> {
    let entries = [
        UInt64Type::from_data(vec![1, 2, 3, 4]).into(),
        BooleanType::from_data(vec![true, false, false, false]).into(),
    ];
    let return_type = DataType::Array(Box::new(UInt64Type::data_type()));
    let expected = (
        Column::Array(Box::new(
            ArrayColumn::<UInt64Type>::new(Buffer::from(vec![1u64]), Buffer::from(vec![0, 1]))
                .upcast(&return_type),
        )),
        return_type,
    );

    let direct_v2 = eval_v2_aggr("array_agg_if", &entries, 4, false)?;
    let serialized_v2 = eval_v2_aggr("array_agg_if", &entries, 4, true)?;
    assert_eq!(direct_v2, expected);
    assert_eq!(serialized_v2, expected);
    Ok(())
}
