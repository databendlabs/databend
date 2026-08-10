use std::io::Write;

use databend_common_expression::BlockEntry;
use databend_common_expression::FromData;
use databend_common_expression::types::Decimal64Type;
use databend_common_expression::types::DecimalSize;
use databend_common_expression::types::Float64Type;
use databend_common_expression::types::NullableType;
use databend_common_expression::types::UInt64Type;
use goldenfile::Mint;

use super::aggregate_case_support::eval_legacy_aggregate;
use super::aggregate_simulation_support::AggregationSimulator;
use super::aggregate_simulation_support::simulate_two_groups_group_by;
use super::aggregate_simulation_support::write_aggregate_expr_case;

fn run_group_array_moving_avg_cases(file: &mut impl Write, simulator: impl AggregationSimulator) {
    let columns = [
        (
            "a",
            databend_common_expression::types::number::Int64Type::from_data(vec![4i64, 3, 2, 1])
                .into(),
        ),
        (
            "b",
            databend_common_expression::types::number::UInt64Type::from_data(vec![1u64, 2, 3, 4])
                .into(),
        ),
        (
            "f",
            Float64Type::from_data(vec![1.0, 2.5, -3.0, 4.5]).into(),
        ),
        (
            "const_null",
            BlockEntry::new_const_column_arg::<NullableType<UInt64Type>>(None, 4),
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
            "y_null",
            databend_common_expression::types::number::UInt64Type::from_data_with_validity(
                vec![1u64, 2, 3, 4],
                vec![false, false, true, true],
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
            "dec",
            Decimal64Type::from_opt_data_with_size(
                vec![Some(110), Some(220), None, Some(330)],
                Some(DecimalSize::new_unchecked(15, 2)),
            )
            .into(),
        ),
        (
            "dec_all_null",
            Decimal64Type::from_opt_data_with_size(
                vec![None, None, None, None],
                Some(DecimalSize::new_unchecked(15, 2)),
            )
            .into(),
        ),
    ];
    let columns = columns.as_slice();

    write_aggregate_expr_case(
        file,
        "group_array_moving_avg(1)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "group_array_moving_avg('a')",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "group_array_moving_avg(NULL)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "group_array_moving_avg(a)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "group_array_moving_avg(10)(a)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "group_array_moving_avg(2)(b)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "group_array_moving_avg(2)(f)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "group_array_moving_avg(const_null)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "group_array_moving_avg(x_null)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "group_array_moving_avg(all_null)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "group_array_moving_avg(1)(y_null)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "group_array_moving_avg(dec)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "group_array_moving_avg(2)(dec)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "group_array_moving_avg(2)(dec_all_null)",
        columns,
        simulator,
        vec![],
    );
}

#[test]
fn test_group_array_moving_avg() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("group_array_moving_avg.txt").unwrap();
    run_group_array_moving_avg_cases(file, eval_legacy_aggregate);
}

#[test]
fn test_group_array_moving_avg_group_by() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint
        .new_goldenfile("group_array_moving_avg_group_by.txt")
        .unwrap();
    run_group_array_moving_avg_cases(file, simulate_two_groups_group_by);
}
