use std::io::Write;

use databend_common_expression::FromData;
use goldenfile::Mint;

use super::aggregate_case_support::eval_legacy_aggregate;
use super::aggregate_simulation_support::AggregationSimulator;
use super::aggregate_simulation_support::simulate_two_groups_group_by;
use super::aggregate_simulation_support::write_aggregate_expr_case;

fn run_window_funnel_cases(file: &mut impl Write, simulator: impl AggregationSimulator) {
    let columns = [
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
            "event2",
            databend_common_expression::types::BooleanType::from_data(vec![
                false, false, false, false,
            ])
            .into(),
        ),
        (
            "event3",
            databend_common_expression::types::BooleanType::from_data(vec![
                false, false, false, false,
            ])
            .into(),
        ),
    ];
    let columns = columns.as_slice();

    write_aggregate_expr_case(
        file,
        "window_funnel(2)(dt, event1, event2, event3)",
        columns,
        simulator,
        vec![],
    );

    let uint64_columns = [
        (
            "dt_uint64",
            databend_common_expression::types::UInt64Type::from_data(vec![0, 1, 2, 3, 4]).into(),
        ),
        (
            "event1_uint64",
            databend_common_expression::types::BooleanType::from_data(vec![
                true, false, false, false, false,
            ])
            .into(),
        ),
        (
            "event2_uint64",
            databend_common_expression::types::BooleanType::from_data(vec![
                false, true, false, false, false,
            ])
            .into(),
        ),
        (
            "event3_uint64",
            databend_common_expression::types::BooleanType::from_data(vec![
                false, false, true, false, false,
            ])
            .into(),
        ),
    ];
    let uint64_columns = uint64_columns.as_slice();

    write_aggregate_expr_case(
        file,
        "window_funnel(2)(dt_uint64, event1_uint64, event2_uint64, event3_uint64)",
        uint64_columns,
        simulator,
        vec![],
    );

    let nullable_columns = [
        (
            "dt_nullable",
            databend_common_expression::types::UInt64Type::from_data_with_validity(
                vec![0, 1, 2, 3, 4],
                vec![true, true, false, true, true],
            )
            .into(),
        ),
        (
            "event1_nullable",
            databend_common_expression::types::BooleanType::from_data_with_validity(
                vec![true, false, false, false, false],
                vec![true, true, true, false, true],
            )
            .into(),
        ),
        (
            "event2",
            databend_common_expression::types::BooleanType::from_data(vec![
                false, true, false, false, false,
            ])
            .into(),
        ),
        (
            "event3_nullable",
            databend_common_expression::types::BooleanType::from_data_with_validity(
                vec![false, false, true, false, true],
                vec![true, true, false, true, true],
            )
            .into(),
        ),
    ];
    let nullable_columns = nullable_columns.as_slice();

    write_aggregate_expr_case(
        file,
        "window_funnel(2)(dt_nullable, event1_nullable, event2, event3_nullable)",
        nullable_columns,
        simulator,
        vec![],
    );

    let typed_columns = [
        (
            "dt_date",
            databend_common_expression::types::DateType::from_data(vec![0, 1, 2, 3]).into(),
        ),
        (
            "date_event1",
            databend_common_expression::types::BooleanType::from_data(vec![
                true, false, false, false,
            ])
            .into(),
        ),
        (
            "date_event2",
            databend_common_expression::types::BooleanType::from_data(vec![
                false, true, false, false,
            ])
            .into(),
        ),
        (
            "date_event3",
            databend_common_expression::types::BooleanType::from_data(vec![
                false, false, true, false,
            ])
            .into(),
        ),
        (
            "dt_int64",
            databend_common_expression::types::Int64Type::from_data(vec![0i64, 1, 2, 3]).into(),
        ),
        (
            "int64_event1",
            databend_common_expression::types::BooleanType::from_data(vec![
                true, false, false, false,
            ])
            .into(),
        ),
        (
            "int64_event2",
            databend_common_expression::types::BooleanType::from_data(vec![
                false, true, false, false,
            ])
            .into(),
        ),
        (
            "int64_event3",
            databend_common_expression::types::BooleanType::from_data(vec![
                false, false, true, false,
            ])
            .into(),
        ),
        (
            "dt_unsorted",
            databend_common_expression::types::UInt64Type::from_data(vec![2, 0, 1, 3]).into(),
        ),
        (
            "unsorted_event1",
            databend_common_expression::types::BooleanType::from_data(vec![
                false, true, false, false,
            ])
            .into(),
        ),
        (
            "unsorted_event2",
            databend_common_expression::types::BooleanType::from_data(vec![
                false, false, true, false,
            ])
            .into(),
        ),
        (
            "unsorted_event3",
            databend_common_expression::types::BooleanType::from_data(vec![
                true, false, false, false,
            ])
            .into(),
        ),
        (
            "single_event",
            databend_common_expression::types::BooleanType::from_data(vec![
                false, true, false, false,
            ])
            .into(),
        ),
        (
            "no_event",
            databend_common_expression::types::BooleanType::from_data(vec![
                false, false, false, false,
            ])
            .into(),
        ),
    ];
    let typed_columns = typed_columns.as_slice();

    write_aggregate_expr_case(
        file,
        "window_funnel(2)(dt_date, date_event1, date_event2, date_event3)",
        typed_columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "window_funnel(2)(dt_int64, int64_event1, int64_event2, int64_event3)",
        typed_columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "window_funnel(2)(dt_unsorted, unsorted_event1, unsorted_event2, unsorted_event3)",
        typed_columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "window_funnel(2)(dt_int64, single_event)",
        typed_columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "window_funnel(2)(dt_int64, no_event)",
        typed_columns,
        simulator,
        vec![],
    );
}

#[test]
fn test_window_funnel() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("window_funnel.txt").unwrap();
    run_window_funnel_cases(file, eval_legacy_aggregate);
}

#[test]
fn test_window_funnel_group_by() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("window_funnel_group_by.txt").unwrap();
    run_window_funnel_cases(file, simulate_two_groups_group_by);
}
