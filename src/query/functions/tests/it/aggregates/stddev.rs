use std::io::Write;

use databend_common_exception::Result;
use databend_common_expression::FromData;
use databend_common_expression::ScalarRef;
use databend_common_expression::types::Float64Type;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::types::number::F64;
use goldenfile::Mint;

use super::aggregate_case_support::eval_legacy_aggregate;
use super::aggregate_function_v2_support::eval_v2_aggr;
use super::aggregate_simulation_support::AggregationSimulator;
use super::aggregate_simulation_support::simulate_two_groups_group_by;
use super::aggregate_simulation_support::write_aggregate_expr_case;

fn run_stddev_cases(file: &mut impl Write, simulator: impl AggregationSimulator) {
    let columns = [
        (
            "a",
            databend_common_expression::types::number::Int64Type::from_data(vec![4i64, 3, 2, 1])
                .into(),
        ),
        (
            "f",
            Float64Type::from_data(vec![1.0, 2.5, -3.0, 4.5]).into(),
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
            "one_valid",
            databend_common_expression::types::number::UInt64Type::from_data_with_validity(
                vec![1u64, 2, 3, 4],
                vec![true, false, false, false],
            )
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
    ];
    let columns = columns.as_slice();

    write_aggregate_expr_case(file, "stddev_pop(a)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "std(a)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "stddev_pop(f)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "stddev_pop(dec)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "stddev_pop(NULL)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "stddev_pop(x_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "stddev_pop(all_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "stddev(x_null)", columns, simulator, vec![]);

    write_aggregate_expr_case(file, "stddev_samp(a)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "stddev_samp(NULL)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "stddev_samp(dec)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "stddev_samp(x_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "stddev_samp(one_valid)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "stddev_samp(all_null)", columns, simulator, vec![]);
}

#[test]
fn test_stddev() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("stddev.txt").unwrap();
    run_stddev_cases(file, eval_legacy_aggregate);
}

#[test]
fn test_stddev_group_by() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("stddev_group_by.txt").unwrap();
    run_stddev_cases(file, simulate_two_groups_group_by);
}

#[test]
fn test_v2_stddev_pop_matches_expected_stddev_pop() -> Result<()> {
    let entries = [UInt64Type::from_data(vec![1, 2, 3, 4]).into()];
    let direct_v2 = eval_v2_aggr("stddev_pop", &entries, 4, false)?;
    let serialized_v2 = eval_v2_aggr("stddev_pop", &entries, 4, true)?;

    assert_eq!(
        unsafe { direct_v2.0.index_unchecked(0) },
        ScalarRef::Number(NumberScalar::Float64(F64::from(1.25_f64.sqrt())))
    );
    assert_eq!(serialized_v2, direct_v2);
    Ok(())
}

#[test]
fn test_v2_stddev_samp_matches_expected_stddev_samp() -> Result<()> {
    let entries = [UInt64Type::from_data(vec![1, 2, 3, 4]).into()];
    let direct_v2 = eval_v2_aggr("stddev_samp", &entries, 4, false)?;
    let serialized_v2 = eval_v2_aggr("stddev_samp", &entries, 4, true)?;

    assert_eq!(
        unsafe { direct_v2.0.index_unchecked(0) },
        ScalarRef::Number(NumberScalar::Float64(F64::from((5.0_f64 / 3.0).sqrt())))
    );
    assert_eq!(serialized_v2, direct_v2);
    Ok(())
}
