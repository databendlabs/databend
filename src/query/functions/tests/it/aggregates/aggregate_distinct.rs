use std::io::Write;

use databend_common_exception::Result;
use databend_common_expression::FromData;
use databend_common_expression::aggregate_function::DistinctPolicy;
use databend_common_expression::aggregate_function::RawAggregateCall;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_functions::aggregates::AGGR_REGISTRY;
use goldenfile::Mint;

use super::aggregate_case_support::eval_aggregate;
use super::aggregate_simulation_support::AggregationSimulator;
use super::aggregate_simulation_support::simulate_two_groups_group_by;
use super::aggregate_simulation_support::write_aggregate_expr_case;

fn run_aggregate_distinct_cases(file: &mut impl Write, simulator: impl AggregationSimulator) {
    let columns = [
        (
            "a",
            databend_common_expression::types::number::Int64Type::from_data(vec![4i64, 3, 2, 1])
                .into(),
        ),
        (
            "c",
            databend_common_expression::types::number::UInt64Type::from_data(vec![1u64, 2, 1, 3])
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
    ];
    let columns = columns.as_slice();

    write_aggregate_expr_case(file, "sum_distinct(a)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "sum_distinct(c)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "sum_distinct(x_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "sum_distinct(all_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "avg_distinct(c)", columns, simulator, vec![]);
}

#[test]
fn test_aggregate_distinct() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("aggregate_distinct.txt").unwrap();
    run_aggregate_distinct_cases(file, eval_aggregate);
}

#[test]
fn test_aggregate_distinct_group_by() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint
        .new_goldenfile("aggregate_distinct_group_by.txt")
        .unwrap();
    run_aggregate_distinct_cases(file, simulate_two_groups_group_by);
}

#[test]
fn test_semantic_distinct_resolves_visible_target_name() -> Result<()> {
    let args_type = [DataType::Number(NumberDataType::UInt64)];
    for (base, target) in [
        ("count", "count_distinct"),
        ("sum", "sum_distinct"),
        ("avg", "avg_distinct"),
        ("SUM_ZERO", "sum_zero_distinct"),
    ] {
        assert_eq!(
            AGGR_REGISTRY
                .descriptor(base)
                .unwrap()
                .features()
                .distinct_policy
                .target_for(base),
            Some(target)
        );
        let semantic = AGGR_REGISTRY.resolve(RawAggregateCall {
            name: base,
            params: &[],
            args_type: &args_type,
            distinct: true,
            order_by: &[],
        })?;
        assert_eq!(semantic.signature().name, target);
        assert!(!semantic.signature().distinct);

        let explicit = AGGR_REGISTRY.resolve(RawAggregateCall {
            name: target,
            params: &[],
            args_type: &args_type,
            distinct: false,
            order_by: &[],
        })?;
        assert_eq!(explicit.signature().name, target);
    }

    assert_eq!(
        AGGR_REGISTRY
            .descriptor("min")
            .unwrap()
            .features()
            .distinct_policy,
        DistinctPolicy::Idempotent
    );
    let min = AGGR_REGISTRY.resolve(RawAggregateCall {
        name: "min",
        params: &[],
        args_type: &args_type,
        distinct: true,
        order_by: &[],
    })?;
    assert_eq!(min.signature().name, "min");
    assert!(!min.signature().distinct);

    let explicit_min_distinct = AGGR_REGISTRY.resolve(RawAggregateCall {
        name: "min_distinct",
        params: &[],
        args_type: &args_type,
        distinct: false,
        order_by: &[],
    })?;
    assert_eq!(explicit_min_distinct.signature().name, "min_distinct");

    for intrinsic_name in ["uniq", "approx_count_distinct"] {
        assert!(
            AGGR_REGISTRY
                .resolve(RawAggregateCall {
                    name: intrinsic_name,
                    params: &[],
                    args_type: &args_type,
                    distinct: true,
                    order_by: &[],
                })
                .is_err()
        );
    }

    let count_multiple_args = AGGR_REGISTRY.resolve(RawAggregateCall {
        name: "count",
        params: &[],
        args_type: &[args_type[0].clone(), args_type[0].clone()],
        distinct: true,
        order_by: &[],
    })?;
    assert_eq!(count_multiple_args.signature().name, "count_distinct");
    Ok(())
}
