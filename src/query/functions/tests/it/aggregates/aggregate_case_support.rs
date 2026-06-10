use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::Scalar;
use databend_common_expression::types::DataType;
use databend_common_functions::aggregates::AggregateFunctionSortDesc;

use super::aggregate_function_v2_support::assert_v2_aggr_matches_legacy_result;
use super::aggregate_simulation_support::eval_legacy_aggregate_for_test;

pub(super) fn eval_legacy_aggregate(
    name: &str,
    params: Vec<Scalar>,
    entries: &[BlockEntry],
    rows: usize,
    sort_descs: Vec<AggregateFunctionSortDesc>,
) -> Result<(Column, DataType)> {
    let res1 = eval_legacy_aggregate_for_test(
        name,
        params.clone(),
        entries,
        rows,
        false,
        true,
        sort_descs.clone(),
    )?;

    let res2 = eval_legacy_aggregate_for_test(
        name,
        params.clone(),
        entries,
        rows,
        true,
        true,
        sort_descs.clone(),
    )?;
    assert_eq!(res1, res2);
    assert_v2_aggr_matches_legacy_result(name, params, entries, rows, sort_descs, &res1)?;
    Ok(res1)
}

pub(super) fn eval_legacy_aggregate_without_each_row(
    name: &str,
    params: Vec<Scalar>,
    entries: &[BlockEntry],
    rows: usize,
    sort_descs: Vec<AggregateFunctionSortDesc>,
) -> Result<(Column, DataType)> {
    let res = eval_legacy_aggregate_for_test(
        name,
        params.clone(),
        entries,
        rows,
        false,
        true,
        sort_descs.clone(),
    )?;
    assert_v2_aggr_matches_legacy_result(name, params, entries, rows, sort_descs, &res)?;
    Ok(res)
}
