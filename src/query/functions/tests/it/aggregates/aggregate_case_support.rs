use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::Scalar;
use databend_common_expression::aggregate_function::AggregateBoundOrderByItem;
use databend_common_expression::types::DataType;

use super::aggregate_simulation_support::eval_aggregate_for_test;

pub(super) fn eval_aggregate(
    name: &str,
    params: Vec<Scalar>,
    entries: &[BlockEntry],
    rows: usize,
    sort_descs: Vec<AggregateBoundOrderByItem>,
) -> Result<(Column, DataType)> {
    eval_aggregate_for_test(name, params, entries, rows, false, true, sort_descs)
}
