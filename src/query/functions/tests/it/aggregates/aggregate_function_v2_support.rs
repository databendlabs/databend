use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::StateSerdeItem;
use databend_common_expression::Symbol;
use databend_common_expression::SymbolOrOffset;
use databend_common_expression::aggregate::aggregate_function_v2 as v2;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberScalar;
use databend_common_functions::aggregates::AggregateFunctionSortDesc;

use super::aggregate_simulation_support::eval_legacy_aggregate_for_test;

pub(super) fn state_serde_data_type(item: &StateSerdeItem) -> DataType {
    match item {
        StateSerdeItem::DataType(data_type) => data_type.clone(),
        StateSerdeItem::Binary(_) => DataType::Binary,
    }
}

pub(super) fn eval_v2_aggr(
    name: &str,
    entries: &[BlockEntry],
    rows: usize,
    with_serialize: bool,
) -> Result<(Column, DataType)> {
    eval_v2_aggr_with_params(name, &[], entries, rows, with_serialize)
}

pub(super) fn eval_v2_aggr_with_params(
    name: &str,
    params: &[Scalar],
    entries: &[BlockEntry],
    rows: usize,
    with_serialize: bool,
) -> Result<(Column, DataType)> {
    eval_v2_aggr_with_params_and_sort(name, params, entries, rows, with_serialize, &[])
}

fn eval_v2_aggr_with_params_and_sort(
    name: &str,
    params: &[Scalar],
    entries: &[BlockEntry],
    rows: usize,
    with_serialize: bool,
    order_by: &[v2::AggregateBoundOrderByItem],
) -> Result<(Column, DataType)> {
    let args_type = entries
        .iter()
        .map(BlockEntry::data_type)
        .collect::<Vec<_>>();
    let registry =
        databend_common_functions::aggregates::aggregate_function_v2_registry::instance();
    let function = registry.resolve(v2::AggregateFunctionRequest {
        name,
        params,
        args_type: &args_type,
        distinct: false,
        order_by,
    })?;
    let data_type = function.signature().return_type.clone();

    let owner = v2::AggregateStateOwner::new(vec![function.clone()])?;
    if entries.is_empty() {
        function.accumulate_row_count(v2::AccumulateRowCountInput {
            state: owner.state(0),
            rows,
        })?;
    } else {
        function.accumulate(v2::AccumulateInput {
            state: owner.state(0),
            columns: entries.into(),
            validity: None,
            order_by: &[],
        })?;
    }

    let result_owner = if with_serialize {
        let data_types = function
            .state()
            .serde_items()
            .iter()
            .map(state_serde_data_type)
            .collect::<Vec<_>>();
        let mut builders = data_types
            .iter()
            .map(|data_type| ColumnBuilder::with_capacity(data_type, 1))
            .collect::<Vec<_>>();
        function.serialize(v2::SerializeInput {
            states: owner.state_set(0),
            builders: &mut builders,
        })?;
        let columns = builders
            .into_iter()
            .map(ColumnBuilder::build)
            .collect::<Vec<_>>();
        let state: BlockEntry = if columns.len() == 1 {
            columns.into_iter().next().unwrap().into()
        } else {
            Column::Tuple(columns).into()
        };

        let serialized_owner = v2::AggregateStateOwner::new(vec![function.clone()])?;
        function.merge_serialized(v2::MergeSerializedInput {
            states: serialized_owner.state_set(0),
            state: &state,
            filter: None,
        })?;
        serialized_owner
    } else {
        owner
    };

    let mut builder = ColumnBuilder::with_capacity(&data_type, 1);
    function.merge_result(v2::MergeResultInput {
        state: result_owner.state(0),
        builder: &mut builder,
    })?;
    Ok((builder.build(), data_type))
}

pub(super) fn eval_v2_aggr_for_test(
    name: &str,
    params: Vec<Scalar>,
    entries: &[BlockEntry],
    rows: usize,
    with_serialize: bool,
    sort_descs: Vec<AggregateFunctionSortDesc>,
) -> Result<(Column, DataType)> {
    let order_by = order_by_from_sort_descs(&sort_descs)?;
    eval_v2_aggr_with_params_and_sort(name, &params, entries, rows, with_serialize, &order_by)
}

pub(super) fn assert_v2_aggr_matches_legacy_result(
    name: &str,
    params: Vec<Scalar>,
    entries: &[BlockEntry],
    rows: usize,
    sort_descs: Vec<AggregateFunctionSortDesc>,
    legacy: &(Column, DataType),
) -> Result<()> {
    let args_type = entries
        .iter()
        .map(BlockEntry::data_type)
        .collect::<Vec<_>>();
    let direct = eval_v2_aggr_for_test(
        name,
        params.clone(),
        entries,
        rows,
        false,
        sort_descs.clone(),
    )?;
    let serialized = eval_v2_aggr_for_test(name, params, entries, rows, true, sort_descs)?;

    assert_eq!(
        direct, *legacy,
        "direct v2 result mismatch for {name}({args_type:?})"
    );
    assert_eq!(
        serialized, *legacy,
        "serialized v2 result mismatch for {name}({args_type:?})"
    );
    Ok(())
}

pub(super) fn assert_two_groups_group_by_v2_matches_legacy_result(
    name: &str,
    params: Vec<Scalar>,
    entries: &[BlockEntry],
    rows: usize,
    sort_descs: Vec<AggregateFunctionSortDesc>,
    legacy: &(Column, DataType),
) -> Result<()> {
    let args_type = entries
        .iter()
        .map(BlockEntry::data_type)
        .collect::<Vec<_>>();
    let v2 = simulate_two_groups_group_by_v2(name, params, entries, rows, sort_descs)?;

    assert_eq!(
        v2, *legacy,
        "group-by v2 result mismatch for {name}({args_type:?})"
    );
    Ok(())
}

pub(super) fn simulate_two_groups_group_by_v2(
    name: &str,
    params: Vec<Scalar>,
    entries: &[BlockEntry],
    rows: usize,
    sort_descs: Vec<AggregateFunctionSortDesc>,
) -> Result<(Column, DataType)> {
    let args_type = entries
        .iter()
        .map(BlockEntry::data_type)
        .collect::<Vec<_>>();
    let order_by = order_by_from_sort_descs(&sort_descs)?;
    let registry =
        databend_common_functions::aggregates::aggregate_function_v2_registry::instance();
    let function = registry.resolve(v2::AggregateFunctionRequest {
        name,
        params: &params,
        args_type: &args_type,
        distinct: false,
        order_by: &order_by,
    })?;
    let data_type = function.signature().return_type.clone();

    let group1 = v2::AggregateStateOwner::new(vec![function.clone()])?;
    let group2 = v2::AggregateStateOwner::new(vec![function.clone()])?;

    if entries.is_empty() {
        let group1_rows = rows.div_ceil(2);
        let group2_rows = rows / 2;
        function.accumulate_row_count(v2::AccumulateRowCountInput {
            state: group1.state(0),
            rows: group1_rows,
        })?;
        function.accumulate_row_count(v2::AccumulateRowCountInput {
            state: group2.state(0),
            rows: group2_rows,
        })?;
    } else {
        for row in 0..rows {
            let state = if row % 2 == 0 {
                group1.state(0)
            } else {
                group2.state(0)
            };
            function.accumulate_row(v2::AccumulateRowInput {
                state,
                columns: entries.into(),
                row,
            })?;
        }
    }

    let mut builder = ColumnBuilder::with_capacity(&data_type, 2);
    function.merge_result(v2::MergeResultInput {
        state: group1.state(0),
        builder: &mut builder,
    })?;
    function.merge_result(v2::MergeResultInput {
        state: group2.state(0),
        builder: &mut builder,
    })?;

    Ok((builder.build(), data_type))
}

fn order_by_from_sort_descs(
    sort_descs: &[AggregateFunctionSortDesc],
) -> Result<Vec<v2::AggregateBoundOrderByItem>> {
    sort_descs
        .iter()
        .map(|desc| {
            let index = match desc.index {
                SymbolOrOffset::Symbol(symbol) => symbol.as_usize(),
                SymbolOrOffset::Offset(offset) => offset,
            };
            if !desc.is_reuse_index {
                return Err(ErrorCode::Unimplemented(
                    "v2 aggregate test evaluator only supports reused sort descriptors",
                ));
            }
            Ok(v2::AggregateBoundOrderByItem {
                symbol: Symbol::new(index),
                source: v2::AggregateBoundOrderBySource::Argument { index },
                data_type: desc.data_type.clone(),
                asc: desc.asc,
                nulls_first: desc.nulls_first,
            })
        })
        .collect()
}

pub(super) fn assert_v2_matches_legacy(
    name: &str,
    entries: &[BlockEntry],
    rows: usize,
) -> Result<()> {
    assert_v2_matches_legacy_with_params(name, vec![], entries, rows)
}

fn assert_v2_matches_legacy_with_params(
    name: &str,
    params: Vec<Scalar>,
    entries: &[BlockEntry],
    rows: usize,
) -> Result<()> {
    let legacy =
        eval_legacy_aggregate_for_test(name, params.clone(), entries, rows, false, true, vec![])?;
    let direct_v2 = eval_v2_aggr_with_params(name, &params, entries, rows, false)?;
    let serialized_v2 = eval_v2_aggr_with_params(name, &params, entries, rows, true)?;

    assert_eq!(direct_v2, legacy, "direct v2 result mismatch for {name}");
    assert_eq!(
        serialized_v2, legacy,
        "serialized v2 result mismatch for {name}"
    );
    Ok(())
}

pub(super) fn assert_v2_read_only_matches_legacy(
    name: &str,
    params: Vec<Scalar>,
    entries: &[BlockEntry],
    rows: usize,
) -> Result<()> {
    let args_type = entries
        .iter()
        .map(BlockEntry::data_type)
        .collect::<Vec<_>>();
    let registry =
        databend_common_functions::aggregates::aggregate_function_v2_registry::instance();
    let function = registry.resolve(v2::AggregateFunctionRequest {
        name,
        params: &params,
        args_type: &args_type,
        distinct: false,
        order_by: &[],
    })?;
    let data_type = function.signature().return_type.clone();

    let owner = v2::AggregateStateOwner::new(vec![function.clone()])?;
    if entries.is_empty() {
        function.accumulate_row_count(v2::AccumulateRowCountInput {
            state: owner.state(0),
            rows,
        })?;
    } else {
        function.accumulate(v2::AccumulateInput {
            state: owner.state(0),
            columns: entries.into(),
            validity: None,
            order_by: &[],
        })?;
    }

    let mut read_only_builder = ColumnBuilder::with_capacity(&data_type, 1);
    function.merge_result_read_only(v2::MergeResultInput {
        state: owner.state(0),
        builder: &mut read_only_builder,
    })?;

    let mut final_builder = ColumnBuilder::with_capacity(&data_type, 1);
    function.merge_result(v2::MergeResultInput {
        state: owner.state(0),
        builder: &mut final_builder,
    })?;

    let read_only = (read_only_builder.build(), data_type.clone());
    let final_result = (final_builder.build(), data_type);
    let legacy = eval_legacy_aggregate_for_test(name, params, entries, rows, false, true, vec![])?;

    assert_eq!(
        read_only, legacy,
        "read-only v2 result mismatch for {name}({args_type:?})"
    );
    assert_eq!(
        final_result, legacy,
        "final v2 result mismatch after read-only finalize for {name}({args_type:?})"
    );
    Ok(())
}

pub(super) fn assert_v2_serialized_read_only_matches_legacy(
    name: &str,
    params: Vec<Scalar>,
    entries: &[BlockEntry],
    rows: usize,
) -> Result<()> {
    let args_type = entries
        .iter()
        .map(BlockEntry::data_type)
        .collect::<Vec<_>>();
    let registry =
        databend_common_functions::aggregates::aggregate_function_v2_registry::instance();
    let function = registry.resolve(v2::AggregateFunctionRequest {
        name,
        params: &params,
        args_type: &args_type,
        distinct: false,
        order_by: &[],
    })?;
    let data_type = function.signature().return_type.clone();

    let owner = v2::AggregateStateOwner::new(vec![function.clone()])?;
    function.accumulate(v2::AccumulateInput {
        state: owner.state(0),
        columns: entries.into(),
        validity: None,
        order_by: &[],
    })?;

    let serialized_types = function
        .state()
        .serde_items()
        .iter()
        .map(state_serde_data_type)
        .collect::<Vec<_>>();
    let mut serialized_builders = serialized_types
        .iter()
        .map(|data_type| ColumnBuilder::with_capacity(data_type, 1))
        .collect::<Vec<_>>();
    function.serialize(v2::SerializeInput {
        states: owner.state_set(0),
        builders: &mut serialized_builders,
    })?;
    let serialized_columns = serialized_builders
        .into_iter()
        .map(ColumnBuilder::build)
        .collect::<Vec<_>>();
    let serialized_state: BlockEntry = if serialized_columns.len() == 1 {
        serialized_columns.into_iter().next().unwrap().into()
    } else {
        Column::Tuple(serialized_columns).into()
    };

    let serialized_owner = v2::AggregateStateOwner::new(vec![function.clone()])?;
    function.merge_serialized(v2::MergeSerializedInput {
        states: serialized_owner.state_set(0),
        state: &serialized_state,
        filter: None,
    })?;

    let mut builder = ColumnBuilder::with_capacity(&data_type, 1);
    function.merge_result_read_only(v2::MergeResultInput {
        state: serialized_owner.state(0),
        builder: &mut builder,
    })?;

    let read_only = (builder.build(), data_type);
    let legacy = eval_legacy_aggregate_for_test(name, params, entries, rows, false, true, vec![])?;

    assert_eq!(
        read_only, legacy,
        "serialized read-only v2 result mismatch for {name}({args_type:?})"
    );
    Ok(())
}

pub(super) fn assert_single_float_close(result: &(Column, DataType), expected: f64) {
    let ScalarRef::Number(NumberScalar::Float64(value)) = (unsafe { result.0.index_unchecked(0) })
    else {
        panic!("expected Float64 result");
    };
    assert!(
        (*value - expected).abs() < 1e-12,
        "expected {expected}, got {}",
        *value
    );
}
