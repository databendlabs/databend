use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::StateSerdeItem;
use databend_common_expression::aggregate::aggregate_function::*;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberScalar;
use databend_common_functions::aggregates::AGGR_REGISTRY;

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
    order_by: &[AggregateBoundOrderByItem],
) -> Result<(Column, DataType)> {
    let args_type = entries
        .iter()
        .map(BlockEntry::data_type)
        .collect::<Vec<_>>();
    let function = AGGR_REGISTRY.resolve(AggregateFunctionRequest {
        name,
        params,
        args_type: &args_type,
        distinct: false,
        order_by,
    })?;
    let data_type = function.signature().return_type.clone();

    let owner = AggregateStateOwner::new(vec![function.clone()])?;
    if entries.is_empty() {
        function.accumulate_row_count(AccumulateRowCountInput {
            state: owner.state(0),
            rows,
        })?;
    } else {
        function.accumulate(AccumulateInput {
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
        function.serialize(SerializeInput {
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

        let serialized_owner = AggregateStateOwner::new(vec![function.clone()])?;
        function.merge_serialized(MergeSerializedInput {
            states: serialized_owner.state_set(0),
            state: &state,
            filter: None,
        })?;
        serialized_owner
    } else {
        owner
    };

    let mut builder = ColumnBuilder::with_capacity(&data_type, 1);
    function.merge_result(MergeResultInput {
        state: result_owner.state(0),
        builder: &mut builder,
    })?;
    Ok((builder.build(), data_type))
}

pub(super) fn assert_v2_direct_matches_serialized(
    name: &str,
    entries: &[BlockEntry],
    rows: usize,
) -> Result<()> {
    let direct = eval_v2_aggr(name, entries, rows, false)?;
    let serialized = eval_v2_aggr(name, entries, rows, true)?;
    assert_eq!(
        direct, serialized,
        "serialized v2 result mismatch for {name}"
    );
    Ok(())
}

pub(super) fn assert_v2_read_only_matches_final_result(
    name: &str,
    params: Vec<Scalar>,
    entries: &[BlockEntry],
    rows: usize,
) -> Result<()> {
    let args_type = entries
        .iter()
        .map(BlockEntry::data_type)
        .collect::<Vec<_>>();
    let function = AGGR_REGISTRY.resolve(AggregateFunctionRequest {
        name,
        params: &params,
        args_type: &args_type,
        distinct: false,
        order_by: &[],
    })?;
    let data_type = function.signature().return_type.clone();

    let owner = AggregateStateOwner::new(vec![function.clone()])?;
    if entries.is_empty() {
        function.accumulate_row_count(AccumulateRowCountInput {
            state: owner.state(0),
            rows,
        })?;
    } else {
        function.accumulate(AccumulateInput {
            state: owner.state(0),
            columns: entries.into(),
            validity: None,
            order_by: &[],
        })?;
    }

    let mut read_only_builder = ColumnBuilder::with_capacity(&data_type, 1);
    function.merge_result_read_only(MergeResultInput {
        state: owner.state(0),
        builder: &mut read_only_builder,
    })?;

    let mut final_builder = ColumnBuilder::with_capacity(&data_type, 1);
    function.merge_result(MergeResultInput {
        state: owner.state(0),
        builder: &mut final_builder,
    })?;

    let read_only = (read_only_builder.build(), data_type.clone());
    let final_result = (final_builder.build(), data_type);
    assert_eq!(
        read_only, final_result,
        "final v2 result mismatch after read-only finalize for {name}({args_type:?})"
    );
    Ok(())
}

pub(super) fn assert_v2_serialized_read_only_matches_final_result(
    name: &str,
    params: Vec<Scalar>,
    entries: &[BlockEntry],
) -> Result<()> {
    let args_type = entries
        .iter()
        .map(BlockEntry::data_type)
        .collect::<Vec<_>>();
    let function = AGGR_REGISTRY.resolve(AggregateFunctionRequest {
        name,
        params: &params,
        args_type: &args_type,
        distinct: false,
        order_by: &[],
    })?;
    let data_type = function.signature().return_type.clone();

    let owner = AggregateStateOwner::new(vec![function.clone()])?;
    function.accumulate(AccumulateInput {
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
    function.serialize(SerializeInput {
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

    let serialized_owner = AggregateStateOwner::new(vec![function.clone()])?;
    function.merge_serialized(MergeSerializedInput {
        states: serialized_owner.state_set(0),
        state: &serialized_state,
        filter: None,
    })?;

    let mut read_only_builder = ColumnBuilder::with_capacity(&data_type, 1);
    function.merge_result_read_only(MergeResultInput {
        state: serialized_owner.state(0),
        builder: &mut read_only_builder,
    })?;

    let mut final_builder = ColumnBuilder::with_capacity(&data_type, 1);
    function.merge_result(MergeResultInput {
        state: serialized_owner.state(0),
        builder: &mut final_builder,
    })?;

    assert_eq!(
        read_only_builder.build(),
        final_builder.build(),
        "final serialized v2 result mismatch after read-only finalize for {name}({args_type:?})"
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
