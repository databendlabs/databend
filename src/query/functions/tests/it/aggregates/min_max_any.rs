use databend_common_exception::Result;
use databend_common_expression::FromData;
use databend_common_expression::ScalarRef;
use databend_common_expression::aggregate_function::AggregateFunctionRequest;
use databend_common_expression::types::AggregateStateDataType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::UInt64Type;
use databend_common_functions::aggregates::aggregate_function_v2_registry::AGGR_REGISTRY;

use super::aggregate_function_v2_support::eval_v2_aggr;

#[test]
fn test_v2_min_max_any_uint64_matches_expected_values() -> Result<()> {
    let entries = [UInt64Type::from_data(vec![9, 2, 5, 7]).into()];

    let min = eval_v2_aggr("min", &entries, 4, false)?;
    let min_serialized = eval_v2_aggr("min", &entries, 4, true)?;
    assert_eq!(
        unsafe { min.0.index_unchecked(0) },
        ScalarRef::Number(NumberScalar::UInt64(2))
    );
    assert_eq!(min_serialized, min);

    let max = eval_v2_aggr("max", &entries, 4, false)?;
    let max_serialized = eval_v2_aggr("max", &entries, 4, true)?;
    assert_eq!(
        unsafe { max.0.index_unchecked(0) },
        ScalarRef::Number(NumberScalar::UInt64(9))
    );
    assert_eq!(max_serialized, max);

    let any = eval_v2_aggr("any", &entries, 4, false)?;
    let any_serialized = eval_v2_aggr("any", &entries, 4, true)?;
    assert_eq!(
        unsafe { any.0.index_unchecked(0) },
        ScalarRef::Number(NumberScalar::UInt64(9))
    );
    assert_eq!(any_serialized, any);
    Ok(())
}

#[test]
fn test_v2_min_max_any_heap_states_require_manual_drop() -> Result<()> {
    let aggregate_state = DataType::AggregateState(Box::new(AggregateStateDataType {
        function_name: "test".to_string(),
        params: vec![],
        argument_types: vec![],
        state_type: Box::new(DataType::Binary),
    }));

    for data_type in [DataType::Binary, aggregate_state] {
        let function = AGGR_REGISTRY.resolve(AggregateFunctionRequest {
            name: "min",
            params: &[],
            args_type: &[data_type],
            distinct: false,
            order_by: &[],
        })?;
        assert!(function.state().need_manual_drop());
    }
    Ok(())
}
