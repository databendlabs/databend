// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::alloc::Layout;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::AggrState;
use databend_common_expression::AggrStateType;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::Scalar;
use databend_common_expression::StateSerdeItem;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;

use crate::aggregates::aggregate_function_v2_impl::adaptors_v2 as v2;

pub(crate) fn try_create_null_argument_result_function(
    request: v2::AggregateFunctionRequest<'_>,
    returns_default_when_only_null: bool,
) -> Result<v2::AggregateFunctionRef> {
    let (data_type, result) = if returns_default_when_only_null {
        (
            DataType::Number(NumberDataType::UInt64),
            Scalar::Number(NumberScalar::UInt64(0)),
        )
    } else {
        (DataType::Null, Scalar::Null)
    };
    let return_type = data_type.clone();
    let signature = v2::AggregateFunctionSignature {
        name: request.name.to_string(),
        params: request.params.to_vec(),
        args_type: request.args_type.to_vec(),
        distinct: request.distinct,
        order_by: request.order_by.to_vec(),
        return_type,
    };
    let state =
        v2::AggregateStateDescription::new(vec![AggrStateType::Custom(Layout::new::<u8>())], vec![
            StateSerdeItem::DataType(data_type),
        ]);
    Ok(Arc::new(v2::AggregateFunction::new(
        signature,
        v2::FunctionFeatures::default(),
        state,
        AggregateFixedResultImplementation { result },
    )))
}

struct AggregateFixedResultImplementation {
    result: Scalar,
}

impl AggregateFixedResultImplementation {
    fn push_result(&self, builder: &mut ColumnBuilder) {
        if let Some(fields) = builder.as_tuple_mut() {
            fields[0].push(self.result.as_ref());
        } else {
            builder.push(self.result.as_ref());
        }
    }
}

impl v2::AggrImpl for AggregateFixedResultImplementation {
    fn init_state(&self, _state: AggrState<'_>) {}

    fn accumulate(&self, _input: v2::AccumulateInput<'_>) -> Result<()> {
        Ok(())
    }

    fn accumulate_keys(&self, _input: v2::AccumulateKeysInput<'_>) -> Result<()> {
        Ok(())
    }

    fn accumulate_row(&self, _input: v2::AccumulateRowInput<'_>) -> Result<()> {
        Ok(())
    }

    fn accumulate_row_count(&self, _input: v2::AccumulateRowCountInput<'_>) -> Result<()> {
        Ok(())
    }

    fn accumulate_row_count_keys(&self, _input: v2::AccumulateRowCountKeysInput<'_>) -> Result<()> {
        Ok(())
    }

    fn serialize(&self, input: v2::SerializeInput<'_>) -> Result<()> {
        for _state in input.states.iter() {
            self.push_result(&mut input.builders[0]);
        }
        Ok(())
    }

    fn merge_serialized(&self, _input: v2::MergeSerializedInput<'_>) -> Result<()> {
        Ok(())
    }

    fn merge_states(&self, _input: v2::MergeStatesInput<'_>) -> Result<()> {
        Ok(())
    }

    fn merge_result(&self, input: v2::MergeResultInput<'_>) -> Result<()> {
        self.push_result(input.builder);
        Ok(())
    }

    fn merge_result_read_only(&self, input: v2::MergeResultInput<'_>) -> Result<()> {
        self.merge_result(input)
    }

    unsafe fn drop_state(&self, _state: AggrState<'_>) {}
}
