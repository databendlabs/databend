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
use std::fmt;
use std::sync::Arc;

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::DataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::ValueType;
use databend_common_expression::AggrStateRegistry;
use databend_common_expression::AggrStateType;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::EvaluateOptions;
use databend_common_expression::Evaluator;
use databend_common_expression::FunctionContext;
use databend_common_expression::InputColumns;
use databend_common_expression::Scalar;
use databend_common_expression::Value;

use super::aggregate_function_factory::AggregateFunctionDescription;
use super::borsh_deserialize_state;
use super::borsh_serialize_state;
use super::AggregateFunctionSortDesc;
use super::StateAddr;
use crate::aggregates::assert_variadic_arguments;
use crate::aggregates::AggrState;
use crate::aggregates::AggrStateLoc;
use crate::aggregates::AggregateFunction;
use crate::BUILTIN_FUNCTIONS;

#[derive(BorshSerialize, BorshDeserialize, Debug)]
pub struct StringAggState {
    values: String,
}

#[derive(Clone)]
pub struct AggregateStringAggFunction {
    display_name: String,
    delimiter: String,
    value_type: DataType,
}

impl AggregateFunction for AggregateStringAggFunction {
    fn name(&self) -> &str {
        "AggregateStringAggFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(DataType::String)
    }

    fn init_state(&self, place: AggrState) {
        place.write(|| StringAggState {
            values: String::new(),
        });
    }

    fn register_state(&self, registry: &mut AggrStateRegistry) {
        registry.register(AggrStateType::Custom(Layout::new::<StringAggState>()));
    }

    fn accumulate(
        &self,
        place: AggrState,
        columns: InputColumns,
        validity: Option<&Bitmap>,
        _input_rows: usize,
    ) -> Result<()> {
        let column = if self.value_type != DataType::String {
            let block = DataBlock::new_from_columns(vec![columns[0].clone()]);
            let func_ctx = &FunctionContext::default();
            let evaluator = Evaluator::new(&block, func_ctx, &BUILTIN_FUNCTIONS);
            let value = evaluator.run_cast(
                None,
                &self.value_type,
                &DataType::String,
                Value::Column(columns[0].clone()),
                None,
                &mut EvaluateOptions::default(),
            )?;
            StringType::try_downcast_column(value.as_column().unwrap()).unwrap()
        } else {
            StringType::try_downcast_column(&columns[0]).unwrap()
        };
        let state = place.get::<StringAggState>();
        match validity {
            Some(validity) => {
                column.iter().zip(validity.iter()).for_each(|(v, b)| {
                    if b {
                        state.values.push_str(v);
                        state.values.push_str(&self.delimiter);
                    }
                });
            }
            None => {
                column.iter().for_each(|v| {
                    state.values.push_str(v);
                    state.values.push_str(&self.delimiter);
                });
            }
        }
        Ok(())
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        columns: InputColumns,
        _input_rows: usize,
    ) -> Result<()> {
        let column = StringType::try_downcast_column(&columns[0]).unwrap();
        let column_iter = StringType::iter_column(&column);
        column_iter.zip(places.iter()).for_each(|(v, place)| {
            let state = AggrState::new(*place, loc).get::<StringAggState>();
            state.values.push_str(v);
            state.values.push_str(&self.delimiter);
        });
        Ok(())
    }

    fn accumulate_row(&self, place: AggrState, columns: InputColumns, row: usize) -> Result<()> {
        let column = StringType::try_downcast_column(&columns[0]).unwrap();
        let v = StringType::index_column(&column, row);
        if let Some(v) = v {
            let state = place.get::<StringAggState>();
            state.values.push_str(v);
            state.values.push_str(&self.delimiter);
        }
        Ok(())
    }

    fn serialize(&self, place: AggrState, writer: &mut Vec<u8>) -> Result<()> {
        let state = place.get::<StringAggState>();
        borsh_serialize_state(writer, state)?;
        Ok(())
    }

    fn merge(&self, place: AggrState, reader: &mut &[u8]) -> Result<()> {
        let state = place.get::<StringAggState>();
        let rhs: StringAggState = borsh_deserialize_state(reader)?;
        state.values.push_str(&rhs.values);
        Ok(())
    }

    fn merge_states(&self, place: AggrState, rhs: AggrState) -> Result<()> {
        let state = place.get::<StringAggState>();
        let other = rhs.get::<StringAggState>();
        state.values.push_str(&other.values);
        Ok(())
    }

    fn merge_result(&self, place: AggrState, builder: &mut ColumnBuilder) -> Result<()> {
        let state = place.get::<StringAggState>();
        let builder = StringType::try_downcast_builder(builder).unwrap();
        if !state.values.is_empty() {
            let len = state.values.len() - self.delimiter.len();
            builder.put_and_commit(&state.values[..len]);
        } else {
            builder.put_and_commit("");
        }
        Ok(())
    }

    fn need_manual_drop_state(&self) -> bool {
        true
    }

    unsafe fn drop_state(&self, place: AggrState) {
        let state = place.get::<StringAggState>();
        std::ptr::drop_in_place(state);
    }
}

impl fmt::Display for AggregateStringAggFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl AggregateStringAggFunction {
    fn try_create(
        display_name: &str,
        delimiter: String,
        value_type: DataType,
    ) -> Result<Arc<dyn AggregateFunction>> {
        let func = AggregateStringAggFunction {
            display_name: display_name.to_string(),
            delimiter,
            value_type,
        };
        Ok(Arc::new(func))
    }
}

pub fn try_create_aggregate_string_agg_function(
    display_name: &str,
    params: Vec<Scalar>,
    argument_types: Vec<DataType>,
    _sort_descs: Vec<AggregateFunctionSortDesc>,
) -> Result<Arc<dyn AggregateFunction>> {
    assert_variadic_arguments(display_name, argument_types.len(), (1, 2))?;
    let value_type = argument_types[0].remove_nullable();
    if !matches!(
        value_type,
        DataType::Boolean
            | DataType::String
            | DataType::Number(_)
            | DataType::Decimal(_)
            | DataType::Timestamp
            | DataType::Date
            | DataType::Variant
            | DataType::Interval
    ) {
        return Err(ErrorCode::BadDataValueType(format!(
            "{} does not support type '{:?}'",
            display_name, value_type
        )));
    }
    let delimiter = if params.len() == 1 {
        params[0].as_string().unwrap().clone()
    } else {
        String::new()
    };
    AggregateStringAggFunction::try_create(display_name, delimiter, value_type)
}

pub fn aggregate_string_agg_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(try_create_aggregate_string_agg_function))
}
