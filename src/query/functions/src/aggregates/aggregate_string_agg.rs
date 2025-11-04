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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::display::scalar_ref_to_string;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::DataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::ValueType;
use databend_common_expression::AggrStateRegistry;
use databend_common_expression::AggrStateType;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::Scalar;
use databend_common_expression::StateSerdeItem;

use super::assert_variadic_arguments;
use super::batch_merge1;
use super::AggrState;
use super::AggrStateLoc;
use super::AggregateFunction;
use super::AggregateFunctionDescription;
use super::AggregateFunctionSortDesc;
use super::StateAddr;

#[derive(Debug)]
pub struct StringAggState {
    values: String,
}

#[derive(Clone)]
pub struct AggregateStringAggFunction {
    display_name: String,
    delimiter: String,
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
        entries: ProjectedBlock,
        validity: Option<&Bitmap>,
        _input_rows: usize,
    ) -> Result<()> {
        let state = place.get::<StringAggState>();
        match validity {
            Some(validity) => {
                if let Some(column) = entries[0].downcast::<StringType>() {
                    column.iter().zip(validity.iter()).for_each(|(v, b)| {
                        if b {
                            state.values.push_str(v);
                            state.values.push_str(&self.delimiter);
                        }
                    });
                } else {
                    entries[0]
                        .downcast::<AnyType>()
                        .unwrap()
                        .iter()
                        .zip(validity.iter())
                        .for_each(|(v, b)| {
                            if b {
                                state.values.push_str(&scalar_ref_to_string(&v));
                                state.values.push_str(&self.delimiter);
                            }
                        });
                }
            }
            None => {
                if let Some(column) = entries[0].downcast::<StringType>() {
                    column.iter().for_each(|v| {
                        state.values.push_str(v);
                        state.values.push_str(&self.delimiter);
                    });
                } else {
                    entries[0]
                        .downcast::<AnyType>()
                        .unwrap()
                        .iter()
                        .for_each(|v| {
                            state.values.push_str(&scalar_ref_to_string(&v));
                            state.values.push_str(&self.delimiter);
                        });
                }
            }
        }
        Ok(())
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        columns: ProjectedBlock,
        _input_rows: usize,
    ) -> Result<()> {
        for (scalar, place) in columns[0]
            .downcast::<AnyType>()
            .unwrap()
            .iter()
            .zip(places.iter())
        {
            let state = AggrState::new(*place, loc).get::<StringAggState>();
            state.values.push_str(&scalar_ref_to_string(&scalar));
            state.values.push_str(&self.delimiter);
        }
        Ok(())
    }

    fn accumulate_row(&self, place: AggrState, columns: ProjectedBlock, row: usize) -> Result<()> {
        let view = columns[0].downcast::<StringType>().unwrap();
        let v = view.index(row).unwrap();
        let state = place.get::<StringAggState>();
        state.values.push_str(v);
        state.values.push_str(&self.delimiter);
        Ok(())
    }

    fn serialize_type(&self) -> Vec<StateSerdeItem> {
        vec![DataType::String.into()]
    }

    fn batch_serialize(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        let builder = builders[0].as_string_mut().unwrap();
        for place in places {
            let state = AggrState::new(*place, loc).get::<StringAggState>();
            builder.put_str(&state.values);
            builder.commit_row();
        }
        Ok(())
    }

    fn batch_merge(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()> {
        batch_merge1::<StringType, StringAggState, _>(
            places,
            loc,
            state,
            filter,
            |state, values| {
                state.values.push_str(values);
                Ok(())
            },
        )
    }

    fn merge_states(&self, place: AggrState, rhs: AggrState) -> Result<()> {
        let state = place.get::<StringAggState>();
        let other = rhs.get::<StringAggState>();
        state.values.push_str(&other.values);
        Ok(())
    }

    fn merge_result(
        &self,
        place: AggrState,
        _read_only: bool,
        builder: &mut ColumnBuilder,
    ) -> Result<()> {
        let state = place.get::<StringAggState>();
        let mut builder = StringType::downcast_builder(builder);
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
    fn try_create(display_name: &str, delimiter: String) -> Result<Arc<dyn AggregateFunction>> {
        let func = AggregateStringAggFunction {
            display_name: display_name.to_string(),
            delimiter,
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
    AggregateStringAggFunction::try_create(display_name, delimiter)
}

pub fn aggregate_string_agg_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(try_create_aggregate_string_agg_function))
}
