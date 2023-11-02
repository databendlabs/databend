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

use common_arrow::arrow::bitmap::Bitmap;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::types::StringType;
use common_expression::types::ValueType;
use common_expression::Column;
use common_expression::ColumnBuilder;
use common_expression::Scalar;
use serde::Deserialize;
use serde::Serialize;

use super::aggregate_function_factory::AggregateFunctionDescription;
use super::deserialize_state;
use super::serialize_state;
use super::StateAddr;
use crate::aggregates::assert_variadic_arguments;
use crate::aggregates::AggregateFunction;

#[derive(Serialize, Deserialize, Debug)]
pub struct StringAggState {
    values: Vec<u8>,
}

#[derive(Clone)]
pub struct AggregateStringAggFunction {
    display_name: String,
    delimiter: Vec<u8>,
}

impl AggregateFunction for AggregateStringAggFunction {
    fn name(&self) -> &str {
        "AggregateStringAggFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(DataType::String)
    }

    fn init_state(&self, place: StateAddr) {
        place.write(|| StringAggState { values: Vec::new() });
    }

    fn state_layout(&self) -> Layout {
        Layout::new::<StringAggState>()
    }

    fn accumulate(
        &self,
        place: StateAddr,
        columns: &[Column],
        validity: Option<&Bitmap>,
        _input_rows: usize,
    ) -> Result<()> {
        let column = StringType::try_downcast_column(&columns[0]).unwrap();
        let state = place.get::<StringAggState>();
        match validity {
            Some(validity) => {
                column.iter().zip(validity.iter()).for_each(|(v, b)| {
                    if b {
                        state.values.extend_from_slice(v);
                        state.values.extend_from_slice(self.delimiter.as_slice());
                    }
                });
            }
            None => {
                column.iter().for_each(|v| {
                    state.values.extend_from_slice(v);
                    state.values.extend_from_slice(self.delimiter.as_slice());
                });
            }
        }
        Ok(())
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        offset: usize,
        columns: &[Column],
        _input_rows: usize,
    ) -> Result<()> {
        let column = StringType::try_downcast_column(&columns[0]).unwrap();
        let column_iter = StringType::iter_column(&column);
        column_iter.zip(places.iter()).for_each(|(v, place)| {
            let addr = place.next(offset);
            let state = addr.get::<StringAggState>();
            state.values.extend_from_slice(v);
            state.values.extend_from_slice(self.delimiter.as_slice());
        });
        Ok(())
    }

    fn accumulate_row(&self, place: StateAddr, columns: &[Column], row: usize) -> Result<()> {
        let column = StringType::try_downcast_column(&columns[0]).unwrap();
        let v = StringType::index_column(&column, row);
        if let Some(v) = v {
            let state = place.get::<StringAggState>();
            state.values.extend_from_slice(v);
            state.values.extend_from_slice(self.delimiter.as_slice());
        }
        Ok(())
    }

    fn serialize(&self, place: StateAddr, writer: &mut Vec<u8>) -> Result<()> {
        let state = place.get::<StringAggState>();
        serialize_state(writer, state)?;
        Ok(())
    }

    fn merge(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state = place.get::<StringAggState>();
        let rhs: StringAggState = deserialize_state(reader)?;
        state.values.extend_from_slice(rhs.values.as_slice());
        Ok(())
    }

    fn merge_states(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let state = place.get::<StringAggState>();
        let other = rhs.get::<StringAggState>();
        state.values.extend_from_slice(other.values.as_slice());
        Ok(())
    }

    fn merge_result(&self, place: StateAddr, builder: &mut ColumnBuilder) -> Result<()> {
        let state = place.get::<StringAggState>();
        let builder = StringType::try_downcast_builder(builder).unwrap();
        if !state.values.is_empty() {
            let len = state.values.len() - self.delimiter.len();
            builder.put_slice(&state.values.as_slice()[..len]);
        }
        builder.commit_row();
        Ok(())
    }

    fn need_manual_drop_state(&self) -> bool {
        true
    }

    unsafe fn drop_state(&self, place: StateAddr) {
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
    fn try_create(display_name: &str, delimiter: Vec<u8>) -> Result<Arc<dyn AggregateFunction>> {
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
) -> Result<Arc<dyn AggregateFunction>> {
    assert_variadic_arguments(display_name, argument_types.len(), (1, 2))?;
    // TODO:(b41sh) support other data types
    if argument_types[0].remove_nullable() != DataType::String {
        return Err(ErrorCode::BadDataValueType(format!(
            "The argument of aggregate function {} must be string",
            display_name
        )));
    }
    let delimiter = if params.len() == 1 {
        params[0].as_string().unwrap().clone()
    } else {
        vec![]
    };
    AggregateStringAggFunction::try_create(display_name, delimiter)
}

pub fn aggregate_string_agg_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(try_create_aggregate_string_agg_function))
}
