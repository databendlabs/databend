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
use databend_common_expression::types::AnyType;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::DataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::ValueType;
use databend_common_expression::AggrStateRegistry;
use databend_common_expression::AggrStateType;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::InputColumns;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::SortColumnDescription;
use databend_common_expression::Value;
use itertools::Itertools;

use super::aggregate_function_factory::AggregateFunctionDescription;
use super::aggregate_function_factory::AggregateFunctionSortDesc;
use super::borsh_deserialize_state;
use super::borsh_serialize_state;
use super::ArrayAggState;
use super::StateAddr;
use crate::aggregates::aggregate_scalar_state::ScalarStateFunc;
use crate::aggregates::assert_variadic_arguments;
use crate::aggregates::AggrState;
use crate::aggregates::AggrStateLoc;
use crate::aggregates::AggregateFunction;

#[derive(BorshSerialize, BorshDeserialize, Debug)]
pub struct StringAggState {
    values: String,
}

#[derive(BorshSerialize, BorshDeserialize, Debug)]
struct SortDesc {
    index: usize,
    ty: DataType,
    asc: bool,
    null_first: bool,
}

#[derive(BorshSerialize, BorshDeserialize, Debug)]
pub struct SortStringAggState {
    inner: ArrayAggState<AnyType>,
    sort_descs: Vec<AggregateFunctionSortDesc>,
    delimiter: String,
}

impl Default for SortStringAggState {
    fn default() -> Self {
        SortStringAggState {
            inner: Default::default(),
            delimiter: "".to_string(),
            sort_descs: vec![],
        }
    }
}

impl ScalarStateFunc<AnyType> for SortStringAggState {
    fn new() -> Self {
        Self::default()
    }

    fn add(&mut self, other: Option<<AnyType as ValueType>::ScalarRef<'_>>) {
        self.inner.add(other);
    }

    fn add_batch(
        &mut self,
        column: &<AnyType as ValueType>::Column,
        validity: Option<&Bitmap>,
    ) -> Result<()> {
        self.inner.add_batch(column, validity)
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.inner.merge(&rhs.inner)
    }

    fn merge_result(&mut self, result_builder: &mut ColumnBuilder) -> Result<()> {
        let values = &self.inner.values;
        let mut builders = Vec::with_capacity(values.len());
        builders.push(ColumnBuilder::with_capacity(
            &Box::new(DataType::String),
            values.len(),
        ));
        builders.push(ColumnBuilder::with_capacity(
            &Box::new(DataType::String),
            values.len(),
        ));
        for desc in self.sort_descs.iter() {
            builders.push(ColumnBuilder::with_capacity(&desc.data_type, values.len()));
        }
        for tuple in values.iter() {
            if let Some(values) = tuple.as_tuple() {
                for (i, value) in values.iter().enumerate() {
                    builders[i].push(value.as_ref());
                }
            }
        }
        let mut block = DataBlock::new(
            builders
                .into_iter()
                .map(|builder| BlockEntry::new(builder.data_type(), Value::Column(builder.build())))
                .collect_vec(),
            values.len(),
        );
        if block.columns().len() > 1 {
            let sort_descs = self
                .sort_descs
                .iter()
                .enumerate()
                .map(|(i, sort_desc)| SortColumnDescription {
                    // after string and delimiter column
                    offset: i + 2,
                    asc: sort_desc.asc(),
                    nulls_first: sort_desc.nulls_first(),
                })
                .collect_vec();

            block = DataBlock::sort(&block, &sort_descs, None)?;
        }
        let builder = StringType::try_downcast_builder(result_builder).unwrap();
        if !block.is_empty() {
            let mut string = String::new();

            for i in 0..block.num_rows() {
                let value = block.value_at(0, i);
                if let Some(v) = value.as_ref().and_then(|v| v.as_string()) {
                    string.push_str(v);
                    string.push_str(&self.delimiter);
                }
            }
            let len = string.len() - self.delimiter.len();
            builder.put_and_commit(&string[..len]);
        } else {
            builder.put_and_commit("");
        }
        Ok(())
    }
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
        columns: InputColumns,
        validity: Option<&Bitmap>,
        _input_rows: usize,
    ) -> Result<()> {
        let column = StringType::try_downcast_column(&columns[0]).unwrap();
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

#[derive(Clone)]
pub struct SortAggregateStringAggFunction {
    display_name: String,
    delimiter: String,
    sort_descs: Vec<AggregateFunctionSortDesc>,
}

impl AggregateFunction for SortAggregateStringAggFunction {
    fn name(&self) -> &str {
        "SortAggregateStringAggFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(DataType::String)
    }

    fn init_state(&self, place: AggrState) {
        place.write(|| SortStringAggState {
            inner: Default::default(),
            sort_descs: self.sort_descs.clone(),
            delimiter: self.delimiter.clone(),
        });
    }

    fn register_state(&self, registry: &mut AggrStateRegistry) {
        registry.register(AggrStateType::Custom(Layout::new::<SortStringAggState>()));
    }

    fn accumulate(
        &self,
        place: AggrState,
        columns: InputColumns,
        validity: Option<&Bitmap>,
        _input_rows: usize,
    ) -> Result<()> {
        let tuples = self.accumulate_tuple(columns, validity)?;
        let state = place.get::<SortStringAggState>();
        state.add_batch(&Column::Tuple(tuples), validity)?;

        Ok(())
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        columns: InputColumns,
        _input_rows: usize,
    ) -> Result<()> {
        let tuples = self.accumulate_tuple(columns, None)?;

        for (i, addr) in places.iter().enumerate().take(tuples.len()) {
            let mut tuple = Vec::with_capacity(tuples.len());
            for column in tuples.iter() {
                tuple.push(column.index(i).unwrap_or_default());
            }

            let state = AggrState::new(*addr, loc).get::<SortStringAggState>();
            state.add(Some(ScalarRef::Tuple(tuple)))
        }

        Ok(())
    }

    fn accumulate_row(&self, place: AggrState, columns: InputColumns, row: usize) -> Result<()> {
        let mut tuples = Vec::with_capacity(self.sort_descs.len() + 1);
        for column in columns.iter() {
            tuples.push(column.slice(row..row + 1));
        }
        let state = place.get::<SortStringAggState>();
        state.add_batch(&Column::Tuple(tuples), None)?;
        Ok(())
    }

    fn serialize(&self, place: AggrState, writer: &mut Vec<u8>) -> Result<()> {
        borsh_serialize_state(writer, place.get::<SortStringAggState>())?;
        Ok(())
    }

    fn merge(&self, place: AggrState, reader: &mut &[u8]) -> Result<()> {
        let rhs: SortStringAggState = borsh_deserialize_state(reader)?;
        place.get::<SortStringAggState>().merge(&rhs)?;
        Ok(())
    }

    fn merge_states(&self, place: AggrState, rhs: AggrState) -> Result<()> {
        let state = place.get::<SortStringAggState>();
        let other = rhs.get::<SortStringAggState>();
        state.merge(other)?;
        Ok(())
    }

    fn merge_result(&self, place: AggrState, builder: &mut ColumnBuilder) -> Result<()> {
        let state = place.get::<SortStringAggState>();
        state.merge_result(builder)?;
        Ok(())
    }

    fn need_manual_drop_state(&self) -> bool {
        true
    }

    unsafe fn drop_state(&self, place: AggrState) {
        let state = place.get::<SortStringAggState>();
        std::ptr::drop_in_place(state);
    }
}

impl SortAggregateStringAggFunction {
    fn accumulate_tuple(
        &self,
        input_columns: InputColumns,
        validity: Option<&Bitmap>,
    ) -> Result<Vec<Column>> {
        let mut tuple = Vec::with_capacity(self.sort_descs.len() + 1);
        for column in input_columns.iter() {
            if let Some(validity) = validity {
                tuple.push(column.filter(validity).clone());
            } else {
                tuple.push(column.clone());
            }
        }
        Ok(tuple)
    }
}

impl fmt::Display for SortAggregateStringAggFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

pub fn try_create_aggregate_string_agg_function(
    display_name: &str,
    params: Vec<Scalar>,
    argument_types: Vec<DataType>,
    sort_descs: Vec<AggregateFunctionSortDesc>,
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
        String::new()
    };
    if !sort_descs.is_empty() {
        let func = SortAggregateStringAggFunction {
            display_name: display_name.to_string(),
            delimiter,
            sort_descs,
        };
        Ok(Arc::new(func))
    } else {
        let func = AggregateStringAggFunction {
            display_name: display_name.to_string(),
            delimiter,
        };
        Ok(Arc::new(func))
    }
}

pub fn aggregate_string_agg_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(try_create_aggregate_string_agg_function))
}
