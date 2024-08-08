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
use std::marker::PhantomData;
use std::sync::Arc;

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_exception::Result;
use databend_common_expression::date_helper::TzLUT;
use databend_common_expression::types::variant::cast_scalar_to_variant;
use databend_common_expression::types::DataType;
use databend_common_expression::types::ValueType;
use databend_common_expression::types::*;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::InputColumns;
use databend_common_expression::Scalar;

use super::aggregate_function_factory::AggregateFunctionDescription;
use super::aggregate_scalar_state::ScalarStateFunc;
use super::borsh_deserialize_state;
use super::borsh_serialize_state;
use super::StateAddr;
use crate::aggregates::assert_unary_arguments;
use crate::aggregates::AggregateFunction;

#[derive(BorshSerialize, BorshDeserialize, Debug)]
pub struct JsonArrayAggState<T>
where
    T: ValueType,
    T::Scalar: BorshSerialize + BorshDeserialize,
{
    values: Vec<T::Scalar>,
}

impl<T> Default for JsonArrayAggState<T>
where
    T: ValueType,
    T::Scalar: BorshSerialize + BorshDeserialize,
{
    fn default() -> Self {
        Self { values: Vec::new() }
    }
}

impl<T> ScalarStateFunc<T> for JsonArrayAggState<T>
where
    T: ValueType,
    T::Scalar: BorshSerialize + BorshDeserialize + Send + Sync,
{
    fn new() -> Self {
        Self::default()
    }

    fn add(&mut self, other: Option<T::ScalarRef<'_>>) {
        if let Some(other) = other {
            self.values.push(T::to_owned_scalar(other));
        }
    }

    fn add_batch(&mut self, column: &T::Column, validity: Option<&Bitmap>) -> Result<()> {
        let column_len = T::column_len(column);
        if column_len == 0 {
            return Ok(());
        }
        let column_iter = T::iter_column(column);
        if let Some(validity) = validity {
            for (val, valid) in column_iter.zip(validity.iter()) {
                if valid {
                    self.values.push(T::to_owned_scalar(val));
                }
            }
        } else {
            for val in column_iter {
                self.values.push(T::to_owned_scalar(val));
            }
        }

        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.values.extend_from_slice(&rhs.values);
        Ok(())
    }

    fn merge_result(&mut self, builder: &mut ColumnBuilder) -> Result<()> {
        let tz = TzLUT::default();
        let mut items = Vec::with_capacity(self.values.len());
        for value in &self.values {
            let v = T::upcast_scalar(value.clone());
            // NULL values are omitted from the output.
            if v == Scalar::Null {
                continue;
            }
            let mut val = vec![];
            cast_scalar_to_variant(v.as_ref(), tz, &mut val);
            items.push(val);
        }
        let mut data = vec![];
        jsonb::build_array(items.iter().map(|b| &b[..]), &mut data).unwrap();

        let array_value = Scalar::Variant(data);
        builder.push(array_value.as_ref());
        Ok(())
    }
}

#[derive(Clone)]
pub struct AggregateJsonArrayAggFunction<T, State> {
    display_name: String,
    return_type: DataType,
    _t: PhantomData<T>,
    _state: PhantomData<State>,
}

impl<T, State> AggregateFunction for AggregateJsonArrayAggFunction<T, State>
where
    T: ValueType + Send + Sync,
    State: ScalarStateFunc<T>,
{
    fn name(&self) -> &str {
        "AggregateJsonArrayAggFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn init_state(&self, place: StateAddr) {
        place.write(|| State::new());
    }

    fn state_layout(&self) -> Layout {
        Layout::new::<State>()
    }

    fn accumulate(
        &self,
        place: StateAddr,
        columns: InputColumns,
        _validity: Option<&Bitmap>,
        _input_rows: usize,
    ) -> Result<()> {
        let state = place.get::<State>();
        match &columns[0] {
            Column::Nullable(box nullable_column) => {
                let column = T::try_downcast_column(&nullable_column.column).unwrap();
                state.add_batch(&column, Some(&nullable_column.validity))
            }
            _ => {
                let column = T::try_downcast_column(&columns[0]).unwrap();
                state.add_batch(&column, None)
            }
        }
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        offset: usize,
        columns: InputColumns,
        _input_rows: usize,
    ) -> Result<()> {
        match &columns[0] {
            Column::Nullable(box nullable_column) => {
                let column = T::try_downcast_column(&nullable_column.column).unwrap();
                let column_iter = T::iter_column(&column);
                column_iter
                    .zip(nullable_column.validity.iter().zip(places.iter()))
                    .for_each(|(v, (valid, place))| {
                        let addr = place.next(offset);
                        let state = addr.get::<State>();
                        if valid {
                            state.add(Some(v.clone()))
                        } else {
                            state.add(None)
                        }
                    });
            }
            _ => {
                let column = T::try_downcast_column(&columns[0]).unwrap();
                let column_iter = T::iter_column(&column);
                column_iter.zip(places.iter()).for_each(|(v, place)| {
                    let addr = place.next(offset);
                    let state = addr.get::<State>();
                    state.add(Some(v.clone()))
                });
            }
        }

        Ok(())
    }

    fn accumulate_row(&self, place: StateAddr, columns: InputColumns, row: usize) -> Result<()> {
        let state = place.get::<State>();
        match &columns[0] {
            Column::Nullable(box nullable_column) => {
                let valid = nullable_column.validity.get_bit(row);
                if valid {
                    let column = T::try_downcast_column(&nullable_column.column).unwrap();
                    let v = T::index_column(&column, row);
                    state.add(v);
                } else {
                    state.add(None);
                }
            }
            _ => {
                let column = T::try_downcast_column(&columns[0]).unwrap();
                let v = T::index_column(&column, row);
                state.add(v);
            }
        }

        Ok(())
    }

    fn serialize(&self, place: StateAddr, writer: &mut Vec<u8>) -> Result<()> {
        let state = place.get::<State>();
        borsh_serialize_state(writer, state)
    }

    fn merge(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state = place.get::<State>();
        let rhs: State = borsh_deserialize_state(reader)?;

        state.merge(&rhs)
    }

    fn merge_states(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let state = place.get::<State>();
        let other = rhs.get::<State>();
        state.merge(other)
    }

    fn merge_result(&self, place: StateAddr, builder: &mut ColumnBuilder) -> Result<()> {
        let state = place.get::<State>();
        state.merge_result(builder)
    }

    fn need_manual_drop_state(&self) -> bool {
        true
    }

    unsafe fn drop_state(&self, place: StateAddr) {
        let state = place.get::<State>();
        std::ptr::drop_in_place(state);
    }
}

impl<T, State> fmt::Display for AggregateJsonArrayAggFunction<T, State> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl<T, State> AggregateJsonArrayAggFunction<T, State>
where
    T: ValueType + Send + Sync,
    State: ScalarStateFunc<T>,
{
    fn try_create(display_name: &str, return_type: DataType) -> Result<Arc<dyn AggregateFunction>> {
        let func = AggregateJsonArrayAggFunction::<T, State> {
            display_name: display_name.to_string(),
            return_type,
            _t: PhantomData,
            _state: PhantomData,
        };
        Ok(Arc::new(func))
    }
}

pub fn try_create_aggregate_json_array_agg_function(
    display_name: &str,
    _params: Vec<Scalar>,
    argument_types: Vec<DataType>,
) -> Result<Arc<dyn AggregateFunction>> {
    assert_unary_arguments(display_name, argument_types.len())?;
    let return_type = DataType::Variant;

    type State = JsonArrayAggState<AnyType>;
    AggregateJsonArrayAggFunction::<AnyType, State>::try_create(display_name, return_type)
}

pub fn aggregate_json_array_agg_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(try_create_aggregate_json_array_agg_function))
}
