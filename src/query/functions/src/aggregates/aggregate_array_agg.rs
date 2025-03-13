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
use std::mem;
use std::sync::Arc;

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_exception::Result;
use databend_common_expression::types::decimal::*;
use databend_common_expression::types::number::*;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::DataType;
use databend_common_expression::types::ValueType;
use databend_common_expression::types::*;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::AggrStateRegistry;
use databend_common_expression::AggrStateType;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::InputColumns;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use ethnum::i256;

use super::aggregate_function_factory::AggregateFunctionDescription;
use super::aggregate_scalar_state::ScalarStateFunc;
use super::borsh_deserialize_state;
use super::borsh_serialize_state;
use super::AggregateFunctionSortDesc;
use super::StateAddr;
use crate::aggregates::assert_unary_arguments;
use crate::aggregates::AggrState;
use crate::aggregates::AggrStateLoc;
use crate::aggregates::AggregateFunction;
use crate::with_simple_no_number_mapped_type;

#[derive(BorshSerialize, BorshDeserialize, Debug)]
pub struct ArrayAggState<T>
where
    T: ValueType,
    T::Scalar: BorshSerialize + BorshDeserialize,
{
    pub(crate) values: Vec<T::Scalar>,
}

impl<T> Default for ArrayAggState<T>
where
    T: ValueType,
    T::Scalar: BorshSerialize + BorshDeserialize,
{
    fn default() -> Self {
        Self { values: Vec::new() }
    }
}

impl<T> ScalarStateFunc<T> for ArrayAggState<T>
where
    T: ValueType,
    T::Scalar: BorshSerialize + BorshDeserialize + Send + Sync,
{
    fn new() -> Self {
        Self::default()
    }

    fn add(&mut self, other: Option<T::ScalarRef<'_>>) {
        self.values.push(T::to_owned_scalar(other.unwrap()));
    }

    fn add_batch(&mut self, column: &T::Column, _validity: Option<&Bitmap>) -> Result<()> {
        let column_len = T::column_len(column);
        if column_len == 0 {
            return Ok(());
        }
        let column_iter = T::iter_column(column);
        for val in column_iter {
            self.values.push(T::to_owned_scalar(val));
        }

        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.values.extend_from_slice(&rhs.values);
        Ok(())
    }

    fn merge_result(&mut self, builder: &mut ColumnBuilder) -> Result<()> {
        let data_type = builder.data_type();
        let inner_type = data_type.as_array().unwrap();

        let mut inner_builder = ColumnBuilder::with_capacity(inner_type, self.values.len());
        match inner_type.remove_nullable() {
            DataType::Decimal(decimal_type) => {
                let size = decimal_type.size();
                let values = mem::take(&mut self.values);
                for value in values.into_iter() {
                    let val = T::upcast_scalar(value);
                    let decimal_val = val.as_decimal().unwrap();
                    let new_val = match decimal_val {
                        DecimalScalar::Decimal128(v, _) => {
                            ScalarRef::Decimal(DecimalScalar::Decimal128(*v, size))
                        }
                        DecimalScalar::Decimal256(v, _) => {
                            ScalarRef::Decimal(DecimalScalar::Decimal256(*v, size))
                        }
                    };
                    inner_builder.push(new_val);
                }
            }
            _ => {
                let values = mem::take(&mut self.values);
                for value in values.into_iter() {
                    let val = T::upcast_scalar(value);
                    inner_builder.push(val.as_ref());
                }
            }
        }
        let array_value = ScalarRef::Array(inner_builder.build());
        builder.push(array_value);
        Ok(())
    }
}

#[derive(BorshSerialize, BorshDeserialize, Debug)]
pub struct NullableArrayAggState<T>
where
    T: ValueType,
    T::Scalar: BorshSerialize + BorshDeserialize,
{
    values: Vec<Option<T::Scalar>>,
}

impl<T> Default for NullableArrayAggState<T>
where
    T: ValueType,
    T::Scalar: BorshSerialize + BorshDeserialize,
{
    fn default() -> Self {
        Self { values: Vec::new() }
    }
}

impl<T> ScalarStateFunc<T> for NullableArrayAggState<T>
where
    T: ValueType,
    T::Scalar: BorshSerialize + BorshDeserialize + Send + Sync,
{
    fn new() -> Self {
        Self::default()
    }

    fn add(&mut self, other: Option<T::ScalarRef<'_>>) {
        match other {
            Some(other) => {
                self.values.push(Some(T::to_owned_scalar(other)));
            }
            None => {
                self.values.push(None);
            }
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
                    self.values.push(Some(T::to_owned_scalar(val)));
                } else {
                    self.values.push(None);
                }
            }
        } else {
            for val in column_iter {
                self.values.push(Some(T::to_owned_scalar(val)));
            }
        }

        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.values.extend_from_slice(&rhs.values);
        Ok(())
    }

    fn merge_result(&mut self, builder: &mut ColumnBuilder) -> Result<()> {
        let data_type = builder.data_type();
        let inner_type = data_type.as_array().unwrap();

        let mut inner_builder = ColumnBuilder::with_capacity(inner_type, self.values.len());
        match inner_type.remove_nullable() {
            DataType::Decimal(decimal_type) => {
                let size = decimal_type.size();
                for value in &self.values {
                    match value {
                        Some(value) => {
                            let val = T::upcast_scalar(value.clone());
                            let decimal_val = val.as_decimal().unwrap();
                            let new_val = match decimal_val {
                                DecimalScalar::Decimal128(v, _) => {
                                    ScalarRef::Decimal(DecimalScalar::Decimal128(*v, size))
                                }
                                DecimalScalar::Decimal256(v, _) => {
                                    ScalarRef::Decimal(DecimalScalar::Decimal256(*v, size))
                                }
                            };
                            inner_builder.push(new_val);
                        }
                        None => {
                            inner_builder.push(ScalarRef::Null);
                        }
                    }
                }
            }
            _ => {
                for value in &self.values {
                    match value {
                        Some(value) => {
                            let val = T::upcast_scalar(value.clone());
                            inner_builder.push(val.as_ref());
                        }
                        None => {
                            inner_builder.push(ScalarRef::Null);
                        }
                    }
                }
            }
        }
        let array_value = ScalarRef::Array(inner_builder.build());
        builder.push(array_value);
        Ok(())
    }
}

#[derive(Clone)]
pub struct AggregateArrayAggFunction<T, State> {
    display_name: String,
    return_type: DataType,
    _t: PhantomData<T>,
    _state: PhantomData<State>,
}

impl<T, State> AggregateFunction for AggregateArrayAggFunction<T, State>
where
    T: ValueType + Send + Sync,
    State: ScalarStateFunc<T>,
{
    fn name(&self) -> &str {
        "AggregateArrayAggFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn init_state(&self, place: AggrState) {
        place.write(State::new);
    }

    fn register_state(&self, registry: &mut AggrStateRegistry) {
        registry.register(AggrStateType::Custom(Layout::new::<State>()));
    }

    fn accumulate(
        &self,
        place: AggrState,
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
        loc: &[AggrStateLoc],
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
                        let state = AggrState::new(*place, loc).get::<State>();
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
                    let state = AggrState::new(*place, loc).get::<State>();
                    state.add(Some(v.clone()))
                });
            }
        }

        Ok(())
    }

    fn accumulate_row(&self, place: AggrState, columns: InputColumns, row: usize) -> Result<()> {
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

    fn serialize(&self, place: AggrState, writer: &mut Vec<u8>) -> Result<()> {
        let state = place.get::<State>();
        borsh_serialize_state(writer, state)
    }

    fn merge(&self, place: AggrState, reader: &mut &[u8]) -> Result<()> {
        let state = place.get::<State>();
        let rhs: State = borsh_deserialize_state(reader)?;

        state.merge(&rhs)
    }

    fn merge_states(&self, place: AggrState, rhs: AggrState) -> Result<()> {
        let state = place.get::<State>();
        let other = rhs.get::<State>();
        state.merge(other)
    }

    fn merge_result(&self, place: AggrState, builder: &mut ColumnBuilder) -> Result<()> {
        let state = place.get::<State>();
        state.merge_result(builder)
    }

    fn need_manual_drop_state(&self) -> bool {
        true
    }

    unsafe fn drop_state(&self, place: AggrState) {
        let state = place.get::<State>();
        std::ptr::drop_in_place(state);
    }
}

impl<T, State> fmt::Display for AggregateArrayAggFunction<T, State> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl<T, State> AggregateArrayAggFunction<T, State>
where
    T: ValueType + Send + Sync,
    State: ScalarStateFunc<T>,
{
    fn try_create(display_name: &str, return_type: DataType) -> Result<Arc<dyn AggregateFunction>> {
        let func = AggregateArrayAggFunction::<T, State> {
            display_name: display_name.to_string(),
            return_type,
            _t: PhantomData,
            _state: PhantomData,
        };
        Ok(Arc::new(func))
    }
}

pub fn try_create_aggregate_array_agg_function(
    display_name: &str,
    _params: Vec<Scalar>,
    argument_types: Vec<DataType>,
    _sort_descs: Vec<AggregateFunctionSortDesc>,
) -> Result<Arc<dyn AggregateFunction>> {
    assert_unary_arguments(display_name, argument_types.len())?;
    let data_type = argument_types[0].clone();
    let is_nullable = data_type.is_nullable_or_null();
    let return_type = DataType::Array(Box::new(data_type.clone()));

    with_simple_no_number_mapped_type!(|T| match data_type.remove_nullable() {
        DataType::T => {
            if is_nullable {
                type State = NullableArrayAggState<T>;
                AggregateArrayAggFunction::<T, State>::try_create(display_name, return_type)
            } else {
                type State = ArrayAggState<T>;
                AggregateArrayAggFunction::<T, State>::try_create(display_name, return_type)
            }
        }
        DataType::Number(num_type) => {
            with_number_mapped_type!(|NUM| match num_type {
                NumberDataType::NUM => {
                    if is_nullable {
                        type State = NullableArrayAggState<NumberType<NUM>>;
                        AggregateArrayAggFunction::<NumberType<NUM>, State>::try_create(
                            display_name,
                            return_type,
                        )
                    } else {
                        type State = ArrayAggState<NumberType<NUM>>;
                        AggregateArrayAggFunction::<NumberType<NUM>, State>::try_create(
                            display_name,
                            return_type,
                        )
                    }
                }
            })
        }
        DataType::Decimal(DecimalDataType::Decimal128(_)) => {
            if is_nullable {
                type State = NullableArrayAggState<DecimalType<i128>>;
                AggregateArrayAggFunction::<DecimalType<i128>, State>::try_create(
                    display_name,
                    return_type,
                )
            } else {
                type State = ArrayAggState<DecimalType<i128>>;
                AggregateArrayAggFunction::<DecimalType<i128>, State>::try_create(
                    display_name,
                    return_type,
                )
            }
        }
        DataType::Decimal(DecimalDataType::Decimal256(_)) => {
            if is_nullable {
                type State = NullableArrayAggState<DecimalType<i256>>;
                AggregateArrayAggFunction::<DecimalType<i256>, State>::try_create(
                    display_name,
                    return_type,
                )
            } else {
                type State = ArrayAggState<DecimalType<i256>>;
                AggregateArrayAggFunction::<DecimalType<i256>, State>::try_create(
                    display_name,
                    return_type,
                )
            }
        }
        _ => {
            if is_nullable {
                type State = NullableArrayAggState<AnyType>;
                AggregateArrayAggFunction::<AnyType, State>::try_create(display_name, return_type)
            } else {
                type State = ArrayAggState<AnyType>;
                AggregateArrayAggFunction::<AnyType, State>::try_create(display_name, return_type)
            }
        }
    })
}

pub fn aggregate_array_agg_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(try_create_aggregate_array_agg_function))
}
