// Copyright 2022 Datafuse Labs.
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

use bytes::BytesMut;
use common_arrow::arrow::bitmap::Bitmap;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::arithmetics_type::ResultTypeOfUnary;
use common_expression::types::number::Number;
use common_expression::types::ArgType;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::types::NumberType;
use common_expression::types::ValueType;
use common_expression::with_number_mapped_type;
use common_expression::Column;
use common_expression::ColumnBuilder;
use common_expression::Scalar;
use common_io::prelude::*;
use num_traits::AsPrimitive;
use serde::de::DeserializeOwned;
use serde::Serialize;

use super::aggregate_function::AggregateFunction;
use super::aggregate_function::AggregateFunctionRef;
use super::aggregate_function_factory::AggregateFunctionDescription;
use super::StateAddr;
use crate::aggregates::aggregator_common::assert_unary_arguments;

struct AggregateSumState<T> {
    pub value: T,
}

impl<T> AggregateSumState<T>
where T: std::ops::AddAssign + Serialize + DeserializeOwned + Copy + Clone + std::fmt::Debug
{
    #[inline(always)]
    fn add(&mut self, other: T) {
        self.value += other;
    }

    fn serialize(&self, writer: &mut BytesMut) -> Result<()> {
        serialize_into_buf(writer, &self.value)
    }

    fn deserialize(&mut self, reader: &mut &[u8]) -> Result<()> {
        self.value = deserialize_from_slice(reader)?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct AggregateSumFunction<T, SumT> {
    display_name: String,
    _arguments: Vec<DataType>,
    t: PhantomData<T>,
    sum_t: PhantomData<SumT>,
}

impl<T, SumT> AggregateFunction for AggregateSumFunction<T, SumT>
where
    T: Number + AsPrimitive<SumT>,
    SumT: Number + Serialize + DeserializeOwned + std::ops::AddAssign,
{
    fn name(&self) -> &str {
        "AggregateSumFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(NumberType::<SumT>::data_type())
    }

    fn init_state(&self, place: StateAddr) {
        place.write(|| AggregateSumState::<SumT> {
            value: SumT::default(),
        });
    }

    fn state_layout(&self) -> Layout {
        Layout::new::<AggregateSumState<SumT>>()
    }

    fn accumulate(
        &self,
        place: StateAddr,
        columns: &[Column],
        validity: Option<&Bitmap>,
        _input_rows: usize,
    ) -> Result<()> {
        let state = place.get::<AggregateSumState<SumT>>();
        let value = sum_primitive::<T, SumT>(&columns[0], validity)?;
        state.add(value);
        Ok(())
    }

    // null bits can be ignored above the level of the aggregate function
    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        offset: usize,
        columns: &[Column],
        _input_rows: usize,
    ) -> Result<()> {
        let darray = NumberType::<T>::try_downcast_column(&columns[0]).unwrap();
        darray.iter().zip(places.iter()).for_each(|(c, place)| {
            let place = place.next(offset);
            let state = place.get::<AggregateSumState<SumT>>();
            state.add(c.as_());
        });

        Ok(())
    }

    fn accumulate_row(&self, place: StateAddr, columns: &[Column], row: usize) -> Result<()> {
        let column = NumberType::<T>::try_downcast_column(&columns[0]).unwrap();
        let state = place.get::<AggregateSumState<SumT>>();
        let v = column[row].as_();
        state.add(v);
        Ok(())
    }

    fn serialize(&self, place: StateAddr, writer: &mut BytesMut) -> Result<()> {
        let state = place.get::<AggregateSumState<SumT>>();
        state.serialize(writer)
    }

    fn deserialize(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state = place.get::<AggregateSumState<SumT>>();
        state.deserialize(reader)
    }

    fn merge(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let rhs = rhs.get::<AggregateSumState<SumT>>();
        let state = place.get::<AggregateSumState<SumT>>();
        state.add(rhs.value);
        Ok(())
    }

    #[allow(unused_mut)]
    fn merge_result(&self, place: StateAddr, builder: &mut ColumnBuilder) -> Result<()> {
        let state = place.get::<AggregateSumState<SumT>>();
        let builder = NumberType::<SumT>::try_downcast_builder(builder).unwrap();
        builder.push(state.value);
        Ok(())
    }
}

impl<T, SumT> fmt::Display for AggregateSumFunction<T, SumT> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl<T, SumT> AggregateSumFunction<T, SumT>
where
    T: Number + AsPrimitive<SumT>,
    SumT: Number + Serialize + DeserializeOwned + std::ops::AddAssign,
{
    pub fn try_create(
        display_name: &str,
        arguments: Vec<DataType>,
    ) -> Result<AggregateFunctionRef> {
        Ok(Arc::new(Self {
            display_name: display_name.to_owned(),
            _arguments: arguments,
            t: PhantomData,
            sum_t: PhantomData,
        }))
    }
}

pub fn try_create_aggregate_sum_function(
    display_name: &str,
    _params: Vec<Scalar>,
    arguments: Vec<DataType>,
) -> Result<AggregateFunctionRef> {
    assert_unary_arguments(display_name, arguments.len())?;
    with_number_mapped_type!(|NUM_TYPE| match &arguments[0] {
        DataType::Number(NumberDataType::NUM_TYPE) => {
            AggregateSumFunction::<NUM_TYPE, <NUM_TYPE as ResultTypeOfUnary>::Sum>::try_create(
                display_name,
                arguments,
            )
        }
        _ => Err(ErrorCode::BadDataValueType(format!(
            "AggregateSumFunction does not support type '{:?}'",
            arguments[0]
        ))),
    })
}

pub fn aggregate_sum_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(try_create_aggregate_sum_function))
}

#[inline]
pub fn sum_primitive<T, SumT>(column: &Column, validity: Option<&Bitmap>) -> Result<SumT>
where
    T: Number + AsPrimitive<SumT>,
    SumT: Number + std::ops::AddAssign,
{
    let inner = NumberType::<T>::try_downcast_column(column).unwrap();
    if let Some(validity) = validity {
        let mut sum = SumT::default();
        inner.iter().zip(validity.iter()).for_each(|(t, b)| {
            if b {
                sum += t.as_();
            }
        });

        Ok(sum)
    } else {
        let mut sum = SumT::default();
        inner.iter().for_each(|t| {
            sum += t.as_();
        });

        Ok(sum)
    }
}
