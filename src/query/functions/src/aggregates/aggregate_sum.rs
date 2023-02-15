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

use common_arrow::arrow::bitmap::Bitmap;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::decimal::Decimal;
use common_expression::types::number::Int8Type;
use common_expression::types::number::Number;
use common_expression::types::ArgType;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::types::NumberType;
use common_expression::types::ValueType;
use common_expression::utils::arithmetics_type::ResultTypeOfUnary;
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

trait SumState: Default {
    fn add(&mut self, other: Self, precision: u8) -> Result<()>;
    fn serialize(&self, writer: &mut Vec<u8>) -> Result<()>;
    fn deserialize(&mut self, reader: &mut &[u8]) -> Result<()>;
    fn accumulate(
        &mut self,
        column: &Column,
        validity: Option<&Bitmap>,
        precision: u8,
    ) -> Result<()>;
    fn merge(&mut self, other: &Self, precision: u8) -> Result<()>;
    fn merge_result(&mut self) -> Result<ValueType>;
}

#[derive(Default)]
struct NumberSumState<T> {
    pub value: T,
}

impl<T> SumState for NumberSumState<T>
where T: Number + std::ops::AddAssign + Serialize + DeserializeOwned + Copy + Clone + std::fmt::Debug
{
    #[inline(always)]
    fn add(&mut self, other: T, precision: u8) -> Result<()> {
        self.value += other;
        Ok(())
    }

    fn serialize(&self, writer: &mut Vec<u8>) -> Result<()> {
        serialize_into_buf(writer, &self.value)
    }

    fn deserialize(&mut self, reader: &mut &[u8]) -> Result<()> {
        self.value = deserialize_from_slice(reader)?;
        Ok(())
    }

    fn accumulate(
        &mut self,
        column: &Column,
        validity: Option<&Bitmap>,
        _precision: u8,
    ) -> Result<()> {
        let value = sum_primitive(column, validity)?;
        self.value += value;
        Ok(())
    }
}

#[derive(Default)]
struct DecimalSumState<T: Decimal> {
    pub value: T,
}

impl<T> DecimalSumState<T>
where T: Decimal
        + std::ops::AddAssign
        + Serialize
        + DeserializeOwned
        + Copy
        + Clone
        + std::fmt::Debug
{
    #[inline]
    pub fn check_over_flow(&self, precision: u8) -> Result<()> {
        if self.value > T::max_for_precision(self.precision) {
            return Err(ErrorCode::Overflow(format!(
                "Decimal overflow: {} > {}",
                self.value,
                T::max_for_precision(self.precision)
            )));
        }
        Ok(())
    }
}

impl<T> SumState for DecimalSumState<T>
where T: Decimal
        + std::ops::AddAssign
        + Serialize
        + DeserializeOwned
        + Copy
        + Clone
        + std::fmt::Debug
{
    #[inline(always)]
    fn add(&mut self, other: T, precision: u8) -> Result<()> {
        self.value += other;
        self.check_over_flow(precision)
    }

    fn serialize(&self, writer: &mut Vec<u8>) -> Result<()> {
        serialize_into_buf(writer, &self.value)
    }

    fn deserialize(&mut self, reader: &mut &[u8]) -> Result<()> {
        self.value = deserialize_from_slice(reader)?;
        Ok(())
    }

    fn accumulate(
        &mut self,
        column: &Column,
        validity: Option<&Bitmap>,
        precision: u8,
    ) -> Result<()> {
        let buffer = T::try_downcast_column(column, validity).unwrap().0;
        match validity {
            Some(validity) => {
                for (i, v) in validity.iter().enumerate() {
                    if v {
                        self.value += buffer[i];
                    }
                }
            }
            None => {
                for v in buffer.iter() {
                    self.value += *v;
                }
            }
        }
        self.check_over_flow(precision)
    }
}

#[derive(Clone)]
pub struct AggregateSumFunction<SumT> {
    display_name: String,
    _arguments: Vec<DataType>,
    precision: u8,
    sum_t: PhantomData<SumT>,
    return_type: DataType,
}

impl<SumT> AggregateFunction for AggregateSumFunction<SumT>
where SumT: SumState
{
    fn name(&self) -> &str {
        "AggregateSumFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(self.return_type)
    }

    fn init_state(&self, place: StateAddr) {
        place.write(|| SumT::default());
    }

    fn state_layout(&self) -> Layout {
        Layout::new::<SumT>()
    }

    fn accumulate(
        &self,
        place: StateAddr,
        columns: &[Column],
        validity: Option<&Bitmap>,
        _input_rows: usize,
    ) -> Result<()> {
        let state = place.get::<SumT>();
        state.accumulate(value, validity, self.precision)
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
            let state = place.get::<SumT>();
            state.add(c.as_());
        });

        Ok(())
    }

    fn accumulate_row(&self, place: StateAddr, columns: &[Column], row: usize) -> Result<()> {
        let state = place.get::<SumT>();
        state.add(&columns[0], row);
        Ok(())
    }

    fn serialize(&self, place: StateAddr, writer: &mut Vec<u8>) -> Result<()> {
        let state = place.get::<SumT>();
        state.serialize(writer)
    }

    fn deserialize(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state = place.get::<SumT>();
        state.deserialize(reader)
    }

    fn merge(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let rhs = rhs.get::<SumT>();
        let state = place.get::<SumT>();
        state.merge(rhs)
    }

    #[allow(unused_mut)]
    fn merge_result(&self, place: StateAddr, builder: &mut ColumnBuilder) -> Result<()> {
        let state = place.get::<SumT>();
        state.merge_result(builder)
    }
}

impl<SumT> fmt::Display for AggregateSumFunction<SumT> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl<SumT> AggregateSumFunction<SumT>
where SumT: SumState
{
    pub fn try_create(
        display_name: &str,
        arguments: Vec<DataType>,
        precision: u8,
        return_type: DataType,
    ) -> Result<AggregateFunctionRef> {
        Ok(Arc::new(Self {
            display_name: display_name.to_owned(),
            _arguments: arguments,
            sum_t: PhantomData,
            precision,
            return_type,
        }))
    }
}

pub fn try_create_aggregate_sum_function(
    display_name: &str,
    _params: Vec<Scalar>,
    arguments: Vec<DataType>,
) -> Result<AggregateFunctionRef> {
    assert_unary_arguments(display_name, arguments.len())?;

    let mut data_type = arguments[0].clone();
    // null use dummy func, it's already covered in `AggregateNullResultFunction`
    if data_type.is_null() {
        data_type = Int8Type::data_type();
    }

    with_number_mapped_type!(|NUM_TYPE| match &data_type {
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
pub fn sum_primitive<SumT>(column: &Column, validity: Option<&Bitmap>) -> Result<SumT>
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
