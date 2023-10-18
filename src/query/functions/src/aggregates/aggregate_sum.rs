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

use common_arrow::arrow::bitmap::Bitmap;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::decimal::*;
use common_expression::types::number::Int8Type;
use common_expression::types::number::Number;
use common_expression::types::ArgType;
use common_expression::types::DataType;
use common_expression::types::DecimalDataType;
use common_expression::types::NumberDataType;
use common_expression::types::NumberType;
use common_expression::types::ValueType;
use common_expression::types::F64;
use common_expression::utils::arithmetics_type::ResultTypeOfUnary;
use common_expression::with_number_mapped_type;
use common_expression::Column;
use common_expression::ColumnBuilder;
use common_expression::Scalar;
use ethnum::i256;
use num_traits::AsPrimitive;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;

use super::aggregate_function::AggregateFunction;
use super::aggregate_function::AggregateFunctionRef;
use super::aggregate_function_factory::AggregateFunctionDescription;
use super::deserialize_state;
use super::serialize_state;
use super::StateAddr;
use crate::aggregates::aggregator_common::assert_unary_arguments;

pub trait SumState: Serialize + DeserializeOwned + Send + Sync + Default + 'static {
    fn merge(&mut self, other: &Self) -> Result<()>;
    fn mem_size() -> Option<usize> {
        None
    }
    fn serialize(&self, writer: &mut Vec<u8>) -> Result<()>;
    fn deserialize(&mut self, reader: &mut &[u8]) -> Result<()>;
    fn accumulate(&mut self, column: &Column, validity: Option<&Bitmap>) -> Result<()>;

    fn accumulate_row(&mut self, column: &Column, row: usize) -> Result<()>;
    fn accumulate_keys(places: &[StateAddr], offset: usize, columns: &Column) -> Result<()>;

    fn merge_result(
        &mut self,
        builder: &mut ColumnBuilder,
        window_size: &Option<usize>,
    ) -> Result<()>;

    fn merge_avg_result(
        &mut self,
        builder: &mut ColumnBuilder,
        count: u64,
        scale_add: u8,
        window_size: &Option<usize>,
    ) -> Result<()>;
}

#[derive(Default, Deserialize, Serialize)]
pub struct NumberSumState<T, TSum> {
    pub value: TSum,
    #[serde(skip)]
    _t: PhantomData<T>,
}

impl<T, TSum> SumState for NumberSumState<T, TSum>
where
    T: Number + AsPrimitive<TSum>,
    TSum: Number + AsPrimitive<f64> + Serialize + DeserializeOwned + std::ops::AddAssign,
{
    fn serialize(&self, writer: &mut Vec<u8>) -> Result<()> {
        serialize_state(writer, &self.value)
    }

    fn deserialize(&mut self, reader: &mut &[u8]) -> Result<()> {
        self.value = deserialize_state(reader)?;
        Ok(())
    }

    fn accumulate_row(&mut self, column: &Column, row: usize) -> Result<()> {
        let darray = NumberType::<T>::try_downcast_column(column).unwrap();
        self.value += darray[row].as_();
        Ok(())
    }

    fn accumulate(&mut self, column: &Column, validity: Option<&Bitmap>) -> Result<()> {
        let value = sum_primitive::<T, TSum>(column, validity)?;
        self.value += value;
        Ok(())
    }

    fn accumulate_keys(places: &[StateAddr], offset: usize, columns: &Column) -> Result<()> {
        let darray = NumberType::<T>::try_downcast_column(columns).unwrap();
        darray.iter().zip(places.iter()).for_each(|(c, place)| {
            let place = place.next(offset);
            let state = place.get::<Self>();
            state.value += c.as_();
        });
        Ok(())
    }

    #[inline(always)]
    fn merge(&mut self, other: &Self) -> Result<()> {
        self.value += other.value;
        Ok(())
    }

    fn merge_result(
        &mut self,
        builder: &mut ColumnBuilder,
        _window_size: &Option<usize>,
    ) -> Result<()> {
        let builder = NumberType::<TSum>::try_downcast_builder(builder).unwrap();
        builder.push(self.value);
        Ok(())
    }

    fn merge_avg_result(
        &mut self,
        builder: &mut ColumnBuilder,
        count: u64,
        _scale_add: u8,
        _window_size: &Option<usize>,
    ) -> Result<()> {
        let builder = NumberType::<F64>::try_downcast_builder(builder).unwrap();

        let value = self.value.as_() / (count as f64);
        builder.push(value.into());
        Ok(())
    }
}

#[derive(Default, Deserialize, Serialize)]
pub struct DecimalSumState<const OVERFLOW: bool, T> {
    pub value: T,
}

impl<const OVERFLOW: bool, T> DecimalSumState<OVERFLOW, T>
where T: Decimal
        + std::ops::AddAssign
        + Serialize
        + DeserializeOwned
        + Copy
        + Clone
        + std::fmt::Debug
        + std::cmp::PartialOrd
{
    #[inline]
    pub fn add(&mut self, value: T) -> Result<()> {
        self.value += value;
        if OVERFLOW && (self.value > T::MAX || self.value < T::MIN) {
            return Err(ErrorCode::Overflow(format!(
                "Decimal overflow: {} not in [{}, {}]",
                self.value,
                T::MIN,
                T::MAX,
            )));
        }
        Ok(())
    }
}

impl<const OVERFLOW: bool, T> SumState for DecimalSumState<OVERFLOW, T>
where T: Decimal
        + std::ops::AddAssign
        + Serialize
        + DeserializeOwned
        + Copy
        + Clone
        + std::fmt::Debug
        + std::cmp::PartialOrd
{
    fn mem_size() -> Option<usize> {
        Some(std::mem::size_of::<T>())
    }

    fn serialize(&self, writer: &mut Vec<u8>) -> Result<()> {
        serialize_state(writer, &self.value)
    }

    fn deserialize(&mut self, reader: &mut &[u8]) -> Result<()> {
        self.value = deserialize_state(reader)?;
        Ok(())
    }

    fn accumulate_row(&mut self, column: &Column, row: usize) -> Result<()> {
        let buffer = T::try_downcast_column(column).unwrap().0;
        self.add(buffer[row])
    }

    fn accumulate(&mut self, column: &Column, validity: Option<&Bitmap>) -> Result<()> {
        let buffer = T::try_downcast_column(column).unwrap().0;
        match validity {
            Some(validity) => {
                for (i, v) in validity.iter().enumerate() {
                    if v {
                        self.add(buffer[i])?;
                    }
                }
            }
            None => {
                for v in buffer.iter() {
                    self.add(*v)?;
                }
            }
        }
        Ok(())
    }

    fn accumulate_keys(places: &[StateAddr], offset: usize, columns: &Column) -> Result<()> {
        let buffer = T::try_downcast_column(columns).unwrap().0;
        for (i, place) in places.iter().enumerate() {
            let state = place.next(offset).get::<DecimalSumState<OVERFLOW, T>>();
            state.add(buffer[i])?;
        }
        Ok(())
    }

    #[inline(always)]
    fn merge(&mut self, other: &Self) -> Result<()> {
        self.add(other.value)
    }

    fn merge_result(
        &mut self,
        builder: &mut ColumnBuilder,
        _window_size: &Option<usize>,
    ) -> Result<()> {
        let builder = T::try_downcast_builder(builder).unwrap();
        builder.push(self.value);
        Ok(())
    }

    fn merge_avg_result(
        &mut self,
        builder: &mut ColumnBuilder,
        count: u64,
        scale_add: u8,
        _window_size: &Option<usize>,
    ) -> Result<()> {
        let builder = T::try_downcast_builder(builder).unwrap();

        match self
            .value
            .checked_mul(T::e(scale_add as u32))
            .and_then(|v| v.checked_div(T::from_u64(count)))
        {
            Some(value) => {
                builder.push(value);
                Ok(())
            }
            None => Err(ErrorCode::Overflow(format!(
                "Decimal overflow: {} mul {}",
                self.value,
                T::e(scale_add as u32)
            ))),
        }
    }
}

#[derive(Clone)]
pub struct AggregateSumFunction<State> {
    display_name: String,
    _arguments: Vec<DataType>,
    sum_t: PhantomData<State>,
    return_type: DataType,
}

impl<State> AggregateFunction for AggregateSumFunction<State>
where State: SumState
{
    fn name(&self) -> &str {
        "AggregateSumFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn init_state(&self, place: StateAddr) {
        place.write(|| State::default());
    }

    fn state_layout(&self) -> Layout {
        Layout::new::<State>()
    }

    fn serialize_size_per_row(&self) -> Option<usize> {
        State::mem_size()
    }

    fn accumulate(
        &self,
        place: StateAddr,
        columns: &[Column],
        validity: Option<&Bitmap>,
        _input_rows: usize,
    ) -> Result<()> {
        let state = place.get::<State>();
        state.accumulate(&columns[0], validity)
    }

    // null bits can be ignored above the level of the aggregate function
    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        offset: usize,
        columns: &[Column],
        _input_rows: usize,
    ) -> Result<()> {
        State::accumulate_keys(places, offset, &columns[0])
    }

    fn accumulate_row(&self, place: StateAddr, columns: &[Column], row: usize) -> Result<()> {
        let state = place.get::<State>();
        state.accumulate_row(&columns[0], row)
    }

    fn serialize(&self, place: StateAddr, writer: &mut Vec<u8>) -> Result<()> {
        let state = place.get::<State>();
        serialize_state(writer, state)
    }

    fn merge(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state = place.get::<State>();
        let rhs: State = deserialize_state(reader)?;
        state.merge(&rhs)
    }

    fn merge_states(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let state = place.get::<State>();
        let other = rhs.get::<State>();
        state.merge(other)
    }

    #[allow(unused_mut)]
    fn merge_result(&self, place: StateAddr, builder: &mut ColumnBuilder) -> Result<()> {
        let state = place.get::<State>();
        state.merge_result(builder, &None)
    }
}

impl<State> fmt::Display for AggregateSumFunction<State> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl<State> AggregateSumFunction<State>
where State: SumState
{
    pub fn try_create(
        display_name: &str,
        arguments: Vec<DataType>,
        return_type: DataType,
    ) -> Result<AggregateFunctionRef> {
        Ok(Arc::new(Self {
            display_name: display_name.to_owned(),
            _arguments: arguments,
            sum_t: PhantomData,
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
            type TSum = <NUM_TYPE as ResultTypeOfUnary>::Sum;
            type State = NumberSumState<NUM_TYPE, TSum>;
            AggregateSumFunction::<State>::try_create(
                display_name,
                arguments,
                NumberType::<TSum>::data_type(),
            )
        }
        DataType::Decimal(DecimalDataType::Decimal128(s)) => {
            let p = MAX_DECIMAL128_PRECISION;
            let decimal_size = DecimalSize {
                precision: p,
                scale: s.scale,
            };

            // DecimalWidth<int64_t> = 18
            let overflow = s.precision > 18;

            if overflow {
                AggregateSumFunction::<DecimalSumState<false, i128>>::try_create(
                    display_name,
                    arguments,
                    DataType::Decimal(DecimalDataType::from_size(decimal_size)?),
                )
            } else {
                AggregateSumFunction::<DecimalSumState<true, i128>>::try_create(
                    display_name,
                    arguments,
                    DataType::Decimal(DecimalDataType::from_size(decimal_size)?),
                )
            }
        }
        DataType::Decimal(DecimalDataType::Decimal256(s)) => {
            let p = MAX_DECIMAL256_PRECISION;
            let decimal_size = DecimalSize {
                precision: p,
                scale: s.scale,
            };

            let overflow = s.precision > 18;

            if overflow {
                AggregateSumFunction::<DecimalSumState<false, i256>>::try_create(
                    display_name,
                    arguments,
                    DataType::Decimal(DecimalDataType::from_size(decimal_size)?),
                )
            } else {
                AggregateSumFunction::<DecimalSumState<true, i256>>::try_create(
                    display_name,
                    arguments,
                    DataType::Decimal(DecimalDataType::from_size(decimal_size)?),
                )
            }
        }
        _ => Err(ErrorCode::BadDataValueType(format!(
            "AggregateSumFunction does not support type '{:?}'",
            arguments[0]
        ))),
    })
}

pub fn aggregate_sum_function_desc() -> AggregateFunctionDescription {
    let features = super::aggregate_function_factory::AggregateFunctionFeatures {
        is_decomposable: true,
        ..Default::default()
    };
    AggregateFunctionDescription::creator_with_features(
        Box::new(try_create_aggregate_sum_function),
        features,
    )
}

#[inline]
pub fn sum_primitive<T, TSum>(column: &Column, validity: Option<&Bitmap>) -> Result<TSum>
where
    T: Number + AsPrimitive<TSum>,
    TSum: Number + std::ops::AddAssign,
{
    let inner = NumberType::<T>::try_downcast_column(column).unwrap();
    if let Some(validity) = validity {
        let mut sum = TSum::default();
        inner.iter().zip(validity.iter()).for_each(|(t, b)| {
            if b {
                sum += t.as_();
            }
        });

        Ok(sum)
    } else {
        let mut sum = TSum::default();
        inner.iter().for_each(|t| {
            sum += t.as_();
        });

        Ok(sum)
    }
}
