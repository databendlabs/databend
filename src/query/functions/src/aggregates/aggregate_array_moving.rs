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
use common_arrow::arrow::buffer::Buffer;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::type_check::check_number;
use common_expression::types::decimal::*;
use common_expression::types::number::Number;
use common_expression::types::ArgType;
use common_expression::types::DataType;
use common_expression::types::DecimalDataType;
use common_expression::types::Float64Type;
use common_expression::types::Int8Type;
use common_expression::types::NumberDataType;
use common_expression::types::NumberType;
use common_expression::types::ValueType;
use common_expression::types::F64;
use common_expression::utils::arithmetics_type::ResultTypeOfUnary;
use common_expression::with_number_mapped_type;
use common_expression::Column;
use common_expression::ColumnBuilder;
use common_expression::Expr;
use common_expression::FunctionContext;
use common_expression::Scalar;
use common_expression::ScalarRef;
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
use crate::aggregates::aggregate_sum::SumState;
use crate::aggregates::assert_unary_arguments;
use crate::aggregates::assert_variadic_params;
use crate::BUILTIN_FUNCTIONS;

#[derive(Default, Debug, Deserialize, Serialize)]
pub struct NumberArrayMovingSumState<T, TSum> {
    values: Vec<T>,
    #[serde(skip)]
    _t: PhantomData<TSum>,
}

impl<T, TSum> SumState for NumberArrayMovingSumState<T, TSum>
where
    T: Number + AsPrimitive<TSum> + Serialize + DeserializeOwned,
    TSum: Number + AsPrimitive<f64> + std::ops::AddAssign + std::ops::SubAssign,
{
    fn serialize(&self, writer: &mut Vec<u8>) -> Result<()> {
        serialize_state(writer, &self.values)
    }

    fn deserialize(&mut self, reader: &mut &[u8]) -> Result<()> {
        self.values = deserialize_state(reader)?;
        Ok(())
    }

    fn accumulate_row(&mut self, column: &Column, row: usize) -> Result<()> {
        let buffer = match column {
            Column::Null { .. } => {
                self.values.push(T::default());
                return Ok(());
            }
            Column::Nullable(box nullable_column) => {
                NumberType::<T>::try_downcast_column(&nullable_column.column).unwrap()
            }
            _ => NumberType::<T>::try_downcast_column(column).unwrap(),
        };
        self.values.push(buffer[row]);
        Ok(())
    }

    fn accumulate(&mut self, column: &Column, validity: Option<&Bitmap>) -> Result<()> {
        let buffer = match column {
            Column::Null { len } => {
                for _ in 0..*len {
                    self.values.push(T::default());
                }
                return Ok(());
            }
            Column::Nullable(box nullable_column) => {
                NumberType::<T>::try_downcast_column(&nullable_column.column).unwrap()
            }
            _ => NumberType::<T>::try_downcast_column(column).unwrap(),
        };
        if let Some(validity) = validity {
            buffer.iter().zip(validity.iter()).for_each(|(v, b)| {
                if b {
                    self.values.push(*v);
                } else {
                    self.values.push(T::default());
                }
            });
        } else {
            buffer.iter().for_each(|v| {
                self.values.push(*v);
            });
        }
        Ok(())
    }

    fn accumulate_keys(places: &[StateAddr], offset: usize, columns: &Column) -> Result<()> {
        let buffer = match columns {
            Column::Null { len } => Buffer::from(vec![T::default(); *len]),
            Column::Nullable(box nullable_column) => {
                NumberType::<T>::try_downcast_column(&nullable_column.column).unwrap()
            }
            _ => NumberType::<T>::try_downcast_column(columns).unwrap(),
        };
        buffer.iter().zip(places.iter()).for_each(|(c, place)| {
            let place = place.next(offset);
            let state = place.get::<Self>();
            state.values.push(*c);
        });
        Ok(())
    }

    #[inline(always)]
    fn merge(&mut self, other: &Self) -> Result<()> {
        self.values.extend_from_slice(&other.values);
        Ok(())
    }

    fn merge_result(
        &mut self,
        builder: &mut ColumnBuilder,
        window_size: &Option<usize>,
    ) -> Result<()> {
        let window_size = match window_size {
            Some(window_size) => *window_size,
            None => self.values.len(),
        };

        let mut sum = TSum::default();
        let mut sum_values: Vec<TSum> = Vec::with_capacity(self.values.len());
        for (i, value) in self.values.iter().enumerate() {
            sum += value.as_();
            if i >= window_size {
                sum -= self.values[i - window_size].as_();
            }
            sum_values.push(sum);
        }

        let inner_col = NumberType::<TSum>::upcast_column(sum_values.into());
        let array_value = ScalarRef::Array(inner_col);
        builder.push(array_value);

        Ok(())
    }

    fn merge_avg_result(
        &mut self,
        builder: &mut ColumnBuilder,
        _count: u64,
        _scale_add: u8,
        window_size: &Option<usize>,
    ) -> Result<()> {
        let window_size = match window_size {
            Some(window_size) => *window_size,
            None => self.values.len(),
        };

        let mut sum = TSum::default();
        let mut avg_values: Vec<F64> = Vec::with_capacity(self.values.len());
        for (i, value) in self.values.iter().enumerate() {
            sum += value.as_();
            if i >= window_size {
                sum -= self.values[i - window_size].as_();
            }
            let avg_val = sum.as_() / (window_size as f64);
            avg_values.push(avg_val.into());
        }

        let inner_col = NumberType::<F64>::upcast_column(avg_values.into());
        let array_value = ScalarRef::Array(inner_col);
        builder.push(array_value);

        Ok(())
    }
}

#[derive(Default, Deserialize, Serialize)]
pub struct DecimalArrayMovingSumState<T> {
    pub values: Vec<T>,
}

impl<T> DecimalArrayMovingSumState<T>
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
    pub fn check_over_flow(&self, value: T) -> Result<()> {
        if value > T::MAX || value < T::MIN {
            return Err(ErrorCode::Overflow(format!(
                "Decimal overflow: {} not in [{}, {}]",
                value,
                T::MIN,
                T::MAX,
            )));
        }
        Ok(())
    }
}

impl<T> SumState for DecimalArrayMovingSumState<T>
where T: Decimal
        + std::ops::AddAssign
        + std::ops::SubAssign
        + Serialize
        + DeserializeOwned
        + Copy
        + Clone
        + std::fmt::Debug
        + std::cmp::PartialOrd
{
    fn serialize(&self, writer: &mut Vec<u8>) -> Result<()> {
        serialize_state(writer, &self.values)
    }

    fn deserialize(&mut self, reader: &mut &[u8]) -> Result<()> {
        self.values = deserialize_state(reader)?;
        Ok(())
    }

    fn accumulate_row(&mut self, column: &Column, row: usize) -> Result<()> {
        let buffer = match column {
            Column::Null { .. } => {
                self.values.push(T::default());
                return Ok(());
            }
            Column::Nullable(box nullable_column) => {
                T::try_downcast_column(&nullable_column.column).unwrap().0
            }
            _ => T::try_downcast_column(column).unwrap().0,
        };
        self.values.push(buffer[row]);
        Ok(())
    }

    fn accumulate(&mut self, column: &Column, validity: Option<&Bitmap>) -> Result<()> {
        let buffer = match column {
            Column::Null { len } => {
                for _ in 0..*len {
                    self.values.push(T::default());
                }
                return Ok(());
            }
            Column::Nullable(box nullable_column) => {
                T::try_downcast_column(&nullable_column.column).unwrap().0
            }
            _ => T::try_downcast_column(column).unwrap().0,
        };
        match validity {
            Some(validity) => {
                for (i, v) in validity.iter().enumerate() {
                    if v {
                        self.values.push(buffer[i]);
                    } else {
                        self.values.push(T::default());
                    }
                }
            }
            None => {
                for v in buffer.iter() {
                    self.values.push(*v);
                }
            }
        }
        Ok(())
    }

    fn accumulate_keys(places: &[StateAddr], offset: usize, columns: &Column) -> Result<()> {
        let buffer = match columns {
            Column::Null { len } => Buffer::from(vec![T::default(); *len]),
            Column::Nullable(box nullable_column) => {
                T::try_downcast_column(&nullable_column.column).unwrap().0
            }
            _ => T::try_downcast_column(columns).unwrap().0,
        };
        buffer.iter().zip(places.iter()).for_each(|(c, place)| {
            let place = place.next(offset);
            let state = place.get::<Self>();
            state.values.push(*c);
        });
        Ok(())
    }

    #[inline(always)]
    fn merge(&mut self, other: &Self) -> Result<()> {
        self.values.extend_from_slice(&other.values);
        Ok(())
    }

    fn merge_result(
        &mut self,
        builder: &mut ColumnBuilder,
        window_size: &Option<usize>,
    ) -> Result<()> {
        let window_size = match window_size {
            Some(window_size) => *window_size,
            None => self.values.len(),
        };

        let mut sum = T::default();
        let mut sum_values: Vec<T> = Vec::with_capacity(self.values.len());
        for (i, value) in self.values.iter().enumerate() {
            sum += *value;
            self.check_over_flow(sum)?;
            if i >= window_size {
                sum -= self.values[i - window_size];
            }
            sum_values.push(sum);
        }

        let data_type = builder.data_type();
        let inner_type = data_type.as_array().unwrap();
        let decimal_type = inner_type.as_decimal().unwrap();

        let inner_col = T::upcast_column(sum_values.into(), decimal_type.size());
        let array_value = ScalarRef::Array(inner_col);
        builder.push(array_value);

        Ok(())
    }

    fn merge_avg_result(
        &mut self,
        builder: &mut ColumnBuilder,
        _count: u64,
        scale_add: u8,
        window_size: &Option<usize>,
    ) -> Result<()> {
        let window_size = match window_size {
            Some(window_size) => *window_size,
            None => self.values.len(),
        };

        let mut sum = T::default();
        let mut avg_values: Vec<T> = Vec::with_capacity(self.values.len());
        for (i, value) in self.values.iter().enumerate() {
            sum += *value;
            self.check_over_flow(sum)?;
            if i >= window_size {
                sum -= self.values[i - window_size];
            }
            let avg_val = match sum
                .checked_mul(T::e(scale_add as u32))
                .and_then(|v| v.checked_div(T::from_u64(window_size as u64)))
            {
                Some(value) => value,
                None => {
                    return Err(ErrorCode::Overflow(format!(
                        "Decimal overflow: {} mul {}",
                        sum,
                        T::e(scale_add as u32)
                    )));
                }
            };
            avg_values.push(avg_val);
        }

        let data_type = builder.data_type();
        let inner_type = data_type.as_array().unwrap();
        let decimal_type = inner_type.as_decimal().unwrap();

        let inner_col = T::upcast_column(avg_values.into(), decimal_type.size());
        let array_value = ScalarRef::Array(inner_col);
        builder.push(array_value);

        Ok(())
    }
}

#[derive(Clone)]
pub struct AggregateArrayMovingAvgFunction<State> {
    display_name: String,
    window_size: Option<usize>,
    sum_t: PhantomData<State>,
    return_type: DataType,
    scale_add: u8,
}

impl<State> AggregateFunction for AggregateArrayMovingAvgFunction<State>
where State: SumState
{
    fn name(&self) -> &str {
        "AggregateArrayMovingAvgFunction"
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

    fn merge_result(&self, place: StateAddr, builder: &mut ColumnBuilder) -> Result<()> {
        let state = place.get::<State>();
        state.merge_avg_result(builder, 0_u64, self.scale_add, &self.window_size)
    }

    fn need_manual_drop_state(&self) -> bool {
        true
    }

    unsafe fn drop_state(&self, place: StateAddr) {
        let state = place.get::<State>();
        std::ptr::drop_in_place(state);
    }
}

impl<State> fmt::Display for AggregateArrayMovingAvgFunction<State> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl<State> AggregateArrayMovingAvgFunction<State>
where State: SumState
{
    pub fn try_create(
        display_name: &str,
        params: Vec<Scalar>,
        return_type: DataType,
        scale_add: u8,
    ) -> Result<AggregateFunctionRef> {
        let window_size = if params.len() == 1 {
            let window_size = check_number::<_, u64>(
                None,
                &FunctionContext::default(),
                &Expr::<usize>::Constant {
                    span: None,
                    scalar: params[0].clone(),
                    data_type: params[0].as_ref().infer_data_type(),
                },
                &BUILTIN_FUNCTIONS,
            )?;
            Some(window_size as usize)
        } else {
            None
        };

        Ok(Arc::new(Self {
            display_name: display_name.to_owned(),
            window_size,
            sum_t: PhantomData,
            return_type,
            scale_add,
        }))
    }
}

pub fn try_create_aggregate_array_moving_avg_function(
    display_name: &str,
    params: Vec<Scalar>,
    arguments: Vec<DataType>,
) -> Result<AggregateFunctionRef> {
    assert_unary_arguments(display_name, arguments.len())?;
    assert_variadic_params(display_name, params.len(), (0, 1))?;

    let data_type = if arguments[0].is_null() {
        Int8Type::data_type()
    } else {
        arguments[0].remove_nullable()
    };
    with_number_mapped_type!(|NUM_TYPE| match &data_type {
        DataType::Number(NumberDataType::NUM_TYPE) => {
            type TSum = <NUM_TYPE as ResultTypeOfUnary>::Sum;
            type State = NumberArrayMovingSumState<NUM_TYPE, TSum>;
            AggregateArrayMovingAvgFunction::<State>::try_create(
                display_name,
                params,
                DataType::Array(Box::new(Float64Type::data_type())),
                0,
            )
        }
        DataType::Decimal(DecimalDataType::Decimal128(s)) => {
            let p = MAX_DECIMAL128_PRECISION;
            let decimal_size = DecimalSize {
                precision: p,
                scale: s.scale.max(4),
            };

            AggregateArrayMovingAvgFunction::<DecimalArrayMovingSumState<i128>>::try_create(
                display_name,
                params,
                DataType::Array(Box::new(DataType::Decimal(DecimalDataType::from_size(
                    decimal_size,
                )?))),
                decimal_size.scale - s.scale,
            )
        }
        DataType::Decimal(DecimalDataType::Decimal256(s)) => {
            let p = MAX_DECIMAL256_PRECISION;
            let decimal_size = DecimalSize {
                precision: p,
                scale: s.scale.max(4),
            };

            AggregateArrayMovingAvgFunction::<DecimalArrayMovingSumState<i256>>::try_create(
                display_name,
                params,
                DataType::Array(Box::new(DataType::Decimal(DecimalDataType::from_size(
                    decimal_size,
                )?))),
                decimal_size.scale - s.scale,
            )
        }
        _ => Err(ErrorCode::BadDataValueType(format!(
            "AggregateArrayMovingAvgFunction does not support type '{:?}'",
            arguments[0]
        ))),
    })
}

pub fn aggregate_array_moving_avg_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(try_create_aggregate_array_moving_avg_function))
}

#[derive(Clone)]
pub struct AggregateArrayMovingSumFunction<State> {
    display_name: String,
    window_size: Option<usize>,
    sum_t: PhantomData<State>,
    return_type: DataType,
}

impl<State> AggregateFunction for AggregateArrayMovingSumFunction<State>
where State: SumState
{
    fn name(&self) -> &str {
        "AggregateArrayMovingSumFunction"
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

    fn merge_result(&self, place: StateAddr, builder: &mut ColumnBuilder) -> Result<()> {
        let state = place.get::<State>();
        state.merge_result(builder, &self.window_size)
    }

    fn need_manual_drop_state(&self) -> bool {
        true
    }

    unsafe fn drop_state(&self, place: StateAddr) {
        let state = place.get::<State>();
        std::ptr::drop_in_place(state);
    }
}

impl<State> fmt::Display for AggregateArrayMovingSumFunction<State> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl<State> AggregateArrayMovingSumFunction<State>
where State: SumState
{
    pub fn try_create(
        display_name: &str,
        params: Vec<Scalar>,
        return_type: DataType,
    ) -> Result<AggregateFunctionRef> {
        let window_size = if params.len() == 1 {
            let window_size = check_number::<_, u64>(
                None,
                &FunctionContext::default(),
                &Expr::<usize>::Constant {
                    span: None,
                    scalar: params[0].clone(),
                    data_type: params[0].as_ref().infer_data_type(),
                },
                &BUILTIN_FUNCTIONS,
            )?;
            Some(window_size as usize)
        } else {
            None
        };

        Ok(Arc::new(Self {
            display_name: display_name.to_owned(),
            window_size,
            sum_t: PhantomData,
            return_type,
        }))
    }
}

pub fn try_create_aggregate_array_moving_sum_function(
    display_name: &str,
    params: Vec<Scalar>,
    arguments: Vec<DataType>,
) -> Result<AggregateFunctionRef> {
    assert_unary_arguments(display_name, arguments.len())?;
    assert_variadic_params(display_name, params.len(), (0, 1))?;

    let data_type = if arguments[0].is_null() {
        Int8Type::data_type()
    } else {
        arguments[0].remove_nullable()
    };
    with_number_mapped_type!(|NUM_TYPE| match &data_type {
        DataType::Number(NumberDataType::NUM_TYPE) => {
            type TSum = <NUM_TYPE as ResultTypeOfUnary>::Sum;
            type State = NumberArrayMovingSumState<NUM_TYPE, TSum>;
            AggregateArrayMovingSumFunction::<State>::try_create(
                display_name,
                params,
                DataType::Array(Box::new(NumberType::<TSum>::data_type())),
            )
        }
        DataType::Decimal(DecimalDataType::Decimal128(s)) => {
            let p = MAX_DECIMAL128_PRECISION;
            let decimal_size = DecimalSize {
                precision: p,
                scale: s.scale,
            };

            AggregateArrayMovingSumFunction::<DecimalArrayMovingSumState<i128>>::try_create(
                display_name,
                params,
                DataType::Array(Box::new(DataType::Decimal(DecimalDataType::from_size(
                    decimal_size,
                )?))),
            )
        }
        DataType::Decimal(DecimalDataType::Decimal256(s)) => {
            let p = MAX_DECIMAL256_PRECISION;
            let decimal_size = DecimalSize {
                precision: p,
                scale: s.scale,
            };

            AggregateArrayMovingSumFunction::<DecimalArrayMovingSumState<i256>>::try_create(
                display_name,
                params,
                DataType::Array(Box::new(DataType::Decimal(DecimalDataType::from_size(
                    decimal_size,
                )?))),
            )
        }
        _ => Err(ErrorCode::BadDataValueType(format!(
            "AggregateArrayMovingSumFunction does not support type '{:?}'",
            arguments[0]
        ))),
    })
}

pub fn aggregate_array_moving_sum_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(try_create_aggregate_array_moving_sum_function))
}
