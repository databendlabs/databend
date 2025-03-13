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

use boolean::TrueIdxIter;
use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_column::types::months_days_micros;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::decimal::*;
use databend_common_expression::types::number::*;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::Buffer;
use databend_common_expression::types::*;
use databend_common_expression::utils::arithmetics_type::ResultTypeOfUnary;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::AggregateFunctionRef;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::Scalar;
use databend_common_expression::StateAddr;
use databend_common_expression::SELECTIVITY_THRESHOLD;
use num_traits::AsPrimitive;

use super::assert_unary_arguments;
use super::FunctionData;
use crate::aggregates::aggregate_function_factory::AggregateFunctionDescription;
use crate::aggregates::aggregate_function_factory::AggregateFunctionSortDesc;
use crate::aggregates::aggregate_unary::UnaryState;
use crate::aggregates::AggrStateLoc;
use crate::aggregates::AggregateUnaryFunction;

pub trait SumState: BorshSerialize + BorshDeserialize + Send + Sync + Default + 'static {
    fn merge(&mut self, other: &Self) -> Result<()>;
    fn mem_size() -> Option<usize> {
        None
    }

    fn accumulate(&mut self, column: &Column, validity: Option<&Bitmap>) -> Result<()>;

    fn accumulate_row(&mut self, column: &Column, row: usize) -> Result<()>;
    fn accumulate_keys(places: &[StateAddr], loc: &[AggrStateLoc], columns: &Column) -> Result<()>;

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

#[derive(BorshSerialize, BorshDeserialize)]
pub struct NumberSumState<N>
where N: ValueType
{
    pub value: N::Scalar,
}

impl<N> Default for NumberSumState<N>
where
    N: ValueType,
    N::Scalar: Number + AsPrimitive<f64> + BorshSerialize + BorshDeserialize + std::ops::AddAssign,
{
    fn default() -> Self {
        NumberSumState::<N> {
            value: N::Scalar::default(),
        }
    }
}

// #[multiversion::multiversion(targets("x86_64+avx", "x86_64+sse"))]
#[inline]
pub fn sum_batch<T, TSum>(inner: Buffer<T>, validity: Option<&Bitmap>) -> TSum
where
    T: Number + AsPrimitive<TSum>,
    TSum: Number + std::ops::AddAssign,
{
    match validity {
        Some(v) => {
            let mut sum = TSum::default();
            if v.true_count() as f64 / v.len() as f64 >= SELECTIVITY_THRESHOLD {
                inner.iter().zip(v.iter()).for_each(|(t, b)| {
                    if b {
                        sum += t.as_();
                    }
                });
            } else {
                TrueIdxIter::new(v.len(), Some(v)).for_each(|idx| {
                    sum += unsafe { inner.get_unchecked(idx).as_() };
                });
            }
            sum
        }
        _ => {
            let mut sum = TSum::default();
            inner.iter().for_each(|t| {
                sum += t.as_();
            });

            sum
        }
    }
}

impl<T, N> UnaryState<T, N> for NumberSumState<N>
where
    T: ValueType + Sync + Send,
    N: ValueType,
    T::Scalar: Number + AsPrimitive<N::Scalar>,
    N::Scalar: Number + AsPrimitive<f64> + BorshSerialize + BorshDeserialize + std::ops::AddAssign,
    for<'a> T::ScalarRef<'a>: Number + AsPrimitive<N::Scalar>,
{
    fn add(
        &mut self,
        other: T::ScalarRef<'_>,
        _function_data: Option<&dyn FunctionData>,
    ) -> Result<()> {
        self.value += other.as_();
        Ok(())
    }

    fn add_batch(
        &mut self,
        other: T::Column,
        validity: Option<&Bitmap>,
        _function_data: Option<&dyn FunctionData>,
    ) -> Result<()> {
        let col = T::upcast_column(other);
        let buffer = NumberType::<T::Scalar>::try_downcast_column(&col).unwrap();
        self.value += sum_batch::<T::Scalar, N::Scalar>(buffer, validity);
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.value += rhs.value;
        Ok(())
    }

    fn merge_result(
        &mut self,
        builder: &mut N::ColumnBuilder,
        _function_data: Option<&dyn FunctionData>,
    ) -> Result<()> {
        N::push_item(builder, N::to_scalar_ref(&self.value));
        Ok(())
    }
}

#[derive(BorshDeserialize, BorshSerialize)]
pub struct DecimalSumState<const OVERFLOW: bool, T>
where
    T: ValueType,
    T::Scalar: Decimal,
    <T::Scalar as Decimal>::U64Array: BorshSerialize + BorshDeserialize,
{
    pub value: <T::Scalar as Decimal>::U64Array,
}

impl<const OVERFLOW: bool, T> Default for DecimalSumState<OVERFLOW, T>
where
    T: ValueType,
    T::Scalar: Decimal + std::ops::AddAssign,
    <T::Scalar as Decimal>::U64Array: BorshSerialize + BorshDeserialize,
{
    fn default() -> Self {
        Self {
            value: <T::Scalar as Decimal>::U64Array::default(),
        }
    }
}

impl<const OVERFLOW: bool, T> UnaryState<T, T> for DecimalSumState<OVERFLOW, T>
where
    T: ValueType,
    T::Scalar: Decimal + std::ops::AddAssign,
    <T::Scalar as Decimal>::U64Array: BorshSerialize + BorshDeserialize,
{
    fn add(
        &mut self,
        other: T::ScalarRef<'_>,
        _function_data: Option<&dyn FunctionData>,
    ) -> Result<()> {
        let mut value = T::Scalar::from_u64_array(self.value);
        value += T::to_owned_scalar(other);

        if OVERFLOW && (value > T::Scalar::MAX || value < T::Scalar::MIN) {
            return Err(ErrorCode::Overflow(format!(
                "Decimal overflow: {:?} not in [{}, {}]",
                self.value,
                T::Scalar::MIN,
                T::Scalar::MAX,
            )));
        }
        self.value = value.to_u64_array();
        Ok(())
    }

    fn add_batch(
        &mut self,
        other: T::Column,
        validity: Option<&Bitmap>,
        function_data: Option<&dyn FunctionData>,
    ) -> Result<()> {
        if !OVERFLOW {
            let mut sum = T::Scalar::from_u64_array(self.value);
            let col = T::upcast_column(other);
            let buffer = DecimalType::<T::Scalar>::try_downcast_column(&col).unwrap();
            match validity {
                Some(validity) if validity.null_count() > 0 => {
                    buffer.iter().zip(validity.iter()).for_each(|(t, b)| {
                        if b {
                            sum += *t;
                        }
                    });
                }
                _ => {
                    buffer.iter().for_each(|t| {
                        sum += *t;
                    });
                }
            }
            self.value = sum.to_u64_array();
        } else {
            match validity {
                Some(validity) => {
                    for (data, valid) in T::iter_column(&other).zip(validity.iter()) {
                        if valid {
                            self.add(data, function_data)?;
                        }
                    }
                }
                None => {
                    for value in T::iter_column(&other) {
                        self.add(value, function_data)?;
                    }
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        let v = T::Scalar::from_u64_array(rhs.value);
        self.add(T::to_scalar_ref(&v), None)
    }

    fn merge_result(
        &mut self,
        builder: &mut T::ColumnBuilder,
        _function_data: Option<&dyn FunctionData>,
    ) -> Result<()> {
        let v = T::Scalar::from_u64_array(self.value);
        T::push_item(builder, T::to_scalar_ref(&v));
        Ok(())
    }
}

#[derive(BorshSerialize, BorshDeserialize, Default)]
pub struct IntervalSumState {
    pub value: months_days_micros,
}

impl UnaryState<IntervalType, IntervalType> for IntervalSumState {
    fn add(
        &mut self,
        other: months_days_micros,
        _function_data: Option<&dyn FunctionData>,
    ) -> Result<()> {
        self.value += other;
        Ok(())
    }

    fn add_batch(
        &mut self,
        other: Buffer<months_days_micros>,
        validity: Option<&Bitmap>,
        _function_data: Option<&dyn FunctionData>,
    ) -> Result<()> {
        let col = IntervalType::upcast_column(other);
        let buffer = IntervalType::try_downcast_column(&col).unwrap();
        match validity {
            Some(validity) if validity.null_count() > 0 => {
                buffer.iter().zip(validity.iter()).for_each(|(t, b)| {
                    if b {
                        self.value += *t;
                    }
                });
            }
            _ => {
                buffer.iter().for_each(|t| {
                    self.value += *t;
                });
            }
        }

        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        let res = self.value.total_micros() + rhs.value.total_micros();
        self.value = months_days_micros(res as i128);
        Ok(())
    }

    fn merge_result(
        &mut self,
        builder: &mut Vec<months_days_micros>,
        _function_data: Option<&dyn FunctionData>,
    ) -> Result<()> {
        IntervalType::push_item(builder, IntervalType::to_scalar_ref(&self.value));
        Ok(())
    }
}

pub fn try_create_aggregate_sum_function(
    display_name: &str,
    params: Vec<Scalar>,
    arguments: Vec<DataType>,
    _sort_descs: Vec<AggregateFunctionSortDesc>,
) -> Result<AggregateFunctionRef> {
    assert_unary_arguments(display_name, arguments.len())?;

    let mut data_type = arguments[0].clone();
    // null use dummy func, it's already covered in `AggregateNullResultFunction`
    if data_type.is_null() {
        data_type = Int8Type::data_type();
    }

    with_number_mapped_type!(|NUM| match &data_type {
        DataType::Number(NumberDataType::NUM) => {
            type TSum = <NUM as ResultTypeOfUnary>::Sum;
            let return_type = NumberType::<TSum>::data_type();
            AggregateUnaryFunction::<
                NumberSumState<NumberType<TSum>>,
                NumberType<NUM>,
                NumberType<TSum>,
            >::try_create_unary(display_name, return_type, params, arguments[0].clone())
        }
        DataType::Decimal(DecimalDataType::Decimal128(s)) => {
            let p = MAX_DECIMAL128_PRECISION;
            let decimal_size = DecimalSize {
                precision: p,
                scale: s.scale,
            };

            // DecimalWidth<int64_t> = 18
            let overflow = s.precision > 18;
            let return_type = DataType::Decimal(DecimalDataType::from_size(decimal_size)?);

            if overflow {
                AggregateUnaryFunction::<
                    DecimalSumState<false, Decimal128Type>,
                    Decimal128Type,
                    Decimal128Type,
                >::try_create_unary(
                    display_name, return_type, params, arguments[0].clone()
                )
            } else {
                AggregateUnaryFunction::<
                    DecimalSumState<true, Decimal128Type>,
                    Decimal128Type,
                    Decimal128Type,
                >::try_create_unary(
                    display_name, return_type, params, arguments[0].clone()
                )
            }
        }
        DataType::Interval => {
            let return_type = DataType::Interval;
            AggregateUnaryFunction::<IntervalSumState, IntervalType, IntervalType>::try_create_unary(
                display_name,
                return_type,
                params,
                arguments[0].clone(),
            )
        }
        DataType::Decimal(DecimalDataType::Decimal256(s)) => {
            let p = MAX_DECIMAL256_PRECISION;
            let decimal_size = DecimalSize {
                precision: p,
                scale: s.scale,
            };

            let overflow = s.precision > 18;
            let return_type = DataType::Decimal(DecimalDataType::from_size(decimal_size)?);

            if overflow {
                AggregateUnaryFunction::<
                    DecimalSumState<false, Decimal256Type>,
                    Decimal256Type,
                    Decimal256Type,
                >::try_create_unary(
                    display_name, return_type, params, arguments[0].clone()
                )
            } else {
                AggregateUnaryFunction::<
                    DecimalSumState<true, Decimal256Type>,
                    Decimal256Type,
                    Decimal256Type,
                >::try_create_unary(
                    display_name, return_type, params, arguments[0].clone()
                )
            }
        }
        _ => Err(ErrorCode::BadDataValueType(format!(
            "{} does not support type '{:?}'",
            display_name, arguments[0]
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
