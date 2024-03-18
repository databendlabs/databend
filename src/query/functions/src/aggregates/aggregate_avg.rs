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

use std::any::Any;
use std::marker::PhantomData;
use std::sync::Arc;

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::decimal::*;
use databend_common_expression::types::*;
use databend_common_expression::utils::arithmetics_type::ResultTypeOfUnary;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::Scalar;
use num_traits::AsPrimitive;

use super::aggregate_sum::DecimalSumState;
use super::AggregateUnaryFunction;
use super::FunctionData;
use super::UnaryState;
use crate::aggregates::aggregate_function_factory::AggregateFunctionDescription;
use crate::aggregates::aggregator_common::assert_unary_arguments;
use crate::aggregates::AggregateFunctionRef;

#[derive(BorshSerialize, BorshDeserialize)]
struct NumberAvgState<T, TSum>
where TSum: ValueType
{
    pub value: TSum::Scalar,
    pub count: u64,
    #[borsh(skip)]
    _t: PhantomData<T>,
}

impl<T, TSum> Default for NumberAvgState<T, TSum>
where
    T: ValueType + Sync + Send,
    TSum: ValueType,
    T::Scalar: Number + AsPrimitive<TSum::Scalar>,
    TSum::Scalar:
        Number + AsPrimitive<f64> + BorshSerialize + BorshDeserialize + std::ops::AddAssign,
{
    fn default() -> Self {
        Self {
            value: TSum::Scalar::default(),
            count: 0,
            _t: PhantomData,
        }
    }
}

impl<T, TSum> UnaryState<T, Float64Type> for NumberAvgState<T, TSum>
where
    T: ValueType + Sync + Send,
    TSum: ValueType,
    T::Scalar: Number + AsPrimitive<TSum::Scalar>,
    TSum::Scalar:
        Number + AsPrimitive<f64> + BorshSerialize + BorshDeserialize + std::ops::AddAssign,
{
    fn add(
        &mut self,
        other: T::ScalarRef<'_>,
        _function_data: Option<&dyn FunctionData>,
    ) -> Result<()> {
        self.count += 1;
        let other = T::to_owned_scalar(other).as_();
        self.value += other;
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.count += rhs.count;
        self.value += rhs.value;
        Ok(())
    }

    fn merge_result(
        &mut self,
        builder: &mut Vec<F64>,
        _function_data: Option<&dyn FunctionData>,
    ) -> Result<()> {
        let value = self.value.as_() / (self.count as f64);
        builder.push(F64::from(value));
        Ok(())
    }
}

struct DecimalAvgData {
    // only for decimals
    // AVG：AVG(DECIMAL(a, b)) -> DECIMAL(38 or 76, max(b, 4))。
    pub scale_add: u8,
}

impl FunctionData for DecimalAvgData {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(BorshSerialize, BorshDeserialize)]
struct DecimalAvgState<const OVERFLOW: bool, T>
where
    T: ValueType,
    T::Scalar: Decimal,
{
    pub value: T::Scalar,
    pub count: u64,
}

impl<const OVERFLOW: bool, T> Default for DecimalAvgState<OVERFLOW, T>
where
    T: ValueType,
    T::Scalar: Decimal + std::ops::AddAssign + BorshSerialize + BorshDeserialize,
{
    fn default() -> Self {
        Self {
            value: T::Scalar::default(),
            count: 0,
        }
    }
}

impl<const OVERFLOW: bool, T> DecimalAvgState<OVERFLOW, T>
where
    T: ValueType,
    T::Scalar: Decimal + std::ops::AddAssign,
{
    fn add_internal(&mut self, count: u64, value: T::ScalarRef<'_>) -> Result<()> {
        self.count += count;
        self.value += T::to_owned_scalar(value);
        if OVERFLOW && (self.value > T::Scalar::MAX || self.value < T::Scalar::MIN) {
            return Err(ErrorCode::Overflow(format!(
                "Decimal overflow: {:?} not in [{}, {}]",
                self.value,
                T::Scalar::MIN,
                T::Scalar::MAX,
            )));
        }
        Ok(())
    }
}

impl<const OVERFLOW: bool, T> UnaryState<T, T> for DecimalAvgState<OVERFLOW, T>
where
    T: ValueType,
    T::Scalar: Decimal + std::ops::AddAssign + BorshSerialize + BorshDeserialize,
{
    fn add(
        &mut self,
        other: T::ScalarRef<'_>,
        _function_data: Option<&dyn FunctionData>,
    ) -> Result<()> {
        self.add_internal(1, other)
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.add_internal(rhs.count, T::to_scalar_ref(&rhs.value))
    }

    fn merge_result(
        &mut self,
        builder: &mut T::ColumnBuilder,
        function_data: Option<&dyn FunctionData>,
    ) -> Result<()> {
        // # Safety
        // `downcast_ref_unchecked` will check type in debug mode using dynamic dispatch,
        let decimal_avg_data = unsafe {
            function_data
                .unwrap()
                .as_any()
                .downcast_ref_unchecked::<DecimalAvgData>()
        };
        match self
            .value
            .checked_mul(T::Scalar::e(decimal_avg_data.scale_add as u32))
            .and_then(|v| v.checked_div(T::Scalar::from_i128(self.count)))
        {
            Some(value) => {
                T::push_item(builder, T::to_scalar_ref(&value));
                Ok(())
            }
            None => Err(ErrorCode::Overflow(format!(
                "Decimal overflow: {} mul {}",
                self.value,
                T::Scalar::e(decimal_avg_data.scale_add as u32)
            ))),
        }
    }
}

pub fn try_create_aggregate_avg_function(
    display_name: &str,
    params: Vec<Scalar>,
    arguments: Vec<DataType>,
) -> Result<AggregateFunctionRef> {
    assert_unary_arguments(display_name, arguments.len())?;

    let data_type = if arguments[0].is_null() {
        Int8Type::data_type()
    } else {
        arguments[0].clone()
    };

    with_number_mapped_type!(|NUM| match &data_type {
        DataType::Number(NumberDataType::NUM) => {
            type TSum = <NUM as ResultTypeOfUnary>::Sum;
            let return_type = Float64Type::data_type();
            AggregateUnaryFunction::<
                NumberAvgState<NumberType<NUM>, NumberType<TSum>>,
                NumberType<NUM>,
                Float64Type,
            >::try_create_unary(display_name, return_type, params, arguments[0].clone())
        }
        DataType::Decimal(DecimalDataType::Decimal128(s)) => {
            let p = MAX_DECIMAL128_PRECISION;
            let decimal_size = DecimalSize {
                precision: p,
                scale: s.scale.max(4),
            };

            // DecimalWidth<int64_t> = 18
            let overflow = s.precision > 18;
            let scale_add = decimal_size.scale - s.scale;
            let return_type = DataType::Decimal(DecimalDataType::from_size(decimal_size)?);

            if overflow {
                let func = AggregateUnaryFunction::<
                    DecimalAvgState<true, Decimal128Type>,
                    Decimal128Type,
                    Decimal128Type,
                >::try_create(
                    display_name, return_type, params, arguments[0].clone()
                )
                .with_function_data(Box::new(DecimalAvgData { scale_add }));
                Ok(Arc::new(func))
            } else {
                let func = AggregateUnaryFunction::<
                    DecimalAvgState<false, Decimal128Type>,
                    Decimal128Type,
                    Decimal128Type,
                >::try_create(
                    display_name, return_type, params, arguments[0].clone()
                )
                .with_function_data(Box::new(DecimalAvgData { scale_add }));
                Ok(Arc::new(func))
            }
        }
        DataType::Decimal(DecimalDataType::Decimal256(s)) => {
            let p = MAX_DECIMAL256_PRECISION;
            let decimal_size = DecimalSize {
                precision: p,
                scale: s.scale.max(4),
            };

            let overflow = s.precision > 18;
            let scale_add = decimal_size.scale - s.scale;
            let return_type = DataType::Decimal(DecimalDataType::from_size(decimal_size)?);

            if overflow {
                let func = AggregateUnaryFunction::<
                    DecimalAvgState<true, Decimal256Type>,
                    Decimal256Type,
                    Decimal256Type,
                >::try_create(
                    display_name, return_type, params, arguments[0].clone()
                )
                .with_function_data(Box::new(DecimalAvgData { scale_add }));
                Ok(Arc::new(func))
            } else {
                let func = AggregateUnaryFunction::<
                    DecimalSumState<false, Decimal256Type>,
                    Decimal256Type,
                    Decimal256Type,
                >::try_create(
                    display_name, return_type, params, arguments[0].clone()
                )
                .with_function_data(Box::new(DecimalAvgData { scale_add }));
                Ok(Arc::new(func))
            }
        }
        _ => Err(ErrorCode::BadDataValueType(format!(
            "{} does not support type '{:?}'",
            display_name, arguments[0]
        ))),
    })
}

pub fn aggregate_avg_function_desc() -> AggregateFunctionDescription {
    let features = super::aggregate_function_factory::AggregateFunctionFeatures {
        is_decomposable: true,
        ..Default::default()
    };
    AggregateFunctionDescription::creator_with_features(
        Box::new(try_create_aggregate_avg_function),
        features,
    )
}
