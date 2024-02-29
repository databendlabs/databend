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
use std::sync::Arc;

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::decimal::Decimal;
use databend_common_expression::types::decimal::Decimal128Type;
use databend_common_expression::types::decimal::Decimal256Type;
use databend_common_expression::types::decimal::MAX_DECIMAL128_PRECISION;
use databend_common_expression::types::decimal::MAX_DECIMAL256_PRECISION;
use databend_common_expression::types::number::Number;
use databend_common_expression::types::number::F64;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DecimalDataType;
use databend_common_expression::types::DecimalSize;
use databend_common_expression::types::Float64Type;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::ValueType;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::Scalar;
use num_traits::AsPrimitive;

use super::AggregateUnaryFunction;
use super::FunctionData;
use super::UnaryState;
use crate::aggregates::aggregate_function_factory::AggregateFunctionDescription;
use crate::aggregates::aggregator_common::assert_unary_arguments;
use crate::aggregates::AggregateFunction;

const POP: u8 = 0;
const SAMP: u8 = 1;
const OVERFLOW_PRECISION: u8 = 18;
const VARIANCE_PRECISION: u8 = 4;

#[derive(Default, BorshSerialize, BorshDeserialize)]
struct NumberAggregateStddevState<const TYPE: u8> {
    pub sum: f64,
    pub count: u64,
    pub variance: f64,
}

impl<T, const TYPE: u8> UnaryState<T, Float64Type> for NumberAggregateStddevState<TYPE>
where
    T: ValueType,
    T::Scalar: Number + AsPrimitive<f64>,
{
    fn add(&mut self, other: T::ScalarRef<'_>) -> Result<()> {
        let value = T::to_owned_scalar(other).as_();
        self.sum += value;
        self.count += 1;
        if self.count > 1 {
            let t = self.count as f64 * value - self.sum;
            self.variance += (t * t) / (self.count * (self.count - 1)) as f64;
        }
        Ok(())
    }

    fn merge(&mut self, other: &Self) -> Result<()> {
        if other.count == 0 {
            return Ok(());
        }
        if self.count == 0 {
            self.count = other.count;
            self.sum = other.sum;
            self.variance = other.variance;
            return Ok(());
        }

        let t = (other.count as f64 / self.count as f64) * self.sum - other.sum;
        self.variance += other.variance
            + ((self.count as f64 / other.count as f64) / (self.count as f64 + other.count as f64))
                * t
                * t;
        self.count += other.count;
        self.sum += other.sum;

        Ok(())
    }

    fn merge_result(
        &mut self,
        builder: &mut Vec<F64>,
        _function_data: Option<&dyn FunctionData>,
    ) -> Result<()> {
        let variance = self.variance / (self.count - TYPE as u64) as f64;
        builder.push(variance.sqrt().into());

        Ok(())
    }
}

struct DecimalFuncData {
    pub scale_add: u8,
}

impl FunctionData for DecimalFuncData {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(BorshDeserialize, BorshSerialize)]
pub struct DecimalNumberAggregateStddevState<const OVERFLOW: bool, T, const TYPE: u8>
where
    T: ValueType,
    T::Scalar: Decimal,
{
    pub sum: T::Scalar,
    pub count: u64,
    pub variance: T::Scalar,
}

impl<const OVERFLOW: bool, T, const TYPE: u8> Default
    for DecimalNumberAggregateStddevState<OVERFLOW, T, TYPE>
where
    T: ValueType,
    T::Scalar: Decimal + std::ops::AddAssign + BorshSerialize + BorshDeserialize,
{
    fn default() -> Self {
        Self {
            sum: T::Scalar::zero(),
            count: 0,
            variance: T::Scalar::zero(),
        }
    }
}

impl<const OVERFLOW: bool, T, const TYPE: u8> UnaryState<T, T>
    for DecimalNumberAggregateStddevState<OVERFLOW, T, TYPE>
where
    T: ValueType,
    T::Scalar: Decimal + std::ops::AddAssign + BorshSerialize + BorshDeserialize,
{
    fn add(&mut self, other: T::ScalarRef<'_>) -> Result<()> {
        let value = T::to_owned_scalar(other);
        self.sum += value;
        if OVERFLOW && (self.sum > T::Scalar::MAX || self.sum < T::Scalar::MIN) {
            return Err(ErrorCode::Overflow(format!(
                "Decimal overflow: {:?} not in [{}, {}]",
                self.sum,
                T::Scalar::MIN,
                T::Scalar::MAX,
            )));
        }
        self.count += 1;
        if self.count > 1 {
            let t = match value
                .checked_mul(T::Scalar::from_i128(self.count))
                .and_then(|v| v.checked_sub(self.sum))
                .and_then(|v| v.checked_mul(T::Scalar::e(VARIANCE_PRECISION as u32)))
            {
                Some(t) => t,
                None => {
                    return Err(ErrorCode::Overflow(format!(
                        "Decimal overflow: ({} * {} - {}) * 10^{} not in [{}, {}]",
                        value,
                        self.count,
                        self.sum,
                        VARIANCE_PRECISION,
                        T::Scalar::MIN,
                        T::Scalar::MAX,
                    )));
                }
            };

            let t = match t.checked_mul(t) {
                Some(t) => t,
                None => {
                    return Err(ErrorCode::Overflow(format!(
                        "Decimal overflow: {} * {} not in [{}, {}]",
                        t,
                        t,
                        T::Scalar::MIN,
                        T::Scalar::MAX,
                    )));
                }
            };

            let count = T::Scalar::from_i128(self.count * (self.count - 1));

            let add_variance = match t.checked_div(count) {
                Some(t) => t,
                None => {
                    return Err(ErrorCode::Overflow(format!(
                        "Decimal overflow: {} / {} not in [{}, {}]",
                        t,
                        count,
                        T::Scalar::MIN,
                        T::Scalar::MAX,
                    )));
                }
            };

            self.variance += add_variance;
        }

        Ok(())
    }

    fn merge(&mut self, other: &Self) -> Result<()> {
        if other.count == 0 {
            return Ok(());
        }
        if self.count == 0 {
            self.count = other.count;
            self.sum = other.sum;
            self.variance = other.variance;
            return Ok(());
        }

        let other_count = T::Scalar::from_i128(other.count);
        let self_count = T::Scalar::from_i128(self.count);
        let t = match other_count
            .checked_mul(self.sum)
            .and_then(|v| v.checked_mul(T::Scalar::e(VARIANCE_PRECISION as u32)))
            .and_then(|v| v.checked_div(self_count))
            .and_then(|v| v.checked_sub(other.sum))
        {
            Some(t) => t,
            None => {
                return Err(ErrorCode::Overflow(format!(
                    "Decimal overflow: {} * {} * 10^{} / {} - {} not in [{}, {}]",
                    other_count,
                    other.sum,
                    VARIANCE_PRECISION,
                    self_count,
                    self.sum,
                    T::Scalar::MIN,
                    T::Scalar::MAX,
                )));
            }
        };

        let count_sum = match self_count.checked_add(other_count) {
            Some(t) => t,
            None => {
                return Err(ErrorCode::Overflow(format!(
                    "Decimal overflow: {} + {} not in [{}, {}]",
                    self_count,
                    other_count,
                    T::Scalar::MIN,
                    T::Scalar::MAX,
                )));
            }
        };
        let add_variance = match t
            .checked_mul(t)
            .and_then(|v| v.checked_mul(self_count))
            .and_then(|v| v.checked_div(other_count))
            .and_then(|v| v.checked_div(count_sum))
        {
            Some(t) => t,
            None => {
                return Err(ErrorCode::Overflow(format!(
                    "Decimal overflow: {} * {} * {} / {} / {} not in [{}, {}]",
                    t,
                    t,
                    self_count,
                    other_count,
                    count_sum,
                    T::Scalar::MIN,
                    T::Scalar::MAX,
                )));
            }
        };
        self.variance += add_variance;
        self.count += other.count;
        self.sum += other.sum;
        if OVERFLOW && (self.sum > T::Scalar::MAX || self.sum < T::Scalar::MIN) {
            return Err(ErrorCode::Overflow(format!(
                "Decimal overflow: {:?} not in [{}, {}]",
                self.sum,
                T::Scalar::MIN,
                T::Scalar::MAX,
            )));
        }

        Ok(())
    }

    fn merge_result(
        &mut self,
        builder: &mut T::ColumnBuilder,
        function_data: Option<&dyn FunctionData>,
    ) -> Result<()> {
        let decimal_func_data = unsafe {
            function_data
                .unwrap()
                .as_any()
                .downcast_ref_unchecked::<DecimalFuncData>()
        };

        let variance = self.variance.to_float64(0);
        let variance_sqrt = (variance / (self.count - TYPE as u64) as f64).sqrt();
        let value = match T::Scalar::from_float(variance_sqrt)
            .checked_div(T::Scalar::e(decimal_func_data.scale_add as u32))
        {
            Some(t) => t,
            None => {
                return Err(ErrorCode::Overflow(format!(
                    "Decimal overflow: {} / 10^{} not in [{}, {}]",
                    variance_sqrt,
                    decimal_func_data.scale_add,
                    T::Scalar::MIN,
                    T::Scalar::MAX,
                )));
            }
        };

        T::push_item(builder, T::to_scalar_ref(&value));

        Ok(())
    }
}

pub fn try_create_aggregate_stddev_pop_function<const TYPE: u8>(
    display_name: &str,
    params: Vec<Scalar>,
    arguments: Vec<DataType>,
) -> Result<Arc<dyn AggregateFunction>> {
    assert_unary_arguments(display_name, arguments.len())?;
    with_number_mapped_type!(|NUM_TYPE| match &arguments[0] {
        DataType::Number(NumberDataType::NUM_TYPE) => {
            let return_type = DataType::Number(NumberDataType::Float64);
            AggregateUnaryFunction::<
                NumberAggregateStddevState<TYPE>,
                NumberType<NUM_TYPE>,
                Float64Type,
            >::try_create_unary(display_name, return_type, params, arguments[0].clone())
        }
        DataType::Decimal(DecimalDataType::Decimal128(s)) => {
            let decimal_size = DecimalSize {
                precision: MAX_DECIMAL128_PRECISION,
                scale: s.scale.max(VARIANCE_PRECISION),
            };
            let scale_add = decimal_size.scale - s.scale;
            let overflow = s.precision > OVERFLOW_PRECISION;
            let return_type = DataType::Decimal(DecimalDataType::from_size(decimal_size)?);
            if overflow {
                let func = AggregateUnaryFunction::<
                    DecimalNumberAggregateStddevState<true, Decimal128Type, TYPE>,
                    Decimal128Type,
                    Decimal128Type,
                >::try_create(
                    display_name, return_type, params, arguments[0].clone()
                )
                .with_function_data(Box::new(DecimalFuncData { scale_add }));
                Ok(Arc::new(func))
            } else {
                let func = AggregateUnaryFunction::<
                    DecimalNumberAggregateStddevState<false, Decimal128Type, TYPE>,
                    Decimal128Type,
                    Decimal128Type,
                >::try_create(
                    display_name, return_type, params, arguments[0].clone()
                )
                .with_function_data(Box::new(DecimalFuncData { scale_add }));
                Ok(Arc::new(func))
            }
        }
        DataType::Decimal(DecimalDataType::Decimal256(s)) => {
            let decimal_size = DecimalSize {
                precision: MAX_DECIMAL256_PRECISION,
                scale: s.scale.max(4),
            };
            let overflow = s.precision > OVERFLOW_PRECISION;
            let return_type = DataType::Decimal(DecimalDataType::from_size(decimal_size)?);
            if overflow {
                AggregateUnaryFunction::<
                    DecimalNumberAggregateStddevState<true, Decimal256Type, TYPE>,
                    Decimal256Type,
                    Decimal256Type,
                >::try_create_unary(
                    display_name, return_type, params, arguments[0].clone()
                )
            } else {
                AggregateUnaryFunction::<
                    DecimalNumberAggregateStddevState<false, Decimal256Type, TYPE>,
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

pub fn aggregate_stddev_pop_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(try_create_aggregate_stddev_pop_function::<POP>))
}

pub fn aggregate_stddev_samp_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(
        try_create_aggregate_stddev_pop_function::<SAMP>,
    ))
}
