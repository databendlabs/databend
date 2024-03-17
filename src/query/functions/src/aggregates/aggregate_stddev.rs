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
use databend_common_expression::types::number::Number;
use databend_common_expression::types::number::F64;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DecimalDataType;
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

const STD_POP: u8 = 0;
const STD_SAMP: u8 = 1;
const VAR_POP: u8 = 2;
const VAR_SAMP: u8 = 3;

// Streaming approximate standard deviation using Welford's
// method, DOI: 10.2307/1266577
#[derive(BorshSerialize, BorshDeserialize, Default)]
struct StddevState<const TYPE: u8> {
    count: u64,    // n
    mean: f64,     // M1
    dsquared: f64, // M2
}

impl<const TYPE: u8> StddevState<TYPE> {
    fn state_add(&mut self, value: f64) -> Result<()> {
        self.count += 1;
        let mean_differential = (value - self.mean) / self.count as f64;
        let new_mean = self.mean + mean_differential;
        let dsquared_increment = (value - new_mean) * (value - self.mean);
        let new_dsquared = self.dsquared + dsquared_increment;

        self.mean = new_mean;
        self.dsquared = new_dsquared;
        Ok(())
    }

    fn state_merge(&mut self, other: &Self) -> Result<()> {
        if other.count == 0 {
            return Ok(());
        }
        if self.count == 0 {
            self.count = other.count;
            self.mean = other.mean;
            self.dsquared = other.dsquared;
            return Ok(());
        }

        let count = self.count + other.count;
        let mean = (self.count as f64 * self.mean + other.count as f64 * other.mean) / count as f64;
        let delta = other.mean - self.mean;

        self.count = count;
        self.mean = mean;
        self.dsquared = other.dsquared
            + self.dsquared
            + delta * delta * other.count as f64 * self.count as f64 / count as f64;

        Ok(())
    }

    fn state_merge_result(&mut self, builder: &mut Vec<F64>) -> Result<()> {
        let result = if self.count <= 1 {
            0f64
        } else {
            match TYPE {
                STD_POP => (self.dsquared / self.count as f64).sqrt(),
                STD_SAMP => (self.dsquared / (self.count - 1) as f64).sqrt(),
                VAR_POP => self.dsquared / self.count as f64,
                VAR_SAMP => self.dsquared / (self.count - 1) as f64,
                _ => unreachable!(),
            }
        };

        builder.push(result.into());

        Ok(())
    }
}

#[derive(BorshSerialize, BorshDeserialize, Default)]
struct NumberAggregateStddevState<const TYPE: u8> {
    state: StddevState<TYPE>,
}

impl<T, const TYPE: u8> UnaryState<T, Float64Type> for NumberAggregateStddevState<TYPE>
where
    T: ValueType,
    T::Scalar: Number + AsPrimitive<f64>,
{
    fn add(
        &mut self,
        other: T::ScalarRef<'_>,
        _function_data: Option<&dyn FunctionData>,
    ) -> Result<()> {
        let value = T::to_owned_scalar(other).as_();
        self.state.state_add(value)
    }

    fn merge(&mut self, other: &Self) -> Result<()> {
        self.state.state_merge(&other.state)
    }

    fn merge_result(
        &mut self,
        builder: &mut Vec<F64>,
        _function_data: Option<&dyn FunctionData>,
    ) -> Result<()> {
        self.state.state_merge_result(builder)
    }
}

struct DecimalFuncData {
    pub scale: u8,
}

impl FunctionData for DecimalFuncData {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(BorshSerialize, BorshDeserialize, Default)]
struct DecimalNumberAggregateStddevState<const TYPE: u8> {
    state: StddevState<TYPE>,
}

impl<T, const TYPE: u8> UnaryState<T, Float64Type> for DecimalNumberAggregateStddevState<TYPE>
where
    T: ValueType,
    T::Scalar: Decimal + BorshSerialize + BorshDeserialize,
{
    fn add(
        &mut self,
        other: T::ScalarRef<'_>,
        function_data: Option<&dyn FunctionData>,
    ) -> Result<()> {
        let stddev_func_data = unsafe {
            function_data
                .unwrap()
                .as_any()
                .downcast_ref_unchecked::<DecimalFuncData>()
        };
        let value = T::to_owned_scalar(other).to_float64(stddev_func_data.scale);
        self.state.state_add(value)
    }

    fn merge(&mut self, other: &Self) -> Result<()> {
        self.state.state_merge(&other.state)
    }

    fn merge_result(
        &mut self,
        builder: &mut Vec<F64>,
        _function_data: Option<&dyn FunctionData>,
    ) -> Result<()> {
        self.state.state_merge_result(builder)
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
            let return_type = DataType::Number(NumberDataType::Float64);
            let func = AggregateUnaryFunction::<
                DecimalNumberAggregateStddevState<TYPE>,
                Decimal128Type,
                Float64Type,
            >::try_create(
                display_name, return_type, params, arguments[0].clone()
            )
            .with_function_data(Box::new(DecimalFuncData { scale: s.scale }));
            Ok(Arc::new(func))
        }
        DataType::Decimal(DecimalDataType::Decimal256(s)) => {
            let return_type = DataType::Number(NumberDataType::Float64);
            let func = AggregateUnaryFunction::<
                DecimalNumberAggregateStddevState<TYPE>,
                Decimal256Type,
                Float64Type,
            >::try_create(
                display_name, return_type, params, arguments[0].clone()
            )
            .with_function_data(Box::new(DecimalFuncData { scale: s.scale }));
            Ok(Arc::new(func))
        }
        _ => Err(ErrorCode::BadDataValueType(format!(
            "{} does not support type '{:?}'",
            display_name, arguments[0]
        ))),
    })
}

pub fn aggregate_stddev_pop_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(
        try_create_aggregate_stddev_pop_function::<STD_POP>,
    ))
}

pub fn aggregate_stddev_samp_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(
        try_create_aggregate_stddev_pop_function::<STD_SAMP>,
    ))
}
