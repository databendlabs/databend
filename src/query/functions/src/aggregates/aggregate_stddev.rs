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

use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::number::Number;
use common_expression::types::number::F64;
use common_expression::types::DataType;
use common_expression::types::Float64Type;
use common_expression::types::NumberDataType;
use common_expression::types::NumberType;
use common_expression::types::ValueType;
use common_expression::with_number_mapped_type;
use common_expression::Scalar;
use num_traits::AsPrimitive;
use serde::Deserialize;
use serde::Serialize;

use super::deserialize_state;
use super::serialize_state;
use super::AggregateUnaryFunction;
use super::FunctionData;
use super::UnaryState;
use crate::aggregates::aggregate_function_factory::AggregateFunctionDescription;
use crate::aggregates::aggregator_common::assert_unary_arguments;
use crate::aggregates::AggregateFunction;

const POP: u8 = 0;
const SAMP: u8 = 1;

#[derive(Default, Serialize, Deserialize)]
struct AggregateStddevState<const TYPE: u8> {
    pub sum: f64,
    pub count: u64,
    pub variance: f64,
}

impl<T, const TYPE: u8> UnaryState<T, Float64Type> for AggregateStddevState<TYPE>
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

    fn serialize(&self, writer: &mut Vec<u8>) -> Result<()> {
        serialize_state(writer, self)
    }

    fn deserialize(reader: &mut &[u8]) -> Result<Self>
    where Self: Sized {
        deserialize_state(reader)
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
                AggregateStddevState<TYPE>,
                NumberType<NUM_TYPE>,
                Float64Type,
            >::try_create_unary(display_name, return_type, params, arguments[0].clone())
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
