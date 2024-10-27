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

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::number::*;
use databend_common_expression::types::*;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::Scalar;
use num_traits::AsPrimitive;

use super::AggregateUnaryFunction;
use super::FunctionData;
use super::UnaryState;
use crate::aggregates::aggregate_function_factory::AggregateFunctionDescription;
use crate::aggregates::assert_unary_arguments;
use crate::aggregates::AggregateFunctionRef;

#[derive(Default, BorshSerialize, BorshDeserialize)]
struct KurtosisState {
    pub n: u64,
    pub sum: F64,
    pub sum_sqr: F64,
    pub sum_cub: F64,
    pub sum_four: F64,
}

impl<T> UnaryState<T, Float64Type> for KurtosisState
where
    T: ValueType + Sync + Send,
    T::Scalar: AsPrimitive<f64>,
{
    fn add(
        &mut self,
        other: T::ScalarRef<'_>,
        _function_data: Option<&dyn FunctionData>,
    ) -> Result<()> {
        let other = T::to_owned_scalar(other).as_();
        self.n += 1;
        self.sum += other;
        self.sum_sqr += other.powi(2);
        self.sum_cub += other.powi(3);
        self.sum_four += other.powi(4);
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        if rhs.n == 0 {
            return Ok(());
        }
        self.n += rhs.n;
        self.sum += rhs.sum;
        self.sum_sqr += rhs.sum_sqr;
        self.sum_cub += rhs.sum_cub;
        self.sum_four += rhs.sum_four;
        Ok(())
    }

    fn merge_result(
        &mut self,
        builder: &mut Vec<F64>,
        _function_data: Option<&dyn FunctionData>,
    ) -> Result<()> {
        if self.n <= 3 {
            builder.push(F64::from(0_f64));
            return Ok(());
        }

        let (n, sum, sum_sqr, sum_cub, sum_four) = (
            self.n as f64,
            *self.sum,
            *self.sum_sqr,
            *self.sum_cub,
            *self.sum_four,
        );

        let temp = 1.0 / n;
        if sum_sqr - sum * sum * temp == 0.0 {
            builder.push(F64::from(0_f64));
            return Ok(());
        }
        let m4 = temp
            * (sum_four - 4.0 * sum_cub * sum * temp + 6.0 * sum_sqr * sum * sum * temp * temp
                - 3.0 * sum.powi(4) * temp.powi(3));
        let m2 = temp * (sum_sqr - sum * sum * temp);
        if m2 <= 0.0 || (n - 2.0) * (n - 3.0) == 0.0 {
            builder.push(F64::from(0_f64));
            return Ok(());
        }
        let value =
            (n - 1.0) * ((n + 1.0) * m4 / (m2 * m2) - 3.0 * (n - 1.0)) / ((n - 2.0) * (n - 3.0));
        if value.is_finite() {
            builder.push(F64::from(value));
        } else {
            builder.push(F64::from(f64::NAN));
        }
        Ok(())
    }
}

pub fn try_create_aggregate_kurtosis_function(
    display_name: &str,
    params: Vec<Scalar>,
    arguments: Vec<DataType>,
) -> Result<AggregateFunctionRef> {
    assert_unary_arguments(display_name, arguments.len())?;

    with_number_mapped_type!(|NUM_TYPE| match &arguments[0] {
        DataType::Number(NumberDataType::NUM_TYPE) => {
            let return_type = DataType::Number(NumberDataType::Float64);
            AggregateUnaryFunction::<
                KurtosisState,
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

pub fn aggregate_kurtosis_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(try_create_aggregate_kurtosis_function))
}
