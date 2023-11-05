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

use std::marker::PhantomData;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::number::*;
use common_expression::types::*;
use common_expression::with_number_mapped_type;
use common_expression::AggregateFunction;
use common_expression::Scalar;
use num_traits::AsPrimitive;
use serde::Deserialize;
use serde::Serialize;

use super::deserialize_state;
use super::serialize_state;
use crate::aggregates::aggregate_unary::AggregateUnaryFunction;
use crate::aggregates::aggregate_unary::UnaryState;

#[derive(Serialize, Deserialize)]
pub struct SkewnessStateV2<T>
where
    T: ValueType + Sync + Send,
    T::Scalar: AsPrimitive<f64> + Sync + Send,
{
    pub n: u64,
    pub sum: f64,
    pub sum_sqr: f64,
    pub sum_cub: f64,
    _ph: PhantomData<T>,
}

impl<T> Default for SkewnessStateV2<T>
where
    T: ValueType + Sync + Send,
    T::Scalar: AsPrimitive<f64> + Sync + Send,
{
    fn default() -> Self {
        Self {
            n: 0,
            sum: 0.0,
            sum_sqr: 0.0,
            sum_cub: 0.0,
            _ph: PhantomData,
        }
    }
}

impl<T> UnaryState<T> for SkewnessStateV2<T>
where
    T: ValueType + Sync + Send,
    T::Scalar: AsPrimitive<f64> + Sync + Send,
{
    fn add(&mut self, other: T::ScalarRef<'_>) {
        let other = T::to_owned_scalar(other).as_();
        self.n += 1;
        self.sum += other;
        self.sum_sqr += other.powi(2);
        self.sum_cub += other.powi(3);
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        if rhs.n == 0 {
            return Ok(());
        }
        self.n += rhs.n;
        self.sum += rhs.sum;
        self.sum_sqr += rhs.sum_sqr;
        self.sum_cub += rhs.sum_cub;
        Ok(())
    }

    fn merge_result(&mut self) -> Result<Option<Scalar>> {
        if self.n <= 2 {
            return Ok(None);
        }
        let n = self.n as f64;
        let temp = 1.0 / n;
        let div = (temp * (self.sum_sqr - self.sum * self.sum * temp))
            .powi(3)
            .sqrt();
        if div == 0.0 {
            return Ok(None);
        }
        let temp1 = (n * (n - 1.0)).sqrt() / (n - 2.0);
        let value = temp1
            * temp
            * (self.sum_cub - 3.0 * self.sum_sqr * self.sum * temp
                + 2.0 * self.sum.powi(3) * temp * temp)
            / div;
        if value.is_infinite() || value.is_nan() {
            Ok(None)
        } else {
            Ok(Some(Float64Type::upcast_scalar(value.into())))
        }
    }

    fn serialize(&self, writer: &mut Vec<u8>) -> Result<()> {
        serialize_state(writer, self)
    }

    fn deserialize(reader: &mut &[u8]) -> Result<Self> {
        deserialize_state::<Self>(reader)
    }
}

pub(crate) fn create_skewness(
    display_name: &str,
    params: Vec<Scalar>,
    arguments: &[DataType],
    data_type: &DataType,
) -> Result<Arc<dyn AggregateFunction>> {
    with_number_mapped_type!(|NUM| match data_type {
        DataType::Number(NumberDataType::NUM) => {
            let return_type =
                DataType::Nullable(Box::new(DataType::Number(NumberDataType::Float64)));
            AggregateUnaryFunction::<
                SkewnessStateV2<NumberType<NUM>>,
                NumberType<NUM>,
                NullableType<Float64Type>,
            >::try_create(display_name, return_type, params, arguments[0].clone())
        }

        _ => Err(ErrorCode::BadDataValueType(format!(
            "{} does not support type '{:?}'",
            display_name, arguments[0]
        ))),
    })
}
