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
use common_expression::types::decimal::*;
use common_expression::types::number::*;
use common_expression::types::*;
use common_expression::utils::arithmetics_type::ResultTypeOfUnary;
use common_expression::with_number_mapped_type;
use common_expression::AggregateFunction;
use common_expression::Scalar;
use num_traits::AsPrimitive;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;

use super::deserialize_state;
use super::serialize_state;
use crate::aggregates::aggregate_unary::UnaryState;
use crate::aggregates::AggregateUnaryFunction;

#[derive(Deserialize, Serialize)]
pub struct NumberSumStateV2<T, TSum> {
    pub value: TSum,
    #[serde(skip)]
    _t: PhantomData<T>,
}

impl<T, TSum> Default for NumberSumStateV2<T, TSum>
where
    T: ValueType + Sync + Send,
    T::Scalar: Number + AsPrimitive<TSum>,
    TSum: Number + AsPrimitive<f64> + Serialize + DeserializeOwned + std::ops::AddAssign,
{
    fn default() -> Self {
        NumberSumStateV2::<T, TSum> {
            value: TSum::default(),
            _t: PhantomData,
        }
    }
}

impl<T, TSum> UnaryState<T> for NumberSumStateV2<T, TSum>
where
    T: ValueType + Sync + Send,
    T::Scalar: Number + AsPrimitive<TSum>,
    TSum: Number + AsPrimitive<f64> + Serialize + DeserializeOwned + std::ops::AddAssign,
{
    fn add(&mut self, other: T::ScalarRef<'_>) -> Result<()> {
        let other = T::to_owned_scalar(other).as_();
        self.value += other;
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.value += rhs.value;
        Ok(())
    }

    fn merge_result(&mut self) -> Result<Option<Scalar>> {
        let value = NumberType::<TSum>::upcast_scalar(self.value);
        Ok(Some(value))
    }

    fn serialize(&self, writer: &mut Vec<u8>) -> Result<()> {
        serialize_state(writer, &self.value)
    }

    fn deserialize(reader: &mut &[u8]) -> Result<Self> {
        let value = deserialize_state(reader)?;
        Ok(Self {
            value,
            _t: PhantomData,
        })
    }
}

#[derive(Deserialize, Serialize)]
pub struct DecimalSumStateV2<const OVERFLOW: bool, T>
where
    T: ValueType,
    T::Scalar: Decimal,
{
    pub value: T::Scalar,
}

impl<const OVERFLOW: bool, T> Default for DecimalSumStateV2<OVERFLOW, T>
where
    T: ValueType,
    T::Scalar: Decimal
        + std::ops::AddAssign
        + Serialize
        + DeserializeOwned
        + Copy
        + Clone
        + std::fmt::Debug
        + std::cmp::PartialOrd,
{
    fn default() -> Self {
        Self {
            value: T::Scalar::zero(),
        }
    }
}

impl<const OVERFLOW: bool, T> UnaryState<T> for DecimalSumStateV2<OVERFLOW, T>
where
    T: ValueType,
    T::Scalar: Decimal
        + std::ops::AddAssign
        + Serialize
        + DeserializeOwned
        + Copy
        + Clone
        + std::fmt::Debug
        + std::cmp::PartialOrd,
{
    fn add(&mut self, other: T::ScalarRef<'_>) -> Result<()> {
        self.value += T::to_owned_scalar(other);
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

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.add(T::to_scalar_ref(&rhs.value))
    }

    fn merge_result(&mut self) -> Result<Option<Scalar>> {
        Ok(Some(T::upcast_scalar(self.value)))
    }

    fn serialize(&self, writer: &mut Vec<u8>) -> Result<()> {
        serialize_state(writer, &self.value)
    }

    fn deserialize(reader: &mut &[u8]) -> Result<Self>
    where Self: Sized {
        let value = deserialize_state(reader)?;
        Ok(Self { value })
    }
}

pub(crate) fn create_sum(
    display_name: &str,
    params: Vec<Scalar>,
    arguments: &[DataType],
    data_type: &DataType,
) -> Result<Arc<dyn AggregateFunction>> {
    with_number_mapped_type!(|NUM| match &data_type {
        DataType::Number(NumberDataType::NUM) => {
            type TSum = <NUM as ResultTypeOfUnary>::Sum;
            let return_type = NumberType::<TSum>::data_type();
            AggregateUnaryFunction::<
                NumberSumStateV2<NumberType<NUM>, TSum>,
                NumberType<NUM>,
                NumberType<TSum>,
            >::try_create(display_name, return_type, params, arguments[0].clone())
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
                    DecimalSumStateV2<false, Decimal128Type>,
                    Decimal128Type,
                    Decimal128Type,
                >::try_create(
                    display_name, return_type, params, arguments[0].clone()
                )
            } else {
                AggregateUnaryFunction::<
                    DecimalSumStateV2<true, Decimal128Type>,
                    Decimal128Type,
                    Decimal128Type,
                >::try_create(
                    display_name, return_type, params, arguments[0].clone()
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
            let return_type = DataType::Decimal(DecimalDataType::from_size(decimal_size)?);

            if overflow {
                AggregateUnaryFunction::<
                    DecimalSumStateV2<false, Decimal256Type>,
                    Decimal256Type,
                    Decimal256Type,
                >::try_create(
                    display_name, return_type, params, arguments[0].clone()
                )
            } else {
                AggregateUnaryFunction::<
                    DecimalSumStateV2<true, Decimal256Type>,
                    Decimal256Type,
                    Decimal256Type,
                >::try_create(
                    display_name, return_type, params, arguments[0].clone()
                )
            }
        }
        _ => Err(ErrorCode::BadDataValueType(format!(
            "AggregateSumFunction does not support type '{:?}'",
            arguments[0]
        ))),
    })
}
