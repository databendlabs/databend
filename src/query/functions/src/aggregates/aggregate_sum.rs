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

use common_arrow::arrow::bitmap::Bitmap;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::decimal::*;
use common_expression::types::number::*;
use common_expression::types::*;
use common_expression::utils::arithmetics_type::ResultTypeOfUnary;
use common_expression::with_number_mapped_type;
use common_expression::AggregateFunctionRef;
use common_expression::Column;
use common_expression::ColumnBuilder;
use common_expression::Scalar;
use common_expression::StateAddr;
use num_traits::AsPrimitive;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;

use super::assert_unary_arguments;
use super::deserialize_state;
use super::serialize_state;
use super::FunctionData;
use crate::aggregates::aggregate_function_factory::AggregateFunctionDescription;
use crate::aggregates::aggregate_unary::UnaryState;
use crate::aggregates::AggregateUnaryFunction;

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

#[derive(Deserialize, Serialize)]
pub struct NumberSumState<R>
where R: ValueType
{
    pub value: R::Scalar,
}

impl<R> Default for NumberSumState<R>
where
    R: ValueType,
    R::Scalar: Number + AsPrimitive<f64> + Serialize + DeserializeOwned + std::ops::AddAssign,
{
    fn default() -> Self {
        NumberSumState::<R> {
            value: R::Scalar::default(),
        }
    }
}

impl<T, R> UnaryState<T, R> for NumberSumState<R>
where
    T: ValueType + Sync + Send,
    R: ValueType,
    T::Scalar: Number + AsPrimitive<R::Scalar>,
    R::Scalar: Number + AsPrimitive<f64> + Serialize + DeserializeOwned + std::ops::AddAssign,
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

    fn merge_result(
        &mut self,
        builder: &mut R::ColumnBuilder,
        _function_data: Option<&dyn FunctionData>,
    ) -> Result<()> {
        R::push_item(builder, R::to_scalar_ref(&self.value));
        Ok(())
    }

    fn serialize(&self, writer: &mut Vec<u8>) -> Result<()> {
        serialize_state(writer, &self.value)
    }

    fn deserialize(reader: &mut &[u8]) -> Result<Self> {
        let value = deserialize_state(reader)?;
        Ok(Self { value })
    }
}

#[derive(Deserialize, Serialize)]
pub struct DecimalSumState<const OVERFLOW: bool, T>
where
    T: ValueType,
    T::Scalar: Decimal,
{
    pub value: T::Scalar,
}

impl<const OVERFLOW: bool, T> Default for DecimalSumState<OVERFLOW, T>
where
    T: ValueType,
    T::Scalar: Decimal + std::ops::AddAssign + Serialize + DeserializeOwned,
{
    fn default() -> Self {
        Self {
            value: T::Scalar::zero(),
        }
    }
}

impl<const OVERFLOW: bool, T> UnaryState<T, T> for DecimalSumState<OVERFLOW, T>
where
    T: ValueType,
    T::Scalar: Decimal + std::ops::AddAssign + Serialize + DeserializeOwned,
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

    fn merge_result(
        &mut self,
        builder: &mut T::ColumnBuilder,
        _function_data: Option<&dyn FunctionData>,
    ) -> Result<()> {
        T::push_item(builder, T::to_scalar_ref(&self.value));
        Ok(())
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

pub fn try_create_aggregate_sum_function(
    display_name: &str,
    params: Vec<Scalar>,
    arguments: Vec<DataType>,
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
