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

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::array::ArrayColumnBuilder;
use databend_common_expression::types::decimal::*;
use databend_common_expression::types::number::*;
use databend_common_expression::types::*;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::Scalar;
use ethnum::i256;

use super::get_levels;
use super::AggregateUnaryFunction;
use super::FunctionData;
use super::QuantileData;
use super::UnaryState;
use crate::aggregates::aggregate_function_factory::AggregateFunctionDescription;
use crate::aggregates::assert_unary_arguments;
use crate::aggregates::AggregateFunctionRef;
use crate::with_simple_no_number_mapped_type;

#[derive(BorshSerialize, BorshDeserialize)]
struct QuantileState<T>
where
    T: ValueType,
    T::Scalar: BorshSerialize + BorshDeserialize,
{
    pub value: Vec<T::Scalar>,
}

impl<T> Default for QuantileState<T>
where
    T: ValueType,
    T::Scalar: BorshSerialize + BorshDeserialize,
{
    fn default() -> Self {
        Self { value: vec![] }
    }
}

impl<T> UnaryState<T, ArrayType<T>> for QuantileState<T>
where
    T: ValueType + Sync + Send,
    T::Scalar: BorshSerialize + BorshDeserialize + Sync + Send + Ord,
{
    fn add(
        &mut self,
        other: T::ScalarRef<'_>,
        _function_data: Option<&dyn FunctionData>,
    ) -> Result<()> {
        self.value.push(T::to_owned_scalar(other));
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.value.extend(
            rhs.value
                .iter()
                .map(|v| T::to_owned_scalar(T::to_scalar_ref(v))),
        );
        Ok(())
    }

    fn merge_result(
        &mut self,
        builder: &mut ArrayColumnBuilder<T>,
        function_data: Option<&dyn FunctionData>,
    ) -> Result<()> {
        let value_len = self.value.len();
        let quantile_disc_data = unsafe {
            function_data
                .unwrap()
                .as_any()
                .downcast_ref_unchecked::<QuantileData>()
        };
        if quantile_disc_data.levels.len() > 1 {
            let indices = quantile_disc_data
                .levels
                .iter()
                .map(|level| ((value_len - 1) as f64 * (*level)).floor() as usize)
                .collect::<Vec<usize>>();
            for idx in indices {
                if idx < value_len {
                    self.value.as_mut_slice().select_nth_unstable(idx);
                    let value = self.value.get(idx).unwrap();
                    builder.put_item(T::to_scalar_ref(value));
                } else {
                    builder.push_default();
                }
            }
            builder.commit_row();
        }
        Ok(())
    }
}

impl<T> UnaryState<T, T> for QuantileState<T>
where
    T: ArgType + Sync + Send,
    T::Scalar: BorshSerialize + BorshDeserialize + Sync + Send + Ord,
{
    fn add(
        &mut self,
        other: T::ScalarRef<'_>,
        _function_data: Option<&dyn FunctionData>,
    ) -> Result<()> {
        self.value.push(T::to_owned_scalar(other));
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.value.extend(
            rhs.value
                .iter()
                .map(|v| T::to_owned_scalar(T::to_scalar_ref(v))),
        );
        Ok(())
    }

    fn merge_result(
        &mut self,
        builder: &mut T::ColumnBuilder,
        function_data: Option<&dyn FunctionData>,
    ) -> Result<()> {
        let value_len = self.value.len();
        let quantile_disc_data = unsafe {
            function_data
                .unwrap()
                .as_any()
                .downcast_ref_unchecked::<QuantileData>()
        };

        let idx = ((value_len - 1) as f64 * quantile_disc_data.levels[0]).floor() as usize;
        if idx >= value_len {
            T::push_default(builder);
        } else {
            self.value.as_mut_slice().select_nth_unstable(idx);
            let value = self.value.get(idx).unwrap();
            T::push_item(builder, T::to_scalar_ref(value));
        }

        Ok(())
    }
}

pub fn try_create_aggregate_quantile_disc_function(
    display_name: &str,
    params: Vec<Scalar>,
    arguments: Vec<DataType>,
) -> Result<AggregateFunctionRef> {
    assert_unary_arguments(display_name, arguments.len())?;
    let data_type = arguments[0].clone();
    let levels = get_levels(&params)?;
    with_simple_no_number_mapped_type!(|T| match data_type {
        DataType::Number(num_type) => {
            with_number_mapped_type!(|NUM_TYPE| match num_type {
                NumberDataType::NUM_TYPE => {
                    if params.len() > 1 {
                        let func = AggregateUnaryFunction::<
                            QuantileState<NumberType<NUM_TYPE>>,
                            NumberType<NUM_TYPE>,
                            ArrayType<NumberType<NUM_TYPE>>,
                        >::try_create(
                            display_name,
                            DataType::Array(Box::new(data_type)),
                            params,
                            arguments[0].clone(),
                        )
                        .with_function_data(Box::new(QuantileData { levels }))
                        .with_need_drop(true);
                        Ok(Arc::new(func))
                    } else {
                        let func = AggregateUnaryFunction::<
                            QuantileState<NumberType<NUM_TYPE>>,
                            NumberType<NUM_TYPE>,
                            NumberType<NUM_TYPE>,
                        >::try_create(
                            display_name, data_type, params, arguments[0].clone()
                        )
                        .with_function_data(Box::new(QuantileData { levels }))
                        .with_need_drop(true);
                        Ok(Arc::new(func))
                    }
                }
            })
        }
        DataType::Decimal(DecimalDataType::Decimal128(s)) => {
            let decimal_size = DecimalSize {
                precision: s.precision,
                scale: s.scale,
            };
            let data_type = DataType::Decimal(DecimalDataType::from_size(decimal_size)?);
            if params.len() > 1 {
                let func = AggregateUnaryFunction::<
                    QuantileState<DecimalType<i128>>,
                    DecimalType<i128>,
                    ArrayType<DecimalType<i128>>,
                >::try_create(
                    display_name,
                    DataType::Array(Box::new(data_type)),
                    params,
                    arguments[0].clone(),
                )
                .with_function_data(Box::new(QuantileData { levels }))
                .with_need_drop(true);
                Ok(Arc::new(func))
            } else {
                let func = AggregateUnaryFunction::<
                    QuantileState<DecimalType<i128>>,
                    DecimalType<i128>,
                    DecimalType<i128>,
                >::try_create(
                    display_name, data_type, params, arguments[0].clone()
                )
                .with_function_data(Box::new(QuantileData { levels }))
                .with_need_drop(true);
                Ok(Arc::new(func))
            }
        }
        DataType::Decimal(DecimalDataType::Decimal256(s)) => {
            let decimal_size = DecimalSize {
                precision: s.precision,
                scale: s.scale,
            };
            let data_type = DataType::Decimal(DecimalDataType::from_size(decimal_size)?);
            if params.len() > 1 {
                let func = AggregateUnaryFunction::<
                    QuantileState<DecimalType<i256>>,
                    DecimalType<i256>,
                    ArrayType<DecimalType<i256>>,
                >::try_create(
                    display_name,
                    DataType::Array(Box::new(data_type)),
                    params,
                    arguments[0].clone(),
                )
                .with_function_data(Box::new(QuantileData { levels }))
                .with_need_drop(true);
                Ok(Arc::new(func))
            } else {
                let func = AggregateUnaryFunction::<
                    QuantileState<DecimalType<i256>>,
                    DecimalType<i256>,
                    DecimalType<i256>,
                >::try_create(
                    display_name, data_type, params, arguments[0].clone()
                )
                .with_function_data(Box::new(QuantileData { levels }))
                .with_need_drop(true);
                Ok(Arc::new(func))
            }
        }
        _ => Err(ErrorCode::BadDataValueType(format!(
            "{} does not support type '{:?}'",
            display_name, data_type
        ))),
    })
}
pub fn aggregate_quantile_disc_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(try_create_aggregate_quantile_disc_function))
}
