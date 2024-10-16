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
use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::compare_columns;
use databend_common_expression::types::array::ArrayColumnBuilder;
use databend_common_expression::types::*;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::AggregateFunctionRef;
use databend_common_expression::Scalar;
use ethnum::i256;
use rand::prelude::SliceRandom;
use rand::rngs::SmallRng;
use rand::thread_rng;
use rand::Rng;
use rand::SeedableRng;

use super::assert_unary_arguments;
use super::AggregateUnaryFunction;
use super::FunctionData;
use super::UnaryState;
use crate::aggregates::aggregate_function_factory::AggregateFunctionDescription;
use crate::with_simple_no_number_mapped_type;

struct RangeBoundData {
    partitions: usize,
    sample_size: usize,
}

impl FunctionData for RangeBoundData {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(BorshSerialize, BorshDeserialize)]
pub struct RangeBoundState<T>
where
    T: ValueType,
    T::Scalar: BorshSerialize + BorshDeserialize,
{
    values: Vec<(u64, Vec<T::Scalar>)>,
    total_rows: usize,
    total_samples: usize,
}

impl<T> Default for RangeBoundState<T>
where
    T: ValueType,
    T::Scalar: BorshSerialize + BorshDeserialize,
{
    fn default() -> Self {
        RangeBoundState::<T> {
            values: vec![],
            total_rows: 0,
            total_samples: 0,
        }
    }
}

impl<T> UnaryState<T, ArrayType<T>> for RangeBoundState<T>
where
    T: ArgType + Sync + Send,
    T::Scalar: Ord + Sync + Send + BorshSerialize + BorshDeserialize,
{
    fn add(
        &mut self,
        other: T::ScalarRef<'_>,
        function_data: Option<&dyn FunctionData>,
    ) -> Result<()> {
        let range_bound_data = unsafe {
            function_data
                .unwrap()
                .as_any()
                .downcast_ref_unchecked::<RangeBoundData>()
        };

        let total_sample_size = std::cmp::min(
            range_bound_data.sample_size * range_bound_data.partitions,
            10_000,
        );

        if self.values.is_empty() {
            self.values.push((0, vec![]));
        }
        let (total_rows, samples) = &mut self.values[0];
        *total_rows += 1;
        self.total_rows += 1;
        if samples.len() < total_sample_size {
            self.total_samples += 1;
            samples.push(T::to_owned_scalar(other));
        } else {
            let mut rng = thread_rng();
            let replacement_index = rng.gen_range(0..*total_rows) as usize;
            if replacement_index < total_sample_size {
                self.total_samples += 1;
                samples[replacement_index] = T::to_owned_scalar(other);
            }
        }
        Ok(())
    }

    fn add_batch(
        &mut self,
        other: T::Column,
        validity: Option<&Bitmap>,
        function_data: Option<&dyn FunctionData>,
    ) -> Result<()> {
        let column_len = T::column_len(&other);
        let unset_bits = validity.map_or(0, |v| v.unset_bits());
        if unset_bits == column_len {
            return Ok(());
        }

        let range_bound_data = unsafe {
            function_data
                .unwrap()
                .as_any()
                .downcast_ref_unchecked::<RangeBoundData>()
        };
        let sample_size = std::cmp::max(
            (column_len - unset_bits) / 100,
            range_bound_data.sample_size,
        );

        let mut indices = validity.map_or_else(
            || (0..column_len).collect::<Vec<_>>(),
            |v| {
                v.iter()
                    .enumerate()
                    .filter_map(|(i, v)| if v { Some(i) } else { None })
                    .collect()
            },
        );

        let valid_size = indices.len();
        let sampled_indices = if valid_size > sample_size {
            let mut rng = SmallRng::from_entropy();
            indices.shuffle(&mut rng);
            &indices[..sample_size]
        } else {
            &indices
        };

        let sample_values = sampled_indices
            .iter()
            .map(|i| T::to_owned_scalar(unsafe { T::index_column_unchecked(&other, *i) }))
            .collect::<Vec<_>>();

        self.total_rows += valid_size;
        self.total_samples += sample_values.len();
        self.values.push((valid_size as u64, sample_values));
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.values.extend_from_slice(&rhs.values);
        self.total_rows += rhs.total_rows;
        self.total_samples += rhs.total_samples;
        Ok(())
    }

    fn merge_result(
        &mut self,
        builder: &mut ArrayColumnBuilder<T>,
        function_data: Option<&dyn FunctionData>,
    ) -> Result<()> {
        let range_bound_data = unsafe {
            function_data
                .unwrap()
                .as_any()
                .downcast_ref_unchecked::<RangeBoundData>()
        };
        let step = self.total_rows as f64 / range_bound_data.partitions as f64;

        let values = std::mem::take(&mut self.values);
        let mut data = Vec::with_capacity(self.total_samples);
        let mut weights = Vec::with_capacity(self.total_samples);
        for (num, values) in values.into_iter() {
            let weight = num as f64 / values.len() as f64;
            values.into_iter().for_each(|v| {
                data.push(v);
                weights.push(weight);
            });
        }
        let col = T::upcast_column(T::column_from_vec(data.clone(), &[]));
        let indices = compare_columns(vec![col], self.total_samples)?;

        let mut cum_weight = 0.0;
        let mut target = step;
        let mut bounds = Vec::with_capacity(range_bound_data.partitions - 1);
        let mut previous_bound = None;

        let mut i = 0;
        let mut j = 0;
        while i < self.total_samples && j < range_bound_data.partitions - 1 {
            let idx = indices[i] as usize;
            let weight = weights[idx];
            cum_weight += weight;
            if cum_weight >= target {
                let data = &data[idx];
                if previous_bound.as_ref().map_or(true, |prev| data > prev) {
                    bounds.push(data.clone());
                    target += step;
                    j += 1;
                    previous_bound = Some(data.clone());
                }
            }
            i += 1;
        }

        let col = T::column_from_vec(bounds, &[]);
        builder.push(col);
        Ok(())
    }
}

pub fn try_create_aggregate_range_bound_function(
    display_name: &str,
    params: Vec<Scalar>,
    arguments: Vec<DataType>,
) -> Result<AggregateFunctionRef> {
    assert_unary_arguments(display_name, arguments.len())?;
    let data_type = arguments[0].clone().remove_nullable();
    let function_data = get_partitions(&params, display_name)?;
    let return_type = DataType::Array(Box::new(data_type.clone()));

    with_simple_no_number_mapped_type!(|T| match data_type {
        DataType::T => {
            let func = AggregateUnaryFunction::<RangeBoundState<T>, T, ArrayType<T>>::try_create(
                display_name,
                return_type,
                params,
                arguments[0].clone(),
            )
            .with_function_data(Box::new(function_data))
            .with_need_drop(true);
            Ok(Arc::new(func))
        }
        DataType::Number(num_type) => {
            with_number_mapped_type!(|NUM| match num_type {
                NumberDataType::NUM => {
                    let func = AggregateUnaryFunction::<
                        RangeBoundState<NumberType<NUM>>,
                        NumberType<NUM>,
                        ArrayType<NumberType<NUM>>,
                    >::try_create(
                        display_name, return_type, params, arguments[0].clone()
                    )
                    .with_function_data(Box::new(function_data))
                    .with_need_drop(true);
                    Ok(Arc::new(func))
                }
            })
        }
        DataType::Decimal(DecimalDataType::Decimal128(_)) => {
            let func = AggregateUnaryFunction::<
                RangeBoundState<DecimalType<i128>>,
                DecimalType<i128>,
                ArrayType<DecimalType<i128>>,
            >::try_create(
                display_name, return_type, params, arguments[0].clone()
            )
            .with_function_data(Box::new(function_data))
            .with_need_drop(true);
            Ok(Arc::new(func))
        }
        DataType::Decimal(DecimalDataType::Decimal256(_)) => {
            let func = AggregateUnaryFunction::<
                RangeBoundState<DecimalType<i256>>,
                DecimalType<i256>,
                ArrayType<DecimalType<i256>>,
            >::try_create(
                display_name, return_type, params, arguments[0].clone()
            )
            .with_function_data(Box::new(function_data))
            .with_need_drop(true);
            Ok(Arc::new(func))
        }
        DataType::Binary => {
            let func = AggregateUnaryFunction::<
                RangeBoundState<BinaryType>,
                BinaryType,
                ArrayType<BinaryType>,
            >::try_create(
                display_name, return_type, params, arguments[0].clone()
            )
            .with_function_data(Box::new(function_data))
            .with_need_drop(true);
            Ok(Arc::new(func))
        }
        _ => Err(ErrorCode::BadDataValueType(format!(
            "{} does not support type '{:?}'",
            display_name, data_type
        ))),
    })
}
pub fn aggregate_range_bound_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(
        crate::aggregates::try_create_aggregate_range_bound_function,
    ))
}

fn get_partitions(params: &[Scalar], display_name: &str) -> Result<RangeBoundData> {
    match params.len() {
        0 => Ok(RangeBoundData {
            partitions: 1000,
            sample_size: 100,
        }),
        1 => {
            let partitions = get_positive_integer(&params[0], display_name)?;
            Ok(RangeBoundData {
                partitions,
                sample_size: 100,
            })
        }
        2 => {
            let partitions = get_positive_integer(&params[0], display_name)?;
            let sample_size = get_positive_integer(&params[1], display_name)?;
            Ok(RangeBoundData {
                partitions,
                sample_size,
            })
        }
        _ => Err(ErrorCode::BadArguments(format!(
            "The number of arguments in aggregate function {} must be [0, 1, 2]",
            display_name,
        ))),
    }
}

fn get_positive_integer(val: &Scalar, display_name: &str) -> Result<usize> {
    if let Scalar::Number(number) = val {
        if let Some(number) = number.integer_to_i128() {
            if number > 0 {
                return Ok(number as usize);
            }
        }
    }
    Err(ErrorCode::BadDataValueType(format!(
        "The argument of aggregate function {} must be positive int",
        display_name
    )))
}
