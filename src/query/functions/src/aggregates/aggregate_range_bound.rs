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
use databend_common_expression::compare_columns;
use databend_common_expression::types::array::ArrayColumnBuilderMut;
use databend_common_expression::types::i256;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::*;
use databend_common_expression::with_decimal_mapped_type;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::AggrStateLoc;
use databend_common_expression::AggregateFunctionRef;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ColumnView;
use databend_common_expression::Scalar;
use databend_common_expression::StateAddr;
use rand::prelude::SliceRandom;
use rand::rngs::SmallRng;
use rand::thread_rng;
use rand::Rng;
use rand::SeedableRng;

use super::assert_unary_arguments;
use super::batch_merge1;
use super::AggrState;
use super::AggregateFunctionDescription;
use super::AggregateFunctionSortDesc;
use super::AggregateUnaryFunction;
use super::SerializeInfo;
use super::StateSerde;
use super::StateSerdeItem;
use super::UnaryState;
use crate::with_simple_no_number_mapped_type;

pub struct RangeBoundData {
    partitions: usize,
    sample_size: usize,
    data_type: DataType,
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
    T: ReturnType,
    T::Scalar: Ord + BorshSerialize + BorshDeserialize,
{
    type FunctionInfo = RangeBoundData;

    fn add(&mut self, other: T::ScalarRef<'_>, range_bound_data: &RangeBoundData) -> Result<()> {
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
        other: ColumnView<T>,
        validity: Option<&Bitmap>,
        range_bound_data: &RangeBoundData,
    ) -> Result<()> {
        let column_len = other.len();
        let unset_bits = validity.map_or(0, |v| v.null_count());
        if unset_bits == column_len {
            return Ok(());
        }

        let valid_size = column_len - unset_bits;
        let sample_size = std::cmp::max(valid_size / 100, range_bound_data.sample_size);

        let mut indices = validity.map_or_else(
            || (0..column_len).collect::<Vec<_>>(),
            |v| {
                v.iter()
                    .enumerate()
                    .filter_map(|(i, v)| v.then_some(i))
                    .collect()
            },
        );

        let sampled_indices = if valid_size > sample_size {
            let mut rng = SmallRng::from_entropy();
            indices.shuffle(&mut rng);
            &indices[..sample_size]
        } else {
            &indices
        };

        let sample_values = sampled_indices
            .iter()
            .map(|i| T::to_owned_scalar(unsafe { other.index_unchecked(*i) }))
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
        mut builder: ArrayColumnBuilderMut<'_, T>,
        range_bound_data: &RangeBoundData,
    ) -> Result<()> {
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
        let col = T::upcast_column_with_type(
            T::column_from_vec(data.clone(), &[]),
            &range_bound_data.data_type,
        );
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
                if previous_bound.as_ref().is_none_or(|prev| data > prev) {
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

impl<T> StateSerde for RangeBoundState<T>
where
    T: ReturnType,
    T::Scalar: BorshSerialize + BorshDeserialize + Ord,
{
    fn serialize_type(_: Option<&dyn SerializeInfo>) -> Vec<StateSerdeItem> {
        vec![StateSerdeItem::Binary(None)]
    }

    fn batch_serialize(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        let binary_builder = builders[0].as_binary_mut().unwrap();
        for place in places {
            let state: &mut Self = AggrState::new(*place, loc).get();
            state.serialize(&mut binary_builder.data)?;
            binary_builder.commit_row();
        }
        Ok(())
    }

    fn batch_merge(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()> {
        batch_merge1::<BinaryType, Self, _>(places, loc, state, filter, |state, mut data| {
            let rhs = Self::deserialize_reader(&mut data)?;
            state.merge(&rhs)
        })
    }
}

pub fn try_create_aggregate_range_bound_function(
    display_name: &str,
    params: Vec<Scalar>,
    arguments: Vec<DataType>,
    _sort_descs: Vec<AggregateFunctionSortDesc>,
) -> Result<AggregateFunctionRef> {
    assert_unary_arguments(display_name, arguments.len())?;
    let data_type = arguments[0].clone().remove_nullable();
    let function_info = get_partitions(&params, display_name, data_type.clone())?;
    let return_type = DataType::Array(Box::new(data_type.clone()));

    with_simple_no_number_mapped_type!(|T| match data_type {
        DataType::T => {
            AggregateUnaryFunction::<RangeBoundState<T>, T, ArrayType<T>>::with_function_info(
                display_name,
                return_type,
                function_info,
            )
            .with_need_drop(true)
            .finish()
        }
        DataType::Number(num_type) => {
            with_number_mapped_type!(|NUM| match num_type {
                NumberDataType::NUM => {
                    AggregateUnaryFunction::<
                        RangeBoundState<NumberType<NUM>>,
                        NumberType<NUM>,
                        ArrayType<NumberType<NUM>>,
                    >::with_function_info(
                        display_name, return_type, function_info
                    )
                    .with_need_drop(true)
                    .finish()
                }
            })
        }
        DataType::Decimal(size) => {
            with_decimal_mapped_type!(|DECIMAL| match size.data_kind() {
                DecimalDataKind::DECIMAL => {
                    AggregateUnaryFunction::<
                        RangeBoundState<DecimalType<DECIMAL>>,
                        DecimalType<DECIMAL>,
                        ArrayType<DecimalType<DECIMAL>>,
                    >::with_function_info(
                        display_name, return_type, function_info
                    )
                    .with_need_drop(true)
                    .finish()
                }
            })
        }
        DataType::Binary => {
            AggregateUnaryFunction::<RangeBoundState<BinaryType>, BinaryType, ArrayType<BinaryType>>::with_function_info(
                display_name,
                return_type,
                function_info,
            )
            .with_need_drop(true)
            .finish()
        }
        _ => Err(ErrorCode::BadDataValueType(format!(
            "{display_name} does not support type '{data_type:?}'",
        ))),
    })
}

/// The `range_bound(partition_num, sample_size)(col)` function calculates the partition boundaries
/// for a given column `col`. It divides the column's data range into `partition_num` partitions,
/// using `sample_size` to determine the number of samples per block. The resulting boundaries
/// define the ranges for each partition.
///
/// Example:
/// For a column with values `(0, 1, 3, 6, 8)` and `partition_num = 3`, the function calculates the
/// partition boundaries based on the distribution of the data. The boundaries might be `[1, 6]`.
pub fn aggregate_range_bound_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(try_create_aggregate_range_bound_function))
}

fn get_partitions(
    params: &[Scalar],
    display_name: &str,
    data_type: DataType,
) -> Result<RangeBoundData> {
    match params.len() {
        0 => Ok(RangeBoundData {
            partitions: 1024,
            sample_size: 100,
            data_type,
        }),
        1 => {
            let partitions = get_positive_integer(&params[0], display_name)?;
            Ok(RangeBoundData {
                partitions,
                sample_size: 100,
                data_type,
            })
        }
        2 => {
            let partitions = get_positive_integer(&params[0], display_name)?;
            let sample_size = get_positive_integer(&params[1], display_name)?;
            Ok(RangeBoundData {
                partitions,
                sample_size,
                data_type,
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
