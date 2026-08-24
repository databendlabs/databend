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

use std::alloc::Layout;

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggrStateType;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ColumnView;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::StateSerdeItem;
use databend_common_expression::compare_columns;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::ArrayType;
use databend_common_expression::types::BinaryType;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DateType;
use databend_common_expression::types::DecimalDataKind;
use databend_common_expression::types::DecimalType;
use databend_common_expression::types::EmptyArrayType;
use databend_common_expression::types::EmptyMapType;
use databend_common_expression::types::NullType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::ReturnType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::ValueType;
use databend_common_expression::types::array::ArrayColumnBuilderMut;
use databend_common_expression::types::i256;
use databend_common_expression::with_decimal_mapped_type;
use databend_common_expression::with_number_mapped_type;
use rand::Rng;
use rand::SeedableRng;
use rand::prelude::SliceRandom;
use rand::rngs::SmallRng;
use rand::thread_rng;

use super::FunctionFactory;
use super::adaptors::*;
use crate::with_simple_no_number_mapped_type;

struct RangeBoundBuilder;

impl RangeBoundBuilder {
    fn register(registry: &mut AggregateFunctionRegistry) {
        DirectNameRoute::new(
            &["range_bound"],
            RangeBoundBuilder::range_bound_arguments(),
            RangeBoundBuilder::RANGE_BOUND_FEATURES,
            NullPolicy::Skip,
        )
        .then(MergeRoute::unary(false, RangeBoundBuilder::create))
        .then(MergeRoute::unary(true, RangeBoundBuilder::create))
        .then(PlainRoute::unary(RangeBoundBuilder::create))
        .then(IfRoute::unary(RangeBoundBuilder::create))
        .then(StateRoute::unary(RangeBoundBuilder::create))
        .register(registry);
    }
}

inventory::submit! {
    FunctionFactory {
        register: RangeBoundBuilder::register,
    }
}

impl RangeBoundBuilder {
    fn range_bound_arguments() -> AggregateArgumentsPattern {
        AggregateArgumentsPattern::fixed(vec![AggregateArgumentPattern::any()])
    }

    const RANGE_BOUND_FEATURES: FunctionFeatures = FunctionFeatures {
        is_decomposable: false,
        supports_filter: false,
        sort_policy: SortPolicy::Unsupported,
        distinct_policy: DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "calculates partition boundaries for a column",
        definition: "range_bound(partition_num, sample_size)(expr)",
        example: "select range_bound(4)(number) from numbers(10)",
    };
}

pub struct RangeBoundData {
    partitions: usize,
    sample_size: usize,
    data_type: DataType,
}

pub struct AggregateRangeBoundState<T: ValueType> {
    values: Vec<(u64, Vec<T::Scalar>)>,
    total_rows: usize,
    total_samples: usize,
}

impl<T> Default for AggregateRangeBoundState<T>
where T: ValueType
{
    fn default() -> Self {
        Self {
            values: vec![],
            total_rows: 0,
            total_samples: 0,
        }
    }
}

impl<T> AggregateRangeBoundState<T>
where
    T: ValueType,
    T::Scalar: BorshSerialize + BorshDeserialize,
{
    pub fn state_description() -> AggregateStateDescription {
        AggregateStateDescription::new(vec![AggrStateType::Custom(Layout::new::<Self>())], vec![
            StateSerdeItem::Binary(None),
        ])
        .with_manual_drop(true)
    }
}

impl<T> BorshSerialize for AggregateRangeBoundState<T>
where
    T: ValueType,
    T::Scalar: BorshSerialize,
{
    fn serialize<W: borsh::io::Write>(&self, writer: &mut W) -> borsh::io::Result<()> {
        BorshSerialize::serialize(&self.values, writer)?;
        BorshSerialize::serialize(&self.total_rows, writer)?;
        BorshSerialize::serialize(&self.total_samples, writer)
    }
}

impl<T> BorshDeserialize for AggregateRangeBoundState<T>
where
    T: ValueType,
    T::Scalar: BorshDeserialize,
{
    fn deserialize_reader<R: borsh::io::Read>(reader: &mut R) -> borsh::io::Result<Self> {
        Ok(Self {
            values: Vec::<(u64, Vec<T::Scalar>)>::deserialize_reader(reader)?,
            total_rows: usize::deserialize_reader(reader)?,
            total_samples: usize::deserialize_reader(reader)?,
        })
    }
}

impl<T> UnaryState<T, ArrayType<T>> for AggregateRangeBoundState<T>
where
    T: ReturnType,
    T::Scalar: Ord + BorshSerialize + BorshDeserialize,
{
    type FunctionInfo = RangeBoundData;

    fn init(_function_info: &Self::FunctionInfo) -> Self {
        Self::default()
    }

    fn add(&mut self, value: T::ScalarRef<'_>, function_info: &Self::FunctionInfo) -> Result<()> {
        let total_sample_size =
            std::cmp::min(function_info.sample_size * function_info.partitions, 10_000);

        if self.values.is_empty() {
            self.values.push((0, vec![]));
        }
        let (total_rows, samples) = &mut self.values[0];
        *total_rows += 1;
        self.total_rows += 1;
        if samples.len() < total_sample_size {
            self.total_samples += 1;
            samples.push(T::to_owned_scalar(value));
        } else {
            let mut rng = thread_rng();
            let replacement_index = rng.gen_range(0..*total_rows) as usize;
            if replacement_index < total_sample_size {
                self.total_samples += 1;
                samples[replacement_index] = T::to_owned_scalar(value);
            }
        }
        Ok(())
    }

    fn add_batch(
        &mut self,
        values: ColumnView<T>,
        validity: Option<&Bitmap>,
        function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        let column_len = values.len();
        let unset_bits = validity.map_or(0, |validity| validity.null_count());
        if unset_bits == column_len {
            return Ok(());
        }

        let valid_size = column_len - unset_bits;
        let sample_size = std::cmp::max(valid_size / 100, function_info.sample_size);

        let mut indices = validity.map_or_else(
            || (0..column_len).collect::<Vec<_>>(),
            |validity| {
                validity
                    .iter()
                    .enumerate()
                    .filter_map(|(index, valid)| valid.then_some(index))
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
            .map(|index| T::to_owned_scalar(unsafe { values.index_unchecked(*index) }))
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

    fn merge_owned(&mut self, rhs: &mut Self) -> Result<()> {
        self.values.append(&mut rhs.values);
        self.total_rows += rhs.total_rows;
        self.total_samples += rhs.total_samples;
        Ok(())
    }

    fn merge_result(
        &mut self,
        mut builder: ArrayColumnBuilderMut<'_, T>,
        function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        let step = self.total_rows as f64 / function_info.partitions as f64;

        let mut data = Vec::with_capacity(self.total_samples);
        let mut weights = Vec::with_capacity(self.total_samples);
        for (num, values) in self.values.iter() {
            let weight = *num as f64 / values.len() as f64;
            values.iter().for_each(|value| {
                data.push(value.clone());
                weights.push(weight);
            });
        }
        let col = T::upcast_column_with_type(
            T::column_from_vec(data.clone(), &[]),
            &function_info.data_type,
        );
        let indices = compare_columns(vec![col], self.total_samples)?;

        let mut cum_weight = 0.0;
        let mut target = step;
        let mut bounds = Vec::with_capacity(function_info.partitions - 1);
        let mut previous_bound = None;

        let mut index = 0;
        let mut partition = 0;
        while index < self.total_samples && partition < function_info.partitions - 1 {
            let idx = indices[index] as usize;
            let weight = weights[idx];
            cum_weight += weight;
            if cum_weight >= target {
                let value = &data[idx];
                if previous_bound.as_ref().is_none_or(|prev| value > prev) {
                    bounds.push(value.clone());
                    target += step;
                    partition += 1;
                    previous_bound = Some(value.clone());
                }
            }
            index += 1;
        }

        let col = T::column_from_vec(bounds, &[]);
        builder.push(col);
        Ok(())
    }

    fn serialize(
        &self,
        builder: &mut ColumnBuilder,
        _function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        let binary_builder = builder.as_binary_mut().unwrap();
        BorshSerialize::serialize(self, &mut binary_builder.data)?;
        binary_builder.commit_row();
        Ok(())
    }

    fn merge_serialized(
        &mut self,
        value: ScalarRef<'_>,
        _function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        let ScalarRef::Binary(mut data) = value else {
            unreachable!()
        };
        let rhs = Self::deserialize_reader(&mut data)?;
        self.merge(&rhs)
    }
}

impl RangeBoundBuilder {
    fn create(build: UnaryBuildContext<'_, impl CombinatorImpl>) -> Result<AggregateFunctionRef> {
        let data_type = build.arg_type().clone();
        let display_name = build.name().to_string();
        let function_info = Self::get_partitions(build.params(), &display_name, data_type.clone())?;
        let return_type = DataType::Array(Box::new(data_type.clone()));

        with_simple_no_number_mapped_type!(|T| match data_type {
            DataType::T => Self::create_instance::<T>(build, return_type, function_info),
            DataType::Number(number_type) => {
                with_number_mapped_type!(|NUM| match number_type {
                    NumberDataType::NUM => {
                        Self::create_instance::<NumberType<NUM>>(build, return_type, function_info)
                    }
                })
            }
            DataType::Decimal(size) => {
                with_decimal_mapped_type!(|DECIMAL| match size.data_kind() {
                    DecimalDataKind::DECIMAL => Self::create_instance::<DecimalType<DECIMAL>>(
                        build,
                        return_type,
                        function_info,
                    ),
                })
            }
            DataType::Binary =>
                Self::create_instance::<BinaryType>(build, return_type, function_info),
            _ => Err(ErrorCode::BadDataValueType(format!(
                "{} does not support type '{:?}'",
                display_name, data_type
            ))),
        })
    }

    fn create_instance<T>(
        build: UnaryBuildContext<'_, impl CombinatorImpl>,
        return_type: DataType,
        function_info: RangeBoundData,
    ) -> Result<AggregateFunctionRef>
    where
        T: AccessType + ReturnType,
        T::Scalar: Ord + BorshSerialize + BorshDeserialize,
    {
        let state = AggregateRangeBoundState::<T>::state_description();

        build.create_unary_or_null::<AggregateRangeBoundState<T>, T, ArrayType<T>>(
            return_type.wrap_nullable(),
            state,
            function_info,
        )
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
}

fn get_positive_integer(value: &Scalar, display_name: &str) -> Result<usize> {
    if let Scalar::Number(number) = value
        && let Some(number) = number.integer_to_i128()
        && number > 0
    {
        return Ok(number as usize);
    }
    Err(ErrorCode::BadDataValueType(format!(
        "The argument of aggregate function {} must be positive int",
        display_name
    )))
}
