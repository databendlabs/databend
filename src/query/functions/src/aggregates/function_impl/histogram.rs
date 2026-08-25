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
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::fmt::Display;
use std::ops::AddAssign;

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggrStateType;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::StateSerdeItem;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::BuilderMut;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DateType;
use databend_common_expression::types::DecimalDataKind;
use databend_common_expression::types::DecimalType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::ValueType;
use databend_common_expression::types::i256;
use databend_common_expression::with_decimal_mapped_type;
use databend_common_expression::with_number_mapped_type;
use serde::Deserialize;
use serde::Serialize;

use super::FunctionFactory;
use super::adaptors::*;

struct HistogramBuilder;

impl HistogramBuilder {
    fn register(registry: &mut AggregateFunctionRegistry) {
        DirectNameRoute::new(
            &["histogram"],
            HistogramBuilder::histogram_arguments(),
            HistogramBuilder::HISTOGRAM_FEATURES,
            NullPolicy::Skip,
        )
        .then(MergeRoute::unary(false, HistogramBuilder::create))
        .then(MergeRoute::unary(true, HistogramBuilder::create))
        .then(PlainRoute::unary(HistogramBuilder::create))
        .then(IfRoute::unary(HistogramBuilder::create))
        .then(StateRoute::unary(HistogramBuilder::create))
        .register(registry);
    }
}

inventory::submit! {
    FunctionFactory {
        register: HistogramBuilder::register,
    }
}

impl HistogramBuilder {
    fn histogram_arguments() -> ArgumentsPattern {
        ArgumentsPattern::variadic(
            vec![ArgumentPattern::any()],
            ArgumentPattern::any(),
            0,
            Some(1),
        )
    }

    const HISTOGRAM_FEATURES: FunctionFeatures = FunctionFeatures {
        is_decomposable: false,
        supports_filter: false,
        sort_policy: SortPolicy::Unsupported,
        distinct_policy: DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "builds an equi-height histogram",
        definition: "histogram(expr[, buckets])",
        example: "select histogram(number) from numbers(10)",
    };
}

pub struct HistogramData {
    max_num_buckets: u64,
    data_type: DataType,
}

#[derive(BorshSerialize, BorshDeserialize)]
pub struct AggregateHistogramState<T>
where
    T: ValueType,
    T::Scalar: Ord + BorshSerialize + BorshDeserialize,
{
    value_map: BTreeMap<T::Scalar, u64>,
}

impl<T> Default for AggregateHistogramState<T>
where
    T: ValueType,
    T::Scalar: Ord + BorshSerialize + BorshDeserialize,
{
    fn default() -> Self {
        Self {
            value_map: BTreeMap::new(),
        }
    }
}

impl<T> AggregateHistogramState<T>
where
    T: ValueType,
    T::Scalar: Ord + BorshSerialize + BorshDeserialize,
{
    pub fn state_description() -> AggregateStateDescription {
        AggregateStateDescription::new(vec![AggrStateType::Custom(Layout::new::<Self>())], vec![
            StateSerdeItem::Binary(None),
        ])
        .with_manual_drop(true)
    }

    fn merge_state(&mut self, rhs: &Self) {
        for (key, value) in rhs.value_map.iter() {
            match self.value_map.get_mut(key) {
                Some(entry) => entry.add_assign(value),
                None => {
                    self.value_map.insert(key.clone(), *value);
                }
            }
        }
    }

    fn merge_owned_state(&mut self, rhs: &mut Self) {
        for (key, value) in std::mem::take(&mut rhs.value_map) {
            match self.value_map.entry(key) {
                Entry::Occupied(entry) => entry.into_mut().add_assign(value),
                Entry::Vacant(entry) => {
                    entry.insert(value);
                }
            }
        }
    }
}

impl<T> UnaryState<T, StringType> for AggregateHistogramState<T>
where
    T: ValueType,
    T::Scalar: Ord + BorshSerialize + BorshDeserialize + Serialize + Display,
{
    type FunctionInfo = HistogramData;

    fn init(_function_info: &Self::FunctionInfo) -> Self {
        Self::default()
    }

    fn add(&mut self, value: T::ScalarRef<'_>, _function_info: &Self::FunctionInfo) -> Result<()> {
        let value = T::to_owned_scalar(value);
        match self.value_map.entry(value) {
            Entry::Occupied(entry) => *entry.into_mut() += 1,
            Entry::Vacant(entry) => {
                entry.insert(1);
            }
        };
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.merge_state(rhs);
        Ok(())
    }

    fn merge_owned(&mut self, rhs: &mut Self) -> Result<()> {
        self.merge_owned_state(rhs);
        Ok(())
    }

    fn merge_result(
        &mut self,
        mut builder: BuilderMut<'_, StringType>,
        histogram_data: &Self::FunctionInfo,
    ) -> Result<()> {
        let mut buckets = build_histogram(&self.value_map, histogram_data.max_num_buckets);
        let format_scalar = |scalar| {
            let scalar = T::upcast_scalar_with_type(scalar, &histogram_data.data_type);
            format!("{scalar}")
        };

        let json_str = serde_json::to_string(
            &buckets
                .drain(..)
                .map(|raw| Bucket {
                    lower: format_scalar(raw.lower),
                    upper: format_scalar(raw.upper),
                    ndv: raw.ndv,
                    count: raw.count,
                    pre_sum: raw.pre_sum,
                })
                .collect::<Vec<Bucket<String>>>(),
        )?;
        builder.put_and_commit(json_str);
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
        self.merge_state(&rhs);
        Ok(())
    }
}

impl HistogramBuilder {
    fn create(build: UnaryBuildContext<'_, impl CombinatorImpl>) -> Result<AggregateFunctionRef> {
        let data_type = build.arg_type().clone();
        let display_name = build.name().to_string();
        let max_num_buckets = Self::get_max_num_buckets(build.params(), &display_name)?;

        with_number_mapped_type!(|NUM| match &data_type {
            DataType::Number(NumberDataType::NUM) =>
                Self::create_instance::<NumberType<NUM>>(build, data_type.clone(), max_num_buckets,),
            DataType::Decimal(size) => {
                with_decimal_mapped_type!(|DECIMAL| match size.data_kind() {
                    DecimalDataKind::DECIMAL => Self::create_instance::<DecimalType<DECIMAL>>(
                        build,
                        data_type.clone(),
                        max_num_buckets,
                    ),
                })
            }
            DataType::String => {
                Self::create_instance::<StringType>(build, data_type.clone(), max_num_buckets)
            }
            DataType::Timestamp => {
                Self::create_instance::<TimestampType>(build, data_type.clone(), max_num_buckets)
            }
            DataType::Date => {
                Self::create_instance::<DateType>(build, data_type.clone(), max_num_buckets)
            }
            _ => Err(ErrorCode::BadDataValueType(format!(
                "{} does not support type '{:?}'",
                display_name, data_type
            ))),
        })
    }

    fn create_instance<T>(
        build: UnaryBuildContext<'_, impl CombinatorImpl>,
        data_type: DataType,
        max_num_buckets: u64,
    ) -> Result<AggregateFunctionRef>
    where
        T: AccessType + ValueType,
        T::Scalar: Ord + BorshSerialize + BorshDeserialize + Serialize + Display,
    {
        let state = AggregateHistogramState::<T>::state_description();
        let return_type = StringType::data_type();

        build.create_unary_or_null::<AggregateHistogramState<T>, T, StringType>(
            return_type.wrap_nullable(),
            state,
            HistogramData {
                max_num_buckets,
                data_type,
            },
        )
    }

    fn get_max_num_buckets(params: &[Scalar], display_name: &str) -> Result<u64> {
        if params.len() != 1 {
            return Ok(128);
        }
        if let Scalar::Number(number) = params[0]
            && let Some(number) = number.integer_to_i128()
            && number > 0
        {
            return Ok(number as u64);
        }
        Err(ErrorCode::BadDataValueType(format!(
            "The argument of aggregate function {} must be positive int",
            display_name
        )))
    }
}

#[derive(Serialize, Deserialize)]
struct Bucket<T> {
    lower: T,
    upper: T,
    ndv: u64,
    count: u64,
    pre_sum: u64,
}

fn can_assign_into_buckets<T: Ord>(
    value_map: &BTreeMap<T, u64>,
    max_bucket_size: u64,
    num_buckets: u64,
) -> bool {
    if value_map.is_empty() {
        return false;
    };

    let mut used_buckets = 1;
    let mut current_bucket_size = 0;

    for count in value_map.values() {
        current_bucket_size += count;
        if current_bucket_size > max_bucket_size {
            used_buckets += 1;
            current_bucket_size = *count;
        }
        if used_buckets > num_buckets {
            return false;
        }
    }

    true
}

fn calculate_bucket_max_values<T: Ord>(value_map: &BTreeMap<T, u64>, num_buckets: u64) -> u64 {
    debug_assert!(!value_map.is_empty());

    let total_values = value_map.values().sum();
    if num_buckets == 1 {
        return total_values;
    }

    let mut upper_bucket_values = 2 * total_values / (num_buckets - 1) + 1;
    let mut lower_bucket_values = 0;
    let mut search_step = 0;
    let max_search_steps = 10;

    while upper_bucket_values > lower_bucket_values + 1 && search_step < max_search_steps {
        let bucket_values = (upper_bucket_values + lower_bucket_values) / 2;

        if can_assign_into_buckets(value_map, bucket_values, num_buckets) {
            upper_bucket_values = bucket_values;
        } else {
            lower_bucket_values = bucket_values;
        }
        search_step += 1;
    }

    upper_bucket_values
}

fn build_histogram<T>(value_map: &BTreeMap<T, u64>, max_num_buckets: u64) -> Vec<Bucket<T>>
where T: Ord + Clone {
    let mut buckets = Vec::new();
    if value_map.is_empty() {
        return buckets;
    }

    let bucket_max_values = calculate_bucket_max_values(value_map, max_num_buckets);
    buckets.reserve(max_num_buckets as usize);

    let mut distinct_values_count = 0;
    let mut values_count = 0;
    let mut cumulative_values = 0;
    let mut remaining_distinct_values = value_map.len();
    let mut iter = value_map.iter().peekable();
    let mut lower_value = iter.peek().unwrap().0;

    while let Some(curr) = iter.next() {
        let count = *curr.1;
        let current_value = curr.0;

        distinct_values_count += 1;
        remaining_distinct_values -= 1;
        values_count += count;
        cumulative_values += count;

        let next = iter.peek();
        let remaining_empty_buckets = max_num_buckets - buckets.len() as u64 - 1;

        if let Some(next) = next
            && remaining_distinct_values as u64 > remaining_empty_buckets
            && values_count + *next.1 <= bucket_max_values
        {
            continue;
        }

        let pre_sum = cumulative_values - values_count;
        buckets.push(Bucket {
            lower: lower_value.clone(),
            upper: current_value.clone(),
            ndv: distinct_values_count,
            count: values_count,
            pre_sum,
        });

        if let Some(next) = next {
            lower_value = next.0;
        }
        values_count = 0;
        distinct_values_count = 0;
    }

    buckets
}
