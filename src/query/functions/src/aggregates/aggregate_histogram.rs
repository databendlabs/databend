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
use std::collections::btree_map::BTreeMap;
use std::collections::btree_map::Entry;
use std::ops::AddAssign;
use std::sync::Arc;

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::decimal::*;
use databend_common_expression::types::number::*;
use databend_common_expression::types::string::StringColumnBuilder;
use databend_common_expression::types::*;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::AggregateFunctionRef;
use databend_common_expression::Scalar;
use serde::Deserialize;
use serde::Serialize;

use super::FunctionData;
use crate::aggregates::aggregate_function_factory::AggregateFunctionDescription;
use crate::aggregates::aggregate_unary::UnaryState;
use crate::aggregates::assert_variadic_arguments;
use crate::aggregates::AggregateUnaryFunction;

struct HistogramData {
    pub max_num_buckets: u64,
}

impl FunctionData for HistogramData {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(BorshSerialize, BorshDeserialize)]
pub struct HistogramState<T>
where
    T: ValueType,
    T::Scalar: Ord + BorshSerialize + BorshDeserialize,
{
    pub value_map: BTreeMap<T::Scalar, u64>,
}

impl<T> Default for HistogramState<T>
where
    T: ValueType,
    T::Scalar: Ord + BorshSerialize + BorshDeserialize,
{
    fn default() -> Self {
        HistogramState::<T> {
            value_map: BTreeMap::new(),
        }
    }
}

impl<T> UnaryState<T, StringType> for HistogramState<T>
where
    T: ValueType + Sync + Send,
    T::Scalar: Ord + BorshSerialize + BorshDeserialize + Serialize + Sync + Send,
{
    fn add(&mut self, other: T::ScalarRef<'_>) -> Result<()> {
        let other = T::to_owned_scalar(other);
        match self.value_map.entry(other) {
            Entry::Occupied(o) => *o.into_mut() += 1,
            Entry::Vacant(v) => {
                v.insert(1);
            }
        };

        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        for (key, value) in rhs.value_map.iter() {
            match self.value_map.get_mut(key) {
                Some(entry) => entry.add_assign(value),
                None => {
                    self.value_map.insert(key.clone(), *value);
                }
            }
        }
        Ok(())
    }

    fn merge_result(
        &mut self,
        builder: &mut StringColumnBuilder,
        function_data: Option<&dyn FunctionData>,
    ) -> Result<()> {
        let histogram_data = unsafe {
            function_data
                .unwrap()
                .as_any()
                .downcast_ref_unchecked::<HistogramData>()
        };

        let buckets = build_histogram(&self.value_map, histogram_data.max_num_buckets);
        let json_str = serde_json::to_string(&buckets)?;
        builder.put_str(&json_str);
        builder.commit_row();

        Ok(())
    }
}

pub fn try_create_aggregate_histogram_function(
    display_name: &str,
    params: Vec<Scalar>,
    arguments: Vec<DataType>,
) -> Result<AggregateFunctionRef> {
    assert_variadic_arguments(display_name, arguments.len(), (1, 2))?;

    let data_type = arguments[0].clone();
    let max_num_buckets = get_max_num_buckets(&params, display_name)?;

    with_number_mapped_type!(|NUM| match &data_type {
        DataType::Number(NumberDataType::NUM) => {
            let func = AggregateUnaryFunction::<
                HistogramState<NumberType<NUM>>,
                NumberType<NUM>,
                StringType,
            >::try_create(display_name, DataType::String, params, data_type)
            .with_function_data(Box::new(HistogramData { max_num_buckets }))
            .with_need_drop(true);
            Ok(Arc::new(func))
        }
        DataType::Decimal(DecimalDataType::Decimal128(_)) => {
            let func = AggregateUnaryFunction::<
                HistogramState<Decimal128Type>,
                Decimal128Type,
                StringType,
            >::try_create(display_name, DataType::String, params, data_type)
            .with_function_data(Box::new(HistogramData { max_num_buckets }))
            .with_need_drop(true);
            Ok(Arc::new(func))
        }
        DataType::Decimal(DecimalDataType::Decimal256(_)) => {
            let func = AggregateUnaryFunction::<
                HistogramState<Decimal256Type>,
                Decimal256Type,
                StringType,
            >::try_create(display_name, DataType::String, params, data_type)
            .with_function_data(Box::new(HistogramData { max_num_buckets }))
            .with_need_drop(true);
            Ok(Arc::new(func))
        }
        DataType::String => {
            let func = AggregateUnaryFunction::<
                HistogramState<StringType>,
                StringType,
                StringType,
            >::try_create(
                display_name, DataType::String, params, data_type
            )
            .with_function_data(Box::new(HistogramData { max_num_buckets }))
            .with_need_drop(true);
            Ok(Arc::new(func))
        }
        DataType::Timestamp => {
            let func = AggregateUnaryFunction::<
                HistogramState<TimestampType>,
                TimestampType,
                StringType,
            >::try_create(display_name, DataType::String, params, data_type)
            .with_function_data(Box::new(HistogramData { max_num_buckets }))
            .with_need_drop(true);
            Ok(Arc::new(func))
        }
        DataType::Date => {
            let func = AggregateUnaryFunction::<
                HistogramState<DateType>,
                DateType,
                StringType,
            >::try_create(display_name, DataType::String, params, data_type)
            .with_function_data(Box::new(HistogramData { max_num_buckets }))
            .with_need_drop(true);
            Ok(Arc::new(func))
        }
        _ => Err(ErrorCode::BadDataValueType(format!(
            "{} does not support type '{:?}'",
            display_name, data_type
        ))),
    })
}

pub fn aggregate_histogram_function_desc() -> AggregateFunctionDescription {
    let features = super::aggregate_function_factory::AggregateFunctionFeatures {
        is_decomposable: false,
        ..Default::default()
    };
    AggregateFunctionDescription::creator_with_features(
        Box::new(try_create_aggregate_histogram_function),
        features,
    )
}

fn get_max_num_buckets(params: &Vec<Scalar>, display_name: &str) -> Result<u64> {
    if params.len() != 1 {
        return Ok(128);
    }
    if let Scalar::Number(number) = params[0] {
        if let Some(number) = number.integer_to_i128() {
            if number > 0 {
                return Ok(number as u64);
            }
        }
    }
    Err(ErrorCode::BadDataValueType(format!(
        "The argument of aggregate function {} must be positive int",
        display_name
    )))
}

// ported from doris: https://github.com/apache/doris/blob/a1114d46e8c3f375325c176b602039987d8dea7b/be/src/vec/utils/histogram_helpers.hpp
#[derive(Serialize, Deserialize)]
struct Bucket<T> {
    lower: T,
    upper: T,
    ndv: u64,
    count: u64,
    pre_sum: u64,
}

/// ported from doris: https://github.com/apache/doris/blob/a1114d46e8c3f375325c176b602039987d8dea7b/be/src/vec/utils/histogram_helpers.hpp
///
/// Checks if it is possible to assign the provided value_map to the given
/// number of buckets such that no bucket has a size larger than max_bucket_size.
///
/// `value_map` A mapping of values to their counts.
/// `max_bucket_size` The maximum size that any bucket is allowed to have.
/// `num_buckets` The number of buckets that we want to assign values to.
///
/// `return` true if the values can be assigned to the buckets, false otherwise.
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

    for (_, count) in value_map.iter() {
        current_bucket_size += count;

        // If adding the current value to the current bucket would exceed max_bucket_size,
        // then we start a new bucket.
        if current_bucket_size > max_bucket_size {
            used_buckets += 1;
            current_bucket_size = *count;
        }

        // If we have used more buckets than num_buckets, we cannot assign the values to buckets.
        if used_buckets > num_buckets {
            return false;
        }
    }

    true
}

/// ported from doris: https://github.com/apache/doris/blob/a1114d46e8c3f375325c176b602039987d8dea7b/be/src/vec/utils/histogram_helpers.hpp
///
/// Calculates the maximum number of values that can fit into each bucket given a set of values
/// and the desired number of buckets.
///
/// `T` the type of the values in the value map
/// `value_map` the map of values and their counts
/// `num_buckets` the desired number of buckets
/// `return` the maximum number of values that can fit into each bucket
fn calculate_bucket_max_values<T: Ord>(value_map: &BTreeMap<T, u64>, num_buckets: u64) -> u64 {
    // Assume that the value map is not empty
    debug_assert!(!value_map.is_empty());

    // Calculate the total number of values in the map using std::accumulate()
    let total_values = value_map.values().sum();

    // If there is only one bucket, then all values will be assigned to that bucket
    if num_buckets == 1 {
        return total_values;
    }

    // To calculate the maximum value count in each bucket, we first calculate a conservative upper
    // bound, which is equal to 2 * total_values / (max_buckets - 1) + 1. This upper bound may exceed
    // the actual maximum value count, but it does not underestimate it. The subsequent binary search
    // algorithm will approach the actual maximum value count.
    let mut upper_bucket_values = 2 * total_values / (num_buckets - 1) + 1;

    // Initialize the lower bound to 0
    let mut lower_bucket_values = 0;

    // Perform a binary search to find the maximum number of values that can fit into each bucket
    let mut search_step = 0;
    let max_search_steps = 10; // Limit the number of search steps to avoid excessive iteration

    while upper_bucket_values > lower_bucket_values + 1 && search_step < max_search_steps {
        // Calculate the midpoint of the upper and lower bounds
        let bucket_values = (upper_bucket_values + lower_bucket_values) / 2;

        // Check if the given number of values can be assigned to the desired number of buckets
        if can_assign_into_buckets(value_map, bucket_values, num_buckets) {
            // If it can, then set the upper bound to the midpoint
            upper_bucket_values = bucket_values;
        } else {
            // If it can't, then set the lower bound to the midpoint
            lower_bucket_values = bucket_values;
        }
        // Increment the search step counter
        search_step += 1;
    }

    upper_bucket_values
}

/// ported from doris: https://github.com/apache/doris/blob/a1114d46e8c3f375325c176b602039987d8dea7b/be/src/vec/utils/histogram_helpers.hpp
///
/// Greedy equi-height histogram construction algorithm, inspired by the MySQL
/// equi_height implementation(https://dev.mysql.com/doc/dev/mysql-server/latest/equi__height_8h.html).
///
/// Given an ordered collection of [value, count] pairs and a maximum bucket
/// size, construct a histogram by inserting values into a bucket while keeping
/// track of its size. If the insertion of a value into a non-empty bucket
/// causes the bucket to exceed the maximum size, create a new empty bucket and
/// continue.
///
/// The algorithm guarantees a selectivity estimation error of at most ~2 *
/// #values / #buckets, often less. Values with a higher relative frequency are
/// guaranteed to be placed in singleton buckets.
///
/// The minimum composite bucket size is used to minimize the worst case
/// selectivity estimation error. In general, the algorithm will adapt to the
/// data distribution to minimize the size of composite buckets. The heavy values
/// can be placed in singleton buckets and the remaining values will be evenly
/// spread across the remaining buckets, leading to a lower composite bucket size.
///
/// Note: The term "value" refers to an entry in a column and the actual value
/// of an entry. The value_map is an ordered collection of [distinct value,
/// value count] pairs. For example, a Value_map<String> could contain the pairs ["a", 1], ["b", 2]
/// to represent one "a" value and two "b" values.
///
/// `value_map` An ordered map of distinct values and their counts.
/// `max_num_buckets` The maximum number of buckets that can be used.
///
/// `return` the histogram buckets.
fn build_histogram<T: Ord + Clone>(
    value_map: &BTreeMap<T, u64>,
    max_num_buckets: u64,
) -> Vec<Bucket<T>> {
    let mut buckets = Vec::new();

    // If the input map is empty, there is nothing to build.
    if value_map.is_empty() {
        return buckets;
    }

    // Calculate the maximum number of values that can be assigned to each bucket.
    let bucket_max_values = calculate_bucket_max_values(value_map, max_num_buckets);

    // Ensure that the capacity is at least max_num_buckets in order to avoid the overhead of additional
    // allocations when inserting buckets.
    buckets.reserve(max_num_buckets as usize);

    // Initialize bucket variables.
    let mut distinct_values_count = 0;
    let mut values_count = 0;
    let mut cumulative_values = 0;

    // Record how many values still need to be assigned.
    let mut remaining_distinct_values = value_map.len();

    let mut iter = value_map.iter().peekable();

    // Lower value of the current bucket.
    // It is guaranteed that value_map is not empty.
    let mut lower_value = iter.peek().unwrap().0;

    // Iterate over the ordered map of distinct values and their counts.
    while let Some(curr) = iter.next() {
        let count = *curr.1;
        let current_value = curr.0;

        // Update the bucket counts and track the number of distinct values assigned.
        distinct_values_count += 1;
        remaining_distinct_values -= 1;
        values_count += count;
        cumulative_values += count;

        // Check whether the current value should be added to the current bucket.
        let next = iter.peek();
        let remaining_empty_buckets = max_num_buckets - buckets.len() as u64 - 1;

        if let Some(next) = next {
            if remaining_distinct_values as u64 > remaining_empty_buckets
                && values_count + *next.1 <= bucket_max_values
            {
                // If the current value is the last in the input map and there are more remaining
                // distinct values than empty buckets and adding the value does not cause the bucket
                // to exceed its max size, skip adding the value to the current bucket.
                continue;
            }
        }

        // Finalize the current bucket and add it to our collection of buckets.
        let pre_sum = cumulative_values - values_count;

        // TODO: move lower_value
        buckets.push(Bucket {
            lower: lower_value.clone(),
            upper: current_value.clone(),
            ndv: distinct_values_count,
            count: values_count,
            pre_sum,
        });

        // Reset variables for the next bucket.
        if let Some(next) = next {
            lower_value = next.0;
        }
        values_count = 0;
        distinct_values_count = 0;
    }

    buckets
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::build_histogram;

    fn create_test_map<T: Ord>(mut values: Vec<T>, mut counts: Vec<u64>) -> BTreeMap<T, u64> {
        std::iter::zip(values.drain(..), counts.drain(..)).collect()
    }

    // Test case 1: Test when input map is empty.
    #[test]
    fn test_empty_map() {
        let max_num_buckets = 10;
        let value_map = create_test_map(vec![], vec![]);
        let buckets = build_histogram::<u32>(&value_map, max_num_buckets);

        assert!(buckets.is_empty());
    }

    // Test case 2: Test when input map has only one element.
    #[test]
    fn test_single_element() {
        let max_num_buckets = 10;
        let value_map = create_test_map(vec![1], vec![5]);
        let buckets = build_histogram::<u32>(&value_map, max_num_buckets);

        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].lower, 1);
        assert_eq!(buckets[0].upper, 1);
        assert_eq!(buckets[0].ndv, 1);
        assert_eq!(buckets[0].count, 5);
        assert_eq!(buckets[0].pre_sum, 0);
    }

    // Test case 3: Test when num_buckets >= number of distinct elements in input map.
    #[test]
    fn test_enough_buckets() {
        let value_map = create_test_map(vec![1, 2, 3], vec![10, 20, 30]);
        let max_num_buckets = value_map.len() as u64;
        let buckets = build_histogram::<u32>(&value_map, max_num_buckets);

        assert_eq!(buckets.len(), 3);

        assert_eq!(buckets[0].lower, 1);
        assert_eq!(buckets[0].upper, 1);
        assert_eq!(buckets[0].ndv, 1);
        assert_eq!(buckets[0].count, 10);
        assert_eq!(buckets[0].pre_sum, 0);

        assert_eq!(buckets[1].lower, 2);
        assert_eq!(buckets[1].upper, 2);
        assert_eq!(buckets[1].ndv, 1);
        assert_eq!(buckets[1].count, 20);
        assert_eq!(buckets[1].pre_sum, 10);

        assert_eq!(buckets[2].lower, 3);
        assert_eq!(buckets[2].upper, 3);
        assert_eq!(buckets[2].ndv, 1);
        assert_eq!(buckets[2].count, 30);
        assert_eq!(buckets[2].pre_sum, 30);
    }

    // Test case 4: Test when num_buckets < number of distinct elements in input map.
    #[test]
    fn test_not_enough_buckets() {
        let value_map = create_test_map(vec![1, 2, 3, 4], vec![5, 10, 20, 30]);
        let max_num_buckets = 2;
        let buckets = build_histogram::<u32>(&value_map, max_num_buckets);

        assert_eq!(buckets.len(), 2);

        assert_eq!(buckets[0].lower, 1);
        assert_eq!(buckets[0].upper, 3);
        assert_eq!(buckets[0].ndv, 3);
        assert_eq!(buckets[0].count, 35);
        assert_eq!(buckets[0].pre_sum, 0);

        assert_eq!(buckets[1].lower, 4);
        assert_eq!(buckets[1].upper, 4);
        assert_eq!(buckets[1].ndv, 1);
        assert_eq!(buckets[1].count, 30);
        assert_eq!(buckets[1].pre_sum, 35);
    }

    // Test case 5: Test when the sum of counts of two adjacent elements is larger than a single bucket's capacity.
    #[test]
    fn test_two_adjacent_values_larger_than_capacity() {
        let value_map = create_test_map(vec![1, 2, 3, 4], vec![20, 30, 40, 50]);
        let max_num_buckets = 3;
        let buckets = build_histogram::<u32>(&value_map, max_num_buckets);

        assert_eq!(buckets.len(), 3);

        assert_eq!(buckets[0].lower, 1);
        assert_eq!(buckets[0].upper, 2);
        assert_eq!(buckets[0].ndv, 2);
        assert_eq!(buckets[0].count, 50);
        assert_eq!(buckets[0].pre_sum, 0);

        assert_eq!(buckets[1].lower, 3);
        assert_eq!(buckets[1].upper, 3);
        assert_eq!(buckets[1].ndv, 1);
        assert_eq!(buckets[1].count, 40);
        assert_eq!(buckets[1].pre_sum, 50);

        assert_eq!(buckets[2].lower, 4);
        assert_eq!(buckets[2].upper, 4);
        assert_eq!(buckets[2].ndv, 1);
        assert_eq!(buckets[2].count, 50);
        assert_eq!(buckets[2].pre_sum, 90);
    }

    // Test case 6: Test when the sum of counts of all elements is smaller than a single bucket's capacity.
    #[test]
    fn test_all_values_in_one_bucket() {
        let value_map = create_test_map(vec![1, 2, 3], vec![5, 10, 15]);
        let max_num_buckets = 1;
        let buckets = build_histogram::<u32>(&value_map, max_num_buckets);

        assert_eq!(buckets.len(), 1);

        assert_eq!(buckets[0].lower, 1);
        assert_eq!(buckets[0].upper, 3);
        assert_eq!(buckets[0].ndv, 3);
        assert_eq!(buckets[0].count, 30);
        assert_eq!(buckets[0].pre_sum, 0);
    }

    // Test case 7: Test when the sum of counts of all elements is larger than the total capacity of all buckets.
    #[test]
    fn test_all_values_in_multiple_buckets() {
        let value_map = create_test_map(vec![1, 2, 3, 4, 5, 6], vec![100, 200, 300, 400, 500, 600]);
        let max_num_buckets = 4;
        let buckets = build_histogram::<u32>(&value_map, max_num_buckets);

        assert_eq!(buckets.len(), 4);

        assert_eq!(buckets[0].lower, 1);
        assert_eq!(buckets[0].upper, 3);
        assert_eq!(buckets[0].ndv, 3);
        assert_eq!(buckets[0].count, 600);
        assert_eq!(buckets[0].pre_sum, 0);

        assert_eq!(buckets[1].lower, 4);
        assert_eq!(buckets[1].upper, 4);
        assert_eq!(buckets[1].ndv, 1);
        assert_eq!(buckets[1].count, 400);
        assert_eq!(buckets[1].pre_sum, 600);

        assert_eq!(buckets[2].lower, 5);
        assert_eq!(buckets[2].upper, 5);
        assert_eq!(buckets[2].ndv, 1);
        assert_eq!(buckets[2].count, 500);
        assert_eq!(buckets[2].pre_sum, 1000);

        assert_eq!(buckets[3].lower, 6);
        assert_eq!(buckets[3].upper, 6);
        assert_eq!(buckets[3].ndv, 1);
        assert_eq!(buckets[3].count, 600);
        assert_eq!(buckets[3].pre_sum, 1500);

        let mut pre_sum = vec![0_u64; max_num_buckets as usize + 1];
        for (i, b) in buckets.iter().enumerate() {
            pre_sum[i + 1] = pre_sum[i] + b.count;
        }
        for (i, b) in buckets.iter().enumerate() {
            assert_eq!(b.pre_sum, pre_sum[i]);
        }
    }
}
