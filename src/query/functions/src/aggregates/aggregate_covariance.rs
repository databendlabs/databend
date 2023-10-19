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
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use common_arrow::arrow::bitmap::Bitmap;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::number::Number;
use common_expression::types::number::F64;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::types::NumberType;
use common_expression::types::ValueType;
use common_expression::with_number_mapped_type;
use common_expression::Column;
use common_expression::ColumnBuilder;
use common_expression::Scalar;
use num_traits::AsPrimitive;
use serde::Deserialize;
use serde::Serialize;

use super::deserialize_state;
use super::serialize_state;
use super::StateAddr;
use crate::aggregates::aggregate_function_factory::AggregateFunctionDescription;
use crate::aggregates::aggregator_common::assert_binary_arguments;
use crate::aggregates::AggregateFunction;
use crate::aggregates::AggregateFunctionRef;

#[derive(Serialize, Deserialize)]
pub struct AggregateCovarianceState {
    pub count: u64,
    pub co_moments: f64,
    pub left_mean: f64,
    pub right_mean: f64,
}

// Source: "Numerically Stable, Single-Pass, Parallel Statistics Algorithms"
// (J. Bennett et al., Sandia National Laboratories,
// 2009 IEEE International Conference on Cluster Computing)
// Paper link: https://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.214.8508&rep=rep1&type=pdf
impl AggregateCovarianceState {
    // The idea is from the formula III.9 in the paper. The original formula is:
    //     co-moments = co-moments + (n-1)/n * (s-left_mean)  * (t-right_mean)
    // Since (n-1)/n may introduce some bias when n >> 1, Clickhouse transform the formula into:
    //     co-moments = co-moments + (s-new_left_mean) * (t-right_mean).
    // This equals the formula in the paper. Here we take the same approach as Clickhouse
    // does. Thanks Clickhouse!
    #[inline(always)]
    fn add(&mut self, s: f64, t: f64) {
        let left_delta = s - self.left_mean;
        let right_delta = t - self.right_mean;

        self.count += 1;
        let new_left_mean = self.left_mean + left_delta / self.count as f64;
        let new_right_mean = self.right_mean + right_delta / self.count as f64;

        self.co_moments += (s - new_left_mean) * (t - self.right_mean);
        self.left_mean = new_left_mean;
        self.right_mean = new_right_mean;
    }

    // The idea is from the formula III.6 in the paper:
    //     co-moments = co-moments + (n1*n2)/(n1+n2) * delta_left_mean * delta_right_mean
    // Clickhouse also has some optimization when two data sets are large and comparable in size.
    // Here we take the same approach as Clickhouse does. Thanks Clickhouse!
    #[inline(always)]
    fn merge(&mut self, other: &Self) {
        let total = self.count + other.count;
        if total == 0 {
            return;
        }

        let factor = self.count as f64 * other.count as f64 / total as f64; // 'n1*n2/n' in the paper
        let left_delta = self.left_mean - other.left_mean;
        let right_delta = self.right_mean - other.right_mean;

        self.co_moments += other.co_moments + left_delta * right_delta * factor;

        if large_and_comparable(self.count, other.count) {
            self.left_mean = (self.left_sum() + other.left_sum()) / total as f64;
            self.right_mean = (self.right_sum() + other.right_sum()) / total as f64;
        } else {
            self.left_mean = other.left_mean + left_delta * self.count as f64 / total as f64;
            self.right_mean = other.right_mean + right_delta * self.count as f64 / total as f64;
        }

        self.count = total;
    }

    #[inline(always)]
    fn left_sum(&self) -> f64 {
        self.count as f64 * self.left_mean
    }

    #[inline(always)]
    fn right_sum(&self) -> f64 {
        self.count as f64 * self.right_mean
    }
}

fn large_and_comparable(a: u64, b: u64) -> bool {
    if a == 0 || b == 0 {
        return false;
    }

    let sensitivity = 0.001_f64;
    let threshold = 10000_f64;

    let min = a.min(b) as f64;
    let max = a.max(b) as f64;
    (1.0 - min / max) < sensitivity && min > threshold
}

#[derive(Clone)]
pub struct AggregateCovarianceFunction<T0, T1, R> {
    display_name: String,
    _t0: PhantomData<T0>,
    _t1: PhantomData<T1>,
    _r: PhantomData<R>,
}

impl<T0, T1, R> AggregateFunction for AggregateCovarianceFunction<T0, T1, R>
where
    T0: Number + AsPrimitive<f64>,
    T1: Number + AsPrimitive<f64>,
    R: AggregateCovariance,
{
    fn name(&self) -> &str {
        R::name()
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(DataType::Number(NumberDataType::Float64))
    }

    fn init_state(&self, place: StateAddr) {
        place.write(|| AggregateCovarianceState {
            count: 0,
            left_mean: 0.0,
            right_mean: 0.0,
            co_moments: 0.0,
        });
    }

    fn state_layout(&self) -> Layout {
        Layout::new::<AggregateCovarianceState>()
    }

    fn accumulate(
        &self,
        place: StateAddr,
        columns: &[Column],
        validity: Option<&Bitmap>,
        _input_rows: usize,
    ) -> Result<()> {
        let state = place.get::<AggregateCovarianceState>();
        let left = NumberType::<T0>::try_downcast_column(&columns[0]).unwrap();
        let right = NumberType::<T1>::try_downcast_column(&columns[1]).unwrap();

        match validity {
            Some(bitmap) => {
                left.iter().zip(right.iter()).zip(bitmap.iter()).for_each(
                    |((left_val, right_val), valid)| {
                        if valid {
                            state.add(left_val.as_(), right_val.as_());
                        }
                    },
                );
            }
            None => {
                left.iter()
                    .zip(right.iter())
                    .for_each(|(left_val, right_val)| {
                        state.add(left_val.as_(), right_val.as_());
                    });
            }
        }
        Ok(())
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        offset: usize,
        columns: &[Column],
        _input_rows: usize,
    ) -> Result<()> {
        let left = NumberType::<T0>::try_downcast_column(&columns[0]).unwrap();
        let right = NumberType::<T1>::try_downcast_column(&columns[1]).unwrap();

        left.iter().zip(right.iter()).zip(places.iter()).for_each(
            |((left_val, right_val), place)| {
                let place = place.next(offset);
                let state = place.get::<AggregateCovarianceState>();
                state.add(left_val.as_(), right_val.as_());
            },
        );
        Ok(())
    }

    fn accumulate_row(&self, place: StateAddr, columns: &[Column], row: usize) -> Result<()> {
        let left = NumberType::<T0>::try_downcast_column(&columns[0]).unwrap();
        let right = NumberType::<T1>::try_downcast_column(&columns[1]).unwrap();

        let left_val = unsafe { left.get_unchecked(row) };
        let right_val = unsafe { right.get_unchecked(row) };

        let state = place.get::<AggregateCovarianceState>();
        state.add(left_val.as_(), right_val.as_());
        Ok(())
    }

    fn serialize(&self, place: StateAddr, writer: &mut Vec<u8>) -> Result<()> {
        let state = place.get::<AggregateCovarianceState>();
        serialize_state(writer, state)
    }

    fn merge(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state = place.get::<AggregateCovarianceState>();
        let rhs: AggregateCovarianceState = deserialize_state(reader)?;
        state.merge(&rhs);
        Ok(())
    }

    fn merge_states(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let state = place.get::<AggregateCovarianceState>();
        let other = rhs.get::<AggregateCovarianceState>();
        state.merge(other);
        Ok(())
    }

    #[allow(unused_mut)]
    fn merge_result(&self, place: StateAddr, builder: &mut ColumnBuilder) -> Result<()> {
        let state = place.get::<AggregateCovarianceState>();
        let builder = NumberType::<F64>::try_downcast_builder(builder).unwrap();
        builder.push(R::apply(state).into());
        Ok(())
    }
}

impl<T0, T1, R> fmt::Display for AggregateCovarianceFunction<T0, T1, R> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl<T0, T1, R> AggregateCovarianceFunction<T0, T1, R>
where
    T0: Number + AsPrimitive<f64>,
    T1: Number + AsPrimitive<f64>,
    R: AggregateCovariance,
{
    pub fn try_create(
        display_name: &str,
        _arguments: Vec<DataType>,
    ) -> Result<AggregateFunctionRef> {
        Ok(Arc::new(Self {
            display_name: display_name.to_string(),
            _t0: PhantomData,
            _t1: PhantomData,
            _r: PhantomData,
        }))
    }
}

pub fn try_create_aggregate_covariance<R: AggregateCovariance>(
    display_name: &str,
    _params: Vec<Scalar>,
    arguments: Vec<DataType>,
) -> Result<AggregateFunctionRef> {
    assert_binary_arguments(display_name, arguments.len())?;

    with_number_mapped_type!(|NUM_TYPE0| match &arguments[0] {
        DataType::Number(NumberDataType::NUM_TYPE0) =>
            with_number_mapped_type!(|NUM_TYPE1| match &arguments[1] {
                DataType::Number(NumberDataType::NUM_TYPE1) => {
                    return AggregateCovarianceFunction::<NUM_TYPE0, NUM_TYPE1, R>::try_create(
                        display_name,
                        arguments,
                    );
                }
                _ => (),
            }),
        _ => (),
    });

    Err(ErrorCode::BadDataValueType(format!(
        "Expected number data type, but got {:?}",
        arguments
    )))
}

pub trait AggregateCovariance: Send + Sync + 'static {
    fn name() -> &'static str;

    fn apply(state: &AggregateCovarianceState) -> f64;
}

// Sample covariance function implementation
struct AggregateCovarianceSampleImpl;

impl AggregateCovariance for AggregateCovarianceSampleImpl {
    fn name() -> &'static str {
        "AggregateCovarianceSampleFunction"
    }

    fn apply(state: &AggregateCovarianceState) -> f64 {
        if state.count < 2 {
            f64::INFINITY
        } else {
            state.co_moments / (state.count - 1) as f64
        }
    }
}

pub fn aggregate_covariance_sample_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(
        try_create_aggregate_covariance::<AggregateCovarianceSampleImpl>,
    ))
}

// Population covariance function implementation
struct AggregateCovariancePopulationImpl;

impl AggregateCovariance for AggregateCovariancePopulationImpl {
    fn name() -> &'static str {
        "AggregateCovariancePopulationFunction"
    }

    fn apply(state: &AggregateCovarianceState) -> f64 {
        if state.count == 0 {
            f64::INFINITY
        } else if state.count == 1 {
            0.0
        } else {
            state.co_moments / state.count as f64
        }
    }
}

pub fn aggregate_covariance_population_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(
        try_create_aggregate_covariance::<AggregateCovariancePopulationImpl>,
    ))
}
