// Copyright 2021 Datafuse Labs.
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
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::*;
use num::cast::AsPrimitive;
use serde::Deserialize;
use serde::Serialize;

use super::StateAddr;
use crate::aggregates::aggregate_function_factory::AggregateFunctionDescription;
use crate::aggregates::aggregator_common::assert_binary_arguments;
use crate::aggregates::AggregateFunction;
use crate::aggregates::AggregateFunctionRef;
use crate::with_match_primitive_type_ids;

#[derive(Serialize, Deserialize)]
pub struct AggregateCovarianceState {
    pub count: u64,
    pub co_moments: f64,
    pub left_mean: f64,
    pub right_mean: f64,
}

/*
 * Source: "Numerically Stable, Single-Pass, Parallel Statistics Algorithms"
 * (J. Bennett et al., Sandia National Laboratories,
 * 2009 IEEE International Conference on Cluster Computing)
 * Paper link: https://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.214.8508&rep=rep1&type=pdf
 */
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
    _arguments: Vec<DataField>,
    t0: PhantomData<T0>,
    t1: PhantomData<T1>,
    r: PhantomData<R>,
}

impl<T0, T1, R> AggregateFunction for AggregateCovarianceFunction<T0, T1, R>
where
    T0: PrimitiveType + AsPrimitive<f64>,
    T1: PrimitiveType + AsPrimitive<f64>,
    R: AggregateCovariance,
{
    fn name(&self) -> &str {
        R::name()
    }

    fn return_type(&self) -> Result<DataTypePtr> {
        Ok(f64::to_data_type())
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
        columns: &[common_datavalues::ColumnRef],
        validity: Option<&Bitmap>,
        _input_rows: usize,
    ) -> Result<()> {
        let state = place.get::<AggregateCovarianceState>();
        let left: &PrimitiveColumn<T0> = unsafe { Series::static_cast(&columns[0]) };
        let right: &PrimitiveColumn<T1> = unsafe { Series::static_cast(&columns[1]) };

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
        columns: &[ColumnRef],
        _input_rows: usize,
    ) -> Result<()> {
        let left: &PrimitiveColumn<T0> = unsafe { Series::static_cast(&columns[0]) };
        let right: &PrimitiveColumn<T1> = unsafe { Series::static_cast(&columns[1]) };

        left.iter().zip(right.iter()).zip(places.iter()).for_each(
            |((left_val, right_val), place)| {
                let place = place.next(offset);
                let state = place.get::<AggregateCovarianceState>();
                state.add(left_val.as_(), right_val.as_());
            },
        );
        Ok(())
    }

    fn accumulate_row(&self, place: StateAddr, columns: &[ColumnRef], row: usize) -> Result<()> {
        let left: &PrimitiveColumn<T0> = unsafe { Series::static_cast(&columns[0]) };
        let right: &PrimitiveColumn<T1> = unsafe { Series::static_cast(&columns[1]) };

        let left_val = unsafe { left.value_unchecked(row) };
        let right_val = unsafe { right.value_unchecked(row) };

        let state = place.get::<AggregateCovarianceState>();
        state.add(left_val.as_(), right_val.as_());
        Ok(())
    }

    fn serialize(&self, place: StateAddr, writer: &mut BytesMut) -> Result<()> {
        let state = place.get::<AggregateCovarianceState>();
        serialize_into_buf(writer, state)
    }

    fn deserialize(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state = place.get::<AggregateCovarianceState>();
        *state = deserialize_from_slice(reader)?;

        Ok(())
    }

    fn merge(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let state = place.get::<AggregateCovarianceState>();
        let rhs = rhs.get::<AggregateCovarianceState>();
        state.merge(rhs);
        Ok(())
    }

    #[allow(unused_mut)]
    fn merge_result(&self, place: StateAddr, column: &mut dyn MutableColumn) -> Result<()> {
        let state = place.get::<AggregateCovarianceState>();
        let column: &mut MutablePrimitiveColumn<f64> = Series::check_get_mutable_column(column)?;
        column.append_value(R::apply(state));
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
    T0: PrimitiveType + AsPrimitive<f64>,
    T1: PrimitiveType + AsPrimitive<f64>,
    R: AggregateCovariance,
{
    pub fn try_create(
        display_name: &str,
        arguments: Vec<DataField>,
    ) -> Result<AggregateFunctionRef> {
        Ok(Arc::new(Self {
            display_name: display_name.to_string(),
            _arguments: arguments,
            t0: PhantomData,
            t1: PhantomData,
            r: PhantomData,
        }))
    }
}

pub fn try_create_aggregate_covariance<R: AggregateCovariance>(
    display_name: &str,
    _params: Vec<DataValue>,
    arguments: Vec<DataField>,
) -> Result<Arc<dyn AggregateFunction>> {
    assert_binary_arguments(display_name, arguments.len())?;

    let data_type0 = arguments[0].data_type();
    let data_type1 = arguments[1].data_type();

    with_match_primitive_type_ids!(data_type0.data_type_id(), data_type1.data_type_id(), |$T0, $T1| {
        AggregateCovarianceFunction::<$T0, $T1, R>::try_create(display_name, arguments)
    },
    {
        Err(ErrorCode::BadDataValueType(format!(
            "AggregateCovarianceFunction does not support type '{:?}' or '{:?}'",
            data_type0, data_type1
        )))
    })
}

pub trait AggregateCovariance: Send + Sync + 'static {
    fn name() -> &'static str;

    fn apply(state: &AggregateCovarianceState) -> f64;
}

///////////////////////////////////////////////////////////////////////////////
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

///////////////////////////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////////////////////////
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

///////////////////////////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////////////////////////
// TODO: correlation function
//struct AggregateCorrelationImpl;
///////////////////////////////////////////////////////////////////////////////
