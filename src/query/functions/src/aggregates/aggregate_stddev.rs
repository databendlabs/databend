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
use crate::aggregates::aggregator_common::assert_unary_arguments;
use crate::aggregates::AggregateFunction;
use crate::aggregates::AggregateFunctionRef;

const POP: u8 = 0;
const SAMP: u8 = 1;

#[derive(Serialize, Deserialize)]
struct AggregateStddevState {
    pub sum: f64,
    pub count: u64,
    pub variance: f64,
}

impl AggregateStddevState {
    #[inline(always)]
    fn add(&mut self, value: f64) {
        self.sum += value;
        self.count += 1;
        if self.count > 1 {
            let t = self.count as f64 * value - self.sum;
            self.variance += (t * t) / (self.count * (self.count - 1)) as f64;
        }
    }

    #[inline(always)]
    fn merge(&mut self, other: &Self) {
        if other.count == 0 {
            return;
        }
        if self.count == 0 {
            self.count = other.count;
            self.sum = other.sum;
            self.variance = other.variance;
            return;
        }

        let t = (other.count as f64 / self.count as f64) * self.sum - other.sum;
        self.variance += other.variance
            + ((self.count as f64 / other.count as f64) / (self.count as f64 + other.count as f64))
                * t
                * t;
        self.count += other.count;
        self.sum += other.sum;
    }
}

#[derive(Clone)]
pub struct AggregateStddevFunction<T, const TYPE: u8> {
    display_name: String,
    _arguments: Vec<DataType>,
    t: PhantomData<T>,
}

impl<T, const TYPE: u8> AggregateFunction for AggregateStddevFunction<T, TYPE>
where T: Number + AsPrimitive<f64>
{
    fn name(&self) -> &str {
        "AggregateStddevPopFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(DataType::Number(NumberDataType::Float64))
    }

    fn init_state(&self, place: StateAddr) {
        place.write(|| AggregateStddevState {
            sum: 0.0,
            count: 0,
            variance: 0.0,
        });
    }

    fn state_layout(&self) -> Layout {
        Layout::new::<AggregateStddevState>()
    }

    fn accumulate(
        &self,
        place: StateAddr,
        columns: &[Column],
        validity: Option<&Bitmap>,
        _input_rows: usize,
    ) -> Result<()> {
        let state = place.get::<AggregateStddevState>();
        let column = NumberType::<T>::try_downcast_column(&columns[0]).unwrap();
        match validity {
            Some(bitmap) => {
                for (value, is_valid) in column.iter().zip(bitmap.iter()) {
                    if is_valid {
                        state.add(value.as_());
                    }
                }
            }
            None => {
                for value in column.iter() {
                    state.add(value.as_());
                }
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
        let column = NumberType::<T>::try_downcast_column(&columns[0]).unwrap();

        column.iter().zip(places.iter()).for_each(|(value, place)| {
            let place = place.next(offset);
            let state = place.get::<AggregateStddevState>();
            let v: f64 = value.as_();
            state.add(v);
        });
        Ok(())
    }

    fn accumulate_row(&self, place: StateAddr, columns: &[Column], row: usize) -> Result<()> {
        let column = NumberType::<T>::try_downcast_column(&columns[0]).unwrap();

        let state = place.get::<AggregateStddevState>();
        let v: f64 = column[row].as_();
        state.add(v);
        Ok(())
    }

    fn serialize(&self, place: StateAddr, writer: &mut Vec<u8>) -> Result<()> {
        let state = place.get::<AggregateStddevState>();
        serialize_state(writer, state)
    }

    fn merge(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state = place.get::<AggregateStddevState>();
        let rhs: AggregateStddevState = deserialize_state(reader)?;
        state.merge(&rhs);
        Ok(())
    }

    fn merge_states(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let state = place.get::<AggregateStddevState>();
        let other = rhs.get::<AggregateStddevState>();
        state.merge(other);
        Ok(())
    }

    #[allow(unused_mut)]
    fn merge_result(&self, place: StateAddr, builder: &mut ColumnBuilder) -> Result<()> {
        let state = place.get::<AggregateStddevState>();
        let builder = NumberType::<F64>::try_downcast_builder(builder).unwrap();
        let variance = state.variance / (state.count - TYPE as u64) as f64;
        builder.push(variance.sqrt().into());
        Ok(())
    }
}

impl<T, const TYPE: u8> fmt::Display for AggregateStddevFunction<T, TYPE> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl<T, const TYPE: u8> AggregateStddevFunction<T, TYPE>
where T: Number + AsPrimitive<f64>
{
    pub fn try_create(
        display_name: &str,
        arguments: Vec<DataType>,
    ) -> Result<AggregateFunctionRef> {
        Ok(Arc::new(Self {
            display_name: display_name.to_string(),
            _arguments: arguments,
            t: PhantomData,
        }))
    }
}

pub fn try_create_aggregate_stddev_pop_function<const TYPE: u8>(
    display_name: &str,
    _params: Vec<Scalar>,
    arguments: Vec<DataType>,
) -> Result<Arc<dyn AggregateFunction>> {
    assert_unary_arguments(display_name, arguments.len())?;
    with_number_mapped_type!(|NUM_TYPE| match &arguments[0] {
        DataType::Number(NumberDataType::NUM_TYPE) => {
            AggregateStddevFunction::<NUM_TYPE, TYPE>::try_create(display_name, arguments)
        }
        _ => Err(ErrorCode::BadDataValueType(format!(
            "AggregateStddevPopFunction does not support type '{:?}'",
            arguments[0]
        ))),
    })
}

pub fn aggregate_stddev_pop_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(try_create_aggregate_stddev_pop_function::<POP>))
}

pub fn aggregate_stddev_samp_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(
        try_create_aggregate_stddev_pop_function::<SAMP>,
    ))
}
