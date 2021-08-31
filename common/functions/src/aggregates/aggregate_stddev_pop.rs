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

use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::*;
use num::cast::AsPrimitive;

use super::StateAddr;
use crate::aggregates::aggregator_common::assert_unary_arguments;
use crate::aggregates::AggregateFunction;
use crate::aggregates::AggregateFunctionRef;
use crate::dispatch_numeric_types;

struct AggregateStddevPopState {
    pub sum: f64,
    pub count: u64,
    pub variance: f64,
}

impl AggregateStddevPopState {
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
pub struct AggregateStddevPopFunction<T> {
    display_name: String,
    arguments: Vec<DataField>,
    t: PhantomData<T>,
}

impl<T> AggregateFunction for AggregateStddevPopFunction<T>
where T: DFPrimitiveType + AsPrimitive<f64>
{
    fn name(&self) -> &str {
        "AggregateStddevPopFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn init_state(&self, place: StateAddr) {
        place.write(|| AggregateStddevPopState {
            sum: 0.0,
            count: 0,
            variance: 0.0,
        });
    }

    fn state_layout(&self) -> Layout {
        Layout::new::<AggregateStddevPopState>()
    }

    fn accumulate(&self, place: StateAddr, arrays: &[Series], _input_rows: usize) -> Result<()> {
        let state = place.get::<AggregateStddevPopState>();
        let array: &DFPrimitiveArray<T> = arrays[0].static_cast();
        for (_row, value) in array.into_no_null_iter().enumerate() {
            let v: f64 = value.as_();
            state.add(v);
        }
        Ok(())
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        offset: usize,
        arrays: &[Series],
        _input_rows: usize,
    ) -> Result<()> {
        let array: &DFPrimitiveArray<T> = arrays[0].static_cast();
        array
            .into_no_null_iter()
            .zip(places.iter())
            .for_each(|(value, place)| {
                let place = place.next(offset);
                let state = place.get::<AggregateStddevPopState>();

                let v: f64 = value.as_();
                state.add(v);
            });
        Ok(())
    }

    fn serialize(&self, place: StateAddr, writer: &mut BytesMut) -> Result<()> {
        let state = place.get::<AggregateStddevPopState>();
        state.sum.serialize_to_buf(writer)?;
        state.count.serialize_to_buf(writer)?;
        state.variance.serialize_to_buf(writer)
    }

    fn deserialize(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state = place.get::<AggregateStddevPopState>();
        state.sum = f64::deserialize(reader)?;
        state.count = u64::deserialize(reader)?;
        state.variance = f64::deserialize(reader)?;
        Ok(())
    }

    fn merge(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let state = place.get::<AggregateStddevPopState>();
        let rhs = rhs.get::<AggregateStddevPopState>();
        state.merge(rhs);
        Ok(())
    }

    fn merge_result(&self, place: StateAddr) -> Result<DataValue> {
        let state = place.get::<AggregateStddevPopState>();
        if state.count == 0 {
            return Ok(DataValue::Float64(None));
        }
        let variance = state.variance / state.count as f64;
        Ok(DataValue::Float64(Some(variance.sqrt())))
    }
}

impl<T> fmt::Display for AggregateStddevPopFunction<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl<T> AggregateStddevPopFunction<T>
where T: DFPrimitiveType + AsPrimitive<f64>
{
    pub fn try_create(
        display_name: &str,
        arguments: Vec<DataField>,
    ) -> Result<AggregateFunctionRef> {
        Ok(Arc::new(Self {
            display_name: display_name.to_string(),
            arguments,
            t: PhantomData,
        }))
    }
}

macro_rules! creator {
    ($T: ident, $data_type: expr, $display_name: expr, $arguments: expr) => {
        if $T::data_type() == $data_type {
            return AggregateStddevPopFunction::<$T>::try_create($display_name, $arguments);
        }
    };
}

pub fn try_create_aggregate_stddev_pop_function(
    display_name: &str,
    _params: Vec<DataValue>,
    arguments: Vec<DataField>,
) -> Result<Arc<dyn AggregateFunction>> {
    assert_unary_arguments(display_name, arguments.len())?;

    let data_type = arguments[0].data_type();
    dispatch_numeric_types! {creator, data_type.clone(), display_name, arguments}

    Err(ErrorCode::BadDataValueType(format!(
        "AggregateStddevPopFunction does not support type '{:?}'",
        data_type
    )))
}
