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
use common_datavalues::with_match_primitive_type_id;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::*;
use num::cast::AsPrimitive;
use num::NumCast;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;

use super::aggregate_sum::sum_primitive;
use super::StateAddr;
use crate::aggregates::aggregate_function_factory::AggregateFunctionDescription;
use crate::aggregates::aggregator_common::assert_unary_arguments;
use crate::aggregates::AggregateFunction;
use crate::aggregates::AggregateFunctionRef;

// count = 0 means it's all nullable
// so we do not need option like sum
#[derive(Serialize, Deserialize)]
struct AggregateAvgState<T: PrimitiveType> {
    #[serde(bound(deserialize = "T: DeserializeOwned"))]
    pub value: T,
    pub count: u64,
}

impl<T> AggregateAvgState<T>
where T: std::ops::AddAssign + PrimitiveType
{
    #[inline(always)]
    fn add_assume(&mut self, value: T, count: u64) {
        self.value += value;
        self.count += count;
    }

    #[inline(always)]
    fn merge(&mut self, other: &Self) {
        self.value += other.value;
        self.count += other.count;
    }
}

#[derive(Clone)]
pub struct AggregateAvgFunction<T, SumT> {
    display_name: String,
    _arguments: Vec<DataField>,
    t: PhantomData<T>,
    sum_t: PhantomData<SumT>,
}

impl<T, SumT> AggregateFunction for AggregateAvgFunction<T, SumT>
where
    T: PrimitiveType + AsPrimitive<SumT>,
    SumT: PrimitiveType + std::ops::AddAssign,
{
    fn name(&self) -> &str {
        "AggregateAvgFunction"
    }

    fn return_type(&self) -> Result<DataTypePtr> {
        Ok(f64::to_data_type())
    }

    fn init_state(&self, place: StateAddr) {
        place.write(|| AggregateAvgState::<SumT> {
            value: SumT::default(),
            count: 0,
        });
    }

    fn state_layout(&self) -> Layout {
        Layout::new::<AggregateAvgState<SumT>>()
    }

    fn accumulate(
        &self,
        place: StateAddr,
        columns: &[ColumnRef],
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
        let state = place.get::<AggregateAvgState<SumT>>();
        let sum = sum_primitive::<T, SumT>(&columns[0], validity)?;
        let cnt = input_rows - validity.map(|v| v.null_count()).unwrap_or(0);

        state.add_assume(sum, cnt as u64);
        Ok(())
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        offset: usize,
        columns: &[ColumnRef],
        _input_rows: usize,
    ) -> Result<()> {
        let array: &PrimitiveColumn<T> = unsafe { Series::static_cast(&columns[0]) };

        array.iter().zip(places.iter()).for_each(|(v, place)| {
            let place = place.next(offset);
            let state = place.get::<AggregateAvgState<SumT>>();
            state.add_assume(v.as_(), 1);
        });

        Ok(())
    }

    fn accumulate_row(&self, place: StateAddr, columns: &[ColumnRef], row: usize) -> Result<()> {
        let array: &PrimitiveColumn<T> = unsafe { Series::static_cast(&columns[0]) };
        let v = unsafe { array.value_unchecked(row) };
        let state = place.get::<AggregateAvgState<SumT>>();
        state.add_assume(v.as_(), 1);
        Ok(())
    }

    fn serialize(&self, place: StateAddr, writer: &mut BytesMut) -> Result<()> {
        let state = place.get::<AggregateAvgState<SumT>>();
        serialize_into_buf(writer, state)
    }

    fn deserialize(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state = place.get::<AggregateAvgState<SumT>>();
        *state = deserialize_from_slice(reader)?;
        Ok(())
    }

    fn merge(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let state = place.get::<AggregateAvgState<SumT>>();
        let rhs = rhs.get::<AggregateAvgState<SumT>>();
        state.merge(rhs);
        Ok(())
    }

    #[allow(unused_mut)]
    fn merge_result(&self, place: StateAddr, array: &mut dyn MutableColumn) -> Result<()> {
        let state = place.get::<AggregateAvgState<SumT>>();

        let builder: &mut MutablePrimitiveColumn<f64> = Series::check_get_mutable_column(array)?;
        let v: f64 = NumCast::from(state.value).unwrap_or_default();
        let val = v / state.count as f64;
        builder.append_value(val);
        Ok(())
    }
}

impl<T, SumT> fmt::Display for AggregateAvgFunction<T, SumT> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl<T, SumT> AggregateAvgFunction<T, SumT>
where
    T: PrimitiveType + AsPrimitive<SumT>,
    SumT: PrimitiveType + std::ops::AddAssign,
{
    pub fn try_create(
        display_name: &str,
        arguments: Vec<DataField>,
    ) -> Result<AggregateFunctionRef> {
        Ok(Arc::new(Self {
            display_name: display_name.to_string(),
            _arguments: arguments,
            t: PhantomData,
            sum_t: PhantomData,
        }))
    }
}

pub fn try_create_aggregate_avg_function(
    display_name: &str,
    _params: Vec<DataValue>,
    arguments: Vec<DataField>,
) -> Result<Arc<dyn AggregateFunction>> {
    assert_unary_arguments(display_name, arguments.len())?;

    let data_type = arguments[0].data_type();
    with_match_primitive_type_id!(data_type.data_type_id(), |$T| {
        AggregateAvgFunction::<$T, <$T as PrimitiveType>::LargestType>::try_create(
            display_name,
            arguments,
        )
    },

    {
        Err(ErrorCode::BadDataValueType(format!(
            "AggregateAvgFunction does not support type '{:?}'",
            data_type
        )))
    })
}

pub fn aggregate_avg_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(try_create_aggregate_avg_function))
}
