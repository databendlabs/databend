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
use std::sync::Arc;

use bytes::BytesMut;
use common_datavalues2::prelude::*;
use common_exception::Result;
use common_io::prelude::*;

use super::aggregate_function::AggregateFunction;
use super::aggregate_function_factory::AggregateFunctionDescription;
use super::StateAddr;
use crate::aggregates::aggregator_common::assert_variadic_arguments;

pub struct AggregateCountState {
    count: u64,
}

#[allow(dead_code)]
#[derive(Clone)]
pub struct AggregateCountFunction {
    display_name: String,
    arguments: Vec<DataField>,
}

impl AggregateCountFunction {
    pub fn try_create(
        display_name: &str,
        _params: Vec<DataValue>,
        arguments: Vec<DataField>,
    ) -> Result<Arc<dyn AggregateFunction>> {
        assert_variadic_arguments(display_name, arguments.len(), (0, 1))?;
        Ok(Arc::new(AggregateCountFunction {
            display_name: display_name.to_string(),
            arguments,
        }))
    }

    pub fn try_create_own(
        display_name: &str,
        _params: Vec<DataValue>,
        arguments: Vec<DataField>,
    ) -> Result<Self> {
        assert_variadic_arguments(display_name, arguments.len(), (0, 1))?;
        Ok(AggregateCountFunction {
            display_name: display_name.to_string(),
            arguments,
        })
    }

    pub fn desc() -> AggregateFunctionDescription {
        AggregateFunctionDescription::creator(Box::new(Self::try_create))
    }
}

impl AggregateFunction for AggregateCountFunction {
    fn name(&self) -> &str {
        "AggregateCountFunction"
    }

    fn return_type(&self) -> Result<DataTypePtr> {
        Ok(u64::to_data_type())
    }

    fn init_state(&self, place: StateAddr) {
        place.write(|| AggregateCountState { count: 0 });
    }

    fn state_layout(&self) -> Layout {
        Layout::new::<AggregateCountState>()
    }

    fn accumulate(
        &self,
        place: StateAddr,
        _columns: &[ColumnRef],
        validity: Option<&common_arrow::arrow::bitmap::Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
        let state = place.get::<AggregateCountState>();
        let nulls = match validity {
            Some(b) => b.null_count(),
            None => 0,
        };

        state.count += (input_rows - nulls) as u64;
        Ok(())
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        offset: usize,
        _columns: &[ColumnRef],
        _input_rows: usize,
    ) -> Result<()> {
        for place in places.iter() {
            let place = place.next(offset);
            let state = place.get::<AggregateCountState>();
            state.count += 1;
        }
        Ok(())
    }

    fn serialize(&self, place: StateAddr, writer: &mut BytesMut) -> Result<()> {
        let state = place.get::<AggregateCountState>();
        serialize_into_buf(writer, &state.count)
    }

    fn deserialize(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state = place.get::<AggregateCountState>();
        state.count = deserialize_from_slice(reader)?;

        Ok(())
    }

    fn merge(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let state = place.get::<AggregateCountState>();
        let rhs = rhs.get::<AggregateCountState>();
        state.count += rhs.count;

        Ok(())
    }

    fn merge_result(&self, place: StateAddr, array: &mut dyn MutableColumn) -> Result<()> {
        let builder: &mut MutablePrimitiveColumn<u64> = Series::check_get_mutable_column(array)?;
        let state = place.get::<AggregateCountState>();
        builder.append_value(state.count);
        Ok(())
    }
}

impl fmt::Display for AggregateCountFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}
