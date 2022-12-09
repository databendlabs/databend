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

use common_arrow::arrow::bitmap::Bitmap;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_io::prelude::*;
use streaming_algorithms::HyperLogLog;

use super::aggregate_function::AggregateFunction;
use super::aggregate_function_factory::AggregateFunctionDescription;
use super::StateAddr;
use crate::aggregates::aggregator_common::assert_unary_arguments;

/// Use Hyperloglog to estimate distinct of values
#[derive(serde::Serialize, serde::Deserialize)]
pub struct AggregateApproxCountDistinctState {
    hll: HyperLogLog<DataValue>,
}

/// S: ScalarType
#[derive(Clone)]
pub struct AggregateApproxCountDistinctFunction {
    display_name: String,
}

impl AggregateApproxCountDistinctFunction {
    pub fn try_create(
        display_name: &str,
        _params: Vec<DataValue>,
        arguments: Vec<DataField>,
    ) -> Result<Arc<dyn AggregateFunction>> {
        assert_unary_arguments(display_name, arguments.len())?;
        Ok(Arc::new(AggregateApproxCountDistinctFunction {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> AggregateFunctionDescription {
        let features = super::aggregate_function_factory::AggregateFunctionFeatures {
            returns_default_when_only_null: true,
            ..Default::default()
        };
        AggregateFunctionDescription::creator_with_features(Box::new(Self::try_create), features)
    }
}

impl AggregateFunction for AggregateApproxCountDistinctFunction {
    fn name(&self) -> &str {
        "AggregateApproxCountDistinctFunction"
    }

    fn return_type(&self) -> Result<DataTypeImpl> {
        Ok(u64::to_data_type())
    }

    fn init_state(&self, place: StateAddr) {
        place.write(|| AggregateApproxCountDistinctState {
            hll: HyperLogLog::new(0.04),
        });
    }

    fn state_layout(&self) -> Layout {
        Layout::new::<AggregateApproxCountDistinctState>()
    }

    fn accumulate(
        &self,
        place: StateAddr,
        columns: &[ColumnRef],
        validity: Option<&Bitmap>,
        _input_rows: usize,
    ) -> Result<()> {
        let state = place.get::<AggregateApproxCountDistinctState>();
        let column = &columns[0];

        let (_, bm) = column.validity();
        let bitmap = combine_validities(bm, validity);
        let nulls = match bitmap {
            Some(ref b) => b.unset_bits(),
            None => 0,
        };
        if column.len() == nulls {
            return Ok(());
        }

        // This is a hot path, to_values will alloc more memory(Vec::with_capacity).
        if let Some(bitmap) = bitmap {
            for i in 0..column.len() {
                let value = &column.get(i);
                if bitmap.get_bit(i) {
                    state.hll.push(value);
                }
            }
        } else {
            for i in 0..column.len() {
                let value = &column.get(i);
                state.hll.push(value);
            }
        }

        Ok(())
    }

    fn accumulate_row(&self, place: StateAddr, columns: &[ColumnRef], _row: usize) -> Result<()> {
        let state = place.get::<AggregateApproxCountDistinctState>();
        for column in columns {
            (0..column.len()).for_each(|i| state.hll.push(&column.get(i)));
        }
        Ok(())
    }

    fn serialize(&self, place: StateAddr, writer: &mut Vec<u8>) -> Result<()> {
        let state = place.get::<AggregateApproxCountDistinctState>();
        serialize_into_buf(writer, state)
    }

    fn deserialize(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state = place.get::<AggregateApproxCountDistinctState>();
        *state = deserialize_from_slice(reader)?;
        Ok(())
    }

    fn merge(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let state = place.get::<AggregateApproxCountDistinctState>();
        let rhs = rhs.get::<AggregateApproxCountDistinctState>();
        state.hll.union(&rhs.hll);

        Ok(())
    }

    fn merge_result(&self, place: StateAddr, array: &mut dyn MutableColumn) -> Result<()> {
        let builder: &mut MutablePrimitiveColumn<u64> = Series::check_get_mutable_column(array)?;
        let state = place.get::<AggregateApproxCountDistinctState>();
        builder.append_value(state.hll.len() as u64);

        Ok(())
    }

    fn need_manual_drop_state(&self) -> bool {
        true
    }

    unsafe fn drop_state(&self, place: StateAddr) {
        let state = place.get::<AggregateApproxCountDistinctState>();
        std::ptr::drop_in_place(state);
    }

    fn get_own_null_adaptor(
        &self,
        _nested_function: super::AggregateFunctionRef,
        _params: Vec<DataValue>,
        _arguments: Vec<DataField>,
    ) -> Result<Option<super::AggregateFunctionRef>> {
        let f = self.clone();
        Ok(Some(Arc::new(f)))
    }
}

impl fmt::Display for AggregateApproxCountDistinctFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}
