// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::fmt;

use common_datavalues::prelude::*;
use common_exception::Result;

use super::GetState;
use super::StateAddr;
use crate::aggregates::aggregator_common::assert_unary_arguments;
use crate::aggregates::AggregateFunction;
use crate::aggregates::AggregateFunctionRef;
use crate::aggregates::AggregateSingeValueState;

#[derive(Clone)]
pub struct AggregateMaxFunction {
    display_name: String,
    arguments: Vec<DataField>,
}

impl AggregateMaxFunction {
    pub fn try_create(
        display_name: &str,
        arguments: Vec<DataField>,
    ) -> Result<AggregateFunctionRef> {
        assert_unary_arguments(display_name, arguments.len())?;

        Ok(Arc::new(AggregateMaxFunction {
            display_name: display_name.to_string(),
            arguments,
        }))
    }
}

impl AggregateFunction for AggregateMaxFunction {
    fn name(&self) -> &str {
        "AggregateMaxFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(self.arguments[0].data_type().clone())
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn allocate_state(&self, arena: &bumpalo::Bump) -> StateAddr {
        let state = arena.alloc(AggregateSingeValueState {
            value: DataValue::from(self.arguments[0].data_type()),
        });

        (state as *mut AggregateSingeValueState) as StateAddr
    }

    fn accumulate(
        &self,
        place: StateAddr,
        columns: &[DataColumn],
        _input_rows: usize,
    ) -> Result<()> {
        let state = AggregateSingeValueState::get(place);
        let value = Self::max_batch(&columns[0])?;
        state.value = DataValue::agg(Max, state.value.clone(), value)?;
        Ok(())
    }

    fn accumulate_row(&self, place: StateAddr, row: usize, columns: &[DataColumn]) -> Result<()> {
        let state = AggregateSingeValueState::get(place);
        let value = columns[0].try_get(row)?;

        state.value = DataValue::agg(Max, state.value.clone(), value)?;
        Ok(())
    }

    fn serialize(&self, place: StateAddr, writer: &mut Vec<u8>) -> Result<()> {
        let state = AggregateSingeValueState::get(place);
        state.serialize(writer)
    }

    fn deserialize(&self, place: StateAddr, reader: &[u8]) -> Result<()> {
        let state = AggregateSingeValueState::get(place);
        state.deserialize(reader)
    }

    fn merge(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let state = AggregateSingeValueState::get(place);
        let rhs = AggregateSingeValueState::get(rhs);

        state.value = DataValue::agg(Max, state.value.clone(), rhs.value.clone())?;
        Ok(())
    }

    fn merge_result(&self, place: StateAddr) -> Result<DataValue> {
        let state = AggregateSingeValueState::get(place);

        Ok(state.value.clone())
    }
}

impl fmt::Display for AggregateMaxFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl AggregateMaxFunction {
    pub fn max_batch(column: &DataColumn) -> Result<DataValue> {
        if column.is_empty() {
            return Ok(DataValue::from(&column.data_type()));
        }
        match column {
            DataColumn::Constant(value, _) => Ok(value.clone()),
            DataColumn::Array(array) => array.max(),
        }
    }
}
