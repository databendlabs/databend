// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::fmt;

use common_datavalues::prelude::*;
use common_exception::Result;

use super::AggregateSingeValueState;
use super::GetState;
use super::StateAddr;
use crate::aggregates::aggregator_common::assert_unary_arguments;
use crate::aggregates::AggregateFunction;
use crate::aggregates::AggregateFunctionRef;
use crate::aggregates::AggregateSumFunction;

#[derive(Clone)]
pub struct AggregateAvgFunction {
    display_name: String,
    arguments: Vec<DataField>,
    sum_type: DataType,
}

impl AggregateAvgFunction {
    pub fn try_create(
        display_name: &str,
        arguments: Vec<DataField>,
    ) -> Result<AggregateFunctionRef> {
        assert_unary_arguments(display_name, arguments.len())?;

        let sum_type = AggregateSumFunction::sum_return_type(arguments[0].data_type())?;
        Ok(Arc::new(AggregateAvgFunction {
            display_name: display_name.to_string(),
            arguments,
            sum_type,
        }))
    }
}

impl AggregateFunction for AggregateAvgFunction {
    fn name(&self) -> &str {
        "AggregateAvgFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn allocate_state(&self, arena: &bumpalo::Bump) -> StateAddr {
        let state = arena.alloc(AggregateSingeValueState {
            value: DataValue::Struct(vec![
                DataValue::from(&self.sum_type),
                DataValue::UInt64(Some(0)),
            ]),
        });

        (state as *mut AggregateSingeValueState) as StateAddr
    }

    fn accumulate(
        &self,
        place: StateAddr,
        columns: &[DataColumn],
        input_rows: usize,
    ) -> Result<()> {
        let state = AggregateSingeValueState::get(place);

        if let DataValue::Struct(values) = state.value.clone() {
            let sum = match &columns[0] {
                DataColumn::Constant(value, size) => {
                    DataValue::arithmetic(Mul, value.clone(), DataValue::UInt64(Some(*size as u64)))
                }
                DataColumn::Array(array) => array.sum(),
            }?;

            let sum = (&sum + &values[0])?;

            let count = DataValue::UInt64(Some(input_rows as u64));
            let count = (&count + &values[1])?;

            state.value = DataValue::Struct(vec![sum, count]);
        }
        Ok(())
    }

    fn accumulate_row(&self, place: StateAddr, row: usize, columns: &[DataColumn]) -> Result<()> {
        let state = AggregateSingeValueState::get(place);
        let value = columns[0].try_get(row)?;

        if let DataValue::Struct(values) = state.value.clone() {
            let sum = (&value + &values[0])?;
            let count = DataValue::UInt64(Some(1_u64));
            let count = (&count + &values[1])?;

            state.value = DataValue::Struct(vec![sum, count]);
        }
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

        if let (DataValue::Struct(current), DataValue::Struct(other)) =
            (state.value.clone(), rhs.value.clone())
        {
            let sum = (&current[0] + &other[0])?;
            let count = (&current[1] + &other[1])?;

            state.value = DataValue::Struct(vec![sum, count]);
        }
        Ok(())
    }

    fn merge_result(&self, place: StateAddr) -> Result<DataValue> {
        let state = AggregateSingeValueState::get(place);

        Ok(if let DataValue::Struct(states) = state.value.clone() {
            if states[1].eq(&DataValue::UInt64(Some(0))) {
                DataValue::Float64(None)
            } else {
                (&states[0] / &states[1])?
            }
        } else {
            state.value.clone()
        })
    }
}

impl fmt::Display for AggregateAvgFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}
