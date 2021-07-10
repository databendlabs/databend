// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::fmt;

use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;

use super::GetState;
use super::StateAddr;
use crate::aggregates::aggregator_common::assert_unary_arguments;
use crate::aggregates::AggregateFunction;
use crate::aggregates::AggregateSingeValueState;

#[derive(Clone)]
pub struct AggregateSumFunction {
    display_name: String,
    arguments: Vec<DataField>,
    return_type: DataType,
}

impl AggregateFunction for AggregateSumFunction {
    fn name(&self) -> &str {
        "AggregateSumFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn allocate_state(&self, arena: &bumpalo::Bump) -> StateAddr {
        let state = arena.alloc(AggregateSingeValueState {
            value: DataValue::from(&self.return_type),
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
        let value = Self::sum_batch(&columns[0])?;
        state.value = (&state.value + &value)?;
        Ok(())
    }

    fn accumulate_row(&self, place: StateAddr, row: usize, columns: &[DataColumn]) -> Result<()> {
        let state = AggregateSingeValueState::get(place);
        let value = columns[0].try_get(row)?;
        state.value = (&state.value + &value)?;
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

        state.value = (&state.value + &rhs.value)?;
        Ok(())
    }

    fn merge_result(&self, place: StateAddr) -> Result<DataValue> {
        let state = AggregateSingeValueState::get(place);

        Ok(state.value.clone())
    }
}

impl fmt::Display for AggregateSumFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl AggregateSumFunction {
    pub fn try_create(
        display_name: &str,
        arguments: Vec<DataField>,
    ) -> Result<Arc<dyn AggregateFunction>> {
        assert_unary_arguments(display_name, arguments.len())?;
        let return_type = Self::sum_return_type(arguments[0].data_type())?;

        Ok(Arc::new(Self {
            display_name: display_name.to_owned(),
            arguments,
            return_type,
        }))
    }

    pub fn sum_return_type(arg_type: &DataType) -> Result<DataType> {
        match arg_type {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                Ok(DataType::Int64)
            }
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                Ok(DataType::UInt64)
            }
            DataType::Float32 => Ok(DataType::Float32),
            DataType::Float64 => Ok(DataType::Float64),

            other => Err(ErrorCode::BadDataValueType(format!(
                "SUM does not support type '{:?}'",
                other
            ))),
        }
    }

    pub fn sum_batch(column: &DataColumn) -> Result<DataValue> {
        if column.is_empty() {
            return Ok(DataValue::from(&Self::sum_return_type(
                &column.data_type(),
            )?));
        }
        match column {
            DataColumn::Constant(value, size) => {
                DataValue::arithmetic(Mul, value.clone(), DataValue::UInt64(Some(*size as u64)))
            }
            DataColumn::Array(array) => array.sum(),
        }
    }
}
