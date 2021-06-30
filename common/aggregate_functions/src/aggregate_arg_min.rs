// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::convert::TryFrom;
use std::convert::TryInto;
use std::fmt;

use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::aggregator_common::assert_binary_arguments;
use crate::AggregateFunction;

#[derive(Clone)]
pub struct AggregateArgMinFunction {
    display_name: String,
    state: DataValue,
    arguments: Vec<DataField>,
}

impl AggregateArgMinFunction {
    pub fn try_create(
        display_name: &str,
        arguments: Vec<DataField>,
    ) -> Result<Box<dyn AggregateFunction>> {
        assert_binary_arguments(display_name, arguments.len())?;

        Ok(Box::new(AggregateArgMinFunction {
            display_name: display_name.to_string(),
            state: DataValue::Struct(vec![
                DataValue::from(arguments[0].data_type()),
                DataValue::from(arguments[1].data_type()),
            ]),
            arguments,
        }))
    }
}

impl AggregateFunction for AggregateArgMinFunction {
    fn name(&self) -> &str {
        "AggregateArgMinFunction"
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

    fn accumulate(&mut self, columns: &[DataColumn], _input_rows: usize) -> Result<()> {
        let value = match &columns[1] {
            DataColumn::Constant(value, _) => Ok(DataValue::Struct(vec![
                DataValue::UInt64(Some(0)),
                value.clone(),
            ])),
            DataColumn::Array(array) => array.arg_min(),
        }?;

        if let DataValue::Struct(min_arg_val) = value {
            if min_arg_val[0].is_null() {
                return Ok(());
            }
            let index: u64 = min_arg_val[0].clone().try_into()?;
            let min_val = min_arg_val[1].clone();

            let min_arg = columns[0].try_get(index as usize)?;

            if let DataValue::Struct(old_min_arg_val) = self.state.clone() {
                let old_min_arg = old_min_arg_val[0].clone();
                let old_min_val = old_min_arg_val[1].clone();

                let new_min_val = old_min_val.min(&min_val)?;

                self.state = DataValue::Struct(vec![
                    if new_min_val == old_min_val {
                        old_min_arg
                    } else {
                        min_arg
                    },
                    new_min_val,
                ]);
            }
        }
        Ok(())
    }

    fn accumulate_scalar(&mut self, values: &[DataValue]) -> Result<()> {
        if let DataValue::Struct(old_min_arg_val) = self.state.clone() {
            let old_min_arg = old_min_arg_val[0].clone();
            let old_min_val = old_min_arg_val[1].clone();
            let new_min_val = old_min_val.min(&values[1])?;

            self.state = DataValue::Struct(vec![
                if new_min_val == old_min_val {
                    old_min_arg
                } else {
                    values[0].clone()
                },
                new_min_val,
            ]);
        }
        Ok(())
    }

    fn accumulate_result(&self) -> Result<Vec<DataValue>> {
        Ok(vec![self.state.clone()])
    }

    fn merge(&mut self, states: &[DataValue]) -> Result<()> {
        let arg_val = states[0].clone();
        if let (DataValue::Struct(new_states), DataValue::Struct(old_states)) =
            (arg_val, self.state.clone())
        {
            let new_min_val = new_states[1].min(&old_states[1])?;

            self.state = DataValue::Struct(vec![
                if new_min_val == old_states[1] {
                    old_states[0].clone()
                } else {
                    new_states[0].clone()
                },
                new_min_val,
            ]);
        }
        Ok(())
    }

    fn merge_result(&self) -> Result<DataValue> {
        Ok(if let DataValue::Struct(state) = self.state.clone() {
            state[0].clone()
        } else {
            self.state.clone()
        })
    }
}

impl fmt::Display for AggregateArgMinFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}
