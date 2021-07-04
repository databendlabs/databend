// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::convert::TryInto;
use std::fmt;

use common_datavalues::prelude::*;
use common_exception::Result;

use crate::aggregates::aggregator_common::assert_binary_arguments;
use crate::aggregates::AggregateFunction;

#[derive(Clone)]
pub struct AggregateArgMaxFunction {
    display_name: String,
    state: DataValue,
    arguments: Vec<DataField>,
}

impl AggregateArgMaxFunction {
    pub fn try_create(
        display_name: &str,
        arguments: Vec<DataField>,
    ) -> Result<Box<dyn AggregateFunction>> {
        assert_binary_arguments(display_name, arguments.len())?;

        Ok(Box::new(AggregateArgMaxFunction {
            display_name: display_name.to_string(),
            state: DataValue::Struct(vec![
                DataValue::from(arguments[0].data_type()),
                DataValue::from(arguments[1].data_type()),
            ]),
            arguments,
        }))
    }
}

impl AggregateFunction for AggregateArgMaxFunction {
    fn name(&self) -> &str {
        "AggregateArgMaxFunction"
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
        if columns[0].is_empty() {
            return Ok(());
        }

        let value = match &columns[1] {
            DataColumn::Constant(value, _) => Ok(DataValue::Struct(vec![
                DataValue::UInt64(Some(0)),
                value.clone(),
            ])),
            DataColumn::Array(array) => array.arg_max(),
        }?;

        if let DataValue::Struct(max_arg_val) = value {
            if max_arg_val[0].is_null() {
                return Ok(());
            }
            let index: u64 = max_arg_val[0].clone().try_into()?;
            let max_val = max_arg_val[1].clone();

            let max_arg = columns[0].try_get(index as usize)?;

            if let DataValue::Struct(old_max_arg_val) = self.state.clone() {
                let old_max_arg = old_max_arg_val[0].clone();
                let old_max_val = old_max_arg_val[1].clone();

                let new_max_val = DataValue::agg(Max, old_max_val.clone(), max_val)?;

                self.state = DataValue::Struct(vec![
                    if new_max_val == old_max_val {
                        old_max_arg
                    } else {
                        max_arg
                    },
                    new_max_val,
                ]);
            }
        }
        Ok(())
    }

    fn accumulate_scalar(&mut self, values: &[DataValue]) -> Result<()> {
        if let DataValue::Struct(old_max_arg_val) = self.state.clone() {
            let old_max_arg = old_max_arg_val[0].clone();
            let old_max_val = old_max_arg_val[1].clone();
            let new_max_val = DataValue::agg(Max, old_max_val.clone(), values[1].clone())?;

            self.state = DataValue::Struct(vec![
                if new_max_val == old_max_val {
                    old_max_arg
                } else {
                    values[0].clone()
                },
                new_max_val,
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
            let new_max_val = DataValue::agg(Max, new_states[1].clone(), old_states[1].clone())?;
            self.state = DataValue::Struct(vec![
                if new_max_val == old_states[1] {
                    old_states[0].clone()
                } else {
                    new_states[0].clone()
                },
                new_max_val,
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

impl fmt::Display for AggregateArgMaxFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}
