// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::convert::TryInto;
use std::fmt;

use common_datavalues::DataArrayAggregate;
use common_datavalues::DataColumnarValue;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_datavalues::DataValueAggregate;
use common_datavalues::DataValueAggregateOperator;
use common_exception::Result;

use crate::IAggregateFunction;

#[derive(Clone)]
pub struct AggregateArgMinFunction {
    display_name: String,
    depth: usize,
    state: DataValue,
}

impl AggregateArgMinFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn IAggregateFunction>> {
        Ok(Box::new(AggregateArgMinFunction {
            display_name: display_name.to_string(),
            depth: 0,
            state: DataValue::Struct(vec![DataValue::Null, DataValue::Null]),
        }))
    }
}

impl IAggregateFunction for AggregateArgMinFunction {
    fn name(&self) -> &str {
        "AggregateArgMinFunction"
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        Ok(args[0].clone())
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn set_depth(&mut self, depth: usize) {
        self.depth = depth;
    }

    fn accumulate(&mut self, columns: &[DataColumnarValue], _input_rows: usize) -> Result<()> {
        if let DataValue::Struct(min_arg_val) = DataArrayAggregate::data_array_aggregate_op(
            DataValueAggregateOperator::ArgMin,
            columns[1].to_array()?,
        )? {
            let index: u64 = min_arg_val[0].clone().try_into()?;
            let min_arg = DataValue::try_from_array(&columns[0].to_array()?, index as usize)?;
            let min_val = min_arg_val[1].clone();

            if let DataValue::Struct(old_min_arg_val) = self.state.clone() {
                let old_min_arg = old_min_arg_val[0].clone();
                let old_min_val = old_min_arg_val[1].clone();
                let new_min_val = DataValueAggregate::data_value_aggregate_op(
                    DataValueAggregateOperator::Min,
                    old_min_val.clone(),
                    min_val,
                )?;
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

    fn accumulate_result(&self) -> Result<Vec<DataValue>> {
        Ok(vec![self.state.clone()])
    }

    fn merge(&mut self, states: &[DataValue]) -> Result<()> {
        let arg_val = states[self.depth].clone();
        if let (DataValue::Struct(new_states), DataValue::Struct(old_states)) =
            (arg_val, self.state.clone())
        {
            let new_min_val = DataValueAggregate::data_value_aggregate_op(
                DataValueAggregateOperator::Min,
                new_states[1].clone(),
                old_states[1].clone(),
            )?;
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
