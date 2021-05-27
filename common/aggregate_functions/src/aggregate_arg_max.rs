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
pub struct AggregateArgMaxFunction {
    display_name: String,
    depth: usize,
    state: DataValue
}

impl AggregateArgMaxFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn IAggregateFunction>> {
        Ok(Box::new(AggregateArgMaxFunction {
            display_name: display_name.to_string(),
            depth: 0,
            state: DataValue::Struct(vec![DataValue::Null, DataValue::Null])
        }))
    }
}

impl IAggregateFunction for AggregateArgMaxFunction {
    fn name(&self) -> &str {
        "AggregateArgMaxFunction"
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
        if let DataValue::Struct(max_arg_val) = DataArrayAggregate::data_array_aggregate_op(
            DataValueAggregateOperator::ArgMax,
            columns[1].to_array()?
        )? {
            let index: u64 = max_arg_val[0].clone().try_into()?;
            let max_arg = DataValue::try_from_array(&columns[0].to_array()?, index as usize)?;
            let max_val = max_arg_val[1].clone();

            if let DataValue::Struct(old_max_arg_val) = self.state.clone() {
                let old_max_arg = old_max_arg_val[0].clone();
                let old_max_val = old_max_arg_val[1].clone();
                let new_max_val = DataValueAggregate::data_value_aggregate_op(
                    DataValueAggregateOperator::Max,
                    old_max_val.clone(),
                    max_val
                )?;
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

    fn accumulate_result(&self) -> Result<Vec<DataValue>> {
        Ok(vec![self.state.clone()])
    }

    fn merge(&mut self, states: &[DataValue]) -> Result<()> {
        let arg_val = states[self.depth].clone();
        if let (DataValue::Struct(new_states), DataValue::Struct(old_states)) =
            (arg_val, self.state.clone())
        {
            let new_max_val = DataValueAggregate::data_value_aggregate_op(
                DataValueAggregateOperator::Max,
                new_states[1].clone(),
                old_states[1].clone()
            )?;
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
        Ok(self.state.clone())
    }
}

impl fmt::Display for AggregateArgMaxFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}
