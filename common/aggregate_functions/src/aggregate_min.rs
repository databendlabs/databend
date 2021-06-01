// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use common_datavalues::DataArrayAggregate;
use common_datavalues::DataColumnarValue;
use common_datavalues::DataField;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_datavalues::DataValueAggregate;
use common_datavalues::DataValueAggregateOperator;
use common_exception::Result;

use crate::IAggregateFunction;

#[derive(Clone)]
pub struct AggregateMinFunction {
    display_name: String,
    state: DataValue,
    arguments: Vec<DataField>,
}

impl AggregateMinFunction {
    pub fn try_create(
        display_name: &str,
        arguments: Vec<DataField>,
    ) -> Result<Box<dyn IAggregateFunction>> {
        Ok(Box::new(AggregateMinFunction {
            display_name: display_name.to_string(),
            state: DataValue::Null,
            arguments,
        }))
    }
}

impl IAggregateFunction for AggregateMinFunction {
    fn name(&self) -> &str {
        "AggregateMinFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(self.arguments[0].data_type().clone())
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn accumulate(&mut self, columns: &[DataColumnarValue], _input_rows: usize) -> Result<()> {
        let value = match &columns[0] {
            DataColumnarValue::Array(array) => DataArrayAggregate::data_array_aggregate_op(
                DataValueAggregateOperator::Min,
                array.clone(),
            ),
            DataColumnarValue::Constant(s, _) => Ok(s.clone()),
        }?;

        self.state = DataValueAggregate::data_value_aggregate_op(
            DataValueAggregateOperator::Min,
            self.state.clone(),
            value,
        )?;

        Ok(())
    }

    fn accumulate_result(&self) -> Result<Vec<DataValue>> {
        Ok(vec![self.state.clone()])
    }

    fn merge(&mut self, states: &[DataValue]) -> Result<()> {
        let val = states[0].clone();
        self.state = DataValueAggregate::data_value_aggregate_op(
            DataValueAggregateOperator::Min,
            self.state.clone(),
            val,
        )?;
        Ok(())
    }

    fn merge_result(&self) -> Result<DataValue> {
        Ok(self.state.clone())
    }
}

impl fmt::Display for AggregateMinFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}
