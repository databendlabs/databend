// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::fmt;

use common_datavalues::*;
use common_exception::ErrorCodes;
use common_exception::Result;

use crate::aggregate_function_factory::FactoryFunc;
use crate::IAggregateFunction;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
struct DataGroupValues(Vec<DataGroupValue>);

#[derive(Clone)]
pub struct AggregateIfCombinator {
    name: String,
    argument_len: usize,
    nested_name: String,
    nested: Box<dyn IAggregateFunction>,
}

impl AggregateIfCombinator {
    pub fn try_create(
        nested_name: &str,
        arguments: Vec<DataField>,
        nested_creator: FactoryFunc,
    ) -> Result<Box<dyn IAggregateFunction>> {
        let name = format!("IfCombinator({})", nested_name);
        let argument_len = arguments.len();

        if argument_len == 0 {
            return Err(ErrorCodes::NumberArgumentsNotMatch(format!(
                "{} expect to have more than one argument",
                name
            )));
        }

        match arguments[argument_len - 1].data_type() {
            DataType::Boolean => {}
            other => {
                return Err(ErrorCodes::BadArguments(format!(
                    "The type of the last argument for {} must be boolean type, but got {:?}",
                    name, other
                )));
            }
        }

        let nested_arguments = &arguments[0..argument_len - 1];
        let nested = nested_creator(nested_name, nested_arguments.to_vec())?;

        Ok(Box::new(AggregateIfCombinator {
            name,
            argument_len,
            nested_name: nested_name.to_owned(),
            nested,
        }))
    }
}

impl IAggregateFunction for AggregateIfCombinator {
    fn name(&self) -> &str {
        &self.name
    }

    fn return_type(&self) -> Result<DataType> {
        self.nested.return_type()
    }

    fn nullable(&self, input_schema: &DataSchema) -> Result<bool> {
        self.nested.nullable(input_schema)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn accumulate_scalar(&mut self, values: &[DataValue]) -> Result<()> {
        if let DataValue::Boolean(Some(flag)) = values[self.argument_len - 1] {
            if flag {
                return self
                    .nested
                    .accumulate_scalar(&values[0..self.argument_len - 1]);
            }
        }
        Ok(())
    }

    fn accumulate_result(&self) -> Result<Vec<DataValue>> {
        self.nested.accumulate_result()
    }

    fn merge(&mut self, states: &[DataValue]) -> Result<()> {
        self.nested.merge(states)
    }

    fn merge_result(&self) -> Result<DataValue> {
        self.nested.merge_result()
    }
}

impl fmt::Display for AggregateIfCombinator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.nested_name)
    }
}
