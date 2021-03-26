// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use crate::common_datablocks::DataBlock;
use crate::common_datavalues::{DataColumnarValue, DataSchema, DataType, DataValue};
use crate::{FunctionError, FunctionResult, IFunction};

#[derive(Clone)]
pub struct ToTypeNameFunction {
    arg: Box<dyn IFunction>,
}

impl ToTypeNameFunction {
    pub fn try_create(args: &[Box<dyn IFunction>]) -> FunctionResult<Box<dyn IFunction>> {
        if args.len() != 1 {
            return Err(FunctionError::build_internal_error(
                "The argument size of function database must be one".to_string(),
            ));
        }

        Ok(Box::new(ToTypeNameFunction {
            arg: args[0].clone(),
        }))
    }
}

impl IFunction for ToTypeNameFunction {
    fn return_type(&self, _input_schema: &DataSchema) -> FunctionResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> FunctionResult<bool> {
        Ok(false)
    }

    fn eval(&self, block: &DataBlock) -> FunctionResult<DataColumnarValue> {
        let type_name = format!("{}", self.arg.return_type(block.schema())?);
        Ok(DataColumnarValue::Scalar(DataValue::String(Some(
            type_name,
        ))))
    }

    fn set_depth(&mut self, _depth: usize) {}

    fn accumulate(&mut self, _block: &DataBlock) -> FunctionResult<()> {
        Err(FunctionError::build_internal_error(
            "Unsupported accumulate for toTypeName Function".to_string(),
        ))
    }

    fn accumulate_result(&self) -> FunctionResult<Vec<DataValue>> {
        Err(FunctionError::build_internal_error(
            "Unsupported accumulate_result for toTypeName Function".to_string(),
        ))
    }

    fn merge(&mut self, _states: &[DataValue]) -> FunctionResult<()> {
        Err(FunctionError::build_internal_error(
            "Unsupported merge for toTypeName Function".to_string(),
        ))
    }

    fn merge_result(&self) -> FunctionResult<DataValue> {
        Err(FunctionError::build_internal_error(
            "Unsupported merge_result for toTypeName Function".to_string(),
        ))
    }
}

impl fmt::Display for ToTypeNameFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "toTypeName({})", self.arg)
    }
}
