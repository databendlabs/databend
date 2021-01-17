// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::fmt;

use crate::datablocks::DataBlock;
use crate::datavalues;
use crate::datavalues::{
    DataColumnarValue, DataSchema, DataType, DataValue, DataValueLogicOperator,
};
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::functions::{FactoryFuncRef, Function};

#[derive(Clone)]
pub struct LogicFunction {
    depth: usize,
    op: DataValueLogicOperator,
    left: Box<Function>,
    right: Box<Function>,
    saved: Option<DataColumnarValue>,
}

impl LogicFunction {
    pub fn register(map: FactoryFuncRef) -> FuseQueryResult<()> {
        let mut map = map.as_ref().lock()?;
        map.insert("and", LogicFunction::try_create_and_func);
        map.insert("or", LogicFunction::try_create_or_func);
        Ok(())
    }

    pub fn try_create_and_func(args: &[Function]) -> FuseQueryResult<Function> {
        Self::try_create(DataValueLogicOperator::And, args)
    }

    pub fn try_create_or_func(args: &[Function]) -> FuseQueryResult<Function> {
        Self::try_create(DataValueLogicOperator::Or, args)
    }

    fn try_create(op: DataValueLogicOperator, args: &[Function]) -> FuseQueryResult<Function> {
        if args.len() != 2 {
            return Err(FuseQueryError::Internal(
                "Logic function args length must be 2".to_string(),
            ));
        }

        Ok(Function::Logic(LogicFunction {
            depth: 0,
            op,
            left: Box::from(args[0].clone()),
            right: Box::from(args[1].clone()),
            saved: None,
        }))
    }

    pub fn return_type(&self, _input_schema: &DataSchema) -> FuseQueryResult<DataType> {
        Ok(DataType::Boolean)
    }

    pub fn nullable(&self, _input_schema: &DataSchema) -> FuseQueryResult<bool> {
        Ok(false)
    }

    pub fn set_depth(&mut self, depth: usize) {
        self.depth = depth;
    }

    pub fn eval(&mut self, block: &DataBlock) -> FuseQueryResult<DataColumnarValue> {
        Ok(DataColumnarValue::Array(datavalues::data_array_logic_op(
            self.op.clone(),
            &self.left.eval(block)?,
            &self.right.eval(block)?,
        )?))
    }

    pub fn accumulate(&mut self, block: &DataBlock) -> FuseQueryResult<()> {
        self.left.accumulate(block)?;
        self.right.accumulate(block)
    }

    pub fn accumulate_result(&self) -> FuseQueryResult<Vec<DataValue>> {
        Err(FuseQueryError::Internal(format!(
            "Unsupported aggregate operation for function {}",
            self.op
        )))
    }

    pub fn merge(&mut self, _states: &[DataValue]) -> FuseQueryResult<()> {
        Err(FuseQueryError::Internal(format!(
            "Unsupported aggregate operation for function {}",
            self.op
        )))
    }

    pub fn merge_result(&self) -> FuseQueryResult<DataValue> {
        Err(FuseQueryError::Internal(format!(
            "Unsupported aggregate operation for function {}",
            self.op
        )))
    }
}

impl fmt::Display for LogicFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?} {} {:?}", self.left, self.op, self.right)
    }
}
