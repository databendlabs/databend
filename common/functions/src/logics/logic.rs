// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use common_datablocks::DataBlock;
use common_datavalues::DataArrayLogic;
use common_datavalues::DataColumnarValue;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_datavalues::DataValueLogicOperator;
use common_exception::ErrorCodes;
use common_exception::Result;

use crate::logics::LogicAndFunction;
use crate::logics::LogicNotFunction;
use crate::logics::LogicOrFunction;
use crate::FactoryFuncRef;
use crate::IFunction;

#[derive(Clone)]
pub struct LogicFunction {
    depth: usize,
    op: DataValueLogicOperator,
    operands: Vec<Box<dyn IFunction>>,
    saved: Option<DataColumnarValue>
}

impl LogicFunction {
    pub fn register(map: FactoryFuncRef) -> Result<()> {
        let mut map = map.write();
        map.insert("and", LogicAndFunction::try_create_func);
        map.insert("or", LogicOrFunction::try_create_func);
        map.insert("not", LogicNotFunction::try_create_func);
        Ok(())
    }

    pub fn try_create_func(
        op: DataValueLogicOperator,
        args: &[Box<dyn IFunction>]
    ) -> Result<Box<dyn IFunction>> {
        match args.len() {
            2 => Result::Ok(Box::new(LogicFunction {
                depth: 0,
                op,
                operands: vec![args[0].clone(), args[1].clone()],
                saved: None
            })),
            1 => Result::Ok(Box::new(LogicFunction {
                depth: 0,
                op,
                operands: vec![args[0].clone()],
                saved: None
            })),
            _ => {
                let num_args = if let DataValueLogicOperator::Not = op {
                    1
                } else {
                    2
                };
                Result::Err(ErrorCodes::BadArguments(format!(
                    "Function Error: Logic function {} args length must be {}",
                    op, num_args
                )))
            }
        }
    }
}

impl IFunction for LogicFunction {
    fn name(&self) -> &str {
        "LogicFunction"
    }

    fn return_type(&self, _input_schema: &DataSchema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, block: &DataBlock) -> Result<DataColumnarValue> {
        let arr = self
            .operands
            .iter()
            .map(|x| x.eval(block).unwrap())
            .collect::<Vec<_>>();
        Ok(DataColumnarValue::Array(
            DataArrayLogic::data_array_logic_op(self.op.clone(), &arr)?
        ))
    }

    fn set_depth(&mut self, depth: usize) {
        self.depth = depth;
    }

    fn is_aggregator(&self) -> bool {
        match self.op {
            DataValueLogicOperator::Not => self.operands[0].is_aggregator(),
            _ => self.operands[0].is_aggregator() || self.operands[1].is_aggregator()
        }
    }
}

impl fmt::Display for LogicFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.op {
            DataValueLogicOperator::Not => write!(f, "{} {}", self.op, self.operands[0]),
            _ => write!(f, "{} {} {}", self.operands[0], self.op, self.operands[1])
        }
    }
}
