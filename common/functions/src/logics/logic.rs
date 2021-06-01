// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use common_datavalues::DataArrayLogic;
use common_datavalues::DataColumnarValue;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_datavalues::DataValueLogicOperator;
use common_exception::Result;

use crate::logics::LogicAndFunction;
use crate::logics::LogicNotFunction;
use crate::logics::LogicOrFunction;
use crate::FactoryFuncRef;
use crate::IFunction;

#[derive(Clone)]
pub struct LogicFunction {
    op: DataValueLogicOperator,
}

impl LogicFunction {
    pub fn register(map: FactoryFuncRef) -> Result<()> {
        let mut map = map.write();
        map.insert("and", LogicAndFunction::try_create_func);
        map.insert("or", LogicOrFunction::try_create_func);
        map.insert("not", LogicNotFunction::try_create_func);
        Ok(())
    }

    pub fn try_create_func(op: DataValueLogicOperator) -> Result<Box<dyn IFunction>> {
        Ok(Box::new(LogicFunction { op }))
    }
}

impl IFunction for LogicFunction {
    fn name(&self) -> &str {
        "LogicFunction"
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, columns: &[DataColumnarValue], _input_rows: usize) -> Result<DataColumnarValue> {
        Ok(DataColumnarValue::Array(
            DataArrayLogic::data_array_logic_op(self.op.clone(), columns)?,
        ))
    }
}

impl fmt::Display for LogicFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.op)
    }
}
