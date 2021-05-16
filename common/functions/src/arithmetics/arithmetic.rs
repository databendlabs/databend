// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use common_datablocks::DataBlock;
use common_datavalues::DataArrayArithmetic;
use common_datavalues::DataColumnarValue;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_datavalues::DataValueArithmetic;
use common_datavalues::DataValueArithmeticOperator;
use common_exception::ErrorCodes;
use common_exception::Result;

use crate::arithmetics::ArithmeticDivFunction;
use crate::arithmetics::ArithmeticMinusFunction;
use crate::arithmetics::ArithmeticModuloFunction;
use crate::arithmetics::ArithmeticMulFunction;
use crate::arithmetics::ArithmeticPlusFunction;
use crate::{FactoryFuncRef, FunctionCtx};
use crate::IFunction;
use std::sync::Arc;

#[derive(Clone)]
pub struct ArithmeticFunction {
    depth: usize,
    op: DataValueArithmeticOperator,
}

impl ArithmeticFunction {
    pub fn register(map: FactoryFuncRef) -> Result<()> {
        let mut map = map.write();
        map.insert("+", ArithmeticPlusFunction::try_create_func);
        map.insert("plus", ArithmeticPlusFunction::try_create_func);
        map.insert("-", ArithmeticMinusFunction::try_create_func);
        map.insert("minus", ArithmeticMinusFunction::try_create_func);
        map.insert("*", ArithmeticMulFunction::try_create_func);
        map.insert("multiply", ArithmeticMulFunction::try_create_func);
        map.insert("/", ArithmeticDivFunction::try_create_func);
        map.insert("divide", ArithmeticDivFunction::try_create_func);
        map.insert("%", ArithmeticModuloFunction::try_create_func);
        map.insert("modulo", ArithmeticModuloFunction::try_create_func);
        Ok(())
    }

    pub fn try_create_func(
        op: DataValueArithmeticOperator,
        _ctx: Arc<dyn FunctionCtx>,
    ) -> Result<Box<dyn IFunction>> {
        Ok(Box::new(ArithmeticFunction {
            depth: 0,
            op,
        }))
    }
}

impl IFunction for ArithmeticFunction {
    fn name(&self) -> &str {
        "ArithmeticFunction"
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        common_datavalues::numerical_arithmetic_coercion(
            &self.op,
            &args[0],
            &args[1],
        )
    }

    fn nullable(&self, input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, columns: &[DataColumnarValue]) -> Result<DataColumnarValue> {
        let result = DataArrayArithmetic::data_array_arithmetic_op(self.op.clone(), columns[0].as_ref(), columns[1].as_ref())?;

        match (left, right) {
            (DataColumnarValue::Scalar(_), DataColumnarValue::Scalar(_)) => {
                let data_value = DataValue::try_from_array(&result, 0)?;
                Ok(DataColumnarValue::Scalar(data_value))
            }
            _ => Ok(DataColumnarValue::Array(result))
        }
    }
}

impl fmt::Display for ArithmeticFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}({}, {})", self.op, self.left, self.right)
    }
}
