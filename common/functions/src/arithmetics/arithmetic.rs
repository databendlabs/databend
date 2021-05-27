// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use common_datavalues::DataArrayArithmetic;
use common_datavalues::DataColumnarValue;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_datavalues::DataValueArithmeticOperator;
use common_exception::Result;

use crate::arithmetics::ArithmeticDivFunction;
use crate::arithmetics::ArithmeticMinusFunction;
use crate::arithmetics::ArithmeticModuloFunction;
use crate::arithmetics::ArithmeticMulFunction;
use crate::arithmetics::ArithmeticPlusFunction;
use crate::FactoryFuncRef;
use crate::IFunction;

#[derive(Clone)]
pub struct ArithmeticFunction {
    depth: usize,
    op: DataValueArithmeticOperator
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

    pub fn try_create_func(op: DataValueArithmeticOperator) -> Result<Box<dyn IFunction>> {
        Ok(Box::new(ArithmeticFunction { depth: 0, op }))
    }
}

impl IFunction for ArithmeticFunction {
    fn name(&self) -> &str {
        "ArithmeticFunction"
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        if args.len() == 1 {
            return Ok(args[0].clone());
        }
        common_datavalues::numerical_arithmetic_coercion(&self.op, &args[0], &args[1])
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, columns: &[DataColumnarValue], input_rows: usize) -> Result<DataColumnarValue> {
        if columns.len() == 1 {
            let result =
                DataArrayArithmetic::data_array_unary_arithmetic_op(self.op.clone(), &columns[0])?;
            match &columns[0] {
                DataColumnarValue::Constant(_, _) => {
                    let data_value = DataValue::try_from_array(&result, 0)?;
                    Ok(DataColumnarValue::Constant(data_value, input_rows))
                }
                _ => Ok(DataColumnarValue::Array(result))
            }
        } else {
            let result = DataArrayArithmetic::data_array_arithmetic_op(
                self.op.clone(),
                &columns[0],
                &columns[1]
            )?;

            match (&columns[0], &columns[1]) {
                (DataColumnarValue::Constant(_, _), DataColumnarValue::Constant(_, _)) => {
                    let data_value = DataValue::try_from_array(&result, 0)?;
                    Ok(DataColumnarValue::Constant(data_value, input_rows))
                }
                _ => Ok(DataColumnarValue::Array(result))
            }
        }
    }
}

impl fmt::Display for ArithmeticFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.op)
    }
}
