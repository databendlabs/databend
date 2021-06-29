// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use common_datavalues::columns::DataColumn;
use common_datavalues::prelude::*;
use common_datavalues::DataSchema;
use common_datavalues::DataValueArithmeticOperator;
use common_exception::Result;

use crate::arithmetics::ArithmeticDivFunction;
use crate::arithmetics::ArithmeticMinusFunction;
use crate::arithmetics::ArithmeticModuloFunction;
use crate::arithmetics::ArithmeticMulFunction;
use crate::arithmetics::ArithmeticPlusFunction;
use crate::FactoryFuncRef;
use crate::Function;

#[derive(Clone)]
pub struct ArithmeticFunction {
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

    pub fn try_create_func(op: DataValueArithmeticOperator) -> Result<Box<dyn Function>> {
        Ok(Box::new(ArithmeticFunction { op }))
    }
}

impl Function for ArithmeticFunction {
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

    fn eval(&self, columns: &[DataColumn], _input_rows: usize) -> Result<DataColumn> {
        match columns.len() {
            1 => std::ops::Neg::neg(&columns[0]),
            _ => columns[0].arithmetic(self.op.clone(), &columns[1]),
        }
    }

    fn num_arguments(&self) -> usize {
        0
    }

    fn variadic_arguments(&self) -> Option<(usize, usize)> {
        Some((1, 2))
    }
}

impl fmt::Display for ArithmeticFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.op)
    }
}
