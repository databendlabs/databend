// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use common_exception::{Result, ErrorCodes};
use common_datablocks::DataBlock;
use common_datavalues::DataArrayArithmetic;
use common_datavalues::DataColumnarValue;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_datavalues::DataValueArithmetic;
use common_datavalues::DataValueArithmeticOperator;

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
    op: DataValueArithmeticOperator,
    left: Box<dyn IFunction>,
    right: Box<dyn IFunction>
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
        args: &[Box<dyn IFunction>]
    ) -> Result<Box<dyn IFunction>> {
        match args.len() {
            2 => {
                Ok(Box::new(ArithmeticFunction {
                    depth: 0,
                    op,
                    left: args[0].clone(),
                    right: args[1].clone(),
                }))
            }
            _ => Result::Err(ErrorCodes::BadArguments(
                format!(
                    "Function Error: Arithmetic function {} args length must be 2",
                    op
                )
            ))
        }
    }
}

impl IFunction for ArithmeticFunction {
    fn name(&self) -> &str {
        "ArithmeticFunction"
    }

    fn return_type(&self, input_schema: &DataSchema) -> Result<DataType> {
        common_datavalues::numerical_arithmetic_coercion(
            &self.op,
            &self.left.return_type(input_schema)?,
            &self.right.return_type(input_schema)?,
        ).map_err(ErrorCodes::from_anyhow)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, block: &DataBlock) -> Result<DataColumnarValue> {
        let left = &self.left.eval(block)?;
        let right = &self.right.eval(block)?;
        let result = DataArrayArithmetic::data_array_arithmetic_op(self.op.clone(), left, right).map_err(ErrorCodes::from_anyhow)?;

        match (left, right) {
            (DataColumnarValue::Scalar(_), DataColumnarValue::Scalar(_)) => {
                let data_value = DataValue::try_from_array(&result, 0).map_err(ErrorCodes::from_anyhow)?;
                Ok(DataColumnarValue::Scalar(data_value))
            }
            _ => Ok(DataColumnarValue::Array(result))
        }
    }

    fn set_depth(&mut self, depth: usize) {
        self.left.set_depth(depth);
        self.right.set_depth(depth + 1);
        self.depth = depth;
    }

    fn accumulate(&mut self, block: &DataBlock) -> Result<()> {
        self.left.accumulate(&block)?;
        self.right.accumulate(&block)
    }

    fn accumulate_result(&self) -> Result<Vec<DataValue>> {
        Ok([
            &self.left.accumulate_result()?[..],
            &self.right.accumulate_result()?[..]
        ]
        .concat())
    }

    fn merge(&mut self, states: &[DataValue]) -> Result<()> {
        self.left.merge(states)?;
        self.right.merge(states)
    }

    fn merge_result(&self) -> Result<DataValue> {
        DataValueArithmetic::data_value_arithmetic_op(
            self.op.clone(),
            self.left.merge_result()?,
            self.right.merge_result()?,
        )
    }

    fn is_aggregator(&self) -> bool {
        self.left.is_aggregator() || self.right.is_aggregator()
    }
}

impl fmt::Display for ArithmeticFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}({}, {})", self.op, self.left, self.right)
    }
}
