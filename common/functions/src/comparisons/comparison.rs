// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use common_datablocks::DataBlock;
use common_datavalues::DataArrayComparison;
use common_datavalues::DataColumnarValue;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_datavalues::DataValueComparisonOperator;
use common_exception::ErrorCodes;
use common_exception::Result;

use crate::comparisons::ComparisonEqFunction;
use crate::comparisons::ComparisonGtEqFunction;
use crate::comparisons::ComparisonGtFunction;
use crate::comparisons::ComparisonLtEqFunction;
use crate::comparisons::ComparisonLtFunction;
use crate::comparisons::ComparisonNotEqFunction;
use crate::{FactoryFuncRef, FunctionCtx};
use crate::IFunction;
use std::sync::Arc;

#[derive(Clone)]
pub struct ComparisonFunction {
    op: DataValueComparisonOperator
}

impl ComparisonFunction {
    pub fn register(map: FactoryFuncRef) -> Result<()> {
        let mut map = map.write();

        map.insert("=", ComparisonEqFunction::try_create_func);
        map.insert("<", ComparisonLtFunction::try_create_func);
        map.insert(">", ComparisonGtFunction::try_create_func);
        map.insert("<=", ComparisonLtEqFunction::try_create_func);
        map.insert(">=", ComparisonGtEqFunction::try_create_func);
        map.insert("!=", ComparisonNotEqFunction::try_create_func);
        map.insert("<>", ComparisonNotEqFunction::try_create_func);
        Ok(())
    }

    pub fn try_create_func(
        op: DataValueComparisonOperator,
        _ctx: Arc<dyn FunctionCtx>,
    ) -> Result<Box<dyn IFunction>> {
        Ok(Box::new(ComparisonFunction {
            op
        }))
    }
}

impl IFunction for ComparisonFunction {
    fn name(&self) -> &str {
        "ComparisonFunction"
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, columns: &[DataColumnarValue], input_rows: usize) -> Result<DataColumnarValue> {
        let result = DataArrayComparison::data_array_comparison_op(self.op.clone(), columns[0].as_ref(), columns[1].as_ref())?;

        match (columns[0].as_ref(), columns[1].as_ref()) {
            (DataColumnarValue::Constant(_), DataColumnarValue::Constant(_)) => {
                let data_value = DataValue::try_from_array(&result, 0)?;
                Ok(DataColumnarValue::Constant(data_value, input_rows))
            }
            _ => Ok(DataColumnarValue::Array(result))
        }
    }
}

impl fmt::Display for ComparisonFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} {} {}", self.left, self.op, self.right)
    }
}
