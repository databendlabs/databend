// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use common_datavalues::DataArrayComparison;
use common_datavalues::DataColumnarValue;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_datavalues::DataValueComparisonOperator;
use common_exception::Result;

use crate::comparisons::ComparisonEqFunction;
use crate::comparisons::ComparisonGtEqFunction;
use crate::comparisons::ComparisonGtFunction;
use crate::comparisons::ComparisonLikeFunction;
use crate::comparisons::ComparisonLtEqFunction;
use crate::comparisons::ComparisonLtFunction;
use crate::comparisons::ComparisonNotEqFunction;
use crate::comparisons::ComparisonNotLikeFunction;
use crate::FactoryFuncRef;
use crate::IFunction;

#[derive(Clone)]
pub struct ComparisonFunction {
    op: DataValueComparisonOperator,
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
        map.insert("like", ComparisonLikeFunction::try_create_func);
        map.insert("not like", ComparisonNotLikeFunction::try_create_func);
        Ok(())
    }

    pub fn try_create_func(op: DataValueComparisonOperator) -> Result<Box<dyn IFunction>> {
        Ok(Box::new(ComparisonFunction { op }))
    }
}

impl IFunction for ComparisonFunction {
    fn name(&self) -> &str {
        "ComparisonFunction"
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, columns: &[DataColumnarValue], _input_rows: usize) -> Result<DataColumnarValue> {
        let result = DataArrayComparison::data_array_comparison_op(
            self.op.clone(),
            &columns[0],
            &columns[1],
        )?;

        Ok(result.into())
    }

    fn num_arguments(&self) -> usize {
        2
    }
}

impl fmt::Display for ComparisonFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.op)
    }
}
