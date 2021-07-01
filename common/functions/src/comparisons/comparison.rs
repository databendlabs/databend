// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use common_datavalues::columns::DataColumn;
use common_datavalues::prelude::*;
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
use crate::Function;

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

    pub fn try_create_func(op: DataValueComparisonOperator) -> Result<Box<dyn Function>> {
        Ok(Box::new(ComparisonFunction { op }))
    }
}

impl Function for ComparisonFunction {
    fn name(&self) -> &str {
        "ComparisonFunction"
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, columns: &[DataColumn], _input_rows: usize) -> Result<DataColumn> {
        columns[0].compare(self.op.clone(), &columns[1])
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
