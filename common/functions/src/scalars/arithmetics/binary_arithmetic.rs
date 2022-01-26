// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt;
use std::sync::Arc;

use common_datavalues2::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::cast_with_type;
use crate::scalars::ArithmeticDivFunction;
use crate::scalars::ArithmeticMinusFunction;
use crate::scalars::ArithmeticMulFunction;
use crate::scalars::ArithmeticPlusFunction;
use crate::scalars::Function2;
use crate::scalars::Monotonicity;
use crate::scalars::ScalarBinaryExpression;
use crate::scalars::ScalarBinaryFunction;
use crate::scalars::DEFAULT_CAST_OPTIONS;

#[derive(Clone)]
pub struct BinaryArithmeticFunction<L: Scalar, R: Scalar, O: Scalar, F> {
    op: DataValueBinaryOperator,
    result_type: DataTypePtr,
    binary: ScalarBinaryExpression<L, R, O, F>,
}

impl<L, R, O, F> BinaryArithmeticFunction<L, R, O, F>
where
    L: Scalar + Send + Sync + Clone,
    R: Scalar + Send + Sync + Clone,
    O: Scalar + Send + Sync + Clone,
    F: ScalarBinaryFunction<L, R, O> + Send + Sync + Clone + 'static,
{
    pub fn try_create_func(
        op: DataValueBinaryOperator,
        result_type: DataTypePtr,
        func: F,
    ) -> Result<Box<dyn Function2>> {
        let binary = ScalarBinaryExpression::<L, R, O, _>::new(func);
        Ok(Box::new(Self {
            op,
            result_type,
            binary,
        }))
    }
}

impl<L, R, O, F> Function2 for BinaryArithmeticFunction<L, R, O, F>
where
    L: Scalar + Send + Sync + Clone,
    R: Scalar + Send + Sync + Clone,
    O: Scalar + Send + Sync + Clone,
    F: ScalarBinaryFunction<L, R, O> + Send + Sync + Clone,
{
    fn name(&self) -> &str {
        "BinaryArithmeticFunction"
    }

    fn return_type(&self, _args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        Ok(self.result_type.clone())
    }

    fn eval(&self, columns: &ColumnsWithField, _input_rows: usize) -> Result<ColumnRef> {
        let col = self.binary.eval(columns[0].column(), columns[1].column())?;
        let col_type = col.data_type();
        let result: ColumnRef = Arc::new(col);
        if col_type != self.result_type {
            return cast_with_type(&result, &col_type, &self.result_type, &DEFAULT_CAST_OPTIONS);
        }
        Ok(result)
    }

    fn get_monotonicity(&self, args: &[Monotonicity]) -> Result<Monotonicity> {
        if args.len() != 2 {
            return Err(ErrorCode::BadArguments(format!(
                "Invalid argument lengths {} for get_monotonicity",
                args.len()
            )));
        }

        match self.op {
            DataValueBinaryOperator::Plus => ArithmeticPlusFunction::get_monotonicity(args),
            DataValueBinaryOperator::Minus => ArithmeticMinusFunction::get_monotonicity(args),
            DataValueBinaryOperator::Mul => ArithmeticMulFunction::get_monotonicity(args),
            DataValueBinaryOperator::Div => ArithmeticDivFunction::get_monotonicity(args),
            _ => unreachable!(),
        }
    }
}

impl<L, R, O, F> fmt::Display for BinaryArithmeticFunction<L, R, O, F>
where
    L: Scalar + Send + Sync + Clone,
    R: Scalar + Send + Sync + Clone,
    O: Scalar + Send + Sync + Clone,
    F: ScalarBinaryFunction<L, R, O> + Send + Sync + Clone,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.op)
    }
}
