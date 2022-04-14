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
use std::marker::PhantomData;
use std::sync::Arc;

use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::scalar_binary_op;
use crate::scalars::ArithmeticDivFunction;
use crate::scalars::ArithmeticMinusFunction;
use crate::scalars::ArithmeticMulFunction;
use crate::scalars::ArithmeticPlusFunction;
use crate::scalars::EvalContext;
use crate::scalars::Function;
use crate::scalars::FunctionContext;
use crate::scalars::Monotonicity;

#[derive(Clone)]
pub struct BinaryArithmeticFunction<L: Scalar, R: Scalar, O: Scalar, F> {
    op: DataValueBinaryOperator,
    result_type: DataTypePtr,
    func: F,
    _phantom: PhantomData<(L, R, O)>,
}

impl<L, R, O, F> BinaryArithmeticFunction<L, R, O, F>
where
    L: Scalar + Send + Sync + Clone,
    R: Scalar + Send + Sync + Clone,
    O: Scalar + Send + Sync + Clone,
    F: Fn(L::RefType<'_>, R::RefType<'_>, &mut EvalContext) -> O + Send + Sync + Clone + 'static,
{
    pub fn try_create_func(
        op: DataValueBinaryOperator,
        result_type: DataTypePtr,
        func: F,
    ) -> Result<Box<dyn Function>> {
        Ok(Box::new(Self {
            op,
            result_type,
            func,
            _phantom: PhantomData,
        }))
    }
}

impl<L, R, O, F> Function for BinaryArithmeticFunction<L, R, O, F>
where
    L: Scalar + Send + Sync + Clone,
    R: Scalar + Send + Sync + Clone,
    O: Scalar + Send + Sync + Clone,
    F: Fn(L::RefType<'_>, R::RefType<'_>, &mut EvalContext) -> O + Send + Sync + Clone,
{
    fn name(&self) -> &str {
        "BinaryArithmeticFunction"
    }

    fn return_type(&self) -> DataTypePtr {
        self.result_type.clone()
    }

    fn eval(
        &self,
        columns: &ColumnsWithField,
        _input_rows: usize,
        _func_ctx: FunctionContext,
    ) -> Result<ColumnRef> {
        let col = scalar_binary_op(
            columns[0].column(),
            columns[1].column(),
            self.func.clone(),
            &mut EvalContext::default(),
        )?;
        Ok(Arc::new(col))
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
            _ => Ok(Monotonicity::default()),
        }
    }
}

impl<L, R, O, F> fmt::Display for BinaryArithmeticFunction<L, R, O, F>
where
    L: Scalar + Send + Sync + Clone,
    R: Scalar + Send + Sync + Clone,
    O: Scalar + Send + Sync + Clone,
    F: Fn(L::RefType<'_>, R::RefType<'_>, &mut EvalContext) -> O + Send + Sync + Clone,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.op)
    }
}
