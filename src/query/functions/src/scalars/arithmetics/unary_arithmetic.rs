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

use crate::scalars::scalar_unary_op;
use crate::scalars::ArithmeticNegateFunction;
use crate::scalars::EvalContext;
use crate::scalars::Function;
use crate::scalars::FunctionContext;
use crate::scalars::Monotonicity;

#[derive(Clone)]
pub struct UnaryArithmeticFunction<L: Scalar, O: Scalar, F> {
    op: DataValueUnaryOperator,
    result_type: DataTypeImpl,
    func: F,
    _phantom: PhantomData<(L, O)>,
}

impl<L, O, F> UnaryArithmeticFunction<L, O, F>
where
    L: Scalar + Send + Sync + Clone,
    O: Scalar + Send + Sync + Clone,
    F: Fn(L::RefType<'_>, &mut EvalContext) -> O + Send + Sync + Clone + 'static,
{
    pub fn try_create_func(
        op: DataValueUnaryOperator,
        result_type: DataTypeImpl,
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

impl<L, O, F> Function for UnaryArithmeticFunction<L, O, F>
where
    L: Scalar + Send + Sync + Clone,
    O: Scalar + Send + Sync + Clone,
    F: Fn(L::RefType<'_>, &mut EvalContext) -> O + Send + Sync + Clone,
{
    fn name(&self) -> &str {
        "UnaryArithmeticFunction"
    }

    fn return_type(&self) -> DataTypeImpl {
        self.result_type.clone()
    }

    fn eval(
        &self,
        _func_ctx: FunctionContext,
        columns: &ColumnsWithField,
        _input_rows: usize,
    ) -> Result<ColumnRef> {
        let col = scalar_unary_op(
            columns[0].column(),
            self.func.clone(),
            &mut EvalContext::default(),
        )?;
        Ok(Arc::new(col))
    }

    fn get_monotonicity(&self, args: &[Monotonicity]) -> Result<Monotonicity> {
        if args.len() != 1 {
            return Err(ErrorCode::BadArguments(format!(
                "Invalid argument lengths {} for get_monotonicity",
                args.len()
            )));
        }

        match self.op {
            DataValueUnaryOperator::Negate => ArithmeticNegateFunction::get_monotonicity(args),
        }
    }
}

impl<L, O, F> fmt::Display for UnaryArithmeticFunction<L, O, F>
where
    L: Scalar + Send + Sync + Clone,
    O: Scalar + Send + Sync + Clone,
    F: Fn(L::RefType<'_>, &mut EvalContext) -> O + Send + Sync + Clone,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.op)
    }
}
