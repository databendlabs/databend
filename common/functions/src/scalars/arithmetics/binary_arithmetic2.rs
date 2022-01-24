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
use std::ops::Add;

use common_datavalues2::prelude::*;
use common_exception::Result;
use num::traits::AsPrimitive;
use num_traits::WrappingAdd;

use crate::scalars::Function2;
use crate::scalars::ScalarBinaryFunction;
use crate::scalars::ScalarExpression;

#[derive(Clone)]
pub struct BinaryArithmeticFunction2 {
    op: DataValueBinaryOperator,
    result_type: DataTypePtr,
    binary: Box<dyn ScalarExpression>,
}

impl BinaryArithmeticFunction2 {
    pub fn try_create_func(
        op: DataValueBinaryOperator,
        result_type: DataTypePtr,
        binary: Box<dyn ScalarExpression>,
    ) -> Result<Box<dyn Function2>> {
        Ok(Box::new(Self {
            op,
            result_type,
            binary,
        }))
    }
}

impl Function2 for BinaryArithmeticFunction2 {
    fn name(&self) -> &str {
        "BinaryArithmeticFunction2"
    }

    fn return_type(&self, _args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        Ok(self.result_type.clone())
    }

    fn eval(&self, _columns: &ColumnsWithField, _input_rows: usize) -> Result<ColumnRef> {
        todo!()
    }
}

impl fmt::Display for BinaryArithmeticFunction2 {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.op)
    }
}

#[derive(Clone, Debug)]
struct AddFunction {}

impl<L, R, O> ScalarBinaryFunction<L, R, O> for AddFunction
where
    L: PrimitiveType + AsPrimitive<O>,
    R: PrimitiveType + AsPrimitive<O>,
    O: PrimitiveType + Add<Output = O>,
{
    fn eval(&self, l: L::RefType<'_>, r: R::RefType<'_>) -> O {
        l.to_owned_scalar().as_() + r.to_owned_scalar().as_()
    }
}

#[derive(Clone, Debug)]
struct WrappingAddFunction {}

impl<L, R, O> ScalarBinaryFunction<L, R, O> for WrappingAddFunction
where
    L: IntegerType + AsPrimitive<O>,
    R: IntegerType + AsPrimitive<O>,
    O: IntegerType + WrappingAdd<Output = O>,
{
    fn eval(&self, l: L::RefType<'_>, r: R::RefType<'_>) -> O {
        l.to_owned_scalar()
            .as_()
            .wrapping_add(&r.to_owned_scalar().as_())
    }
}
