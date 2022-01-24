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
use std::sync::Arc;

use common_datavalues2::prelude::*;
use common_exception::Result;
use num::traits::AsPrimitive;
use num_traits::WrappingAdd;

use crate::scalars::Arithmetic2Description;
use crate::scalars::Function2;
use crate::scalars::ScalarBinaryExpression;
use crate::scalars::ScalarBinaryFunction;

#[derive(Clone)]
pub struct BinaryArithmeticFunction2<L: Scalar, R: Scalar, O: Scalar, F> {
    op: DataValueBinaryOperator,
    result_type: DataTypePtr,
    binary: ScalarBinaryExpression<L, R, O, F>,
}

impl<L, R, O, F> BinaryArithmeticFunction2<L, R, O, F>
where
    L: Scalar + Send + Sync + Clone,
    R: Scalar + Send + Sync + Clone,
    O: Scalar + Send + Sync + Clone,
    F: ScalarBinaryFunction<L, R, O> + Send + Sync + Clone + 'static,
{
    pub fn try_create_func(
        op: DataValueBinaryOperator,
        result_type: DataTypePtr,
        binary: ScalarBinaryExpression<L, R, O, F>,
    ) -> Result<Box<dyn Function2>> {
        Ok(Box::new(Self {
            op,
            result_type,
            binary,
        }))
    }
}

impl<L, R, O, F> Function2 for BinaryArithmeticFunction2<L, R, O, F>
where
    L: Scalar + Send + Sync + Clone,
    R: Scalar + Send + Sync + Clone,
    O: Scalar + Send + Sync + Clone,
    F: ScalarBinaryFunction<L, R, O> + Send + Sync + Clone,
{
    fn name(&self) -> &str {
        "BinaryArithmeticFunction2"
    }

    fn return_type(&self, _args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        Ok(self.result_type.clone())
    }

    fn eval(&self, columns: &ColumnsWithField, _input_rows: usize) -> Result<ColumnRef> {
        let col = self.binary.eval(columns[0].column(), columns[1].column())?;
        Ok(Arc::new(col))
    }
}

impl<L, R, O, F> fmt::Display for BinaryArithmeticFunction2<L, R, O, F>
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

pub struct ArithmeticPlusFunction2;

impl ArithmeticPlusFunction2 {
    pub fn try_create_func(
        _display_name: &str,
        _args: &[&DataTypePtr],
    ) -> Result<Box<dyn Function2>> {
        todo!()
    }

    pub fn desc() -> Arithmetic2Description {
        todo!()
    }
}
