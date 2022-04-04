// Copyright 2022 Datafuse Labs.
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

use std::marker::PhantomData;

use common_datavalues::prelude::*;
use common_exception::Result;

use super::xor::LogicXorFunction;
use super::LogicAndFunction;
use super::LogicNotFunction;
use super::LogicOrFunction;
use crate::scalars::Function;
use crate::scalars::FunctionContext;
use crate::scalars::FunctionFactory;

#[derive(Clone)]
pub struct LogicFunction;

impl LogicFunction {
    pub fn register(factory: &mut FunctionFactory) {
        factory.register_typed("and", LogicAndFunction::desc());
        factory.register_typed("or", LogicOrFunction::desc());
        factory.register_typed("not", LogicNotFunction::desc());
        factory.register_typed("xor", LogicXorFunction::desc());
    }
}

#[derive(Clone, Debug)]
pub enum LogicOperator {
    Not,
    And,
    Or,
    Xor,
}

#[derive(Clone)]
pub struct LogicFunctionImpl<F> {
    op: LogicOperator,
    nullable: bool,
    f: PhantomData<F>,
}

pub trait LogicExpression: Sync + Send {
    fn eval(columns: &ColumnsWithField, input_rows: usize, _nullable: bool) -> Result<ColumnRef>;
}

impl<F> LogicFunctionImpl<F>
where F: LogicExpression + Clone + 'static
{
    pub fn try_create(op: LogicOperator, args: &[&DataTypePtr]) -> Result<Box<dyn Function>> {
        let nullable = match op {
            LogicOperator::And | LogicOperator::Or
                if args[0].is_nullable()
                    || args[1].is_nullable()
                    || args[0].is_null()
                    || args[1].is_null() =>
            {
                true
            }
            _ => false,
        };

        Ok(Box::new(Self {
            op,
            nullable,
            f: PhantomData,
        }))
    }
}

impl<F> Function for LogicFunctionImpl<F>
where F: LogicExpression + Clone
{
    fn name(&self) -> &str {
        "LogicFunction"
    }

    fn return_type(&self, _args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        if self.nullable {
            Ok(NullableType::arc(BooleanType::arc()))
        } else {
            Ok(BooleanType::arc())
        }
    }

    fn eval(
        &self,
        columns: &ColumnsWithField,
        input_rows: usize,
        _func_ctx: FunctionContext,
    ) -> Result<ColumnRef> {
        F::eval(columns, input_rows, self.nullable)
    }
}

impl<F> std::fmt::Display for LogicFunctionImpl<F>
where F: LogicExpression
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.op)
    }
}
