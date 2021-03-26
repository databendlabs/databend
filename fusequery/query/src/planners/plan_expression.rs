// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use crate::common_datavalues::{DataField, DataSchemaRef, DataValue};
use crate::common_functions::{
    AliasFunction, ColumnFunction, FunctionFactory, IFunction, LiteralFunction,
};
use crate::error::FuseQueryResult;

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub enum ExpressionPlan {
    /// An expression with a alias name.
    Alias(String, Box<ExpressionPlan>),
    /// Column name.
    Column(String),
    /// Constant value.
    Literal(DataValue),
    /// A binary expression such as "age > 40"
    BinaryExpression {
        left: Box<ExpressionPlan>,
        op: String,
        right: Box<ExpressionPlan>,
    },
    /// Functions with a set of arguments.
    Function {
        op: String,
        args: Vec<ExpressionPlan>,
    },
    /// All fields(*) in a schema.
    Wildcard,
}

impl ExpressionPlan {
    fn to_function_with_depth(&self, depth: usize) -> FuseQueryResult<Box<dyn IFunction>> {
        match self {
            ExpressionPlan::Column(ref v) => Ok(ColumnFunction::try_create(v.as_str())?),
            ExpressionPlan::Literal(ref v) => {
                let field_value = v.to_field_value();
                Ok(LiteralFunction::try_create(field_value)?)
            }
            ExpressionPlan::BinaryExpression { left, op, right } => {
                let l = left.to_function_with_depth(depth)?;
                let r = right.to_function_with_depth(depth + 1)?;
                let mut func = FunctionFactory::get(op, &[l, r])?;
                func.set_depth(depth);
                Ok(func)
            }
            ExpressionPlan::Function { op, args } => {
                let mut funcs = Vec::with_capacity(args.len());
                for arg in args {
                    let mut func = arg.to_function_with_depth(depth + 1)?;
                    func.set_depth(depth);
                    funcs.push(func);
                }
                let mut func = FunctionFactory::get(op, &funcs)?;
                func.set_depth(depth);
                Ok(func)
            }
            ExpressionPlan::Alias(alias, expr) => {
                let mut func = expr.to_function_with_depth(depth)?;
                func.set_depth(depth);
                Ok(AliasFunction::try_create(alias.clone(), func)?)
            }
            ExpressionPlan::Wildcard => Ok(ColumnFunction::try_create("*")?),
        }
    }

    pub fn to_function(&self) -> FuseQueryResult<Box<dyn IFunction>> {
        self.to_function_with_depth(0)
    }

    pub fn to_data_field(&self, input_schema: &DataSchemaRef) -> FuseQueryResult<DataField> {
        let func = self.to_function()?;
        Ok(DataField::new(
            format!("{}", func).as_str(),
            func.return_type(&input_schema)?,
            func.nullable(&input_schema)?,
        ))
    }

    pub fn has_aggregator(&self) -> FuseQueryResult<bool> {
        Ok(self.to_function()?.is_aggregator())
    }
}

impl fmt::Debug for ExpressionPlan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ExpressionPlan::Alias(alias, v) => write!(f, "{:?} as {:#}", v, alias),
            ExpressionPlan::Column(ref v) => write!(f, "{:#}", v),
            ExpressionPlan::Literal(ref v) => write!(f, "{:#}", v),
            ExpressionPlan::BinaryExpression { left, op, right } => {
                write!(f, "({:?} {} {:?})", left, op, right,)
            }
            ExpressionPlan::Function { op, args } => write!(f, "{}({:?})", op, args),
            ExpressionPlan::Wildcard => write!(f, "*"),
        }
    }
}
