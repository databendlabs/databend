// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::fmt;

use crate::datavalues::{DataField, DataSchemaRef, DataValue};
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::functions::{
    AliasFunction, ConstantFunction, FieldFunction, Function, ScalarFunctionFactory,
};

#[derive(Clone)]
pub enum ExpressionPlan {
    Alias(String, Box<ExpressionPlan>),
    Field(String),
    Constant(DataValue),
    BinaryExpression {
        left: Box<ExpressionPlan>,
        op: String,
        right: Box<ExpressionPlan>,
    },
    Function {
        op: String,
        args: Vec<ExpressionPlan>,
    },
    Wildcard,
}

impl ExpressionPlan {
    pub fn to_field(&self, input_schema: &DataSchemaRef) -> FuseQueryResult<DataField> {
        let func = self.to_function()?;
        Ok(DataField::new(
            format!("{:?}", func).as_str(),
            func.return_type(&input_schema)?,
            func.nullable(&input_schema)?,
        ))
    }

    fn plan_to_function(&self, depth: usize) -> FuseQueryResult<Function> {
        match self {
            ExpressionPlan::Field(ref v) => FieldFunction::try_create(v.as_str()),
            ExpressionPlan::Constant(ref v) => ConstantFunction::try_create(v.clone()),
            ExpressionPlan::BinaryExpression { left, op, right } => {
                let l = left.plan_to_function(depth)?;
                let r = right.plan_to_function(depth + 1)?;
                let mut func = ScalarFunctionFactory::get(op, &[l, r])?;
                func.set_depth(depth);
                Ok(func)
            }
            ExpressionPlan::Function { op, args } => {
                let mut funcs = Vec::with_capacity(args.len());
                for arg in args {
                    let mut func = arg.plan_to_function(depth + 1)?;
                    func.set_depth(depth);
                    funcs.push(func);
                }
                let mut func = ScalarFunctionFactory::get(op, &funcs)?;
                func.set_depth(depth);
                Ok(func)
            }
            ExpressionPlan::Alias(alias, expr) => {
                let mut func = expr.plan_to_function(depth)?;
                func.set_depth(depth);
                AliasFunction::try_create(alias.clone(), func)
            }
            ExpressionPlan::Wildcard => Err(FuseQueryError::Internal(
                "Cannot transform wildcard to function".to_string(),
            )),
        }
    }

    pub fn to_function(&self) -> FuseQueryResult<Function> {
        self.plan_to_function(0)
    }

    pub fn is_aggregate(&self) -> bool {
        match self {
            ExpressionPlan::Alias(_, expr) => expr.is_aggregate(),
            ExpressionPlan::BinaryExpression { left, right, .. } => {
                left.is_aggregate() || right.is_aggregate()
            }
            ExpressionPlan::Function { op, .. } => matches!(
                op.to_lowercase().as_str(),
                "max" | "min" | "avg" | "count" | "sum"
            ),
            _ => false,
        }
    }
}

impl fmt::Debug for ExpressionPlan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ExpressionPlan::Alias(alias, v) => write!(f, "{:?} as {:#}", v, alias),
            ExpressionPlan::Field(ref v) => write!(f, "{:#}", v),
            ExpressionPlan::Constant(ref v) => write!(f, "{:#}", v),
            ExpressionPlan::BinaryExpression { left, op, right } => {
                write!(f, "({:?} {} {:?})", left, op, right,)
            }
            ExpressionPlan::Function { op, args } => write!(f, "{}({:?})", op, args),
            ExpressionPlan::Wildcard => write!(f, "*"),
        }
    }
}
