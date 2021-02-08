// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::fmt;

use crate::contexts::FuseQueryContextRef;
use crate::datavalues::{DataField, DataSchemaRef, DataValue};
use crate::error::FuseQueryResult;
use crate::functions::{
    AliasFunction, ConstantFunction, FieldFunction, FunctionFactory, IFunction,
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
    fn to_function_with_depth(
        &self,
        ctx: FuseQueryContextRef,
        depth: usize,
    ) -> FuseQueryResult<Box<dyn IFunction>> {
        match self {
            ExpressionPlan::Field(ref v) => FieldFunction::try_create(v.as_str()),
            ExpressionPlan::Constant(ref v) => {
                let field_value = v.to_field_value();
                ConstantFunction::try_create(field_value)
            }
            ExpressionPlan::BinaryExpression { left, op, right } => {
                let l = left.to_function_with_depth(ctx.clone(), depth)?;
                let r = right.to_function_with_depth(ctx.clone(), depth + 1)?;
                let mut func = FunctionFactory::get(ctx, op, &[l, r])?;
                func.set_depth(depth);
                Ok(func)
            }
            ExpressionPlan::Function { op, args } => {
                let mut funcs = Vec::with_capacity(args.len());
                for arg in args {
                    let mut func = arg.to_function_with_depth(ctx.clone(), depth + 1)?;
                    func.set_depth(depth);
                    funcs.push(func);
                }
                let mut func = FunctionFactory::get(ctx, op, &funcs)?;
                func.set_depth(depth);
                Ok(func)
            }
            ExpressionPlan::Alias(alias, expr) => {
                let mut func = expr.to_function_with_depth(ctx, depth)?;
                func.set_depth(depth);
                AliasFunction::try_create(alias.clone(), func)
            }
            ExpressionPlan::Wildcard => FieldFunction::try_create("*"),
        }
    }

    pub fn to_function(&self, ctx: FuseQueryContextRef) -> FuseQueryResult<Box<dyn IFunction>> {
        self.to_function_with_depth(ctx, 0)
    }

    pub fn to_field(
        &self,
        ctx: FuseQueryContextRef,
        input_schema: &DataSchemaRef,
    ) -> FuseQueryResult<DataField> {
        let func = self.to_function(ctx)?;
        Ok(DataField::new(
            format!("{}", func).as_str(),
            func.return_type(&input_schema)?,
            func.nullable(&input_schema)?,
        ))
    }

    pub fn has_aggregator(&self, ctx: FuseQueryContextRef) -> FuseQueryResult<bool> {
        Ok(self.to_function(ctx)?.is_aggregator())
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
