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

use std::collections::HashMap;
use std::collections::HashSet;

use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::scalars::FunctionFactory;

use crate::Expression;
use crate::ExpressionVisitor;
use crate::Recursion;

/// Resolves an `Expression::Wildcard` to a collection of `Expression::Column`'s.
pub fn expand_wildcard(expr: &Expression, schema: &DataSchemaRef) -> Vec<Expression> {
    match expr {
        Expression::Wildcard => schema
            .fields()
            .iter()
            .map(|f| Expression::Column(f.name().to_string()))
            .collect::<Vec<Expression>>(),
        _ => vec![expr.clone()],
    }
}

/// Collect all deeply nested `Expression::AggregateFunction` and
/// `Expression::AggregateUDF`. They are returned in order of occurrence (depth
/// first), with duplicates omitted.
pub fn find_aggregate_exprs(exprs: &[Expression]) -> Vec<Expression> {
    find_exprs_in_exprs(exprs, &|nest_exprs| {
        matches!(nest_exprs, Expression::AggregateFunction { .. })
    })
}

pub fn find_aggregate_exprs_in_expr(expr: &Expression) -> Vec<Expression> {
    find_exprs_in_expr(expr, &|nest_exprs| {
        matches!(nest_exprs, Expression::AggregateFunction { .. })
    })
}

/// Collect all arguments from aggregation function and append to this exprs
/// [ColumnExpr(b), Aggr(sum(a, b))] ---> [ColumnExpr(b), ColumnExpr(a)]

pub fn expand_aggregate_arg_exprs(exprs: &[Expression]) -> Vec<Expression> {
    let mut res = vec![];
    for expr in exprs {
        match expr {
            Expression::AggregateFunction { args, .. } => {
                for arg in args {
                    if !res.contains(arg) {
                        res.push(arg.clone());
                    }
                }
            }
            _ => {
                if !res.contains(expr) {
                    res.push(expr.clone());
                }
            }
        }
    }
    res
}

/// Collect all deeply nested `Expression::Column`'s. They are returned in order of
/// appearance (depth first), with duplicates omitted.
pub fn find_column_exprs(exprs: &[Expression]) -> Vec<Expression> {
    find_exprs_in_exprs(exprs, &|nest_exprs| {
        matches!(nest_exprs, Expression::Column(_))
    })
}

/// Search the provided `Expression`'s, and all of their nested `Expression`, for any that
/// pass the provided test. The returned `Expression`'s are deduplicated and returned
/// in order of appearance (depth first).
fn find_exprs_in_exprs<F>(exprs: &[Expression], test_fn: &F) -> Vec<Expression>
where F: Fn(&Expression) -> bool {
    exprs
        .iter()
        .flat_map(|expr| find_exprs_in_expr(expr, test_fn))
        .fold(vec![], |mut acc, expr| {
            if !acc.contains(&expr) {
                acc.push(expr)
            }
            acc
        })
}

// Visitor that find Expressions that match a particular predicate
struct Finder<'a, F>
where F: Fn(&Expression) -> bool
{
    test_fn: &'a F,
    exprs: Vec<Expression>,
}

impl<'a, F> Finder<'a, F>
where F: Fn(&Expression) -> bool
{
    /// Create a new finder with the `test_fn`
    fn new(test_fn: &'a F) -> Self {
        Self {
            test_fn,
            exprs: Vec::new(),
        }
    }
}

impl<'a, F> ExpressionVisitor for Finder<'a, F>
where F: Fn(&Expression) -> bool
{
    fn pre_visit(mut self, expr: &Expression) -> Result<Recursion<Self>> {
        if (self.test_fn)(expr) {
            if !(self.exprs.contains(expr)) {
                self.exprs.push(expr.clone())
            }
            // stop recursing down this expr once we find a match
            return Ok(Recursion::Stop(self));
        }

        Ok(Recursion::Continue(self))
    }
}

/// Search an `Expression`, and all of its nested `Expression`'s, for any that pass the
/// provided test. The returned `Expression`'s are deduplicated and returned in order
/// of appearance (depth first).
fn find_exprs_in_expr<F>(expr: &Expression, test_fn: &F) -> Vec<Expression>
where F: Fn(&Expression) -> bool {
    let Finder { exprs, .. } = expr
        .accept(Finder::new(test_fn))
        // pre_visit always returns OK, so this will always too
        .expect("no way to return error during recursion");

    exprs
}

/// Convert any `Expression` to an `Expression::Column`.
pub fn expr_as_column_expr(expr: &Expression) -> Result<Expression> {
    match expr {
        Expression::Column(_) => Ok(expr.clone()),
        _ => Ok(Expression::Column(expr.column_name())),
    }
}

/// Rebuilds an `expr` as a projection on top of a collection of `Expression`'s.
///
/// For example, the Expression `a + b < 1` would require, as input, the 2
/// individual columns, `a` and `b`. But, if the base exprs already
/// contain the `a + b` result, then that may be used in lieu of the `a` and
/// `b` columns.
///
/// This is useful in the context of a query like:
///
/// SELECT a + b < 1 ... GROUP BY a + b
///
/// where post-aggregation, `a + b` need not be a projection against the
/// individual columns `a` and `b`, but rather it is a projection against the
/// `a + b` found in the GROUP BY.
pub fn rebase_expr(expr: &Expression, base_exprs: &[Expression]) -> Result<Expression> {
    clone_with_replacement(expr, &|nest_exprs| {
        if base_exprs.contains(nest_exprs) {
            Ok(Some(expr_as_column_expr(nest_exprs)?))
        } else {
            Ok(None)
        }
    })
}

// Rebuilds an `expr` to ColumnExpr when some expressions already processed in upstream
// Skip Sort, Alias because we can go into the inner nest_exprs
pub fn rebase_expr_from_input(expr: &Expression, schema: &DataSchemaRef) -> Result<Expression> {
    clone_with_replacement(expr, &|nest_exprs| match nest_exprs {
        Expression::Sort { .. }
        | Expression::Column(_)
        | Expression::Literal {
            column_name: None, ..
        }
        | Expression::Alias(_, _) => Ok(None),
        _ => {
            if schema.field_with_name(&nest_exprs.column_name()).is_ok() {
                Ok(Some(expr_as_column_expr(nest_exprs)?))
            } else {
                Ok(None)
            }
        }
    })
}

pub fn sort_to_inner_expr(expr: &Expression) -> Expression {
    match expr {
        Expression::Sort {
            expr: nest_exprs, ..
        } => *nest_exprs.clone(),
        _ => expr.clone(),
    }
}

/// Determines if the set of `Expression`'s are a valid projection on the input
/// `Expression::Column`'s.
pub fn find_columns_not_satisfy_exprs(
    columns: &[Expression],
    exprs: &[Expression],
) -> Result<Option<Expression>> {
    columns.iter().try_for_each(|c| match c {
        Expression::Column(_) => Ok(()),

        _ => Err(ErrorCode::SyntaxException(
            "Expression::Column are required".to_string(),
        )),
    })?;

    let exprs = find_column_exprs(exprs);
    for expr in &exprs {
        if !columns.contains(expr) {
            return Ok(Some(expr.clone()));
        }
    }
    Ok(None)
}

/// Returns a cloned `expr`, but any of the `expr`'s in the tree may be
/// replaced/customized by the replacement function.
///
/// The replacement function is called repeatedly with `expr`, starting with
/// the argument `expr`, then descending depth-first through its
/// descendants. The function chooses to replace or keep (clone) each `expr`.
///
/// The function's return type is `Result<Option<Expression>>>`, where:
///
/// * `Ok(Some(replacement_expr))`: A replacement `expr` is provided; it is
///       swapped in at the particular node in the tree. Any nested `expr` are
///       not subject to cloning/replacement.
/// * `Ok(None)`: A replacement `expr` is not provided. The `expr` is
///       recreated, with all of its nested `expr`'s subject to
///       cloning/replacement.
/// * `Err(err)`: Any error returned by the function is returned as-is by
///       `clone_with_replacement()`.
fn clone_with_replacement<F>(expr: &Expression, replacement_fn: &F) -> Result<Expression>
where F: Fn(&Expression) -> Result<Option<Expression>> {
    let replacement_opt = replacement_fn(expr)?;

    match replacement_opt {
        // If we were provided a replacement, use the replacement. Do not
        // descend further.
        Some(replacement) => Ok(replacement),
        // No replacement was provided, clone the node and recursively call
        // clone_with_replacement() on any nested Expressionessions.
        None => match expr {
            Expression::Wildcard => Ok(Expression::Wildcard),
            Expression::Alias(alias_name, nested_expr) => Ok(Expression::Alias(
                alias_name.clone(),
                Box::new(clone_with_replacement(&**nested_expr, replacement_fn)?),
            )),

            Expression::UnaryExpression {
                op,
                expr: nested_expr,
            } => Ok(Expression::UnaryExpression {
                op: op.clone(),
                expr: Box::new(clone_with_replacement(&**nested_expr, replacement_fn)?),
            }),

            Expression::BinaryExpression { left, op, right } => Ok(Expression::BinaryExpression {
                left: Box::new(clone_with_replacement(&**left, replacement_fn)?),
                op: op.clone(),
                right: Box::new(clone_with_replacement(&**right, replacement_fn)?),
            }),

            Expression::ScalarFunction { op, args } => Ok(Expression::ScalarFunction {
                op: op.clone(),
                args: args
                    .iter()
                    .map(|e| clone_with_replacement(e, replacement_fn))
                    .collect::<Result<Vec<Expression>>>()?,
            }),

            Expression::AggregateFunction {
                op,
                distinct,
                params,
                args,
            } => Ok(Expression::AggregateFunction {
                op: op.clone(),
                distinct: *distinct,
                params: params.clone(),
                args: args
                    .iter()
                    .map(|e| clone_with_replacement(e, replacement_fn))
                    .collect::<Result<Vec<Expression>>>()?,
            }),

            Expression::Sort {
                expr: nested_expr,
                asc,
                nulls_first,
                origin_expr,
            } => Ok(Expression::Sort {
                expr: Box::new(clone_with_replacement(&**nested_expr, replacement_fn)?),
                asc: *asc,
                nulls_first: *nulls_first,
                origin_expr: origin_expr.clone(),
            }),
            Expression::Cast {
                expr: nested_expr,
                data_type,
                is_nullable,
            } => Ok(Expression::Cast {
                expr: Box::new(clone_with_replacement(&**nested_expr, replacement_fn)?),
                data_type: data_type.clone(),
                is_nullable: *is_nullable,
            }),

            Expression::Column(_)
            | Expression::QualifiedColumn(_)
            | Expression::Literal { .. }
            | Expression::Subquery { .. }
            | Expression::ScalarSubquery { .. } => Ok(expr.clone()),
        },
    }
}

/// Returns mapping of each alias (`String`) to the exprs (`Expression`) it is
/// aliasing.
pub fn extract_aliases(exprs: &[Expression]) -> HashMap<String, Expression> {
    exprs
        .iter()
        .filter_map(|expr| match expr {
            Expression::Alias(alias_name, nest_exprs) => {
                Some((alias_name.clone(), *nest_exprs.clone()))
            }
            _ => None,
        })
        .collect::<HashMap<String, Expression>>()
}

/// Rebuilds an `expr` with columns that refer to aliases replaced by the
/// alias' underlying `expr`.
pub fn resolve_aliases_to_exprs(
    expr: &Expression,
    aliases: &HashMap<String, Expression>,
) -> Result<Expression> {
    clone_with_replacement(expr, &|nest_exprs| match nest_exprs {
        Expression::Column(name) => {
            if let Some(aliased_expr) = aliases.get(name) {
                Ok(Some(aliased_expr.clone()))
            } else {
                Ok(None)
            }
        }
        _ => Ok(None),
    })
}

/// Rebuilds an `expr` using the inner expr for expression
///  `(a + b) as c` ---> `(a + b)`
pub fn unwrap_alias_exprs(expr: &Expression) -> Result<Expression> {
    clone_with_replacement(expr, &|nest_exprs| match nest_exprs {
        Expression::Alias(_, nested_expr) => Ok(Some(*nested_expr.clone())),
        _ => Ok(None),
    })
}

pub struct ExpressionDataTypeVisitor {
    stack: Vec<DataTypePtr>,
    input_schema: DataSchemaRef,
}

impl ExpressionDataTypeVisitor {
    pub fn create(input_schema: DataSchemaRef) -> ExpressionDataTypeVisitor {
        ExpressionDataTypeVisitor {
            input_schema,
            stack: vec![],
        }
    }

    pub fn finalize(mut self) -> Result<DataTypePtr> {
        match self.stack.len() {
            1 => Ok(self.stack.remove(0)),
            _ => Err(ErrorCode::LogicalError(
                "Stack has too many elements in ExpressionDataTypeVisitor::finalize",
            )),
        }
    }

    fn visit_function(mut self, op: &str, args_size: usize) -> Result<ExpressionDataTypeVisitor> {
        let mut arguments = Vec::with_capacity(args_size);
        for index in 0..args_size {
            arguments.push(match self.stack.pop() {
                None => Err(ErrorCode::LogicalError(format!(
                    "Expected {} arguments, actual {}.",
                    args_size, index
                ))),
                Some(element) => Ok(element),
            }?);
        }

        let arguments: Vec<&DataTypePtr> = arguments.iter().collect();

        let function = FunctionFactory::instance().get(op, &arguments)?;
        let return_type = function.return_type(&arguments)?;
        self.stack.push(return_type);
        Ok(self)
    }
}

impl ExpressionVisitor for ExpressionDataTypeVisitor {
    fn pre_visit(self, _expr: &Expression) -> Result<Recursion<Self>> {
        Ok(Recursion::Continue(self))
    }

    fn post_visit(mut self, expr: &Expression) -> Result<Self> {
        match expr {
            Expression::Column(s) => {
                let field = self.input_schema.field_with_name(s)?;
                self.stack.push(field.data_type().clone());
                Ok(self)
            }
            Expression::Wildcard => Result::Err(ErrorCode::IllegalDataType(
                "Wildcard expressions are not valid to get return type",
            )),
            Expression::QualifiedColumn(_) => Err(ErrorCode::LogicalError(
                "QualifiedColumn should be resolve in analyze.",
            )),
            Expression::Literal { data_type, .. } => {
                self.stack.push(data_type.clone());
                Ok(self)
            }
            Expression::Subquery { query_plan, .. } => {
                let data_type = Expression::to_subquery_type(query_plan);
                self.stack.push(data_type);
                Ok(self)
            }
            Expression::ScalarSubquery { query_plan, .. } => {
                let data_type = Expression::to_subquery_type(query_plan);
                self.stack.push(data_type);
                Ok(self)
            }
            Expression::BinaryExpression { op, .. } => self.visit_function(op, 2),
            Expression::UnaryExpression { op, .. } => self.visit_function(op, 1),
            Expression::ScalarFunction { op, args } => self.visit_function(op, args.len()),
            expr @ Expression::AggregateFunction { args, .. } => {
                // Pop arguments.
                for index in 0..args.len() {
                    if self.stack.pop().is_none() {
                        return Err(ErrorCode::LogicalError(format!(
                            "Expected {} arguments, actual {}.",
                            args.len(),
                            index
                        )));
                    }
                }

                let aggregate_function = expr.to_aggregate_function(&self.input_schema)?;
                let return_type = aggregate_function.return_type()?;

                self.stack.push(return_type);
                Ok(self)
            }
            Expression::Cast { data_type, .. } => {
                let inner_type = match self.stack.pop() {
                    None => Err(ErrorCode::LogicalError(
                        "Cast expr expected 1 arguments, actual 0.",
                    )),
                    Some(_) => Ok(data_type),
                }?;

                self.stack.push(inner_type.clone());
                Ok(self)
            }
            Expression::Alias(_, _) | Expression::Sort { .. } => Ok(self),
        }
    }
}

// This visitor is for recursively visiting expression tree and collects all columns.
pub struct RequireColumnsVisitor {
    pub required_columns: HashSet<String>,
}

impl RequireColumnsVisitor {
    pub fn default() -> Self {
        Self {
            required_columns: HashSet::new(),
        }
    }

    pub fn collect_columns_from_expr(expr: &Expression) -> Result<HashSet<String>> {
        let mut visitor = Self::default();
        visitor = expr.accept(visitor)?;
        Ok(visitor.required_columns)
    }
}

impl ExpressionVisitor for RequireColumnsVisitor {
    fn pre_visit(self, expr: &Expression) -> Result<Recursion<Self>> {
        match expr {
            Expression::Column(c) => {
                let mut v = self;
                v.required_columns.insert(c.clone());
                Ok(Recursion::Continue(v))
            }
            _ => Ok(Recursion::Continue(self)),
        }
    }
}
