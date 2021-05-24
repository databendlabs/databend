// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;

use common_datavalues::DataSchemaRef;
use common_exception::ErrorCodes;
use common_exception::Result;
use common_planners::ExpressionAction;
use common_planners::ExpressionVisitor;
use common_planners::Recursion;

/// Resolves an `ExpressionAction::Wildcard` to a collection of `ExpressionAction::Column`'s.
pub fn expand_wildcard(expr: &ExpressionAction, schema: &DataSchemaRef) -> Vec<ExpressionAction> {
    match expr {
        ExpressionAction::Wildcard => schema
            .fields()
            .iter()
            .map(|f| ExpressionAction::Column(f.name().to_string()))
            .collect::<Vec<ExpressionAction>>(),
        _ => vec![expr.clone()]
    }
}

/// Collect all deeply nested `ExpressionAction::AggregateFunction` and
/// `ExpressionAction::AggregateUDF`. They are returned in order of occurrence (depth
/// first), with duplicates omitted.
pub fn find_aggregate_exprs(exprs: &[ExpressionAction]) -> Vec<ExpressionAction> {
    find_exprs_in_exprs(exprs, &|nest_exprs| {
        matches!(nest_exprs, ExpressionAction::AggregateFunction { .. })
    })
}

/// Collect all arguments from aggregation function and append to this exprs
/// [ColumnExpr(b), Aggr(sum(a, b))] ---> [ColumnExpr(b), ColumnExpr(a)]

pub fn expand_aggregate_arg_exprs(exprs: &[ExpressionAction]) -> Vec<ExpressionAction> {
    let mut res = vec![];
    for expr in exprs {
        match expr {
            ExpressionAction::AggregateFunction { args, .. } => {
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

/// Collect all deeply nested `ExpressionAction::Column`'s. They are returned in order of
/// appearance (depth first), with duplicates omitted.
pub fn find_column_exprs(exprs: &[ExpressionAction]) -> Vec<ExpressionAction> {
    find_exprs_in_exprs(exprs, &|nest_exprs| {
        matches!(nest_exprs, ExpressionAction::Column(_))
    })
}

/// Search the provided `ExpressionAction`'s, and all of their nested `ExpressionAction`, for any that
/// pass the provided test. The returned `ExpressionAction`'s are deduplicated and returned
/// in order of appearance (depth first).
fn find_exprs_in_exprs<F>(exprs: &[ExpressionAction], test_fn: &F) -> Vec<ExpressionAction>
where F: Fn(&ExpressionAction) -> bool {
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

// Visitor that find ExpressionActionessions that match a particular predicate
struct Finder<'a, F>
where F: Fn(&ExpressionAction) -> bool
{
    test_fn: &'a F,
    exprs: Vec<ExpressionAction>
}

impl<'a, F> Finder<'a, F>
where F: Fn(&ExpressionAction) -> bool
{
    /// Create a new finder with the `test_fn`
    fn new(test_fn: &'a F) -> Self {
        Self {
            test_fn,
            exprs: Vec::new()
        }
    }
}

impl<'a, F> ExpressionVisitor for Finder<'a, F>
where F: Fn(&ExpressionAction) -> bool
{
    fn pre_visit(mut self, expr: &ExpressionAction) -> Result<Recursion<Self>> {
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

/// Search an `ExpressionAction`, and all of its nested `ExpressionAction`'s, for any that pass the
/// provided test. The returned `ExpressionAction`'s are deduplicated and returned in order
/// of appearance (depth first).
fn find_exprs_in_expr<F>(expr: &ExpressionAction, test_fn: &F) -> Vec<ExpressionAction>
where F: Fn(&ExpressionAction) -> bool {
    let Finder { exprs, .. } = expr
        .accept(Finder::new(test_fn))
        // pre_visit always returns OK, so this will always too
        .expect("no way to return error during recursion");

    exprs
}

/// Convert any `ExpressionAction` to an `ExpressionAction::Column`.
pub fn expr_as_column_expr(expr: &ExpressionAction) -> Result<ExpressionAction> {
    match expr {
        ExpressionAction::Column(_) => Ok(expr.clone()),
        _ => Ok(ExpressionAction::Column(expr.column_name()))
    }
}

/// Rebuilds an `expr` as a projection on top of a collection of `ExpressionAction`'s.
///
/// For example, the ExpressionActionession `a + b < 1` would require, as input, the 2
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
pub fn rebase_expr(
    expr: &ExpressionAction,
    base_exprs: &[ExpressionAction]
) -> Result<ExpressionAction> {
    clone_with_replacement(expr, &|nest_exprs| {
        if base_exprs.contains(nest_exprs) {
            Ok(Some(expr_as_column_expr(nest_exprs)?))
        } else {
            Ok(None)
        }
    })
}

// Rebuilds an `expr` to ColumnExpr when some expressions already processed in upstream
pub fn rebase_expr_from_input(
    expr: &ExpressionAction,
    schema: &DataSchemaRef
) -> Result<ExpressionAction> {
    clone_with_replacement(expr, &|nest_exprs| match nest_exprs {
        ExpressionAction::Sort { .. } | ExpressionAction::Column(_) => Ok(None),
        _ => {
            if let Ok(_) = schema.field_with_name(&nest_exprs.column_name()) {
                Ok(Some(expr_as_column_expr(nest_exprs)?))
            } else {
                Ok(None)
            }
        }
    })
}

/// Determines if the set of `ExpressionAction`'s are a valid projection on the input
/// `ExpressionAction::Column`'s.
pub fn find_columns_not_satisfy_exprs(
    columns: &[ExpressionAction],
    exprs: &[ExpressionAction]
) -> Result<Option<ExpressionAction>> {
    columns.iter().try_for_each(|c| match c {
        ExpressionAction::Column(_) => Ok(()),

        _ => Err(ErrorCodes::SyntaxException(
            "ExpressionAction::Column are required".to_string()
        ))
    })?;

    let exprs = find_column_exprs(exprs);
    for expr in &exprs {
        if !columns.contains(expr) {
            return Ok(Some(expr.clone()));
        }
    }
    return Ok(None);
}

/// Returns a cloned `expr`, but any of the `expr`'s in the tree may be
/// replaced/customized by the replacement function.
///
/// The replacement function is called repeatedly with `expr`, starting with
/// the argument `expr`, then descending depth-first through its
/// descendants. The function chooses to replace or keep (clone) each `expr`.
///
/// The function's return type is `Result<Option<ExpressionAction>>>`, where:
///
/// * `Ok(Some(replacement_expr))`: A replacement `expr` is provided; it is
///       swapped in at the particular node in the tree. Any nested `expr` are
///       not subject to cloning/replacement.
/// * `Ok(None)`: A replacement `expr` is not provided. The `expr` is
///       recreated, with all of its nested `expr`'s subject to
///       cloning/replacement.
/// * `Err(err)`: Any error returned by the function is returned as-is by
///       `clone_with_replacement()`.
fn clone_with_replacement<F>(
    expr: &ExpressionAction,
    replacement_fn: &F
) -> Result<ExpressionAction>
where
    F: Fn(&ExpressionAction) -> Result<Option<ExpressionAction>>
{
    let replacement_opt = replacement_fn(expr)?;

    match replacement_opt {
        // If we were provided a replacement, use the replacement. Do not
        // descend further.
        Some(replacement) => Ok(replacement),
        // No replacement was provided, clone the node and recursively call
        // clone_with_replacement() on any nested ExpressionActionessions.
        None => match expr {
            ExpressionAction::Wildcard => Ok(ExpressionAction::Wildcard),
            ExpressionAction::Alias(alias_name, nested_expr) => Ok(ExpressionAction::Alias(
                alias_name.clone(),
                Box::new(clone_with_replacement(&**nested_expr, replacement_fn)?)
            )),

            ExpressionAction::BinaryExpression { left, op, right } => {
                Ok(ExpressionAction::BinaryExpression {
                    left: Box::new(clone_with_replacement(&**left, replacement_fn)?),
                    op: op.clone(),
                    right: Box::new(clone_with_replacement(&**right, replacement_fn)?)
                })
            }

            ExpressionAction::ScalarFunction { op, args } => Ok(ExpressionAction::ScalarFunction {
                op: op.clone(),
                args: args
                    .iter()
                    .map(|e| clone_with_replacement(e, replacement_fn))
                    .collect::<Result<Vec<ExpressionAction>>>()?
            }),

            ExpressionAction::AggregateFunction { op, args } => {
                Ok(ExpressionAction::AggregateFunction {
                    op: op.clone(),
                    args: args
                        .iter()
                        .map(|e| clone_with_replacement(e, replacement_fn))
                        .collect::<Result<Vec<ExpressionAction>>>()?
                })
            }

            ExpressionAction::Sort {
                expr: nested_expr,
                asc,
                nulls_first
            } => Ok(ExpressionAction::Sort {
                expr: Box::new(clone_with_replacement(&**nested_expr, replacement_fn)?),
                asc: *asc,
                nulls_first: *nulls_first
            }),

            ExpressionAction::Cast {
                expr: nested_expr,
                data_type
            } => Ok(ExpressionAction::Cast {
                expr: Box::new(clone_with_replacement(&**nested_expr, replacement_fn)?),
                data_type: data_type.clone()
            }),

            ExpressionAction::Column(_) | ExpressionAction::Literal(_) => Ok(expr.clone())
        }
    }
}

/// Returns mapping of each alias (`String`) to the exprs (`ExpressionAction`) it is
/// aliasing.
pub fn extract_aliases(exprs: &[ExpressionAction]) -> HashMap<String, ExpressionAction> {
    exprs
        .iter()
        .filter_map(|expr| match expr {
            ExpressionAction::Alias(alias_name, nest_exprs) => {
                Some((alias_name.clone(), *nest_exprs.clone()))
            }
            _ => None
        })
        .collect::<HashMap<String, ExpressionAction>>()
}

/// Rebuilds an `expr` with columns that refer to aliases replaced by the
/// alias' underlying `expr`.
pub fn resolve_aliases_to_exprs(
    expr: &ExpressionAction,
    aliases: &HashMap<String, ExpressionAction>
) -> Result<ExpressionAction> {
    clone_with_replacement(expr, &|nest_exprs| match nest_exprs {
        ExpressionAction::Column(name) => {
            if let Some(aliased_expr) = aliases.get(name) {
                Ok(Some(aliased_expr.clone()))
            } else {
                Ok(None)
            }
        }
        _ => Ok(None)
    })
}

/// Rebuilds an `expr` using the inner expr for expression
///  `(a + b) as c` ---> `(a + b)`
pub fn unwrap_alias_exprs(expr: &ExpressionAction) -> Result<ExpressionAction> {
    clone_with_replacement(expr, &|nest_exprs| match nest_exprs {
        ExpressionAction::Alias(_, nested_expr) => Ok(Some(*nested_expr.clone())),
        _ => Ok(None)
    })
}
