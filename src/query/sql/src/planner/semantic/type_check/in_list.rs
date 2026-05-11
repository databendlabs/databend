// Copyright 2021 Datafuse Labs
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

use databend_common_ast::Span;
use databend_common_ast::ast::Expr;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ConstantFolder;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::Scalar;
use databend_common_expression::type_check::common_super_type;
use databend_common_expression::types::DataType;
use databend_common_functions::BUILTIN_FUNCTIONS;

use super::TypeChecker;
use super::core_expr::CoreExpr;
use super::core_expr::CoreExprArena;
use super::core_expr::CoreExprArgs;
use super::core_expr::CoreExprId;
use crate::ColumnBindingBuilder;
use crate::ColumnSet;
use crate::Visibility;
use crate::binder::wrap_cast;
use crate::optimizer::ir::SExpr;
use crate::plans::Aggregate;
use crate::plans::AggregateMode;
use crate::plans::BoundColumnRef;
use crate::plans::ConstantExpr;
use crate::plans::ConstantTableScan;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;
use crate::plans::SubqueryComparisonOp;
use crate::plans::SubqueryExpr;
use crate::plans::SubqueryType;

impl<'a> CoreExprArena<'a> {
    pub(super) fn in_list(
        &mut self,
        span: Span,
        expr: &'a Expr,
        list: &'a [Expr],
        not: bool,
    ) -> Result<CoreExprId> {
        let expr = self.lower_ast_expr(expr)?;
        let list = self.lower_expr_args(list)?;
        Ok(self.alloc(CoreExpr::InList {
            span,
            expr,
            list,
            not,
        }))
    }
}

impl<'a, A> TypeChecker<'a, A>
where A: super::TypeCheckAdapter
{
    pub(super) fn resolve_in_list(
        &mut self,
        arena: &CoreExprArena<'_>,
        span: Span,
        expr: CoreExprId,
        list: &CoreExprArgs,
        not: bool,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let inlist_to_join_threshold = self.adapter.settings().get_inlist_to_join_threshold()?;
        if list.len() >= inlist_to_join_threshold {
            let box (expr_scalar, expr_ty) = self.resolve_core(arena, expr)?;
            let box (subquery, data_type) =
                self.resolve_in_list_as_subquery(arena, span, expr_scalar, expr_ty, list)?;
            return if not {
                self.resolve_scalar_function_call(span, "not", vec![], vec![subquery])
            } else {
                Ok(Box::new((subquery, data_type)))
            };
        }

        let box (expr_scalar, _) = self.resolve_core(arena, expr)?;
        let max_inlist_to_or = self.adapter.settings().get_max_inlist_to_or()? as usize;

        if list.len() > max_inlist_to_or
            && list
                .iter()
                .all(|item| satisfy_core_contain_func(arena, *item))
        {
            let (list_scalars, _) = self.resolve_expr_args(arena, list)?;
            let box (array, _) =
                self.resolve_scalar_function_call(span, "array", vec![], list_scalars)?;
            let box (array, _) =
                self.resolve_scalar_function_call(span, "array_distinct", vec![], vec![array])?;
            let box (contains, data_type) =
                self.resolve_scalar_function_call(span, "contains", vec![], vec![
                    array,
                    expr_scalar,
                ])?;
            return if not {
                self.resolve_scalar_function_call(span, "not", vec![], vec![contains])
            } else {
                Ok(Box::new((contains, data_type)))
            };
        }

        let mut predicate_levels = Vec::with_capacity(list.len().max(1).ilog2() as usize + 1);
        for item in list {
            let box (item, _) = self.resolve_core(arena, *item)?;
            let box (predicate, _) =
                self.resolve_scalar_function_call(span, "eq", vec![], vec![
                    expr_scalar.clone(),
                    item,
                ])?;
            self.merge_or_level(span, &mut predicate_levels, predicate)?;
        }

        let result = self
            .fold_or_levels(span, predicate_levels)?
            .expect("IN list should not be empty");
        let data_type = result.data_type()?;
        if not {
            self.resolve_scalar_function_call(span, "not", vec![], vec![result])
        } else {
            Ok(Box::new((result, data_type)))
        }
    }

    fn resolve_in_list_as_subquery(
        &mut self,
        arena: &CoreExprArena<'_>,
        span: Span,
        child_scalar: ScalarExpr,
        child_type: DataType,
        list: &CoreExprArgs,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let (list_scalars, list_types) = self.resolve_expr_args(arena, list)?;
        let common_type = list_types
            .iter()
            .cloned()
            .try_reduce(|a, b| {
                common_super_type(a.clone(), b.clone(), &BUILTIN_FUNCTIONS.default_cast_rules)
                    .ok_or_else(|| {
                        ErrorCode::SemanticError(
                            format!("{a} and {b} don't have common data type",),
                        )
                        .set_span(span)
                    })
            })?
            .unwrap();

        let num_rows = list_scalars.len();
        let mut builder = ColumnBuilder::with_capacity(&common_type, num_rows);
        for (scalar, data_type) in list_scalars.into_iter().zip(list_types.iter()) {
            let scalar = if *data_type != common_type {
                let cast = wrap_cast(&scalar, &common_type);
                let expr = cast.as_expr()?;
                let (expr, _) = ConstantFolder::fold(&expr, &self.func_ctx, &BUILTIN_FUNCTIONS);
                match expr.into_constant() {
                    Ok(constant) => ScalarExpr::ConstantExpr(ConstantExpr {
                        span: scalar.span(),
                        value: constant.scalar,
                    }),
                    Err(_) => {
                        return Err(ErrorCode::SemanticError(
                            "Values can't contain subquery, aggregate functions, window functions, or UDFs",
                        ).set_span(span));
                    }
                }
            } else {
                scalar
            };
            let ScalarExpr::ConstantExpr(ConstantExpr { value, .. }) = scalar else {
                return Err(ErrorCode::SemanticError(
                    "Values can't contain subquery, aggregate functions, window functions, or UDFs",
                )
                .set_span(span));
            };
            builder.push(value.as_ref());
        }

        let mut metadata = self.metadata.write();
        let value_index = metadata.add_derived_column("col0".to_string(), common_type.clone());
        drop(metadata);

        let value_column = ColumnBindingBuilder::new(
            "col0".to_string(),
            value_index,
            Box::new(common_type.clone()),
            Visibility::Visible,
        )
        .build();
        let distinct_const_scan = SExpr::create_leaf(ConstantTableScan {
            values: vec![builder.build()],
            num_rows,
            schema: DataSchemaRefExt::create(vec![DataField::new(
                &value_index.to_string(),
                common_type.clone(),
            )]),
            columns: ColumnSet::from_iter([value_index]),
        })
        .build_unary(Aggregate {
            mode: AggregateMode::Initial,
            group_items: vec![ScalarItem {
                scalar: ScalarExpr::BoundColumnRef(BoundColumnRef {
                    span: None,
                    column: value_column.clone(),
                }),
                index: value_index,
            }],
            ..Default::default()
        });

        let rel_prop = distinct_const_scan.derive_relational_prop()?;
        let subquery_expr = SubqueryExpr {
            span,
            subquery: Box::new(distinct_const_scan),
            child_expr: Some(Box::new(child_scalar)),
            compare_op: Some(SubqueryComparisonOp::Equal),
            output_column: value_column,
            projection_index: None,
            data_type: Box::new(if child_type.is_nullable() {
                common_type.wrap_nullable()
            } else {
                common_type
            }),
            typ: SubqueryType::Any,
            outer_columns: rel_prop.outer_columns.clone(),
            contain_agg: None,
        };
        let data_type = subquery_expr.output_data_type();
        Ok(Box::new((subquery_expr.into(), data_type)))
    }

    fn merge_or_level(
        &self,
        span: Span,
        predicate_levels: &mut Vec<Option<ScalarExpr>>,
        mut predicate: ScalarExpr,
    ) -> Result<()> {
        let mut level = 0;

        loop {
            if predicate_levels.len() == level {
                predicate_levels.push(Some(predicate));
                return Ok(());
            }

            if let Some(left) = predicate_levels[level].take() {
                let box (or_predicate, _) =
                    self.resolve_scalar_function_call(span, "or", vec![], vec![left, predicate])?;
                predicate = or_predicate;
                level += 1;
            } else {
                predicate_levels[level] = Some(predicate);
                return Ok(());
            }
        }
    }

    fn fold_or_levels(
        &self,
        span: Span,
        predicate_levels: Vec<Option<ScalarExpr>>,
    ) -> Result<Option<ScalarExpr>> {
        let mut result = None;

        for predicate in predicate_levels.into_iter().rev().flatten() {
            result = Some(match result {
                None => predicate,
                Some(acc) => {
                    let box (or_predicate, _) =
                        self.resolve_scalar_function_call(span, "or", vec![], vec![
                            acc, predicate,
                        ])?;
                    or_predicate
                }
            });
        }

        Ok(result)
    }
}

fn satisfy_core_contain_func(arena: &CoreExprArena<'_>, expr: CoreExprId) -> bool {
    match arena.get(expr) {
        CoreExpr::Literal { value, .. } => !matches!(value, Scalar::Null),
        CoreExpr::Tuple { exprs, .. } | CoreExpr::Array { exprs, .. } => exprs
            .iter()
            .all(|expr| satisfy_core_contain_func(arena, *expr)),
        _ => false,
    }
}
