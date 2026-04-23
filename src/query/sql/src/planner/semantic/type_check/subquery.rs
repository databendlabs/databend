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

use std::collections::HashSet;

use databend_common_ast::ast::Expr;
use databend_common_ast::ast::FunctionCall as ASTFunctionCall;
use databend_common_ast::ast::Query;
use databend_common_ast::ast::SelectTarget;
use databend_common_ast::ast::SetExpr;
use databend_common_catalog::catalog::CatalogManager;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Symbol;
use databend_common_expression::types::DataType;
use databend_common_functions::aggregates::AggregateFunctionFactory;
use derive_visitor::Drive;
use derive_visitor::Visitor;

use super::TypeChecker;
use crate::BindContext;
use crate::ColumnSet;
use crate::binder::Binder;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::SExpr;
use crate::plans::RelOperator;
use crate::plans::ScalarExpr;
use crate::plans::SubqueryComparisonOp;
use crate::plans::SubqueryExpr;
use crate::plans::SubqueryType;

impl<'a> TypeChecker<'a> {
    pub fn resolve_subquery(
        &mut self,
        typ: SubqueryType,
        subquery: &Query,
        child_expr: Option<Expr>,
        compare_op: Option<SubqueryComparisonOp>,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        enum ScalarOutputKind {
            OuterOnlyNoAggregate,
            OuterOnlyWithAggregate,
            Other,
        }

        fn merge_scalar_output_kind(
            lhs: ScalarOutputKind,
            rhs: ScalarOutputKind,
        ) -> ScalarOutputKind {
            match (lhs, rhs) {
                (ScalarOutputKind::Other, _) | (_, ScalarOutputKind::Other) => {
                    ScalarOutputKind::Other
                }
                (ScalarOutputKind::OuterOnlyWithAggregate, _)
                | (_, ScalarOutputKind::OuterOnlyWithAggregate) => {
                    ScalarOutputKind::OuterOnlyWithAggregate
                }
                _ => ScalarOutputKind::OuterOnlyNoAggregate,
            }
        }

        fn find_output_scalar(s_expr: &SExpr, target: Symbol) -> Option<ScalarExpr> {
            fn is_identity_projection(target: Symbol, scalar: &ScalarExpr) -> bool {
                matches!(
                    scalar,
                    ScalarExpr::BoundColumnRef(column_ref) if column_ref.column.index == target
                )
            }

            match s_expr.plan() {
                RelOperator::EvalScalar(eval_scalar) => {
                    if let Some(item) = eval_scalar.items.iter().find(|item| item.index == target)
                        && !is_identity_projection(target, &item.scalar)
                    {
                        return Some(item.scalar.clone());
                    }
                }
                RelOperator::Aggregate(aggregate) => {
                    if let Some(item) = aggregate
                        .aggregate_functions
                        .iter()
                        .find(|item| item.index == target)
                        && !is_identity_projection(target, &item.scalar)
                    {
                        return Some(item.scalar.clone());
                    }
                    if let Some(item) = aggregate
                        .group_items
                        .iter()
                        .find(|item| item.index == target)
                        && !is_identity_projection(target, &item.scalar)
                    {
                        return Some(item.scalar.clone());
                    }
                }
                _ => {}
            }

            for child in s_expr.children() {
                if let Some(scalar) = find_output_scalar(child, target) {
                    return Some(scalar);
                }
            }

            None
        }

        fn classify_scalar_output(
            s_expr: &SExpr,
            scalar: &ScalarExpr,
            outer_columns: &ColumnSet,
            visiting: &mut HashSet<Symbol>,
        ) -> ScalarOutputKind {
            match scalar {
                ScalarExpr::BoundColumnRef(column_ref) => {
                    let index = column_ref.column.index;
                    if outer_columns.contains(&index) {
                        return ScalarOutputKind::OuterOnlyNoAggregate;
                    }

                    if !visiting.insert(index) {
                        return ScalarOutputKind::Other;
                    }

                    let kind = find_output_scalar(s_expr, index)
                        .map(|scalar| {
                            classify_scalar_output(s_expr, &scalar, outer_columns, visiting)
                        })
                        .unwrap_or(ScalarOutputKind::Other);
                    visiting.remove(&index);
                    kind
                }
                ScalarExpr::ConstantExpr(_) | ScalarExpr::TypedConstantExpr(_, _) => {
                    ScalarOutputKind::OuterOnlyNoAggregate
                }
                ScalarExpr::AggregateFunction(aggregate) => {
                    let used_columns = aggregate
                        .exprs()
                        .flat_map(|expr| expr.used_columns())
                        .collect::<ColumnSet>();
                    if !used_columns.is_empty() && used_columns.is_subset(outer_columns) {
                        ScalarOutputKind::OuterOnlyWithAggregate
                    } else {
                        ScalarOutputKind::Other
                    }
                }
                ScalarExpr::UDAFCall(udaf) => {
                    let used_columns = udaf
                        .arguments
                        .iter()
                        .flat_map(|expr| expr.used_columns())
                        .collect::<ColumnSet>();
                    if !used_columns.is_empty() && used_columns.is_subset(outer_columns) {
                        ScalarOutputKind::OuterOnlyWithAggregate
                    } else {
                        ScalarOutputKind::Other
                    }
                }
                ScalarExpr::FunctionCall(func) => {
                    let mut kind = ScalarOutputKind::OuterOnlyNoAggregate;
                    for arg in &func.arguments {
                        kind = merge_scalar_output_kind(
                            kind,
                            classify_scalar_output(s_expr, arg, outer_columns, visiting),
                        );
                    }
                    kind
                }
                ScalarExpr::CastExpr(cast) => {
                    classify_scalar_output(s_expr, &cast.argument, outer_columns, visiting)
                }
                ScalarExpr::LambdaFunction(lambda) => {
                    let mut kind = ScalarOutputKind::OuterOnlyNoAggregate;
                    for arg in &lambda.args {
                        kind = merge_scalar_output_kind(
                            kind,
                            classify_scalar_output(s_expr, arg, outer_columns, visiting),
                        );
                    }
                    kind
                }
                ScalarExpr::UDFCall(udf) => {
                    let mut kind = ScalarOutputKind::OuterOnlyNoAggregate;
                    for arg in &udf.arguments {
                        kind = merge_scalar_output_kind(
                            kind,
                            classify_scalar_output(s_expr, arg, outer_columns, visiting),
                        );
                    }
                    kind
                }
                ScalarExpr::UDFLambdaCall(udf_lambda) => {
                    classify_scalar_output(s_expr, &udf_lambda.scalar, outer_columns, visiting)
                }
                ScalarExpr::AsyncFunctionCall(async_call) => {
                    let mut kind = ScalarOutputKind::OuterOnlyNoAggregate;
                    for arg in &async_call.arguments {
                        kind = merge_scalar_output_kind(
                            kind,
                            classify_scalar_output(s_expr, arg, outer_columns, visiting),
                        );
                    }
                    kind
                }
                ScalarExpr::WindowFunction(_) | ScalarExpr::SubqueryExpr(_) => {
                    ScalarOutputKind::Other
                }
            }
        }

        fn has_outer_only_aggregate_output(
            s_expr: &SExpr,
            output_column: Symbol,
            outer_columns: &ColumnSet,
        ) -> bool {
            let Some(output_scalar) = find_output_scalar(s_expr, output_column) else {
                return false;
            };

            matches!(
                classify_scalar_output(s_expr, &output_scalar, outer_columns, &mut HashSet::new()),
                ScalarOutputKind::OuterOnlyWithAggregate
            )
        }

        let mut binder = Binder::new(
            self.ctx.clone(),
            CatalogManager::instance(),
            self.name_resolution_ctx.clone(),
            self.metadata.clone(),
        );

        // Create new `BindContext` with current `bind_context` as its parent, so we can resolve outer columns.
        let mut bind_context = BindContext::with_parent(self.bind_context.clone())?;
        let (s_expr, output_context) = binder.bind_query(&mut bind_context, subquery)?;
        self.bind_context
            .cte_context
            .set_cte_context_and_name(output_context.cte_context);

        if (typ == SubqueryType::Scalar || typ == SubqueryType::Any)
            && output_context.columns.len() > 1
        {
            return Err(ErrorCode::SemanticError(format!(
                "Subquery must return only one column, but got {} columns",
                output_context.columns.len()
            )));
        }

        let mut contain_agg = None;
        if let SetExpr::Select(select_stmt) = &subquery.body {
            if typ == SubqueryType::Scalar {
                let select = &select_stmt.select_list[0];
                if matches!(select, SelectTarget::AliasedExpr { .. }) {
                    // Check if contain aggregation function
                    #[derive(Visitor)]
                    #[visitor(Expr(enter), ASTFunctionCall(enter))]
                    struct AggFuncVisitor {
                        contain_agg: bool,
                    }
                    impl AggFuncVisitor {
                        fn enter_ast_function_call(&mut self, func: &ASTFunctionCall) {
                            self.contain_agg = self.contain_agg
                                || AggregateFunctionFactory::instance()
                                    .contains(func.name.to_string());
                        }
                        fn enter_expr(&mut self, expr: &Expr) {
                            self.contain_agg = self.contain_agg
                                || matches!(expr, Expr::CountAll { window: None, .. });
                        }
                    }
                    let mut visitor = AggFuncVisitor { contain_agg: false };
                    select.drive(&mut visitor);
                    contain_agg = Some(visitor.contain_agg);
                }
            }
        }

        let box mut data_type = output_context.columns[0].data_type.clone();

        let rel_expr = RelExpr::with_s_expr(&s_expr);
        let rel_prop = rel_expr.derive_relational_prop()?;

        if typ == SubqueryType::Scalar
            && contain_agg == Some(true)
            && !rel_prop.outer_columns.is_empty()
            && has_outer_only_aggregate_output(
                &s_expr,
                output_context.columns[0].index,
                &rel_prop.outer_columns,
            )
        {
            return Err(ErrorCode::SemanticError(
                "unsupported scalar subquery: aggregate output references only outer columns"
                    .to_string(),
            )
            .set_span(subquery.span));
        }

        let mut child_scalar = None;
        if let Some(expr) = child_expr {
            assert_eq!(output_context.columns.len(), 1);
            let box (scalar, expr_ty) = self.resolve(&expr)?;
            child_scalar = Some(Box::new(scalar));
            // wrap nullable to make sure expr and list values have common type.
            if expr_ty.is_nullable() {
                data_type = data_type.wrap_nullable();
            }
        }

        if typ.eq(&SubqueryType::Scalar) {
            data_type = data_type.wrap_nullable();
        }
        let subquery_expr = SubqueryExpr {
            span: subquery.span,
            subquery: Box::new(s_expr),
            child_expr: child_scalar,
            compare_op,
            output_column: output_context.columns[0].clone(),
            projection_index: None,
            data_type: Box::new(data_type),
            typ,
            outer_columns: rel_prop.outer_columns.clone(),
            contain_agg,
        };
        let data_type = subquery_expr.output_data_type();
        Ok(Box::new((subquery_expr.into(), data_type)))
    }
}
