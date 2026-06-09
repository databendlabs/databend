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
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchemaRefExt;
use log::info;

use super::AggIndexViewInfo;
use super::EquivalenceClasses;
use super::IndexOutputColumn;
use super::QueryInfo;
use super::ResidualClasses;
use super::ScalarExprMatcher;
use super::ScalarMatchContext;
use super::prepare::CompensatingRange;
use crate::ScalarExpr;
use crate::optimizer::ir::SExpr;
use crate::plans::AggIndexInfo;
use crate::plans::ConstantExpr;
use crate::plans::FunctionCall;
use crate::plans::RelOperator;
use crate::plans::ScalarItem;

// Aggregating index rewriting logic is based on "Optimizing Queries Using Materialized Views:
// A Practical, Scalable Solution" by Goldstein and Larson."
impl EquivalenceClasses {
    fn check(
        &self,
        view_equi_classes: &EquivalenceClasses,
        query_info: &QueryInfo,
        view_info: &QueryInfo,
    ) -> bool {
        let query_match_context = ScalarMatchContext {
            column_map: &query_info.column_map,
            column_identities: &query_info.column_identities,
        };
        let view_match_context = ScalarMatchContext {
            column_map: &view_info.column_map,
            column_identities: &view_info.column_identities,
        };
        let matcher = ScalarExprMatcher {
            left: query_match_context,
            right: view_match_context,
        };
        for view_class in &view_equi_classes.classes {
            if self.classes.iter().any(|query_class| {
                view_class
                    .iter()
                    .all(|view_scalar| matcher.list_contains(query_class, view_scalar))
            }) {
                continue;
            }
            return false;
        }
        true
    }
}

impl ResidualClasses {
    fn check(
        &self,
        view_residual_classes: &ResidualClasses,
        query_info: &QueryInfo,
        view_info: &QueryInfo,
    ) -> (bool, Option<Vec<ScalarExpr>>) {
        let query_match_context = ScalarMatchContext {
            column_map: &query_info.column_map,
            column_identities: &query_info.column_identities,
        };
        let view_match_context = ScalarMatchContext {
            column_map: &view_info.column_map,
            column_identities: &view_info.column_identities,
        };
        let matcher = ScalarExprMatcher {
            left: query_match_context,
            right: view_match_context,
        };
        let reverse_matcher = ScalarExprMatcher {
            left: view_match_context,
            right: query_match_context,
        };
        let mut extra_residual_preds = Vec::new();
        for view_residual_pred in &view_residual_classes.residual_preds {
            if !matcher.list_contains(&self.residual_preds, view_residual_pred) {
                return (false, None);
            }
        }
        for query_residual_pred in &self.residual_preds {
            if !reverse_matcher
                .list_contains(&view_residual_classes.residual_preds, query_residual_pred)
            {
                extra_residual_preds.push(query_residual_pred.clone());
            }
        }
        if extra_residual_preds.is_empty() {
            (true, None)
        } else {
            (true, Some(extra_residual_preds))
        }
    }
}

impl QueryInfo {
    pub(super) fn try_rewrite_index(
        &self,
        s_expr: &SExpr,
        index_id: u64,
        sql: &str,
        view_info: &AggIndexViewInfo,
    ) -> Result<Option<SExpr>> {
        let mut new_predicates = Vec::new();
        let mut new_selection_set = HashSet::new();

        if !self.check_predicates(view_info, &mut new_predicates, &mut new_selection_set) {
            return Ok(None);
        }

        if !self.check_output_scalars(
            self.output_cols.iter().map(|item| &item.scalar),
            view_info,
            &mut new_selection_set,
        ) {
            return Ok(None);
        }

        if !self.check_aggregation(view_info, &mut new_selection_set) {
            return Ok(None);
        }

        if !self.check_sort_items(view_info, &mut new_selection_set) {
            return Ok(None);
        }

        let mut new_selection: Vec<_> = new_selection_set.into_iter().collect();
        new_selection.sort_by_key(|i| i.index);

        let is_agg = self.aggregate.is_some();
        let num_agg_funcs = self
            .aggregate
            .as_ref()
            .map(|agg| agg.aggregate_functions.len())
            .unwrap_or_default();

        let result = push_down_index_scan(s_expr, AggIndexInfo {
            index_id,
            selection: new_selection,
            predicates: new_predicates,
            schema: TableSchemaRefExt::create(view_info.index_fields.clone()),
            is_agg,
            num_agg_funcs,
        })?;

        info!("Use aggregating index: {sql}");

        Ok(Some(result))
    }

    fn add_compensating_range_predicates(
        &self,
        view_info: &AggIndexViewInfo,
        extra_ranges: &[CompensatingRange],
        new_predicates: &mut Vec<ScalarExpr>,
    ) -> bool {
        let query_match_context = ScalarMatchContext {
            column_map: &self.column_map,
            column_identities: &self.column_identities,
        };
        let view_match_context = ScalarMatchContext {
            column_map: &view_info.query_info.column_map,
            column_identities: &view_info.query_info.column_identities,
        };
        let index_output_matcher = ScalarExprMatcher {
            left: view_match_context,
            right: query_match_context,
        };

        for extra_range in extra_ranges {
            let Some((new_scalar, _)) = index_output_matcher
                .find_index_output_col(&view_info.index_output_cols, extra_range.column())
            else {
                return false;
            };

            let (lower, upper) = extra_range.comparison_bounds();

            if let (Some((lower_val, "gte")), Some((upper_val, "lte"))) = (&lower, &upper) {
                if lower_val == upper_val {
                    new_predicates.push(comparison_predicate(
                        "eq",
                        new_scalar.clone(),
                        lower_val.clone(),
                    ));
                    continue;
                }
            }

            if let Some((value, func_name)) = lower {
                new_predicates.push(comparison_predicate(func_name, new_scalar.clone(), value));
            }
            if let Some((value, func_name)) = upper {
                new_predicates.push(comparison_predicate(func_name, new_scalar.clone(), value));
            }
        }

        true
    }

    fn check_output_scalars<'a>(
        &self,
        scalars: impl IntoIterator<Item = &'a ScalarExpr>,
        view_info: &AggIndexViewInfo,
        new_selection_set: &mut HashSet<ScalarItem>,
    ) -> bool {
        for scalar in scalars {
            if self
                .check_output_cols(
                    scalar,
                    &view_info.index_output_cols,
                    ScalarMatchContext {
                        column_map: &view_info.query_info.column_map,
                        column_identities: &view_info.query_info.column_identities,
                    },
                    new_selection_set,
                )
                .is_err()
            {
                return false;
            }
        }

        true
    }

    fn rewrite_output_args(
        &self,
        args: &[ScalarExpr],
        index_output_cols: &[IndexOutputColumn],
        index_match_context: ScalarMatchContext<'_>,
        new_selection_set: &mut HashSet<ScalarItem>,
    ) -> Result<Option<Vec<ScalarExpr>>> {
        let mut rewritten = Vec::with_capacity(args.len());
        let mut all_rewritten = true;
        for arg in args {
            let new_arg = self.check_output_cols(
                arg,
                index_output_cols,
                index_match_context,
                new_selection_set,
            )?;
            if let Some(new_arg) = new_arg {
                rewritten.push(new_arg);
            } else {
                all_rewritten = false;
            }
        }
        if all_rewritten {
            Ok(Some(rewritten))
        } else {
            Ok(None)
        }
    }

    fn check_output_cols(
        &self,
        scalar: &ScalarExpr,
        index_output_cols: &[IndexOutputColumn],
        index_match_context: ScalarMatchContext<'_>,
        new_selection_set: &mut HashSet<ScalarItem>,
    ) -> Result<Option<ScalarExpr>> {
        let query_match_context = ScalarMatchContext {
            column_map: &self.column_map,
            column_identities: &self.column_identities,
        };
        let output_matcher = ScalarExprMatcher {
            left: index_match_context,
            right: query_match_context,
        };
        let query_matcher = ScalarExprMatcher {
            left: query_match_context,
            right: query_match_context,
        };

        if let Some((new_scalar, is_agg)) =
            output_matcher.find_index_output_col(index_output_cols, scalar)
        {
            if let Some(index) = query_matcher.find_column_index(&self.column_exprs, scalar) {
                let new_item = ScalarItem {
                    index,
                    scalar: new_scalar.clone(),
                };
                new_selection_set.insert(new_item);
            }
            return if is_agg {
                Ok(None)
            } else {
                Ok(Some(new_scalar.clone()))
            };
        }

        let new_scalar = match scalar {
            ScalarExpr::BoundColumnRef(_) => {
                if let Some(actual_column) = self.resolve_column_ref(scalar) {
                    return self.check_output_cols(
                        actual_column,
                        index_output_cols,
                        index_match_context,
                        new_selection_set,
                    );
                }
                return Err(ErrorCode::Internal("Can't found column from index"));
            }
            ScalarExpr::ConstantExpr(_) => scalar.clone(),
            ScalarExpr::FunctionCall(func) => {
                let Some(new_args) = self.rewrite_output_args(
                    &func.arguments,
                    index_output_cols,
                    index_match_context,
                    new_selection_set,
                )?
                else {
                    return Ok(None);
                };
                let mut new_func = func.clone();
                new_func.arguments = new_args;
                ScalarExpr::FunctionCall(new_func)
            }
            ScalarExpr::CastExpr(cast) => {
                if let Some(new_arg) = self.check_output_cols(
                    &cast.argument,
                    index_output_cols,
                    index_match_context,
                    new_selection_set,
                )? {
                    let mut new_cast = cast.clone();
                    new_cast.argument = Box::new(new_arg);
                    ScalarExpr::CastExpr(new_cast)
                } else {
                    return Ok(None);
                }
            }
            ScalarExpr::AggregateFunction(func) => {
                for expr in func.exprs() {
                    self.check_output_cols(
                        expr,
                        index_output_cols,
                        index_match_context,
                        new_selection_set,
                    )?;
                }
                return Ok(None);
            }
            ScalarExpr::UDAFCall(udaf) => {
                for arg in &udaf.arguments {
                    self.check_output_cols(
                        arg,
                        index_output_cols,
                        index_match_context,
                        new_selection_set,
                    )?;
                }
                return Ok(None);
            }
            ScalarExpr::UDFCall(udf) => {
                let Some(new_args) = self.rewrite_output_args(
                    &udf.arguments,
                    index_output_cols,
                    index_match_context,
                    new_selection_set,
                )?
                else {
                    return Ok(None);
                };
                let mut new_udf = udf.clone();
                new_udf.arguments = new_args;
                ScalarExpr::UDFCall(new_udf)
            }
            _ => unreachable!(),
        };

        if let Some(index) = query_matcher.find_column_index(&self.column_exprs, scalar) {
            let new_item = ScalarItem {
                index,
                scalar: new_scalar.clone(),
            };
            new_selection_set.insert(new_item);
            return Ok(None);
        }

        Ok(Some(new_scalar))
    }

    fn resolve_column_ref<'a>(&'a self, scalar: &ScalarExpr) -> Option<&'a ScalarExpr> {
        let ScalarExpr::BoundColumnRef(col) = scalar else {
            return None;
        };

        let mapped = self.column_map.get(&col.column.index)?;
        match mapped {
            ScalarExpr::BoundColumnRef(mapped_col)
                if mapped_col.column.index == col.column.index
                    && mapped_col.column.table_index == col.column.table_index =>
            {
                None
            }
            _ => Some(mapped),
        }
    }

    fn add_missing_residual_predicates(
        &self,
        view_info: &AggIndexViewInfo,
        extra_residual_preds: &[ScalarExpr],
        new_predicates: &mut Vec<ScalarExpr>,
        new_selection_set: &mut HashSet<ScalarItem>,
    ) -> bool {
        for extra_residual_pred in extra_residual_preds {
            match self.check_output_cols(
                extra_residual_pred,
                &view_info.index_output_cols,
                ScalarMatchContext {
                    column_map: &view_info.query_info.column_map,
                    column_identities: &view_info.query_info.column_identities,
                },
                new_selection_set,
            ) {
                Ok(Some(new_residual_pred)) => new_predicates.push(new_residual_pred),
                Ok(None) => {}
                Err(_) => return false,
            }
        }

        true
    }

    fn group_items_match(&self, view_info: &AggIndexViewInfo) -> bool {
        let query_group_items = self
            .aggregate
            .as_ref()
            .map(|agg| agg.group_items.as_slice())
            .unwrap_or_default();
        let view_group_items = view_info
            .query_info
            .aggregate
            .as_ref()
            .map(|agg| agg.group_items.as_slice())
            .unwrap_or_default();
        if query_group_items.len() != view_group_items.len() {
            return false;
        }

        let query_match_context = ScalarMatchContext {
            column_map: &self.column_map,
            column_identities: &self.column_identities,
        };
        let query_group_matcher = ScalarExprMatcher {
            left: query_match_context,
            right: query_match_context,
        };
        let mut query_group_names = Vec::with_capacity(query_group_items.len());
        for item in query_group_items {
            if !query_group_matcher.list_contains(&query_group_names, &item.scalar) {
                query_group_names.push(item.scalar.clone());
            }
        }

        let view_match_context = ScalarMatchContext {
            column_map: &view_info.query_info.column_map,
            column_identities: &view_info.query_info.column_identities,
        };
        let view_group_matcher = ScalarExprMatcher {
            left: view_match_context,
            right: view_match_context,
        };
        let mut view_group_names = Vec::with_capacity(view_group_items.len());
        for item in view_group_items {
            if !view_group_matcher.list_contains(&view_group_names, &item.scalar) {
                view_group_names.push(item.scalar.clone());
            }
        }

        let group_matcher = ScalarExprMatcher {
            left: view_match_context,
            right: query_match_context,
        };
        query_group_names.into_iter().all(|query_group_name| {
            group_matcher.list_contains(&view_group_names, &query_group_name)
        })
    }

    fn check_predicates(
        &self,
        view_info: &AggIndexViewInfo,
        new_predicates: &mut Vec<ScalarExpr>,
        new_selection_set: &mut HashSet<ScalarItem>,
    ) -> bool {
        if !self.equi_classes.check(
            &view_info.query_info.equi_classes,
            self,
            &view_info.query_info,
        ) {
            return false;
        }
        let (range_res, extra_ranges) = self.range_classes.check(
            &view_info.query_info.range_classes,
            self,
            &view_info.query_info,
        );
        if !range_res {
            return false;
        }

        let (residual_res, extra_residual_preds) = self.residual_classes.check(
            &view_info.query_info.residual_classes,
            self,
            &view_info.query_info,
        );
        if !residual_res {
            return false;
        }

        if let Some(extra_ranges) = extra_ranges
            && !self.add_compensating_range_predicates(view_info, &extra_ranges, new_predicates)
        {
            return false;
        }

        if let Some(extra_residual_preds) = extra_residual_preds
            && !self.add_missing_residual_predicates(
                view_info,
                &extra_residual_preds,
                new_predicates,
                new_selection_set,
            )
        {
            return false;
        }

        true
    }

    fn check_aggregation(
        &self,
        view_info: &AggIndexViewInfo,
        new_selection_set: &mut HashSet<ScalarItem>,
    ) -> bool {
        let query_group_items = self
            .aggregate
            .as_ref()
            .map(|agg| agg.group_items.as_slice())
            .unwrap_or_default();
        let view_group_items = view_info
            .query_info
            .aggregate
            .as_ref()
            .map(|agg| agg.group_items.as_slice())
            .unwrap_or_default();

        match (query_group_items.is_empty(), view_group_items.is_empty()) {
            (false, false) => {
                if !self.group_items_match(view_info) {
                    return false;
                }
                if !self.check_output_scalars(
                    query_group_items.iter().map(|item| &item.scalar),
                    view_info,
                    new_selection_set,
                ) {
                    return false;
                }
            }
            (false, true) => {
                if !self.check_output_scalars(
                    query_group_items.iter().map(|item| &item.scalar),
                    view_info,
                    new_selection_set,
                ) {
                    return false;
                }
            }
            (true, true) => {}
            (true, false) => return false,
        }

        true
    }

    fn check_sort_items(
        &self,
        view_info: &AggIndexViewInfo,
        new_selection_set: &mut HashSet<ScalarItem>,
    ) -> bool {
        let Some(sort_items) = &self.sort_items else {
            return true;
        };

        for item in sort_items {
            let Some(scalar) = self.column_map.get(&item.index) else {
                return false;
            };

            if !self.check_output_scalars([scalar], view_info, new_selection_set) {
                return false;
            }
        }

        true
    }
}

fn comparison_predicate(func_name: &str, column: ScalarExpr, value: Scalar) -> ScalarExpr {
    ScalarExpr::FunctionCall(FunctionCall {
        span: None,
        func_name: func_name.to_string(),
        params: vec![],
        arguments: vec![
            column,
            ScalarExpr::ConstantExpr(ConstantExpr { span: None, value }),
        ],
    })
}

fn push_down_index_scan(s_expr: &SExpr, agg_info: AggIndexInfo) -> Result<SExpr> {
    Ok(match s_expr.plan() {
        RelOperator::Scan(scan) => {
            let mut new_scan = scan.clone();
            new_scan.agg_index = Some(agg_info);
            s_expr.replace_plan(Arc::new(new_scan.into()))
        }
        _ => {
            let child = push_down_index_scan(s_expr.child(0)?, agg_info)?;
            s_expr.replace_children(vec![Arc::new(child)])
        }
    })
}
