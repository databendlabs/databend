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

use databend_common_exception::Result;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchemaRefExt;
use log::info;

use super::prepare::AggIndexViewInfo;
use super::prepare::CompensatingRange;
use super::prepare::QueryInfo;
use super::prepare::ScalarExprMatcher;
use crate::ScalarExpr;
use crate::optimizer::ir::SExpr;
use crate::plans::AggIndexInfo;
use crate::plans::ConstantExpr;
use crate::plans::FunctionCall;
use crate::plans::RelOperator;
use crate::plans::ScalarItem;

// Aggregating index rewriting logic is based on "Optimizing Queries Using Materialized Views:
// A Practical, Scalable Solution" by Goldstein and Larson."
pub(super) struct AggIndexMatcher<'a> {
    pub(super) query_info: &'a QueryInfo,
}

impl AggIndexMatcher<'_> {
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

        if !self.check_output_expressions(view_info, &mut new_selection_set) {
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

        let is_agg = self.query_info.aggregate.is_some();
        let num_agg_funcs = self
            .query_info
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
        let index_output_matcher = ScalarExprMatcher::new(
            &view_info.query_info.column_map,
            &self.query_info.column_map,
        );

        for extra_range in extra_ranges {
            let Some((new_scalar, _)) = index_output_matcher
                .find_index_output_col(&view_info.index_output_cols, &extra_range.column)
            else {
                return false;
            };

            let lower = extra_range.lower_bound.comparison_bound("gte", "gt");
            let upper = extra_range.upper_bound.comparison_bound("lte", "lt");

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

            push_optional_comparison_predicate(new_predicates, &new_scalar, lower);
            push_optional_comparison_predicate(new_predicates, &new_scalar, upper);
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
                .query_info
                .check_output_cols(
                    scalar,
                    &view_info.index_output_cols,
                    &view_info.query_info.column_map,
                    new_selection_set,
                )
                .is_err()
            {
                return false;
            }
        }

        true
    }

    fn add_missing_residual_predicates(
        &self,
        view_info: &AggIndexViewInfo,
        extra_residual_preds: &[ScalarExpr],
        new_predicates: &mut Vec<ScalarExpr>,
        new_selection_set: &mut HashSet<ScalarItem>,
    ) -> bool {
        for extra_residual_pred in extra_residual_preds {
            match self.query_info.check_output_cols(
                extra_residual_pred,
                &view_info.index_output_cols,
                &view_info.query_info.column_map,
                new_selection_set,
            ) {
                Ok(Some(new_residual_pred)) => new_predicates.push(new_residual_pred),
                Ok(None) => {}
                Err(_) => return false,
            }
        }

        true
    }

    fn query_group_items(&self) -> &[ScalarItem] {
        self.query_info
            .aggregate
            .as_ref()
            .map(|agg| agg.group_items.as_slice())
            .unwrap_or_default()
    }

    fn view_group_items<'a>(&self, view_info: &'a AggIndexViewInfo) -> &'a [ScalarItem] {
        view_info
            .query_info
            .aggregate
            .as_ref()
            .map(|agg| agg.group_items.as_slice())
            .unwrap_or_default()
    }

    fn group_items_match(&self, view_info: &AggIndexViewInfo) -> bool {
        let query_group_items = self.query_group_items();
        let view_group_items = self.view_group_items(view_info);
        if query_group_items.len() != view_group_items.len() {
            return false;
        }

        let query_group_matcher = ScalarExprMatcher::same(&self.query_info.column_map);
        let mut query_group_names = Vec::with_capacity(query_group_items.len());
        for item in query_group_items {
            query_group_matcher.push_unique_scalar(&mut query_group_names, item.scalar.clone());
        }

        let view_group_matcher = ScalarExprMatcher::same(&view_info.query_info.column_map);
        let mut view_group_names = Vec::with_capacity(view_group_items.len());
        for item in view_group_items {
            view_group_matcher.push_unique_scalar(&mut view_group_names, item.scalar.clone());
        }

        let group_matcher = ScalarExprMatcher::new(
            &view_info.query_info.column_map,
            &self.query_info.column_map,
        );
        query_group_names.into_iter().all(|query_group_name| {
            group_matcher.list_contains(&view_group_names, &query_group_name)
        })
    }

    fn check_group_items_output_cols(
        &self,
        group_items: &[ScalarItem],
        view_info: &AggIndexViewInfo,
        new_selection_set: &mut HashSet<ScalarItem>,
    ) -> bool {
        self.check_output_scalars(
            group_items.iter().map(|item| &item.scalar),
            view_info,
            new_selection_set,
        )
    }

    fn check_predicates(
        &self,
        view_info: &AggIndexViewInfo,
        new_predicates: &mut Vec<ScalarExpr>,
        new_selection_set: &mut HashSet<ScalarItem>,
    ) -> bool {
        if !self.query_info.equi_classes.check(
            &view_info.query_info.equi_classes,
            &self.query_info.column_map,
            &view_info.query_info.column_map,
        ) {
            return false;
        }
        let (range_res, extra_ranges) = self.query_info.range_classes.check(
            &view_info.query_info.range_classes,
            &self.query_info.column_map,
            &view_info.query_info.column_map,
        );
        if !range_res {
            return false;
        }

        let (residual_res, extra_residual_preds) = self.query_info.residual_classes.check(
            &view_info.query_info.residual_classes,
            &self.query_info.column_map,
            &view_info.query_info.column_map,
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

    fn check_output_expressions(
        &self,
        view_info: &AggIndexViewInfo,
        new_selection_set: &mut HashSet<ScalarItem>,
    ) -> bool {
        self.check_output_scalars(
            self.query_info.output_cols.iter().map(|item| &item.scalar),
            view_info,
            new_selection_set,
        )
    }

    fn check_aggregation(
        &self,
        view_info: &AggIndexViewInfo,
        new_selection_set: &mut HashSet<ScalarItem>,
    ) -> bool {
        let query_group_items = self.query_group_items();
        let view_group_items = self.view_group_items(view_info);

        match (query_group_items.is_empty(), view_group_items.is_empty()) {
            (false, false) => {
                if !self.group_items_match(view_info) {
                    return false;
                }
                if !self.check_group_items_output_cols(
                    query_group_items,
                    view_info,
                    new_selection_set,
                ) {
                    return false;
                }
            }
            (false, true) => {
                if !self.check_group_items_output_cols(
                    query_group_items,
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
        let Some(sort_items) = &self.query_info.sort_items else {
            return true;
        };

        for item in sort_items {
            let Some(scalar) = self.query_info.column_map.get(&item.index) else {
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

fn push_optional_comparison_predicate(
    predicates: &mut Vec<ScalarExpr>,
    column: &ScalarExpr,
    bound: Option<(Scalar, &'static str)>,
) {
    let Some((value, func_name)) = bound else {
        return;
    };
    predicates.push(comparison_predicate(func_name, column.clone(), value));
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
