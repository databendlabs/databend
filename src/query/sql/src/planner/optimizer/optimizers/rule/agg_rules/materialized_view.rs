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

use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use databend_common_exception::Result;
use log::info;

use super::view_rewrite::QueryInfo;
use super::view_rewrite::ViewInfo;
use super::view_rewrite::ViewMatcher;
use super::view_rewrite::ViewRewriteMatch;
use super::view_rewrite::format_scalar;
use crate::IndexType;
use crate::MaterializedViewCandidate;
use crate::MaterializedViewCandidateReadMode;
use crate::Metadata;
use crate::ScalarExpr;
use crate::Symbol;
use crate::Visibility;
use crate::binder::ColumnBindingBuilder;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::SExpr;
use crate::plans::Aggregate;
use crate::plans::AggregateFunction;
use crate::plans::AggregateMode;
use crate::plans::BoundColumnRef;
use crate::plans::EvalScalar;
use crate::plans::Filter;
use crate::plans::RelOperator;
use crate::plans::ScalarItem;
use crate::plans::Scan;

/// Fallback cost when a scan has no collected table statistics.
/// Prefer a candidate with known cheaper IO over an unknown plan.
const UNKNOWN_CARDINALITY_COST: f64 = 1e12;
const COMPUTE_PER_ROW: f64 = 1.0;
const AGGREGATE_PER_ROW: f64 = 5.0;

struct RewriteCandidate {
    replacement: SExpr,
    mv_table_id: u64,
    cost: f64,
    read_mode: MaterializedViewCandidateReadMode,
    logical_sql: String,
    requires_aggregate_rollup: bool,
}

impl RewriteCandidate {
    fn is_better(&self, other: &Self) -> bool {
        match self.cost.partial_cmp(&other.cost) {
            Some(Ordering::Less) => true,
            Some(Ordering::Greater) => false,
            _ => match (self.read_mode, other.read_mode) {
                (
                    MaterializedViewCandidateReadMode::Fresh,
                    MaterializedViewCandidateReadMode::Hybrid,
                ) => true,
                (
                    MaterializedViewCandidateReadMode::Hybrid,
                    MaterializedViewCandidateReadMode::Fresh,
                ) => false,
                _ => self.mv_table_id < other.mv_table_id,
            },
        }
    }
}

/// Choose the cheapest semantically valid materialized-view rewrite.
/// When several candidates can answer the query, compare the full rewritten
/// plan cost (scan IO plus extra operators such as rollup aggregates) instead
/// of taking the first match.
pub(crate) fn try_rewrite(
    table_index: IndexType,
    table_name: &str,
    metadata: &Metadata,
    s_expr: &SExpr,
    candidates: &[MaterializedViewCandidate],
    required_output_columns: Option<&HashSet<Symbol>>,
) -> Result<Option<(SExpr, u64)>> {
    let mut query_info = QueryInfo::new(
        table_index,
        table_name,
        metadata.columns_by_table_index(table_index),
        s_expr,
    )?;
    if let Some(required_output_columns) = required_output_columns {
        query_info.retain_output_columns(required_output_columns);
    }
    let query_aggregate = query_info.aggregate.clone();
    let matcher = ViewMatcher::new(query_info).with_aggregate_rollup();

    let mut best: Option<RewriteCandidate> = None;
    for candidate in candidates {
        let Some((replacement, requires_aggregate_rollup)) = try_build_replacement(
            table_index,
            table_name,
            metadata,
            candidate,
            &matcher,
            query_aggregate.as_ref(),
        )?
        else {
            continue;
        };
        let cost = estimate_rewrite_cost(&replacement);
        let current = RewriteCandidate {
            replacement,
            mv_table_id: candidate.mv_table_id,
            cost,
            read_mode: candidate.read_mode,
            logical_sql: candidate.logical_sql.clone(),
            requires_aggregate_rollup,
        };
        if best.as_ref().is_none_or(|best| current.is_better(best)) {
            best = Some(current);
        }
    }

    if let Some(best) = best {
        if best.requires_aggregate_rollup {
            info!(
                "Use materialized view {} with aggregate-state rollup (cost={}): {}",
                best.mv_table_id, best.cost, best.logical_sql
            );
        } else {
            info!(
                "Use materialized view {} (cost={}): {}",
                best.mv_table_id, best.cost, best.logical_sql
            );
        }
        Ok(Some((best.replacement, best.mv_table_id)))
    } else {
        Ok(None)
    }
}

fn try_build_replacement(
    table_index: IndexType,
    table_name: &str,
    metadata: &Metadata,
    candidate: &MaterializedViewCandidate,
    matcher: &ViewMatcher,
    query_aggregate: Option<&Aggregate>,
) -> Result<Option<(SExpr, bool)>> {
    let definition_info = QueryInfo::new(
        table_index,
        table_name,
        metadata.columns_by_table_index(table_index),
        &candidate.definition,
    )?;
    if candidate.definition_output_columns.len() != candidate.read_output_columns.len() {
        return Ok(None);
    }

    let mut outputs = HashMap::with_capacity(definition_info.output_cols().len());
    for definition_output in definition_info.output_cols() {
        let Some(position) = candidate
            .definition_output_columns
            .iter()
            .position(|column| *column == definition_output.index)
        else {
            continue;
        };
        let read_output = candidate.read_output_columns[position];
        let display_name = format_scalar(&definition_output.scalar, definition_info.column_map());
        let replacement = ScalarExpr::BoundColumnRef(BoundColumnRef {
            span: None,
            column: ColumnBindingBuilder::new(
                metadata.column(read_output).name(),
                read_output,
                Box::new(metadata.column(read_output).data_type()),
                Visibility::Visible,
            )
            .build(),
        });
        // Aggregate MVs expose finalized logical aggregate values. The
        // first implementation supports only equal-granularity aggregate
        // matching; the shared matcher rejects use as an ordinary scalar.
        let is_aggregate = definition_info.aggregate.as_ref().is_some_and(|aggregate| {
            aggregate
                .aggregate_functions
                .iter()
                .any(|item| item.index == definition_output.index)
        });
        outputs.insert(display_name, (replacement, is_aggregate));
    }

    let view_info = ViewInfo::new(
        table_index,
        table_name,
        metadata.columns_by_table_index(table_index),
        &candidate.definition,
        outputs,
    )?;
    let Some(matched) = matcher.try_match(&view_info)? else {
        return Ok(None);
    };
    let post_aggregate_predicates = matched.post_aggregate_predicates.clone();
    let requires_aggregate_rollup = matched.requires_aggregate_rollup;

    if requires_aggregate_rollup {
        let Some(query_aggregate) = query_aggregate else {
            return Ok(None);
        };
        if let Some(mut replacement) = try_build_state_rollup(candidate, &matched, query_aggregate)?
        {
            replacement = apply_post_aggregate_filter(replacement, &post_aggregate_predicates);
            return Ok(Some((replacement, true)));
        }
    }

    let mut replacement = candidate.read_plan.clone();
    if !matched.predicates.is_empty() {
        replacement = SExpr::create_unary(
            Arc::new(
                Filter {
                    predicates: matched.predicates,
                }
                .into(),
            ),
            Arc::new(replacement),
        );
    }
    if !matched.selection.is_empty() {
        replacement = SExpr::create_unary(
            Arc::new(
                EvalScalar {
                    items: matched.selection,
                }
                .into(),
            ),
            Arc::new(replacement),
        );
    }
    if requires_aggregate_rollup {
        let Some(query_aggregate) = query_aggregate else {
            return Ok(None);
        };
        let Some(aggregate) = build_rollup_aggregate(metadata, query_aggregate)? else {
            return Ok(None);
        };
        replacement = SExpr::create_unary(Arc::new(aggregate.into()), Arc::new(replacement));
    }
    replacement = apply_post_aggregate_filter(replacement, &post_aggregate_predicates);
    Ok(Some((replacement, requires_aggregate_rollup)))
}

fn estimate_rewrite_cost(s_expr: &SExpr) -> f64 {
    let children_cost: f64 = s_expr.children().map(estimate_rewrite_cost).sum();
    let node_cost = match s_expr.plan() {
        RelOperator::Scan(scan) => scan_cost(scan),
        RelOperator::Aggregate(_) => {
            input_cardinality(s_expr).unwrap_or(UNKNOWN_CARDINALITY_COST) * AGGREGATE_PER_ROW
        }
        RelOperator::UnionAll(_) => {
            output_cardinality(s_expr).unwrap_or(UNKNOWN_CARDINALITY_COST) * COMPUTE_PER_ROW
        }
        RelOperator::Filter(_)
        | RelOperator::EvalScalar(_)
        | RelOperator::Sort(_)
        | RelOperator::TopN(_)
        | RelOperator::Limit(_) => {
            output_cardinality(s_expr).unwrap_or(UNKNOWN_CARDINALITY_COST) * COMPUTE_PER_ROW
        }
        _ => output_cardinality(s_expr).unwrap_or(UNKNOWN_CARDINALITY_COST) * COMPUTE_PER_ROW,
    };
    children_cost + node_cost
}

fn scan_cost(scan: &Scan) -> f64 {
    scan.statistics
        .table_stats
        .as_ref()
        .and_then(|stats| stats.num_rows)
        .map(|rows| rows as f64 * COMPUTE_PER_ROW)
        .unwrap_or(UNKNOWN_CARDINALITY_COST)
}

fn input_cardinality(s_expr: &SExpr) -> Option<f64> {
    s_expr.child(0).ok().and_then(output_cardinality)
}

fn output_cardinality(s_expr: &SExpr) -> Option<f64> {
    RelExpr::with_s_expr(s_expr)
        .derive_cardinality()
        .ok()
        .map(|stat| stat.cardinality)
        .filter(|cardinality| cardinality.is_finite())
}

fn apply_post_aggregate_filter(
    replacement: SExpr,
    post_aggregate_predicates: &[ScalarExpr],
) -> SExpr {
    if post_aggregate_predicates.is_empty() {
        replacement
    } else {
        SExpr::create_unary(
            Arc::new(
                Filter {
                    predicates: post_aggregate_predicates.to_vec(),
                }
                .into(),
            ),
            Arc::new(replacement),
        )
    }
}

fn try_build_state_rollup(
    candidate: &MaterializedViewCandidate,
    matched: &ViewRewriteMatch,
    query_aggregate: &Aggregate,
) -> Result<Option<SExpr>> {
    let Some(final_projection) = candidate.read_plan.plan().as_eval_scalar() else {
        return Ok(None);
    };
    let state_merge_expr = candidate.read_plan.unary_child();
    let Some(state_merge) = state_merge_expr.plan().as_aggregate() else {
        return Ok(None);
    };

    let mut logical_to_merged = HashMap::with_capacity(final_projection.items.len());
    for item in &final_projection.items {
        let ScalarExpr::BoundColumnRef(column) = &item.scalar else {
            return Ok(None);
        };
        logical_to_merged.insert(item.index, column.column.clone());
    }

    let mut query_to_logical = HashMap::with_capacity(matched.selection.len());
    for item in &matched.selection {
        let ScalarExpr::BoundColumnRef(column) = &item.scalar else {
            return Ok(None);
        };
        query_to_logical.insert(item.index, column.column.index);
    }

    let mut group_projection = Vec::with_capacity(query_aggregate.group_items.len());
    for query_item in &query_aggregate.group_items {
        let Some(logical_index) = query_to_logical.get(&query_item.index) else {
            return Ok(None);
        };
        let Some(merged_column) = logical_to_merged.get(logical_index) else {
            return Ok(None);
        };
        let Some(merged_group_item) = state_merge
            .group_items
            .iter()
            .find(|item| item.index == merged_column.index)
        else {
            return Ok(None);
        };
        group_projection.push(ScalarItem {
            index: query_item.index,
            scalar: merged_group_item.scalar.clone(),
        });
    }

    let mut aggregate_functions = Vec::with_capacity(query_aggregate.aggregate_functions.len());
    for query_item in &query_aggregate.aggregate_functions {
        let ScalarExpr::AggregateFunction(query_function) = &query_item.scalar else {
            return Ok(None);
        };
        let Some(logical_index) = query_to_logical.get(&query_item.index) else {
            return Ok(None);
        };
        let Some(merged_column) = logical_to_merged.get(logical_index) else {
            return Ok(None);
        };
        let Some(merged_item) = state_merge
            .aggregate_functions
            .iter()
            .find(|item| item.index == merged_column.index)
        else {
            return Ok(None);
        };
        let ScalarExpr::AggregateFunction(merge_function) = &merged_item.scalar else {
            return Ok(None);
        };
        if merge_function.return_type != query_function.return_type {
            return Ok(None);
        }
        let mut merge_function = merge_function.clone();
        merge_function.display_name = query_function.display_name.clone();
        aggregate_functions.push(ScalarItem {
            index: query_item.index,
            scalar: ScalarExpr::AggregateFunction(merge_function),
        });
    }

    let mut child = state_merge_expr.unary_child().clone();
    if !matched.predicates.is_empty() {
        let mut predicates = matched.predicates.clone();
        for predicate in &mut predicates {
            for (logical_index, merged_column) in &logical_to_merged {
                predicate.replace_column_binding(*logical_index, merged_column)?;
            }
        }
        child = SExpr::create_unary(Arc::new(Filter { predicates }.into()), Arc::new(child));
    }
    if !group_projection.is_empty() {
        child = SExpr::create_unary(
            Arc::new(
                EvalScalar {
                    items: group_projection,
                }
                .into(),
            ),
            Arc::new(child),
        );
    }

    Ok(Some(SExpr::create_unary(
        Arc::new(
            Aggregate {
                mode: AggregateMode::Initial,
                group_items: query_aggregate.group_items.clone(),
                aggregate_functions,
                from_distinct: false,
                rank_limit: None,
                grouping_sets: None,
            }
            .into(),
        ),
        Arc::new(child),
    )))
}

fn build_rollup_aggregate(
    metadata: &Metadata,
    query_aggregate: &Aggregate,
) -> Result<Option<Aggregate>> {
    let group_items = query_aggregate
        .group_items
        .iter()
        .map(|item| ScalarItem {
            index: item.index,
            scalar: column_ref(metadata, item.index),
        })
        .collect();
    let mut aggregate_functions = Vec::with_capacity(query_aggregate.aggregate_functions.len());
    for item in &query_aggregate.aggregate_functions {
        let ScalarExpr::AggregateFunction(function) = &item.scalar else {
            return Ok(None);
        };
        if function.distinct || !function.params.is_empty() || !function.sort_descs.is_empty() {
            return Ok(None);
        }
        let rollup_name = match function.func_name.as_str() {
            "sum" => "sum",
            "count" => "sum0",
            "min" => "min",
            "max" => "max",
            _ => return Ok(None),
        };
        aggregate_functions.push(ScalarItem {
            index: item.index,
            scalar: ScalarExpr::AggregateFunction(AggregateFunction {
                span: None,
                func_name: rollup_name.to_string(),
                distinct: false,
                params: vec![],
                args: vec![column_ref(metadata, item.index)],
                return_type: function.return_type.clone(),
                sort_descs: vec![],
                display_name: function.display_name.clone(),
            }),
        });
    }

    Ok(Some(Aggregate {
        mode: AggregateMode::Initial,
        group_items,
        aggregate_functions,
        from_distinct: false,
        rank_limit: None,
        grouping_sets: None,
    }))
}

fn column_ref(metadata: &Metadata, index: crate::Symbol) -> ScalarExpr {
    ScalarExpr::BoundColumnRef(BoundColumnRef {
        span: None,
        column: ColumnBindingBuilder::new(
            metadata.column(index).name(),
            index,
            Box::new(metadata.column(index).data_type()),
            Visibility::Visible,
        )
        .build(),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use databend_common_catalog::table::TableStatistics;

    use super::*;
    use crate::plans::Statistics;

    fn scan_with_rows(rows: Option<u64>) -> SExpr {
        SExpr::create_leaf(Scan {
            statistics: Arc::new(Statistics {
                table_stats: Some(TableStatistics {
                    num_rows: rows,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        })
    }

    fn rewrite_candidate(
        cost: f64,
        read_mode: MaterializedViewCandidateReadMode,
        mv_table_id: u64,
    ) -> RewriteCandidate {
        RewriteCandidate {
            replacement: scan_with_rows(Some(1)),
            mv_table_id,
            cost,
            read_mode,
            logical_sql: String::new(),
            requires_aggregate_rollup: false,
        }
    }

    #[test]
    fn cheaper_rewrite_wins() {
        let cheaper = rewrite_candidate(10.0, MaterializedViewCandidateReadMode::Hybrid, 2);
        let expensive = rewrite_candidate(20.0, MaterializedViewCandidateReadMode::Fresh, 1);
        assert!(cheaper.is_better(&expensive));
        assert!(!expensive.is_better(&cheaper));
    }

    #[test]
    fn fresh_beats_hybrid_at_same_cost() {
        let fresh = rewrite_candidate(10.0, MaterializedViewCandidateReadMode::Fresh, 2);
        let hybrid = rewrite_candidate(10.0, MaterializedViewCandidateReadMode::Hybrid, 1);
        assert!(fresh.is_better(&hybrid));
        assert!(!hybrid.is_better(&fresh));
    }

    #[test]
    fn smaller_table_id_breaks_remaining_ties() {
        let left = rewrite_candidate(10.0, MaterializedViewCandidateReadMode::Fresh, 1);
        let right = rewrite_candidate(10.0, MaterializedViewCandidateReadMode::Fresh, 2);
        assert!(left.is_better(&right));
        assert!(!right.is_better(&left));
    }

    #[test]
    fn smaller_scan_is_cheaper_than_larger_scan() {
        let small = estimate_rewrite_cost(&scan_with_rows(Some(10)));
        let large = estimate_rewrite_cost(&scan_with_rows(Some(100)));
        assert!(small < large);
    }

    #[test]
    fn extra_aggregate_increases_cost() {
        let scan = scan_with_rows(Some(100));
        let with_aggregate = SExpr::create_unary(
            Arc::new(
                Aggregate {
                    mode: AggregateMode::Initial,
                    ..Default::default()
                }
                .into(),
            ),
            Arc::new(scan.clone()),
        );
        assert!(estimate_rewrite_cost(&scan) < estimate_rewrite_cost(&with_aggregate));
    }

    #[test]
    fn unknown_scan_stats_are_more_expensive_than_known_small_scan() {
        let unknown = estimate_rewrite_cost(&scan_with_rows(None));
        let known = estimate_rewrite_cost(&scan_with_rows(Some(1)));
        assert!(known < unknown);
    }
}
