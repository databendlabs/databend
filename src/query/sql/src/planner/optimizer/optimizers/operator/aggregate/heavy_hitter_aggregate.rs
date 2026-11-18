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

//! Heavy hitter aggregation for `GROUP BY key ORDER BY COUNT(*) DESC LIMIT n`.
//!
//! A local, single-key, pure `COUNT(*)` aggregation is annotated for the
//! adaptive executor (`AdaptiveTopKCount`): each worker keeps bounded
//! residuals and exact counts of never-missed keys. When the K-th exact count
//! beats every unknown-key upper bound, the executor returns those counts after
//! one scan. Otherwise it either rescans only the rows of a provably complete
//! candidate set, through `is_topk_count_candidate`, or runs the unchanged
//! exact aggregation. See `databend_common_functions::topk_count`.
//!
//! The rewrite applies only when a rescan reads the same rows: the input must
//! be deterministic operators over one scan of a FUSE table. It also applies
//! only when cost heuristics expect it to be faster, see
//! `HeavyHitterAggregateOptimizer::worth_rewriting`. Distributed plans and
//! aggregations with other functions are not rewritten.

use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_functions::topk_count::is_topk_count_key_type;
use databend_common_functions::topk_count::topk_count_capacity;

use crate::ColumnEntry;
use crate::Metadata;
use crate::ScalarExpr;
use crate::Symbol;
use crate::optimizer::Optimizer;
use crate::optimizer::OptimizerContext;
use crate::optimizer::ir::SExpr;
use crate::plans::AdaptiveTopKCountInfo;
use crate::plans::Aggregate;
use crate::plans::AggregateFunction;
use crate::plans::EvalScalar;
use crate::plans::Operator;
use crate::plans::RelOperator;
use crate::plans::Scan;
use crate::plans::Sort;
use crate::plans::SortItem;

/// The largest `limit + offset` the rewrite applies to.
const MAX_LIMIT: usize = 1000;
/// Below this many groups the aggregation is cheap and the rewrite gains little.
const MIN_GROUPS: f64 = 1_000_000.0;
/// A filter keeping a smaller share of the scanned rows costs more to evaluate
/// again than the aggregation it saves.
const MIN_FILTER_SELECTIVITY: f64 = 0.2;
/// Without top-N stats, a key with more groups per row is assumed to have too
/// few frequent keys for the summary to prove the candidates.
const MAX_GROUPS_PER_ROW: f64 = 0.9;

pub struct HeavyHitterAggregateOptimizer {
    opt_ctx: Arc<OptimizerContext>,
    /// Applies the rewrite to every matching query, skipping the cost heuristics.
    force: bool,
}

impl HeavyHitterAggregateOptimizer {
    pub fn new(opt_ctx: Arc<OptimizerContext>, force: bool) -> Self {
        Self { opt_ctx, force }
    }

    pub fn optimize_sync(&self, s_expr: SExpr) -> Result<SExpr> {
        self.rewrite(&s_expr)
    }

    #[recursive::recursive]
    fn rewrite(&self, s_expr: &SExpr) -> Result<SExpr> {
        if let Some(rewritten) = self.try_rewrite(s_expr)? {
            return Ok(rewritten);
        }
        let children = s_expr
            .children()
            .map(|child| Ok(Arc::new(self.rewrite(child)?)))
            .collect::<Result<Vec<_>>>()?;
        if children
            .iter()
            .zip(s_expr.children())
            .all(|(new, old)| new.as_ref() == old)
        {
            return Ok(s_expr.clone());
        }
        Ok(s_expr.replace_children(children))
    }

    /// Rewrites `s_expr` if it is a matching `TopN`, or a `Sort` with a limit.
    fn try_rewrite(&self, s_expr: &SExpr) -> Result<Option<SExpr>> {
        let (order_by, limit) = match s_expr.plan() {
            RelOperator::TopN(top_n) => (&top_n.items, top_n.candidate_count()),
            RelOperator::Sort(Sort {
                items,
                limit: Some(limit),
                ..
            }) => (items, *limit),
            _ => return Ok(None),
        };
        let (eval_scalar, agg_expr) = match s_expr.unary_child().plan() {
            RelOperator::Aggregate(_) => (None, s_expr.unary_child()),
            RelOperator::EvalScalar(eval_scalar) => {
                let child = s_expr.unary_child().unary_child();
                if !matches!(child.plan(), RelOperator::Aggregate(_)) {
                    return Ok(None);
                }
                (Some(eval_scalar), child)
            }
            _ => return Ok(None),
        };
        let RelOperator::Aggregate(aggregate) = agg_expr.plan() else {
            unreachable!()
        };
        if !(1..=MAX_LIMIT).contains(&limit) || !orders_by_count(order_by, eval_scalar, aggregate) {
            return Ok(None);
        }
        let [group_item] = aggregate.group_items.as_slice() else {
            return Ok(None);
        };
        if aggregate.grouping_sets.is_some()
            || !is_topk_count_key_type(&group_item.scalar.data_type())
        {
            return Ok(None);
        }

        // The adaptive executor runs on one node and only produces COUNT(*).
        if self.opt_ctx.get_enable_distributed_optimization()
            || aggregate.aggregate_functions.len() != 1
        {
            return Ok(None);
        }

        let input = agg_expr.unary_child();
        if !input
            .derive_relational_prop()?
            .output_columns
            .contains(&group_item.index)
        {
            return Ok(None);
        }
        let capacity = topk_count_capacity(limit);
        if !self.force && !self.worth_rewriting(input, group_item.index, limit, capacity)? {
            return Ok(None);
        }
        if !can_rescan_input(input, &self.opt_ctx.get_metadata().read()) {
            return Ok(None);
        }

        // Keep the original input and exact aggregation as a lazy fallback.
        let mut adaptive = aggregate.clone();
        adaptive.adaptive_topk_count = Some(AdaptiveTopKCountInfo {
            limit,
            likely_candidates: has_frequency_evidence(input, group_item.index, limit, capacity)
                .unwrap_or(false),
        });
        let agg_expr = agg_expr.replace_plan(Arc::new(adaptive.into()));
        let rewritten = match eval_scalar {
            None => s_expr.replace_children([Arc::new(agg_expr)]),
            Some(_) => s_expr.replace_children([Arc::new(
                s_expr.unary_child().replace_children([Arc::new(agg_expr)]),
            )]),
        };
        Ok(Some(rewritten))
    }

    /// Whether the rewrite is expected to be faster than aggregating every row.
    ///
    /// A failed proof costs an extra (partial) scan and only saves hash table
    /// work, so:
    /// - the aggregation must hold many groups;
    /// - a filter must be cheap to evaluate again and keep enough rows;
    /// - the summary must be likely to prove the result, since otherwise
    ///   every row is aggregated after the extra scan. Top-N stats of the key,
    ///   even stale ones, tell whether `limit` keys are frequent enough.
    ///   Without them, a key that is almost unique is assumed to fail.
    ///
    /// These are estimates; the result is exact whatever they decide.
    fn worth_rewriting(
        &self,
        input: &SExpr,
        key: Symbol,
        limit: usize,
        capacity: usize,
    ) -> Result<bool> {
        let Some(scan) = find_scan(input) else {
            return Ok(false);
        };
        let stat_info = input.derive_cardinality(self.opt_ctx.get_stat_context())?;
        let rows = stat_info.cardinality;
        let groups = stat_info.statistics.column_stats.get(&key).map(|stat| {
            let ndv = stat.ndv();
            ndv.expected.unwrap_or(ndv.upper).min(rows)
        });
        if groups.unwrap_or(rows) < MIN_GROUPS.max(capacity as f64) {
            return Ok(false);
        }

        let mut predicates = vec![];
        collect_predicates(input, &mut predicates);
        if !predicates.is_empty() {
            if !predicates
                .iter()
                .all(|predicate| is_cheap_predicate(predicate))
            {
                return Ok(false);
            }
            let scanned = scan
                .statistics
                .table_stats
                .and_then(|stats| stats.num_rows)
                .map_or(rows, |num_rows| num_rows as f64);
            if scanned > 0.0 && rows / scanned < MIN_FILTER_SELECTIVITY {
                return Ok(false);
            }
        }

        if let Some(likely) = has_frequency_evidence(input, key, limit, capacity) {
            return Ok(likely);
        }
        Ok(groups.is_none_or(|groups| groups / rows <= MAX_GROUPS_PER_ROW))
    }
}

#[async_trait::async_trait]
impl Optimizer for HeavyHitterAggregateOptimizer {
    fn name(&self) -> String {
        "HeavyHitterAggregateOptimizer".to_string()
    }

    async fn optimize(&mut self, s_expr: SExpr) -> Result<SExpr> {
        self.optimize_sync(s_expr)
    }
}

/// Cost evidence only, including stale ANALYZE. Assuming filters retain the
/// same fraction of each key, scaling frequencies and rows cancels out. None
/// means the column has no usable frequency hint, not that it has no hot keys.
fn has_frequency_evidence(
    input: &SExpr,
    key: Symbol,
    limit: usize,
    capacity: usize,
) -> Option<bool> {
    let (top_n, rows) = find_scan(input)?.statistics.approx_top_n.get(&key)?;
    if *rows == 0 {
        return None;
    }
    let threshold = 2.0 * *rows as f64 / (capacity as f64 + 1.0);
    Some(
        top_n
            .values
            .iter()
            .filter(|entry| entry.count.saturating_sub(entry.error) as f64 > threshold)
            .count()
            >= limit,
    )
}

/// Whether the first sort key is `COUNT(*) DESC` of `aggregate`.
///
/// Later sort keys only break ties: a key outside the candidates occurs
/// fewer times than each of the first `limit` keys, so it ranks after them
/// whatever the later sort keys are.
fn orders_by_count(
    order_by: &[SortItem],
    eval_scalar: Option<&EvalScalar>,
    aggregate: &Aggregate,
) -> bool {
    let Some(first) = order_by.first() else {
        return false;
    };
    if first.asc {
        return false;
    }
    let mut index = first.index;
    if let Some(eval_scalar) = eval_scalar
        && let Some(item) = eval_scalar.items.iter().find(|item| item.index == index)
    {
        let ScalarExpr::BoundColumnRef(column) = &item.scalar else {
            return false;
        };
        index = column.column.index;
    }
    aggregate.aggregate_functions.iter().any(|item| {
        item.index == index
            && matches!(
                &item.scalar,
                ScalarExpr::AggregateFunction(AggregateFunction {
                    func_name,
                    distinct: false,
                    params,
                    args,
                    sort_descs,
                    ..
                }) if func_name == "count" && params.is_empty() && args.is_empty() && sort_descs.is_empty()
            )
    })
}

/// The scan of an input accepted by [`can_rescan_input`].
fn find_scan(s_expr: &SExpr) -> Option<&Scan> {
    match s_expr.plan() {
        RelOperator::Scan(scan) => Some(scan),
        _ => s_expr.children().find_map(find_scan),
    }
}

/// The filter predicates of an input accepted by [`can_rescan_input`].
fn collect_predicates<'a>(s_expr: &'a SExpr, predicates: &mut Vec<&'a ScalarExpr>) {
    match s_expr.plan() {
        RelOperator::Filter(filter) => predicates.extend(&filter.predicates),
        RelOperator::Scan(scan) => {
            predicates.extend(scan.push_down_predicates.iter().flatten());
        }
        _ => {}
    }
    for child in s_expr.children() {
        collect_predicates(child, predicates);
    }
}

/// Whether evaluating `predicate` again is cheap: comparisons of a column with
/// constants, NULL tests, and their combinations. Such predicates are also
/// the ones block statistics can prune by.
fn is_cheap_predicate(predicate: &ScalarExpr) -> bool {
    let is_operand = |scalar: &ScalarExpr| {
        matches!(
            scalar,
            ScalarExpr::BoundColumnRef(_)
                | ScalarExpr::ConstantExpr(_)
                | ScalarExpr::TypedConstantExpr(_, _)
        )
    };
    match predicate {
        ScalarExpr::ConstantExpr(_) | ScalarExpr::TypedConstantExpr(_, _) => true,
        ScalarExpr::FunctionCall(call) => match call.func_name.as_str() {
            "and" | "or" | "and_filters" | "or_filters" | "not" | "is_true" => {
                call.arguments.iter().all(is_cheap_predicate)
            }
            "eq" | "noteq" | "gt" | "gte" | "lt" | "lte" | "is_null" | "is_not_null" => {
                call.arguments.iter().all(is_operand)
            }
            _ => false,
        },
        _ => false,
    }
}

/// Whether scanning `input` again in the same query reads the same rows.
fn can_rescan_input(input: &SExpr, metadata: &Metadata) -> bool {
    let mut scans = 0;
    can_rescan_inner(input, metadata, &mut scans) && scans == 1
}

fn can_rescan_inner(s_expr: &SExpr, metadata: &Metadata, scans: &mut usize) -> bool {
    let deterministic = s_expr
        .plan()
        .scalar_expr_iter()
        .all(ScalarExpr::is_deterministic);
    let supported = match s_expr.plan() {
        RelOperator::Filter(_) | RelOperator::EvalScalar(_) => true,
        RelOperator::Scan(scan) => {
            *scans += 1;
            can_rescan(scan, metadata)
        }
        _ => false,
    };
    deterministic
        && supported
        && s_expr
            .children()
            .all(|child| can_rescan_inner(child, metadata, scans))
}

fn can_rescan(scan: &Scan, metadata: &Metadata) -> bool {
    let table = metadata.table(scan.table_index).table();
    table.engine() == "FUSE"
        && scan.limit.is_none()
        && scan.sample.is_none()
        && scan.change_type.is_none()
        && scan.secure_predicates.is_none()
        && scan.inverted_index.is_none()
        && scan.vector_index.is_none()
        && scan
            .push_down_predicates
            .iter()
            .flatten()
            .chain(
                scan.prewhere
                    .iter()
                    .flat_map(|prewhere| &prewhere.predicates),
            )
            .all(ScalarExpr::is_deterministic)
        && scan.prewhere.as_ref().is_none_or(|prewhere| {
            prewhere.output_columns.is_subset(&scan.columns)
                && prewhere.prewhere_columns.is_subset(&scan.columns)
        })
        && scan.columns.iter().all(|column| {
            matches!(
                metadata.column(*column),
                ColumnEntry::BaseTableColumn(column)
                    if column.path_indices.is_none() && column.virtual_expr.is_none()
            )
        })
}
