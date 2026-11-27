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

use std::collections::HashMap;

use databend_common_exception::Result;
use databend_common_expression::FunctionContext;
use databend_common_expression::RemoteExpr;
use databend_common_expression::conversion::common_super_type_with_conversion;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_sql::MetadataRef;
use databend_common_sql::Symbol;
use databend_common_sql::optimizer::ir::PExpr;
use databend_common_sql::plans::JoinEquiCondition;
use databend_common_sql::plans::JoinType;
use databend_common_sql::plans::PhysicalRelOperator;
use databend_common_sql::plans::ScalarExpr;

use super::builder::normalize_probe_target;
use super::builder::push_unique_target;
use super::builder::supported_probe_key_for_runtime_filter;
use super::types::resolve_runtime_filter_probe_expr;

pub(super) fn find_probe_targets(
    metadata: &MetadataRef,
    s_expr: &PExpr,
    probe_key: RemoteExpr<String>,
    probe_scan_id: usize,
    probe_key_col_idx: Symbol,
    func_ctx: &FunctionContext,
) -> Result<Vec<(RemoteExpr<String>, usize)>> {
    if !probe_path_is_safe(s_expr, probe_scan_id, probe_key_col_idx) {
        return Ok(vec![]);
    }

    let mut relations = Vec::new();
    for cond in collect_equi_conditions(s_expr)? {
        if let (Some(left), Some(right)) = (
            scalar_to_probe_target(metadata, &cond.left, func_ctx)?,
            scalar_to_probe_target(metadata, &cond.right, func_ctx)?,
        ) {
            // Check both connector and leaf endpoints before they enter the
            // graph: an unsafe endpoint must not become an equivalence bridge.
            if probe_path_is_safe(s_expr, left.scan_id, left.column_idx)
                && probe_path_is_safe(s_expr, right.scan_id, right.column_idx)
            {
                relations.push((left, right));
            }
        }
    }

    propagate_probe_targets(
        probe_key,
        probe_scan_id,
        probe_key_col_idx,
        relations,
        func_ctx,
    )
}

fn propagate_probe_targets(
    probe_key: RemoteExpr<String>,
    probe_scan_id: usize,
    probe_key_col_idx: Symbol,
    relations: Vec<(ProbeTarget, ProbeTarget)>,
    func_ctx: &FunctionContext,
) -> Result<Vec<(RemoteExpr<String>, usize)>> {
    let mut uf = UnionFind::new();
    let mut column_to_remote: HashMap<Symbol, (RemoteExpr<String>, usize)> = HashMap::new();
    let mut leaf_targets: HashMap<Symbol, Vec<(RemoteExpr<String>, usize)>> = HashMap::new();
    let target_type = probe_key.as_expr(&BUILTIN_FUNCTIONS).data_type().clone();
    column_to_remote.insert(probe_key_col_idx, (probe_key, probe_scan_id));

    for (left, right) in relations {
        let left_type = left
            .remote_expr
            .as_expr(&BUILTIN_FUNCTIONS)
            .data_type()
            .clone();
        let right_type = right
            .remote_expr
            .as_expr(&BUILTIN_FUNCTIONS)
            .data_type()
            .clone();
        if !common_super_type_with_conversion(left_type, right_type)
            .is_some_and(|conversion| conversion.is_safe_for_equality_inference())
        {
            continue;
        }

        match (left.is_connector, right.is_connector) {
            (true, true) => {
                uf.union(left.column_idx, right.column_idx);
                column_to_remote
                    .entry(left.column_idx)
                    .or_insert((left.remote_expr, left.scan_id));
                column_to_remote
                    .entry(right.column_idx)
                    .or_insert((right.remote_expr, right.scan_id));
            }
            (true, false) => leaf_targets
                .entry(left.column_idx)
                .or_default()
                .push((right.remote_expr, right.scan_id)),
            (false, true) => leaf_targets
                .entry(right.column_idx)
                .or_default()
                .push((left.remote_expr, left.scan_id)),
            (false, false) => {}
        }
    }

    let equiv_class = uf.get_equivalence_class(probe_key_col_idx);

    let mut result = Vec::new();
    for idx in equiv_class {
        if let Some((remote_expr, scan_id)) = column_to_remote.get(&idx) {
            if let Some(remote_expr) = normalize_probe_target(remote_expr, &target_type, func_ctx)?
            {
                push_unique_target(&mut result, remote_expr, *scan_id);
            }
        }
        if let Some(targets) = leaf_targets.get(&idx) {
            for (remote_expr, scan_id) in targets {
                if let Some(remote_expr) =
                    normalize_probe_target(remote_expr, &target_type, func_ctx)?
                {
                    push_unique_target(&mut result, remote_expr, *scan_id);
                }
            }
        }
    }

    Ok(result)
}

/// One policy for target paths and equivalence collection. Only traverse
/// inputs to which a parent scan predicate can commute. Creating a filter
/// *at* a join does not imply that another join's filter may cross it.
pub(super) fn probe_child_is_safe(plan: &PhysicalRelOperator, child_index: usize) -> bool {
    match plan {
        // Check every item, not just the RF key: changing the input block can
        // also change projected values or which rows a stateful filter keeps.
        PhysicalRelOperator::EvalScalar(eval) => {
            eval.items.iter().all(|item| item.scalar.is_deterministic())
        }
        PhysicalRelOperator::Filter(filter) => {
            filter.predicates.iter().all(ScalarExpr::is_deterministic)
        }
        PhysicalRelOperator::Exchange(_) => true,
        PhysicalRelOperator::Sort(sort) => sort.limit.is_none() && sort.window_partition.is_none(),
        PhysicalRelOperator::Aggregate(aggregate) => {
            !aggregate.aggregate.group_items.is_empty()
                && aggregate.aggregate.grouping_sets.is_none()
                && aggregate.aggregate.rank_limit.is_none()
        }
        PhysicalRelOperator::Join(join) => {
            // Rewritten scalar joins still enforce single-row cardinality.
            // Join conditions can be evaluated directly by hash/range joins,
            // without an intervening EvalScalar. A parent's RF must not change
            // the input block seen by any such condition, even on another key.
            if join.single_to_inner.is_some()
                || !join.equi_conditions.iter().all(|condition| {
                    condition.left.is_deterministic() && condition.right.is_deterministic()
                })
                || !join
                    .non_equi_conditions
                    .iter()
                    .all(ScalarExpr::is_deterministic)
            {
                return false;
            }
            match join.join_type {
                JoinType::Inner | JoinType::Cross => true,
                // Mark names describe the marker side, not the retained side.
                JoinType::Left | JoinType::LeftSemi | JoinType::LeftAnti | JoinType::RightMark => {
                    child_index == 0
                }
                JoinType::Right
                | JoinType::RightSemi
                | JoinType::RightAnti
                | JoinType::LeftMark => child_index == 1,
                // Binder puts window(original right) in child0 and original
                // left in child1. Keep the window itself as a barrier below.
                JoinType::Asof | JoinType::LeftAsof => child_index == 1,
                JoinType::RightAsof => child_index == 0,
                // Full joins can create NULL-extended rows on either side.
                // ANY requires separate validation of representative-row
                // selection. Single joins can hide cardinality errors.
                JoinType::Full
                | JoinType::InnerAny
                | JoinType::LeftAny
                | JoinType::RightAny
                | JoinType::LeftSingle
                | JoinType::RightSingle
                | JoinType::FullAsof => false,
            }
        }
        // Shared CTEs, windows, TopN and operators needing column remapping
        // (e.g. UnionAll) remain barriers until separately proven safe.
        _ => false,
    }
}

/// Verify the complete path to a scan *column*, not just scan reachability.
/// Plain aggregation is transparent only for an unchanged grouping column in
/// either physical phase. Computed/renamed group keys and aggregate outputs
/// are intentionally not inferred from metadata's scan reachability.
/// If an id occurs more than once, every occurrence must be safe: filtering a
/// shared scan on behalf of just one consumer can change another consumer.
fn probe_path_is_safe(s_expr: &PExpr, scan_id: usize, column: Symbol) -> bool {
    fn visit(s_expr: &PExpr, scan_id: usize, column: Symbol, safe: bool) -> Option<bool> {
        let plan = s_expr.plan();
        let mut safe = safe;
        if let PhysicalRelOperator::Aggregate(aggregate) = plan {
            safe &= aggregate.aggregate.group_items.iter().any(|item| {
                item.index == column
                    && matches!(&item.scalar, ScalarExpr::BoundColumnRef(reference)
                        if reference.column.index == column)
            });
        }
        if let PhysicalRelOperator::EvalScalar(eval) = plan {
            // EvalScalar normally introduces fresh symbols. Do not assume a
            // redefinition preserves the base column used by the filter.
            safe &= eval.items.iter().all(|item| {
                item.index != column
                    || matches!(&item.scalar, ScalarExpr::BoundColumnRef(reference)
                        if reference.column.index == column)
            });
        }
        if let PhysicalRelOperator::Scan(scan) = plan {
            return (scan.scan_id == scan_id).then_some(
                safe && scan.columns.contains(&column)
                    && scan.limit.is_none()
                    && scan.sample.is_none()
                    && scan.vector_index.is_none()
                    && scan
                        .push_down_predicates
                        .iter()
                        .flatten()
                        .all(ScalarExpr::is_deterministic)
                    && scan
                        .prewhere
                        .as_ref()
                        .is_none_or(|p| p.predicates.iter().all(ScalarExpr::is_deterministic))
                    && scan
                        .secure_predicates
                        .iter()
                        .flatten()
                        .all(ScalarExpr::is_deterministic),
            );
        }
        let mut result = None;
        // Visit even below barriers to detect unsafe duplicate occurrences.
        for (child_index, child) in s_expr.children().enumerate() {
            let child_safe = safe && probe_child_is_safe(plan, child_index);
            if let Some(child_safe) = visit(child, scan_id, column, child_safe) {
                result = Some(result.unwrap_or(true) && child_safe);
            }
        }
        result
    }
    visit(s_expr, scan_id, column, true).unwrap_or(false)
}

fn collect_equi_conditions(s_expr: &PExpr) -> Result<Vec<JoinEquiCondition>> {
    // Every endpoint is checked against the full column path before entering
    // the graph. Across a plain aggregate, this permits only unchanged group
    // keys on both ends; non-group columns cannot become equivalence bridges.
    let mut conditions = Vec::new();

    if let PhysicalRelOperator::Join(join) = s_expr.plan() {
        if matches!(join.join_type, JoinType::Inner) && probe_child_is_safe(s_expr.plan(), 0) {
            conditions.extend(join.equi_conditions.clone());
        }
    }

    for (child_index, child) in s_expr.children().enumerate() {
        if probe_child_is_safe(s_expr.plan(), child_index) {
            conditions.extend(collect_equi_conditions(child)?);
        }
    }

    Ok(conditions)
}

struct ProbeTarget {
    remote_expr: RemoteExpr<String>,
    scan_id: usize,
    column_idx: Symbol,
    is_connector: bool,
}

fn scalar_to_probe_target(
    metadata: &MetadataRef,
    scalar: &ScalarExpr,
    func_ctx: &FunctionContext,
) -> Result<Option<ProbeTarget>> {
    let Some(probe) = resolve_runtime_filter_probe_expr(metadata, scalar)? else {
        return Ok(None);
    };
    let Some(column_idx) = scalar.used_columns().into_iter().next() else {
        return Ok(None);
    };
    let Some(scan_id) = metadata.read().base_column_scan_id(column_idx) else {
        return Ok(None);
    };
    let remote_expr = probe.as_remote_expr();

    if supported_probe_key_for_runtime_filter(&remote_expr, func_ctx) {
        let (remote_expr, is_connector) = match canonical_equivalence_connector(remote_expr) {
            Ok(connector) => (connector, true),
            Err(leaf) => (leaf, false),
        };
        return Ok(Some(ProbeTarget {
            remote_expr,
            scan_id,
            column_idx,
            is_connector,
        }));
    }

    Ok(None)
}

struct UnionFind {
    parent: HashMap<Symbol, Symbol>,
}

impl UnionFind {
    fn new() -> Self {
        Self {
            parent: HashMap::new(),
        }
    }

    fn find(&mut self, x: Symbol) -> Symbol {
        if !self.parent.contains_key(&x) {
            self.parent.insert(x, x);
            return x;
        }

        let parent = *self.parent.get(&x).unwrap();
        if parent != x {
            let root = self.find(parent);
            self.parent.insert(x, root);
        }
        *self.parent.get(&x).unwrap()
    }

    fn union(&mut self, x: Symbol, y: Symbol) {
        let root_x = self.find(x);
        let root_y = self.find(y);
        if root_x != root_y {
            self.parent.insert(root_x, root_y);
        }
    }

    fn get_equivalence_class(&mut self, x: Symbol) -> Vec<Symbol> {
        let root = self.find(x);
        let all_keys: Vec<_> = self.parent.keys().copied().collect();
        all_keys
            .into_iter()
            .filter(|&k| self.find(k) == root)
            .collect()
    }
}

/// Only a direct column, optionally wrapped in a nullability-only cast, can
/// connect equivalence classes. Every value-changing expression is a leaf.
fn canonical_equivalence_connector(
    probe_key: RemoteExpr<String>,
) -> std::result::Result<RemoteExpr<String>, RemoteExpr<String>> {
    match probe_key {
        probe_key @ RemoteExpr::ColumnRef { .. } => Ok(probe_key),
        RemoteExpr::Cast {
            expr,
            dest_type,
            is_try: false,
            span,
        } => match expr.as_ref() {
            RemoteExpr::ColumnRef { data_type, .. }
                if dest_type.remove_nullable() == *data_type =>
            {
                Ok(*expr)
            }
            _ => Err(RemoteExpr::Cast {
                span,
                is_try: false,
                expr,
                dest_type,
            }),
        },
        probe_key => Err(probe_key),
    }
}
