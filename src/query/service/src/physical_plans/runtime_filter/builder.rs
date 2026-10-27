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

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::Domain;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionEval;
use databend_common_expression::RemoteExpr;
use databend_common_expression::conversion::classify_conversion;
use databend_common_expression::conversion::common_super_type_with_conversion;
use databend_common_expression::type_check::check_cast;
use databend_common_expression::types::DataType;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_sql::ColumnEntry;
use databend_common_sql::IndexType;
use databend_common_sql::MetadataRef;
use databend_common_sql::Symbol;
use databend_common_sql::optimizer::ir::PExpr;
use databend_common_sql::plans::AggregateMode;
use databend_common_sql::plans::Exchange;
use databend_common_sql::plans::Join;
use databend_common_sql::plans::JoinEquiCondition;
use databend_common_sql::plans::JoinType;
use databend_common_sql::plans::PhysicalRelOperator;
use databend_common_sql::plans::ScalarExpr;

use super::types::PhysicalRuntimeFilter;
use super::types::PhysicalRuntimeFilters;
use super::types::RuntimeFilterProbeKey;
use super::types::canonical_equivalence_connector;
use super::types::resolve_runtime_filter_probe_expr;
use crate::sessions::TableContext;

/// Check if a data type is supported for bloom filter
///
/// Currently supports: numbers and strings
pub fn is_type_supported_for_bloom_filter(data_type: &DataType) -> bool {
    data_type.is_number() || data_type.is_string()
}

/// Check if a data type is supported for min-max filter
///
/// Currently supports: numbers, dates, and strings
pub fn is_type_supported_for_min_max_filter(data_type: &DataType) -> bool {
    data_type.is_number() || data_type.is_date() || data_type.is_string()
}

/// Check if the join type is supported for runtime filter
///
/// Runtime filters are only applicable to certain join types where
/// filtering the probe side can reduce processing
pub fn supported_join_type_for_runtime_filter(join_type: &JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Inner
            | JoinType::LeftSemi
            | JoinType::Right
            | JoinType::RightSemi
            | JoinType::RightAnti
            | JoinType::LeftMark
    )
}

/// Resolve statistics for the bloom selectivity estimate, not permission to push
/// a probe predicate through an operator. Keep the existing single-base-column
/// expression behavior; derived keys must be direct MIN/MAX results.
pub(in crate::physical_plans) fn resolve_runtime_filter_build_table_index(
    metadata: &MetadataRef,
    build_side: &PExpr,
    build_key: &ScalarExpr,
) -> Option<IndexType> {
    let columns = build_key.used_columns();
    if columns.len() != 1 {
        return None;
    }
    let column = *columns.iter().next()?;
    if let ColumnEntry::BaseTableColumn(base) = metadata.read().column(column) {
        return Some(base.table_index);
    }
    let ScalarExpr::BoundColumnRef(_) = build_key else {
        return None;
    };
    min_max_output_table_index(metadata, build_side, column)
}

#[recursive::recursive]
fn min_max_output_table_index(
    metadata: &MetadataRef,
    expr: &PExpr,
    column: Symbol,
) -> Option<IndexType> {
    match expr.plan() {
        PhysicalRelOperator::Aggregate(physical) => {
            // A Partial output is an aggregate state, not a MIN/MAX value. Main
            // clones the same logical arguments into Final and Partial; Final
            // arguments still name pre-Partial columns, not state columns.
            if physical.mode != AggregateMode::Final || physical.aggregate.grouping_sets.is_some() {
                return None;
            }
            let item = physical
                .aggregate
                .aggregate_functions
                .iter()
                .find(|item| item.index == column)?;
            let ScalarExpr::AggregateFunction(function) = &item.scalar else {
                return None;
            };
            if !matches!(function.func_name.as_str(), "min" | "max")
                || function.distinct
                || !function.params.is_empty()
                || !function.sort_descs.is_empty()
                || function.args.len() != 1
            {
                return None;
            }
            let ScalarExpr::BoundColumnRef(source) = &function.args[0] else {
                return None;
            };
            // MIN/MAX may add nullability (empty/all-NULL groups), but do not
            // change the underlying value type. Do not mistake state types for
            // value types or require nullable and non-nullable types to match.
            if function.return_type.remove_nullable() != source.column.data_type.remove_nullable() {
                return None;
            }
            let input = expr.unary_child();
            let partial = match input.plan() {
                PhysicalRelOperator::Exchange(_) => input.unary_child(),
                _ => input,
            };
            let PhysicalRelOperator::Aggregate(partial_agg) = partial.plan() else {
                return None;
            };
            if partial_agg.mode != AggregateMode::Partial
                || partial_agg.aggregate.grouping_sets.is_some()
                || !partial_agg
                    .aggregate
                    .aggregate_functions
                    .iter()
                    .any(|partial_item| partial_item == item)
            {
                return None;
            }
            direct_scan_table_index(metadata, partial.unary_child(), source.column.index)
        }
        PhysicalRelOperator::EvalScalar(eval) => {
            let column = match eval.items.iter().find(|item| item.index == column) {
                Some(item) => match &item.scalar {
                    ScalarExpr::BoundColumnRef(source) => source.column.index,
                    _ => return None,
                },
                None => column,
            };
            min_max_output_table_index(metadata, expr.unary_child(), column)
        }
        PhysicalRelOperator::Filter(_)
        | PhysicalRelOperator::Sort(_)
        | PhysicalRelOperator::Limit(_)
        | PhysicalRelOperator::Exchange(_) => {
            min_max_output_table_index(metadata, expr.unary_child(), column)
        }
        _ => None,
    }
}

/// Require one actual scan and an unchanged column path to it. Metadata alone
/// is insufficient: a derived/CTE output is not a base column, and even a base
/// symbol must belong to this scan's table and output columns. Joins (including
/// self joins), shared CTEs, nested aggregates and unknown operators are barriers.
#[recursive::recursive]
fn direct_scan_table_index(
    metadata: &MetadataRef,
    expr: &PExpr,
    column: Symbol,
) -> Option<IndexType> {
    match expr.plan() {
        PhysicalRelOperator::Scan(scan) => match metadata.read().column(column) {
            ColumnEntry::BaseTableColumn(base)
                if base.table_index == scan.table_index && scan.columns.contains(&column) =>
            {
                Some(scan.table_index)
            }
            _ => None,
        },
        PhysicalRelOperator::EvalScalar(eval) => {
            let column = match eval.items.iter().find(|item| item.index == column) {
                Some(item) => match &item.scalar {
                    ScalarExpr::BoundColumnRef(source) => source.column.index,
                    _ => return None,
                },
                None => column,
            };
            direct_scan_table_index(metadata, expr.unary_child(), column)
        }
        PhysicalRelOperator::Filter(_)
        | PhysicalRelOperator::Sort(_)
        | PhysicalRelOperator::Limit(_)
        | PhysicalRelOperator::Exchange(_) => {
            direct_scan_table_index(metadata, expr.unary_child(), column)
        }
        _ => None,
    }
}

/// Build runtime filters for a join operation
///
/// Creates one runtime filter per eligible probe key and propagates it to
/// equality-safe connector columns and their attached expression leaves.
///
/// # Arguments
/// * `ctx` - Table context
/// * `metadata` - Metadata reference
/// * `join` - Join plan
/// * `s_expr` - PExpr for the join
/// * `build_keys` - Build side keys
/// * `probe_keys` - Probe keys with their scan, source column, connector, and null-equality metadata
///
/// # Returns
/// Collection of runtime filters to be applied
pub async fn build_runtime_filter(
    ctx: Arc<dyn TableContext>,
    metadata: &MetadataRef,
    join: &Join,
    s_expr: &PExpr,
    build_keys: &[RemoteExpr],
    probe_keys: Vec<Option<RuntimeFilterProbeKey<RemoteExpr<String>>>>,
    build_table_indexes: Vec<Option<IndexType>>,
) -> Result<PhysicalRuntimeFilters> {
    if !ctx.get_settings().get_enable_join_runtime_filter()? {
        return Ok(Default::default());
    }

    if !supported_join_type_for_runtime_filter(&join.join_type) {
        return Ok(Default::default());
    }

    let build_side = s_expr.build_side_child();
    let build_side_data_distribution = build_side.get_data_distribution()?;
    if build_side_data_distribution.as_ref().is_some_and(|e| {
        !matches!(
            e,
            Exchange::Broadcast
                | Exchange::NodeToNodeHash(_)
                | Exchange::GlobalHash(_)
                | Exchange::Merge
        )
    }) {
        return Ok(Default::default());
    }

    let mut filters = Vec::new();
    let func_ctx = ctx.get_function_context()?;

    let probe_side = s_expr.probe_side_child();

    // Process each probe key that has runtime filter information
    for (build_key, probe, build_table_index) in build_keys
        .iter()
        .zip(probe_keys)
        .zip(build_table_indexes)
        .filter_map(|((build_key, probe), table_index)| {
            probe.map(|probe| (build_key, probe, table_index))
        })
    {
        let RuntimeFilterProbeKey {
            probe_key,
            scan_id,
            column_idx,
            is_connector,
            is_null_equal,
        } = probe;
        if !supported_probe_key_for_runtime_filter(&probe_key, &func_ctx) {
            continue;
        }

        let probe_targets = find_probe_targets(
            metadata,
            probe_side,
            probe_key.clone(),
            scan_id,
            column_idx,
            is_connector,
            &func_ctx,
        )?;

        // A valid probe expression may have no safe scan target (e.g. a window
        // output). Skip filter creation and ID allocation when no targets remain.
        if probe_targets.is_empty() {
            continue;
        }

        let build_table_rows =
            get_build_table_rows(ctx.clone(), metadata, build_table_index).await?;
        let probe_table_index = resolve_probe_table_index(
            metadata,
            probe_side,
            &probe_targets,
            &probe_key,
            scan_id,
            column_idx,
        );
        let probe_table_rows =
            get_build_table_rows(ctx.clone(), metadata, probe_table_index).await?;

        let data_type = build_key
            .as_expr(&BUILTIN_FUNCTIONS)
            .data_type()
            .remove_nullable();
        let id = metadata.write().next_runtime_filter_id();

        let enable_bloom_runtime_filter =
            !is_null_equal && is_type_supported_for_bloom_filter(&data_type);

        let enable_min_max_runtime_filter =
            !is_null_equal && is_type_supported_for_min_max_filter(&data_type);

        let enable_inlist_runtime_filter = !is_null_equal;

        // Create and add the runtime filter
        let runtime_filter = PhysicalRuntimeFilter {
            id,
            build_key: build_key.clone(),
            probe_targets,
            build_table_rows,
            probe_table_rows,
            enable_bloom_runtime_filter,
            enable_inlist_runtime_filter,
            enable_min_max_runtime_filter,
        };
        filters.push(runtime_filter);
    }

    Ok(PhysicalRuntimeFilters { filters })
}

// Only the original, single target may supply the new probe denominator.
// Do not reuse the root table's statistics for propagated targets, and do not
// impose any new restriction on the existing build-statistics path.
fn resolve_probe_table_index(
    metadata: &MetadataRef,
    probe_side: &PExpr,
    targets: &[(RemoteExpr<String>, usize)],
    source_key: &RemoteExpr<String>,
    source_scan_id: usize,
    source_column: Symbol,
) -> Option<IndexType> {
    let [(target_key, target_scan_id)] = targets else {
        return None;
    };
    if *target_scan_id != source_scan_id || target_key != source_key {
        return None;
    }
    let metadata = metadata.read();
    if metadata.base_column_scan_id(source_column) != Some(source_scan_id) {
        return None;
    }
    let ColumnEntry::BaseTableColumn(column) = metadata.column(source_column) else {
        return None;
    };
    fn find_scan(expr: &PExpr, scan_id: usize) -> Option<&databend_common_sql::plans::Scan> {
        if let PhysicalRelOperator::Scan(scan) = expr.plan() {
            if scan.scan_id == scan_id {
                return Some(scan);
            }
        }
        expr.children().find_map(|child| find_scan(child, scan_id))
    }
    let scan = find_scan(probe_side, source_scan_id)?;
    (scan.table_index == column.table_index && scan.columns.contains(&source_column))
        .then_some(scan.table_index)
}

async fn get_build_table_rows(
    ctx: Arc<dyn TableContext>,
    metadata: &MetadataRef,
    build_table_index: Option<IndexType>,
) -> Result<Option<u64>> {
    if let Some(table_index) = build_table_index {
        let table = {
            let metadata_read = metadata.read();
            metadata_read.table(table_index).table().clone()
        };

        let table_stats = table.table_statistics(ctx, false, None).await?;
        return Ok(table_stats.and_then(|s| s.num_rows));
    }

    Ok(None)
}

fn find_probe_targets(
    metadata: &MetadataRef,
    s_expr: &PExpr,
    probe_key: RemoteExpr<String>,
    probe_scan_id: usize,
    probe_key_col_idx: Symbol,
    probe_key_is_connector: bool,
    func_ctx: &FunctionContext,
) -> Result<Vec<(RemoteExpr<String>, usize)>> {
    // Expression roots obey the same path barriers as column roots. This
    // check must precede the nonconnector early return below.
    if !probe_path_is_safe(s_expr, probe_scan_id, probe_key_col_idx) {
        return Ok(vec![]);
    }

    // A value-changing expression is a leaf in the equivalence graph. Its
    // underlying column is not equivalent to the expression result, so the
    // root expression can only be installed on its own scan.
    if !probe_key_is_connector {
        return Ok(vec![(probe_key, probe_scan_id)]);
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
fn probe_child_is_safe(plan: &PhysicalRelOperator, child_index: usize) -> bool {
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
            if join.single_to_inner.is_some() {
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
    let remote_expr = probe.probe_key.as_remote_expr();

    if supported_probe_key_for_runtime_filter(&remote_expr, func_ctx) {
        let (remote_expr, is_connector) = match canonical_equivalence_connector(remote_expr) {
            Ok(connector) => (connector, true),
            Err(leaf) => (leaf, false),
        };
        return Ok(Some(ProbeTarget {
            remote_expr,
            scan_id: probe.scan_id,
            column_idx: probe.column_idx,
            is_connector,
        }));
    }

    Ok(None)
}

fn supported_probe_key_for_runtime_filter(
    probe_key: &RemoteExpr<String>,
    func_ctx: &FunctionContext,
) -> bool {
    let expr = probe_key.as_expr(&BUILTIN_FUNCTIONS);
    expr.column_refs().len() == 1
        && expr.is_deterministic(&BUILTIN_FUNCTIONS)
        && safe_expression_domain(&expr, func_ctx).is_some()
}

fn safe_expression_domain(expr: &Expr<String>, func_ctx: &FunctionContext) -> Option<Domain> {
    match expr {
        Expr::Constant(constant) => Some(constant.scalar.as_ref().domain(&constant.data_type)),
        Expr::ColumnRef(column) => Some(Domain::full(&column.data_type)),
        Expr::Cast(cast) => {
            safe_expression_domain(&cast.expr, func_ctx)?;
            if !cast.is_try
                && !classify_conversion(cast.expr.data_type(), &cast.dest_type)
                    .is_lossless_injective()
            {
                return None;
            }
            Some(Domain::full(&cast.dest_type))
        }
        Expr::FunctionCall(call) => {
            let domains = call
                .args
                .iter()
                .map(|arg| safe_expression_domain(arg, func_ctx))
                .collect::<Option<Vec<_>>>()?;
            let FunctionEval::Scalar { calc_domain, .. } = &call.function.eval else {
                return None;
            };
            match calc_domain.domain_eval(func_ctx, &domains) {
                FunctionDomain::MayThrow => None,
                FunctionDomain::Full => Some(Domain::full(&call.return_type)),
                FunctionDomain::Domain(domain) => Some(domain),
            }
        }
        Expr::LambdaFunctionCall(_) => None,
    }
}

fn normalize_probe_target(
    target: &RemoteExpr<String>,
    target_type: &DataType,
    func_ctx: &FunctionContext,
) -> Result<Option<RemoteExpr<String>>> {
    let expr = target.as_expr(&BUILTIN_FUNCTIONS);
    if !classify_conversion(expr.data_type(), target_type).is_lossless_injective() {
        return Ok(None);
    }
    let expr = check_cast(expr.span(), false, expr, target_type, &BUILTIN_FUNCTIONS)?;
    let expr = databend_common_expression::ConstantFolder::fold(
        Cow::Owned(expr),
        func_ctx,
        &BUILTIN_FUNCTIONS,
    )
    .0;
    Ok(Some(expr.as_remote_expr()))
}

fn push_unique_target(
    targets: &mut Vec<(RemoteExpr<String>, usize)>,
    expr: RemoteExpr<String>,
    scan_id: usize,
) {
    if !targets
        .iter()
        .any(|(target, target_scan_id)| target == &expr && *target_scan_id == scan_id)
    {
        targets.push((expr, scan_id));
    }
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

#[cfg(test)]
mod tests {
    use databend_common_expression::ColumnRef;
    use databend_common_expression::Constant;
    use databend_common_expression::Expr;
    use databend_common_expression::FunctionContext;
    use databend_common_expression::LambdaFunctionCall;
    use databend_common_expression::RemoteExpr;
    use databend_common_expression::Scalar;
    use databend_common_expression::type_check::check_cast;
    use databend_common_expression::type_check::check_function;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::NumberScalar;
    use databend_common_functions::BUILTIN_FUNCTIONS;
    use databend_common_sql::Symbol;

    use super::ProbeTarget;
    use super::canonical_equivalence_connector;
    use super::propagate_probe_targets;
    use super::supported_probe_key_for_runtime_filter;

    fn column(name: &str, data_type: DataType) -> Expr<String> {
        Expr::ColumnRef(ColumnRef {
            span: None,
            id: name.to_string(),
            data_type,
            display_name: name.to_string(),
        })
    }

    fn int32(value: i32) -> Expr<String> {
        Expr::Constant(Constant {
            span: None,
            scalar: Scalar::Number(NumberScalar::Int32(value)),
            data_type: DataType::Number(NumberDataType::Int32),
        })
    }

    fn string(value: &str) -> Expr<String> {
        Expr::Constant(Constant {
            span: None,
            scalar: Scalar::String(value.to_string()),
            data_type: DataType::String,
        })
    }

    fn supported(expr: Expr<String>) -> bool {
        supported_probe_key_for_runtime_filter(&expr.as_remote_expr(), &FunctionContext::default())
    }

    fn target(
        remote_expr: databend_common_expression::RemoteExpr<String>,
        scan_id: usize,
        column_idx: usize,
        is_connector: bool,
    ) -> ProbeTarget {
        ProbeTarget {
            remote_expr,
            scan_id,
            column_idx: Symbol::new(column_idx),
            is_connector,
        }
    }

    fn path_scan(scan_id: usize, index: usize) -> databend_common_sql::optimizer::ir::PExpr {
        databend_common_sql::optimizer::ir::PExpr::create_leaf(databend_common_sql::plans::Scan {
            scan_id,
            columns: [Symbol::new(index)].into_iter().collect(),
            ..Default::default()
        })
    }

    fn path_column(index: usize, number_type: NumberDataType) -> databend_common_sql::ScalarExpr {
        use databend_common_sql::ColumnBindingBuilder;
        use databend_common_sql::Visibility;
        use databend_common_sql::plans::BoundColumnRef;
        databend_common_sql::ScalarExpr::BoundColumnRef(BoundColumnRef {
            span: None,
            column: ColumnBindingBuilder::new(
                format!("c{index}"),
                Symbol::new(index),
                Box::new(DataType::Number(number_type)),
                Visibility::Visible,
            )
            .build(),
        })
    }

    fn path_targets(
        plan: &databend_common_sql::optimizer::ir::PExpr,
        connector: bool,
    ) -> Vec<(RemoteExpr<String>, usize)> {
        let key = column("c0", DataType::Number(NumberDataType::Int32));
        let key = if connector {
            key
        } else {
            check_function(None, "plus", &[], &[key, int32(1)], &BUILTIN_FUNCTIONS).unwrap()
        };
        super::find_probe_targets(
            &Default::default(),
            plan,
            key.as_remote_expr(),
            7,
            Symbol::new(0),
            connector,
            &FunctionContext::default(),
        )
        .unwrap()
    }

    // Check selected targets, not just results: late RF delivery can mask bad
    // placement, and an implementation that disables all RF would pass SQL too.
    #[test]
    fn test_probe_paths_respect_row_sensitive_barriers() {
        use databend_common_sql::optimizer::ir::PExpr;
        use databend_common_sql::plans::*;
        let sort = Sort {
            items: vec![],
            limit: None,
            after_exchange: None,
            pre_projection: None,
            window_partition: None,
        };
        let window = Window {
            span: None,
            index: Symbol::new(1),
            function: WindowFuncType::RowNumber,
            arguments: vec![],
            partition_by: vec![],
            order_by: vec![],
            frame: Default::default(),
            limit: None,
            top: None,
        };
        let group = Aggregate {
            group_items: vec![ScalarItem {
                index: Symbol::new(0),
                scalar: path_column(0, NumberDataType::Int32),
            }],
            ..Default::default()
        };
        let mut grouping_sets = group.clone();
        grouping_sets.grouping_sets = Some(GroupingSets {
            grouping_id_index: Symbol::new(2),
            sets: vec![vec![Symbol::new(0)], vec![]],
            dup_group_items: vec![],
        });
        let mut ranked = group.clone();
        ranked.rank_limit = Some((vec![], 1));
        let mut renamed = group.clone();
        renamed.group_items[0].index = Symbol::new(3);
        let barriers = vec![
            PExpr::create_unary(
                Limit {
                    before_exchange: false,
                    limit: Some(1),
                    offset: 0,
                    lazy_columns: Default::default(),
                },
                path_scan(7, 0),
            ),
            PExpr::create_unary(
                Limit {
                    before_exchange: false,
                    limit: None,
                    offset: 1,
                    lazy_columns: Default::default(),
                },
                path_scan(7, 0),
            ),
            PExpr::create_unary(
                Sort {
                    limit: Some(1),
                    ..sort.clone()
                },
                path_scan(7, 0),
            ),
            PExpr::create_unary(
                TopN {
                    items: vec![],
                    limit: 1,
                    offset: 0,
                    lazy_columns: Default::default(),
                    after_exchange: None,
                },
                path_scan(7, 0),
            ),
            PExpr::create_unary(window.clone(), path_scan(7, 0)),
            PExpr::create_unary(
                WindowGroup {
                    windows: vec![window],
                    scalar_items: vec![],
                },
                path_scan(7, 0),
            ),
            PExpr::create_unary(
                PhysicalAggregate::new(grouping_sets, AggregateMode::Final),
                path_scan(7, 0),
            ),
            PExpr::create_unary(
                PhysicalAggregate::new(ranked, AggregateMode::Partial),
                path_scan(7, 0),
            ),
            PExpr::create_unary(
                PhysicalAggregate::new(renamed, AggregateMode::Final),
                path_scan(7, 0),
            ),
            PExpr::create_unary(
                MaterializedCTE::new("shared".to_string(), None),
                path_scan(7, 0),
            ),
            PExpr::create_leaf(Scan {
                limit: Some(1),
                scan_id: 7,
                columns: [Symbol::new(0)].into_iter().collect(),
                ..Default::default()
            }),
        ];
        for (index, plan) in barriers.iter().enumerate() {
            for connector in [true, false] {
                assert!(
                    path_targets(plan, connector).is_empty(),
                    "barrier {index}, connector={connector}"
                );
            }
        }
        let safe = PExpr::create_unary(
            PhysicalAggregate::new(group, AggregateMode::Final),
            PExpr::create_unary(sort, path_scan(7, 0)),
        );
        for connector in [true, false] {
            assert_eq!(path_targets(&safe, connector).len(), 1);
            // Only an identity definition may retain the original scan key.
            for (index, scalar, allowed) in [
                (0, path_column(0, NumberDataType::Int32), true),
                (
                    0,
                    ScalarExpr::ConstantExpr(ConstantExpr {
                        span: None,
                        value: Scalar::Number(NumberScalar::Int32(0)),
                    }),
                    false,
                ),
                (1, path_column(0, NumberDataType::Int32), true),
            ] {
                let plan = PExpr::create_unary(
                    EvalScalar {
                        items: vec![ScalarItem {
                            index: Symbol::new(index),
                            scalar,
                        }],
                    },
                    path_scan(7, 0),
                );
                assert_eq!(path_targets(&plan, connector).len(), usize::from(allowed));
            }
            // A safe occurrence must not hide a limited occurrence of a
            // shared scan. Reuse the same subtree, not two distinct scan IDs.
            let shared = path_scan(7, 0);
            let plan = PExpr::create_binary(
                Join {
                    join_type: JoinType::Cross,
                    ..Default::default()
                },
                shared.clone(),
                PExpr::create_unary(
                    Limit {
                        before_exchange: false,
                        limit: Some(1),
                        offset: 0,
                        lazy_columns: Default::default(),
                    },
                    shared,
                ),
            );
            assert!(path_targets(&plan, connector).is_empty());
        }
    }

    #[test]
    fn test_equivalent_targets_keep_only_safe_branches() {
        use databend_common_expression::TableDataType;
        use databend_common_sql::MetadataRef;
        use databend_common_sql::optimizer::ir::PExpr;
        use databend_common_sql::plans::*;
        let metadata = MetadataRef::default();
        for (index, number_type) in [NumberDataType::Int64, NumberDataType::Int32]
            .into_iter()
            .enumerate()
        {
            metadata.write().add_base_table_column(
                format!("c{index}"),
                TableDataType::Number(number_type),
                0,
                Some(vec![index]),
                index as u32,
                None,
                None,
            );
        }
        metadata.write().add_base_column_scan_id(
            [(Symbol::new(0), 0), (Symbol::new(1), 1)]
                .into_iter()
                .collect(),
        );
        let left = path_column(0, NumberDataType::Int64);
        let right = path_column(1, NumberDataType::Int32);
        let expression = ScalarExpr::FunctionCall(FunctionCall {
            span: None,
            func_name: "plus".to_string(),
            params: vec![],
            arguments: vec![
                right.clone(),
                ScalarExpr::ConstantExpr(ConstantExpr {
                    span: None,
                    value: Scalar::Number(NumberScalar::Int32(1)),
                }),
            ],
            return_type: Box::new(DataType::Number(NumberDataType::Int64)),
        });
        // The safe sibling must survive even when the other endpoint is unsafe;
        // a computed expression must stay an expression, not become a column.
        for endpoint in [right, expression] {
            for blocked in [false, true] {
                let mut branch = path_scan(1, 1);
                if blocked {
                    branch = PExpr::create_unary(
                        Limit {
                            before_exchange: false,
                            limit: Some(1),
                            offset: 0,
                            lazy_columns: Default::default(),
                        },
                        branch,
                    );
                }
                let plan = PExpr::create_binary(
                    Join {
                        join_type: JoinType::Inner,
                        equi_conditions: vec![JoinEquiCondition::new(
                            left.clone(),
                            endpoint.clone(),
                            false,
                        )],
                        ..Default::default()
                    },
                    path_scan(0, 0),
                    branch,
                );
                // With grouping, both endpoints must remain unchanged keys.
                // An input equality must not bridge a non-grouped column.
                for group_keys in [None, Some(1), Some(2)] {
                    let plan = if let Some(count) = group_keys {
                        let keys = [left.clone(), path_column(1, NumberDataType::Int32)];
                        PExpr::create_unary(
                            PhysicalAggregate::new(
                                Aggregate {
                                    group_items: keys
                                        .into_iter()
                                        .take(count)
                                        .enumerate()
                                        .map(|(index, scalar)| ScalarItem {
                                            index: Symbol::new(index),
                                            scalar,
                                        })
                                        .collect(),
                                    ..Default::default()
                                },
                                AggregateMode::Final,
                            ),
                            plan.clone(),
                        )
                    } else {
                        plan.clone()
                    };
                    let targets = super::find_probe_targets(
                        &metadata,
                        &plan,
                        column("c0", DataType::Number(NumberDataType::Int64)).as_remote_expr(),
                        0,
                        Symbol::new(0),
                        true,
                        &FunctionContext::default(),
                    )
                    .unwrap();
                    let mut scans: Vec<_> = targets.iter().map(|(_, id)| *id).collect();
                    scans.sort_unstable();
                    let blocked = blocked || group_keys == Some(1);
                    assert_eq!(scans, if blocked { vec![0] } else { vec![0, 1] });
                    if !blocked && matches!(endpoint, ScalarExpr::FunctionCall(_)) {
                        assert!(matches!(
                            targets.iter().find(|(_, id)| *id == 1).unwrap().0,
                            RemoteExpr::FunctionCall { .. }
                        ));
                    }
                }
            }
        }
    }

    #[test]
    fn test_stateful_scalars_block_paths_and_equalities() {
        use databend_common_sql::ScalarExpr;
        use databend_common_sql::optimizer::ir::PExpr;
        use databend_common_sql::plans::*;

        use super::PhysicalRelOperator;

        let value = path_column(1, NumberDataType::Int64);
        let zero = ScalarExpr::ConstantExpr(ConstantExpr {
            span: None,
            value: Scalar::Number(NumberScalar::Int64(0)),
        });
        let running = ScalarExpr::FunctionCall(FunctionCall {
            span: None,
            func_name: "running_difference".into(),
            params: vec![],
            arguments: vec![value.clone()],
            return_type: Box::new(DataType::Number(NumberDataType::Int64)),
        });
        let positive = |arg| {
            ScalarExpr::FunctionCall(FunctionCall {
                span: None,
                func_name: "gt".into(),
                params: vec![],
                arguments: vec![arg, zero.clone()],
                return_type: Box::new(DataType::Boolean),
            })
        };
        let mut scan = path_scan(7, 0).plan().as_scan().unwrap().clone();
        scan.columns.insert(Symbol::new(1));
        let input = PExpr::create_leaf(scan.clone());
        // The RF key is c0. A different projected/predicate column must still
        // block it if evaluating that column depends on other input rows.
        let row_local = ScalarExpr::FunctionCall(FunctionCall {
            span: None,
            func_name: "plus".into(),
            params: vec![],
            arguments: vec![value, zero.clone()],
            return_type: Box::new(DataType::Number(NumberDataType::Int64)),
        });
        for (expr, safe) in [(row_local, true), (running, false)] {
            let eval = EvalScalar {
                items: vec![ScalarItem {
                    index: Symbol::new(2),
                    scalar: expr.clone(),
                }],
            };
            let predicate = positive(expr);
            let filter = Filter {
                predicates: vec![predicate.clone()],
            };
            for kind in 0..3 {
                let mut leaf = scan.clone();
                match kind {
                    0 => leaf.push_down_predicates = Some(vec![predicate.clone()]),
                    1 => leaf.secure_predicates = Some(vec![predicate.clone()]),
                    _ => {
                        leaf.prewhere = Some(Prewhere {
                            predicates: vec![predicate.clone()],
                            output_columns: leaf.columns.clone(),
                            prewhere_columns: predicate.used_columns(),
                        })
                    }
                }
                assert_eq!(
                    path_targets(&PExpr::create_leaf(leaf), true).is_empty(),
                    !safe
                );
            }
            for plan in [
                PhysicalRelOperator::EvalScalar(eval),
                PhysicalRelOperator::Filter(filter),
            ] {
                let tree = PExpr::create_unary(plan.clone(), input.clone());
                for connector in [true, false] {
                    assert_eq!(path_targets(&tree, connector).is_empty(), !safe);
                }
                let join = PExpr::create_binary(
                    Join {
                        join_type: JoinType::Inner,
                        equi_conditions: vec![JoinEquiCondition::new(
                            path_column(0, NumberDataType::Int32),
                            path_column(3, NumberDataType::Int32),
                            false,
                        )],
                        ..Default::default()
                    },
                    input.clone(),
                    path_scan(8, 3),
                );
                let tree = PExpr::create_unary(plan, join);
                assert_eq!(
                    super::collect_equi_conditions(&tree).unwrap().len(),
                    usize::from(safe)
                );
            }
        }
    }

    fn assert_output_side_probe_targets(join_type: databend_common_sql::plans::JoinType) {
        use databend_common_expression::TableDataType;
        use databend_common_sql::MetadataRef;
        use databend_common_sql::optimizer::ir::PExpr;
        use databend_common_sql::plans::Join;
        use databend_common_sql::plans::JoinEquiCondition;
        use databend_common_sql::plans::JoinType;

        let metadata = MetadataRef::default();
        for index in 0..4 {
            metadata.write().add_base_table_column(
                format!("c{index}"),
                TableDataType::Number(NumberDataType::Int32),
                0,
                Some(vec![index]),
                index as u32,
                None,
                None,
            );
        }
        metadata
            .write()
            .add_base_column_scan_id((0..4).map(|index| (Symbol::new(index), index)).collect());
        let marker = join_type.is_mark_join().then(|| {
            metadata.write().add_derived_column(
                "marker".to_string(),
                DataType::Nullable(Box::new(DataType::Boolean)),
            )
        });
        let col = |index| path_column(index, NumberDataType::Int32);
        let inner = |left, right| {
            PExpr::create_binary(
                Join {
                    join_type: JoinType::Inner,
                    equi_conditions: vec![JoinEquiCondition::new(col(left), col(right), false)],
                    ..Default::default()
                },
                path_scan(left, left),
                path_scan(right, right),
            )
        };
        // The output subtree has c0=c1; the hidden subtree has c2=c3.
        // Neither the join's own equality nor the non-retained subtree's
        // equality may be exported to connect these two inputs.
        let (left, right, condition) = if matches!(
            join_type,
            JoinType::Left | JoinType::LeftSemi | JoinType::LeftAnti | JoinType::RightMark
        ) {
            (
                inner(0, 1),
                inner(2, 3),
                JoinEquiCondition::new(col(0), col(2), false),
            )
        } else {
            (
                inner(2, 3),
                inner(0, 1),
                JoinEquiCondition::new(col(2), col(0), false),
            )
        };
        let plan = PExpr::create_binary(
            Join {
                join_type,
                marker_index: marker,
                equi_conditions: vec![condition],
                ..Default::default()
            },
            left,
            right,
        );
        if let Some(index) = marker {
            use databend_common_sql::ColumnBindingBuilder;
            use databend_common_sql::Visibility;
            use databend_common_sql::plans::BoundColumnRef;
            use databend_common_sql::plans::FunctionCall;
            use databend_common_sql::plans::ScalarExpr;
            let data_type = DataType::Nullable(Box::new(DataType::Boolean));
            let scalar: ScalarExpr = BoundColumnRef {
                span: None,
                column: ColumnBindingBuilder::new(
                    "marker".into(),
                    index,
                    Box::new(data_type.clone()),
                    Visibility::Visible,
                )
                .build(),
            }
            .into();
            let not_marker: ScalarExpr = FunctionCall {
                span: None,
                func_name: "not".into(),
                params: vec![],
                arguments: vec![scalar.clone()],
                return_type: Box::new(data_type),
            }
            .into();
            for expression in [scalar, not_marker] {
                assert!(
                    super::scalar_to_probe_target(
                        &metadata,
                        &expression,
                        &FunctionContext::default()
                    )
                    .unwrap()
                    .is_none()
                );
            }
            assert!(!super::probe_path_is_safe(&plan, 0, index));
        }
        for index in 0..4 {
            for connector in [true, false] {
                let key = column(
                    &format!("c{index}"),
                    DataType::Number(NumberDataType::Int32),
                );
                let key = if connector {
                    key
                } else {
                    check_function(None, "plus", &[], &[key, int32(1)], &BUILTIN_FUNCTIONS).unwrap()
                };
                let targets = super::find_probe_targets(
                    &metadata,
                    &plan,
                    key.as_remote_expr(),
                    index,
                    Symbol::new(index),
                    connector,
                    &FunctionContext::default(),
                )
                .unwrap();
                let mut scans: Vec<_> = targets.iter().map(|(_, scan_id)| *scan_id).collect();
                scans.sort_unstable();
                let expected = if index >= 2 {
                    vec![]
                } else if connector {
                    vec![0, 1]
                } else {
                    vec![index]
                };
                assert_eq!(
                    scans, expected,
                    "{join_type:?}: root c{index}, connector={connector}"
                );
            }
        }
        let conditions = super::collect_equi_conditions(&plan).unwrap();
        assert_eq!(conditions.len(), 1);
        assert_eq!(conditions[0].left, col(0));
        assert_eq!(conditions[0].right, col(1));
    }

    #[test]
    fn test_left_semi_keeps_output_side_targets() {
        assert_output_side_probe_targets(databend_common_sql::plans::JoinType::LeftSemi);
    }

    #[test]
    fn test_right_semi_keeps_output_side_targets() {
        assert_output_side_probe_targets(databend_common_sql::plans::JoinType::RightSemi);
    }

    #[test]
    fn test_left_anti_keeps_output_side_targets() {
        assert_output_side_probe_targets(databend_common_sql::plans::JoinType::LeftAnti);
    }

    #[test]
    fn test_right_anti_keeps_output_side_targets() {
        assert_output_side_probe_targets(databend_common_sql::plans::JoinType::RightAnti);
    }

    #[test]
    fn test_left_mark_keeps_right_input_and_rejects_marker() {
        assert_output_side_probe_targets(databend_common_sql::plans::JoinType::LeftMark);
    }

    #[test]
    fn test_right_mark_keeps_left_input_and_rejects_marker() {
        assert_output_side_probe_targets(databend_common_sql::plans::JoinType::RightMark);
    }

    #[test]
    fn test_outer_keeps_only_retained_input_targets() {
        use databend_common_sql::plans::JoinType;
        for kind in [JoinType::Left, JoinType::Right] {
            assert_output_side_probe_targets(kind);
        }
    }

    #[test]
    fn test_all_join_types_have_explicit_child_policy() {
        use databend_common_sql::plans::Join;
        use databend_common_sql::plans::JoinType;
        use databend_common_sql::plans::PhysicalRelOperator;
        let cases = [
            (JoinType::Inner, [true, true]),
            (JoinType::Cross, [true, true]),
            (JoinType::Left, [true, false]),
            (JoinType::Right, [false, true]),
            (JoinType::Full, [false, false]),
            (JoinType::InnerAny, [false, false]),
            (JoinType::LeftAny, [false, false]),
            (JoinType::RightAny, [false, false]),
            (JoinType::LeftSemi, [true, false]),
            (JoinType::RightSemi, [false, true]),
            (JoinType::LeftAnti, [true, false]),
            (JoinType::RightAnti, [false, true]),
            (JoinType::LeftMark, [false, true]),
            (JoinType::RightMark, [true, false]),
            (JoinType::LeftSingle, [false, false]),
            (JoinType::RightSingle, [false, false]),
            (JoinType::Asof, [false, true]),
            (JoinType::LeftAsof, [false, true]),
            (JoinType::RightAsof, [true, false]),
            (JoinType::FullAsof, [false, false]),
        ];
        for (kind, expected) in cases {
            let plan: PhysicalRelOperator = Join {
                join_type: kind,
                ..Default::default()
            }
            .into();
            for (child, safe) in expected.into_iter().enumerate() {
                assert_eq!(
                    super::probe_child_is_safe(&plan, child),
                    safe,
                    "{kind:?} child{child}"
                );
            }
        }
    }

    #[test]
    fn test_converted_single_blocks_targets_and_equalities() {
        use databend_common_sql::optimizer::ir::PExpr;
        use databend_common_sql::plans::*;
        for original in [
            None,
            Some(JoinType::LeftSingle),
            Some(JoinType::RightSingle),
        ] {
            let plan = PExpr::create_binary(
                Join {
                    join_type: JoinType::Inner,
                    single_to_inner: original,
                    equi_conditions: vec![JoinEquiCondition::new(
                        path_column(0, NumberDataType::Int32),
                        path_column(1, NumberDataType::Int32),
                        false,
                    )],
                    ..Default::default()
                },
                path_scan(7, 0),
                path_scan(8, 1),
            );
            for (scan_id, index) in [(7, 0), (8, 1)] {
                assert_eq!(
                    super::probe_path_is_safe(&plan, scan_id, Symbol::new(index)),
                    original.is_none()
                );
            }
            assert_eq!(
                super::collect_equi_conditions(&plan).unwrap().len(),
                usize::from(original.is_none())
            );
            if original.is_some() {
                for connector in [true, false] {
                    assert!(path_targets(&plan, connector).is_empty());
                }
            } else {
                // A barrier must also hide equalities from deeper ordinary joins.
                let limited = PExpr::create_unary(
                    Limit {
                        before_exchange: false,
                        limit: Some(1),
                        offset: 0,
                        lazy_columns: Default::default(),
                    },
                    plan,
                );
                assert!(super::collect_equi_conditions(&limited).unwrap().is_empty());
            }
        }
    }

    #[test]
    fn test_probe_statistics_only_for_the_confirmed_target_scan() {
        use databend_common_expression::TableDataType;
        use databend_common_sql::Metadata;
        use databend_common_sql::optimizer::ir::PExpr;
        use databend_common_sql::plans::Join;
        use databend_common_sql::plans::Scan;
        use parking_lot::RwLock;

        let mut metadata = Metadata::default();
        let source = metadata.add_base_table_column(
            "a".into(),
            TableDataType::Number(NumberDataType::Int32),
            3,
            None,
            0,
            None,
            None,
        );
        metadata.add_base_column_scan_id(std::collections::HashMap::from([(source, 7)]));
        let metadata = std::sync::Arc::new(RwLock::new(metadata));
        let scan = |scan_id, table_index| {
            PExpr::create_leaf(Scan {
                scan_id,
                table_index,
                columns: [source].into_iter().collect(),
                ..Default::default()
            })
        };
        let key = column("a", DataType::Number(NumberDataType::Int32)).as_remote_expr();
        let targets = vec![(key.clone(), 7)];
        let resolve = |plan: &PExpr, targets: &[(RemoteExpr<String>, usize)]| {
            super::resolve_probe_table_index(&metadata, plan, targets, &key, 7, source)
        };
        assert_eq!(resolve(&scan(7, 3), &targets), Some(3));
        assert_eq!(resolve(&scan(8, 3), &targets), None);
        assert_eq!(resolve(&scan(7, 4), &targets), None);
        assert_eq!(resolve(&scan(7, 3), &[(key.clone(), 8)]), None);
        assert_eq!(
            resolve(&scan(7, 3), &[(key.clone(), 7), (key.clone(), 8)]),
            None
        );
        let joined = PExpr::create_binary(Join::default(), scan(7, 3), scan(8, 4));
        assert_eq!(resolve(&joined, &targets), Some(3)); // No whole-side single-scan guard.
        let derived = metadata
            .write()
            .add_derived_column("d".into(), DataType::Boolean);
        assert_eq!(
            super::resolve_probe_table_index(&metadata, &scan(7, 3), &targets, &key, 7, derived,),
            None
        );
    }

    #[test]
    fn test_safe_single_column_probe_expressions() {
        let int16 = DataType::Number(NumberDataType::Int16);
        let int32_type = DataType::Number(NumberDataType::Int32);
        let int64 = DataType::Number(NumberDataType::Int64);

        let widened = check_cast(
            None,
            false,
            column("a", int16),
            &int32_type,
            &BUILTIN_FUNCTIONS,
        )
        .unwrap();
        let nested = check_cast(None, false, widened, &int64, &BUILTIN_FUNCTIONS).unwrap();
        assert!(supported(nested));

        let plus = check_function(
            None,
            "plus",
            &[],
            &[column("a", int32_type.clone()), int32(10)],
            &BUILTIN_FUNCTIONS,
        )
        .unwrap();
        assert!(supported(plus));

        let repeated_column = check_function(
            None,
            "plus",
            &[],
            &[
                column("a", int32_type.clone()),
                column("a", int32_type.clone()),
            ],
            &BUILTIN_FUNCTIONS,
        )
        .unwrap();
        assert!(supported(repeated_column));

        let replace = check_function(
            None,
            "replace",
            &[],
            &[column("s", DataType::String), string("a"), string("b")],
            &BUILTIN_FUNCTIONS,
        )
        .unwrap();
        assert!(supported(replace));

        let try_cast = check_cast(
            None,
            true,
            column("s", DataType::String),
            &int64,
            &BUILTIN_FUNCTIONS,
        )
        .unwrap();
        assert!(supported(try_cast));
    }

    #[test]
    fn test_unsafe_probe_expressions_are_rejected() {
        let int32_type = DataType::Number(NumberDataType::Int32);
        let int64 = DataType::Number(NumberDataType::Int64);

        let value_dependent_cast = check_cast(
            None,
            false,
            column("s", DataType::String),
            &int64,
            &BUILTIN_FUNCTIONS,
        )
        .unwrap();
        assert!(!supported(value_dependent_cast));

        let multiple_columns = check_function(
            None,
            "plus",
            &[],
            &[
                column("a", int32_type.clone()),
                column("b", int32_type.clone()),
            ],
            &BUILTIN_FUNCTIONS,
        )
        .unwrap();
        assert!(!supported(multiple_columns));

        let may_throw = check_function(
            None,
            "divide",
            &[],
            &[column("a", int32_type), int32(1)],
            &BUILTIN_FUNCTIONS,
        )
        .unwrap();
        assert!(!supported(may_throw));

        let nondeterministic = check_function(
            None,
            "rand",
            &[],
            &[column("a", DataType::Number(NumberDataType::UInt64))],
            &BUILTIN_FUNCTIONS,
        )
        .unwrap();
        assert!(!supported(nondeterministic));

        let srf = check_function(
            None,
            "generate_series",
            &[],
            &[
                column("a", DataType::Number(NumberDataType::Int64)),
                int32(10),
            ],
            &BUILTIN_FUNCTIONS,
        )
        .unwrap();
        assert!(!supported(srf));

        let lambda = Expr::LambdaFunctionCall(LambdaFunctionCall {
            span: None,
            name: "test_lambda".to_string(),
            args: vec![column("a", DataType::Number(NumberDataType::Int64))],
            lambda_expr: Box::new(RemoteExpr::Constant {
                span: None,
                scalar: Scalar::Number(NumberScalar::Int32(1)),
                data_type: DataType::Number(NumberDataType::Int32),
            }),
            lambda_display: "x -> 1".to_string(),
            return_type: DataType::Number(NumberDataType::Int64),
        });
        assert!(!supported(lambda));
    }

    #[test]
    fn test_nullable_wrapper_of_expression_is_leaf() {
        let int32_type = DataType::Number(NumberDataType::Int32);
        let int64 = DataType::Number(NumberDataType::Int64);
        let nullable_int64 = DataType::Nullable(Box::new(int64.clone()));

        let direct_column = column("connector", int64);
        let direct_nullable_cast = check_cast(
            None,
            false,
            direct_column,
            &nullable_int64,
            &BUILTIN_FUNCTIONS,
        )
        .unwrap();
        assert!(canonical_equivalence_connector(direct_nullable_cast.as_remote_expr()).is_ok());

        let plus = check_function(
            None,
            "plus",
            &[],
            &[column("a", int32_type), int32(10)],
            &BUILTIN_FUNCTIONS,
        )
        .unwrap();
        let wrapped = check_cast(None, false, plus, &nullable_int64, &BUILTIN_FUNCTIONS).unwrap();
        assert!(canonical_equivalence_connector(wrapped.as_remote_expr()).is_err());
    }

    #[test]
    fn test_connector_to_leaf_propagation_keeps_expression_as_leaf() {
        let int32_type = DataType::Number(NumberDataType::Int32);
        let int64 = DataType::Number(NumberDataType::Int64);
        let root = column("root", int64.clone()).as_remote_expr();
        let connector = column("connector", int64).as_remote_expr();
        let leaf = check_function(
            None,
            "plus",
            &[],
            &[column("a", int32_type.clone()), int32(10)],
            &BUILTIN_FUNCTIONS,
        )
        .unwrap()
        .as_remote_expr();
        let raw_leaf_column = column("a", int32_type).as_remote_expr();

        let targets = propagate_probe_targets(
            root.clone(),
            0,
            Symbol::new(0),
            vec![
                (
                    target(root.clone(), 0, 0, true),
                    target(connector.clone(), 1, 1, true),
                ),
                (
                    target(connector, 1, 1, true),
                    target(leaf.clone(), 2, 2, false),
                ),
            ],
            &FunctionContext::default(),
        )
        .unwrap();

        assert!(targets.contains(&(root, 0)));
        assert!(targets.contains(&(leaf, 2)));
        assert!(!targets.contains(&(raw_leaf_column, 2)));
    }
}
