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
use databend_common_sql::plans::JoinType;
use databend_common_sql::plans::PhysicalRelOperator;
use databend_common_sql::plans::ScalarExpr;
use databend_common_sql::plans::Scan;

use super::types::PhysicalRuntimeFilter;
use super::types::PhysicalRuntimeFilters;
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

/// Build the runtime filters planned for a join.
///
/// The optimizer records the runtime filters on the join and pushes each
/// filter to the scans of the probe side (see `RuntimeFilterOptimizer`). This
/// function keeps the filters that the chosen physical plan can execute, and
/// the scan targets that the scan can evaluate.
///
/// # Arguments
/// * `ctx` - Table context
/// * `metadata` - Metadata reference
/// * `join` - Join plan
/// * `s_expr` - PExpr for the join
/// * `build_keys` - Build side keys, cast to the common type of each equi condition
///
/// # Returns
/// Join filters, and the subset that supplies materialized CTE producers by equi-condition index.
pub async fn build_runtime_filter(
    ctx: Arc<dyn TableContext>,
    metadata: &MetadataRef,
    join: &Join,
    s_expr: &PExpr,
    build_keys: &[RemoteExpr],
    cte_sources: &[usize],
) -> Result<(PhysicalRuntimeFilters, Vec<(usize, PhysicalRuntimeFilter)>)> {
    if (join.runtime_filters.is_empty() && cte_sources.is_empty())
        || !ctx.get_settings().get_enable_join_runtime_filter()?
    {
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

    let func_ctx = ctx.get_function_context()?;
    // Only scans below the probe side may wait for the filters of this join.
    let mut scan_targets = HashMap::new();
    collect_scan_runtime_filters(s_expr.probe_side_child(), &mut scan_targets);

    let mut filters = Vec::new();
    let mut cte_source_filters = Vec::new();
    for runtime_filter in join.runtime_filters.iter() {
        let Some(index) = join.equi_conditions.iter().position(|condition| {
            !condition.is_null_equal
                && condition.canonical_keys()
                    == (&runtime_filter.probe_key, &runtime_filter.build_key)
        }) else {
            continue;
        };
        let targets = scan_targets.get(&runtime_filter.id).into_iter().flatten();

        let build_key = &build_keys[index];
        let target_type = build_key.as_expr(&BUILTIN_FUNCTIONS).data_type().clone();
        let mut probe_targets = Vec::new();
        // The scan-side key of each kept target, in the order of `probe_targets`.
        let mut probe_target_keys = Vec::new();
        for (scan_probe_key, scan_id) in targets {
            let Some(probe_key) = resolve_runtime_filter_probe_expr(metadata, scan_probe_key)?
            else {
                continue;
            };
            let Some(probe_key) =
                normalize_probe_target(&probe_key.as_remote_expr(), &target_type, &func_ctx)?
            else {
                continue;
            };
            if supported_probe_key_for_runtime_filter(&probe_key, &func_ctx)
                && push_unique_target(&mut probe_targets, probe_key, *scan_id)
            {
                probe_target_keys.push((scan_probe_key, *scan_id));
            }
        }
        if probe_targets.is_empty() && !cte_sources.contains(&index) {
            continue;
        }

        let build_table_index = resolve_runtime_filter_build_table_index(
            metadata,
            build_side,
            &runtime_filter.build_key,
        );
        let build_table_rows = get_table_rows(ctx.clone(), metadata, build_table_index).await?;
        let probe_table_index = resolve_probe_table_index(
            metadata,
            s_expr.probe_side_child(),
            &runtime_filter.probe_key,
            &probe_target_keys,
        );
        let probe_table_rows = get_table_rows(ctx.clone(), metadata, probe_table_index).await?;
        let data_type = target_type.remove_nullable();
        let id = metadata.write().next_runtime_filter_id();
        let filter = PhysicalRuntimeFilter {
            id,
            build_key: build_key.clone(),
            probe_targets,
            build_table_rows,
            probe_table_rows,
            enable_bloom_runtime_filter: is_type_supported_for_bloom_filter(&data_type),
            enable_inlist_runtime_filter: true,
            enable_min_max_runtime_filter: is_type_supported_for_min_max_filter(&data_type),
        };
        if cte_sources.contains(&index) {
            cte_source_filters.push((index, filter.clone()));
        }
        filters.push(filter);
    }

    Ok((PhysicalRuntimeFilters { filters }, cte_source_filters))
}

/// Collect the runtime filters that reached scans, as (probe key, scan id) per filter.
#[recursive::recursive]
fn collect_scan_runtime_filters(
    s_expr: &PExpr,
    targets: &mut HashMap<usize, Vec<(ScalarExpr, usize)>>,
) {
    if let PhysicalRelOperator::Scan(scan) = s_expr.plan() {
        for runtime_filter in scan.runtime_filters.iter() {
            targets
                .entry(runtime_filter.id)
                .or_default()
                .push((runtime_filter.probe_key.clone(), scan.scan_id));
        }
    }
    for child in s_expr.children() {
        collect_scan_runtime_filters(child, targets);
    }
}

/// Resolve statistics for the bloom selectivity estimate, not permission to push
/// a probe predicate through an operator. Keep the existing single-base-column
/// expression behavior; derived keys must be direct MIN/MAX results.
fn resolve_runtime_filter_build_table_index(
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

// Only a filter with one scan target, on the probe key's own base column, may
// supply the probe denominator. Do not reuse the probe key's table statistics
// for a target pushed to another column or scan, and do not impose any new
// restriction on the existing build-statistics path.
fn resolve_probe_table_index(
    metadata: &MetadataRef,
    probe_side: &PExpr,
    probe_key: &ScalarExpr,
    targets: &[(&ScalarExpr, usize)],
) -> Option<IndexType> {
    let [(target_key, target_scan_id)] = targets else {
        return None;
    };
    let columns = probe_key.used_columns();
    if columns.len() != 1 || target_key.used_columns() != columns {
        return None;
    }
    let column = *columns.iter().next()?;
    let metadata = metadata.read();
    if metadata.base_column_scan_id(column) != Some(*target_scan_id) {
        return None;
    }
    let ColumnEntry::BaseTableColumn(base) = metadata.column(column) else {
        return None;
    };
    let scan = find_scan(probe_side, *target_scan_id)?;
    (scan.table_index == base.table_index && scan.columns.contains(&column))
        .then_some(scan.table_index)
}

#[recursive::recursive]
fn find_scan(expr: &PExpr, scan_id: usize) -> Option<&Scan> {
    if let PhysicalRelOperator::Scan(scan) = expr.plan() {
        if scan.scan_id == scan_id {
            return Some(scan);
        }
    }
    expr.children().find_map(|child| find_scan(child, scan_id))
}

async fn get_table_rows(
    ctx: Arc<dyn TableContext>,
    metadata: &MetadataRef,
    table_index: Option<IndexType>,
) -> Result<Option<u64>> {
    let Some(table_index) = table_index else {
        return Ok(None);
    };
    let table = metadata.read().table(table_index).table().clone();

    // A change scan needs its ChangeType to define the row set. This
    // denominator is optional: do not request whole-table statistics for
    // CHANGES/stream sources or turn their absence into a planning error.
    if table.has_changes_source() {
        return Ok(None);
    }
    let table_stats = table.table_statistics(ctx, false, None).await?;
    Ok(table_stats.and_then(|s| s.num_rows))
}

pub(super) fn supported_probe_key_for_runtime_filter(
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
                FunctionDomain::MayThrow | FunctionDomain::MayThrowDomain(_) => None,
                FunctionDomain::Full => Some(Domain::full(&call.return_type)),
                FunctionDomain::Domain(domain) => Some(domain),
            }
        }
        Expr::LambdaFunctionCall(_) => None,
    }
}

pub(super) fn normalize_probe_target(
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

/// Push a target unless it is already present. Returns whether it was pushed.
pub(super) fn push_unique_target(
    targets: &mut Vec<(RemoteExpr<String>, usize)>,
    expr: RemoteExpr<String>,
    scan_id: usize,
) -> bool {
    if targets
        .iter()
        .any(|(target, target_scan_id)| target == &expr && *target_scan_id == scan_id)
    {
        return false;
    }
    targets.push((expr, scan_id));
    true
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
    fn test_probe_statistics_only_for_the_confirmed_target_scan() {
        use databend_common_expression::TableDataType;
        use databend_common_sql::ColumnBindingBuilder;
        use databend_common_sql::Metadata;
        use databend_common_sql::Symbol;
        use databend_common_sql::Visibility;
        use databend_common_sql::optimizer::ir::PExpr;
        use databend_common_sql::plans::BoundColumnRef;
        use databend_common_sql::plans::Join;
        use databend_common_sql::plans::ScalarExpr;
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
        let other = metadata.add_base_table_column(
            "b".into(),
            TableDataType::Number(NumberDataType::Int32),
            3,
            None,
            1,
            None,
            None,
        );
        metadata
            .add_base_column_scan_id(std::collections::HashMap::from([(source, 7), (other, 7)]));
        let derived = metadata.add_derived_column("d".into(), DataType::Boolean);
        let metadata = std::sync::Arc::new(RwLock::new(metadata));
        let column_ref = |index: Symbol| {
            ScalarExpr::BoundColumnRef(BoundColumnRef {
                span: None,
                column: ColumnBindingBuilder::new(
                    index.to_string(),
                    index,
                    Box::new(DataType::Number(NumberDataType::Int32)),
                    Visibility::Visible,
                )
                .build(),
            })
        };
        let scan = |scan_id, table_index| {
            PExpr::create_leaf(Scan {
                scan_id,
                table_index,
                columns: [source, other].into_iter().collect(),
                ..Default::default()
            })
        };
        let key = column_ref(source);
        let resolve = |plan: &PExpr, targets: &[(&ScalarExpr, usize)]| {
            super::resolve_probe_table_index(&metadata, plan, &key, targets)
        };
        assert_eq!(resolve(&scan(7, 3), &[(&key, 7)]), Some(3));
        assert_eq!(resolve(&scan(8, 3), &[(&key, 7)]), None);
        assert_eq!(resolve(&scan(7, 4), &[(&key, 7)]), None);
        assert_eq!(resolve(&scan(7, 3), &[(&key, 8)]), None);
        assert_eq!(resolve(&scan(7, 3), &[(&key, 7), (&key, 8)]), None);
        // A target pushed to another column does not use the key's statistics.
        assert_eq!(resolve(&scan(7, 3), &[(&column_ref(other), 7)]), None);
        let joined = PExpr::create_binary(Join::default(), scan(7, 3), scan(8, 4));
        assert_eq!(resolve(&joined, &[(&key, 7)]), Some(3)); // No whole-side single-scan guard.
        let derived = column_ref(derived);
        assert_eq!(
            super::resolve_probe_table_index(&metadata, &scan(7, 3), &derived, &[(&derived, 7)]),
            None
        );
    }
}
