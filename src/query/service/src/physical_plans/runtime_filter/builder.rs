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
use databend_common_sql::MetadataRef;
use databend_common_sql::optimizer::ir::PExpr;
use databend_common_sql::plans::Exchange;
use databend_common_sql::plans::Join;
use databend_common_sql::plans::JoinType;
use databend_common_sql::plans::PhysicalRelOperator;
use databend_common_sql::plans::ScalarExpr;

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
/// Collection of runtime filters to be applied
pub async fn build_runtime_filter(
    ctx: Arc<dyn TableContext>,
    metadata: &MetadataRef,
    join: &Join,
    s_expr: &PExpr,
    build_keys: &[RemoteExpr],
) -> Result<PhysicalRuntimeFilters> {
    if join.runtime_filters.is_empty() || !ctx.get_settings().get_enable_join_runtime_filter()? {
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
    for runtime_filter in join.runtime_filters.iter() {
        let Some(index) = join.equi_conditions.iter().position(|condition| {
            !condition.is_null_equal
                && condition.canonical_keys()
                    == (&runtime_filter.probe_key, &runtime_filter.build_key)
        }) else {
            continue;
        };
        let Some(targets) = scan_targets.get(&runtime_filter.id) else {
            continue;
        };

        let build_key = &build_keys[index];
        let target_type = build_key.as_expr(&BUILTIN_FUNCTIONS).data_type().clone();
        let mut probe_targets = Vec::new();
        for (probe_key, scan_id) in targets {
            let Some(probe_key) = resolve_runtime_filter_probe_expr(metadata, probe_key)? else {
                continue;
            };
            let Some(probe_key) =
                normalize_probe_target(&probe_key.as_remote_expr(), &target_type, &func_ctx)?
            else {
                continue;
            };
            if supported_probe_key_for_runtime_filter(&probe_key, &func_ctx) {
                push_unique_target(&mut probe_targets, probe_key, *scan_id);
            }
        }
        if probe_targets.is_empty() {
            continue;
        }

        let build_table_rows =
            get_build_table_rows(ctx.clone(), metadata, &runtime_filter.build_key).await?;
        let data_type = target_type.remove_nullable();
        let id = metadata.write().next_runtime_filter_id();
        filters.push(PhysicalRuntimeFilter {
            id,
            build_key: build_key.clone(),
            probe_targets,
            build_table_rows,
            enable_bloom_runtime_filter: is_type_supported_for_bloom_filter(&data_type),
            enable_inlist_runtime_filter: true,
            enable_min_max_runtime_filter: is_type_supported_for_min_max_filter(&data_type),
        });
    }

    Ok(PhysicalRuntimeFilters { filters })
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

async fn get_build_table_rows(
    ctx: Arc<dyn TableContext>,
    metadata: &MetadataRef,
    build_key: &ScalarExpr,
) -> Result<Option<u64>> {
    let used_columns = build_key.used_columns();
    if used_columns.len() != 1 {
        return Ok(None);
    }
    let column_idx = *used_columns.iter().next().unwrap();
    let table = {
        let metadata = metadata.read();
        let ColumnEntry::BaseTableColumn(column) = metadata.column(column_idx) else {
            return Ok(None);
        };
        metadata.table(column.table_index).table().clone()
    };

    let table_stats = table.table_statistics(ctx, false, None).await?;
    Ok(table_stats.and_then(|s| s.num_rows))
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
}
