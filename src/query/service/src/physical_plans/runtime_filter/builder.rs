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
use databend_common_sql::IndexType;
use databend_common_sql::MetadataRef;
use databend_common_sql::Symbol;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::plans::Exchange;
use databend_common_sql::plans::Join;
use databend_common_sql::plans::JoinEquiCondition;
use databend_common_sql::plans::JoinType;
use databend_common_sql::plans::RelOperator;
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

/// Build runtime filters for a join operation
///
/// Creates one runtime filter per eligible probe key and propagates it to
/// equality-safe connector columns and their attached expression leaves.
///
/// # Arguments
/// * `ctx` - Table context
/// * `metadata` - Metadata reference
/// * `join` - Join plan
/// * `s_expr` - SExpr for the join
/// * `build_keys` - Build side keys
/// * `probe_keys` - Probe keys with their scan, source column, connector, and null-equality metadata
///
/// # Returns
/// Collection of runtime filters to be applied
pub async fn build_runtime_filter(
    ctx: Arc<dyn TableContext>,
    metadata: &MetadataRef,
    join: &Join,
    s_expr: &SExpr,
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
        .zip(probe_keys.into_iter())
        .zip(build_table_indexes.into_iter())
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
            probe_key,
            scan_id,
            column_idx,
            is_connector,
            &func_ctx,
        )?;

        let build_table_rows =
            get_build_table_rows(ctx.clone(), metadata, build_table_index).await?;

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
            enable_bloom_runtime_filter,
            enable_inlist_runtime_filter,
            enable_min_max_runtime_filter,
        };
        filters.push(runtime_filter);
    }

    Ok(PhysicalRuntimeFilters { filters })
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
    s_expr: &SExpr,
    probe_key: RemoteExpr<String>,
    probe_scan_id: usize,
    probe_key_col_idx: Symbol,
    probe_key_is_connector: bool,
    func_ctx: &FunctionContext,
) -> Result<Vec<(RemoteExpr<String>, usize)>> {
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
            relations.push((left, right));
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

fn collect_equi_conditions(s_expr: &SExpr) -> Result<Vec<JoinEquiCondition>> {
    let mut conditions = Vec::new();

    if let RelOperator::Join(join) = s_expr.plan() {
        if matches!(join.join_type, JoinType::Inner) {
            conditions.extend(join.equi_conditions.clone());
        }
    }

    for child in s_expr.children() {
        conditions.extend(collect_equi_conditions(child)?);
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
    let expr =
        databend_common_expression::ConstantFolder::fold(&expr, func_ctx, &BUILTIN_FUNCTIONS).0;
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
