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
use std::cmp::Ordering;
use std::collections::HashMap;
use std::mem::discriminant;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use databend_common_exception::Result;
use databend_common_expression::ColumnRef;
use databend_common_expression::Constant;
use databend_common_expression::ConstantFolder;
use databend_common_expression::Domain;
use databend_common_expression::Expr;
use databend_common_expression::ExprVisitor;
use databend_common_expression::FunctionContext;
use databend_common_expression::RangeConstraint;
use databend_common_expression::RemoteExpr;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::expr::Cast;
use databend_common_expression::expr::FunctionCall;
use databend_common_expression::types::DataType;
use databend_common_expression::types::number::NumberScalar;
use databend_common_expression::visit_expr;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_storages_common_cache::CacheLockStats;
use databend_storages_common_index::eliminate_cast;
use databend_storages_common_index::statistics_to_domain;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ClusterKey;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use databend_storages_common_table_meta::meta::GranuleIndexLayout;
use opendal::Operator;

use crate::io::GranuleMins;
use crate::io::PrefetchedGranuleMins;
use crate::io::num_granules_of;

const GRANULE_KEY_COLUMN_PREFIX: &str = "__granule_cluster_key_";

/// Applies cluster-key predicates to per-granule mins.
pub struct SparseGranuleIndexPruner {
    evaluator: GranulePredicateEvaluator,
    dal: Operator,
    read_settings: ReadSettings,
    cluster_key_types: Vec<DataType>,
    table_cluster_key_id: u32,
}

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct SparseGranulePruneProfile {
    pub(crate) load: Duration,
    pub(crate) evaluate: Duration,
}

/// A block's sparse prune whose mins file requests are already in flight.
pub(crate) struct SparsePrunePrefetch {
    mins: PrefetchedGranuleMins,
    block_max: Scalar,
}

impl SparseGranuleIndexPruner {
    #[allow(clippy::too_many_arguments)]
    pub fn try_create(
        func_ctx: FunctionContext,
        _schema: &TableSchemaRef,
        filter_expr: Option<&Expr<String>>,
        cluster_key_meta: Option<ClusterKey>,
        cluster_keys: Vec<RemoteExpr<String>>,
        dal: Operator,
        read_settings: ReadSettings,
    ) -> Result<Option<Arc<SparseGranuleIndexPruner>>> {
        let Some(cluster_key_meta) = cluster_key_meta else {
            return Ok(None);
        };
        let Some(filter) = filter_expr else {
            return Ok(None);
        };
        if cluster_keys.is_empty() {
            return Ok(None);
        }

        let cluster_keys = cluster_keys
            .into_iter()
            .map(|expr| expr.as_expr(&BUILTIN_FUNCTIONS))
            .collect::<Vec<_>>();
        let cluster_key_types = cluster_keys
            .iter()
            .map(|expr| expr.data_type().clone())
            .collect::<Vec<_>>();
        let evaluator =
            GranulePredicateEvaluator::try_create(func_ctx, cluster_keys, filter.clone())?;
        if !evaluator.touches_cluster_key() {
            return Ok(None);
        }

        Ok(Some(Arc::new(SparseGranuleIndexPruner {
            evaluator,
            dal,
            read_settings,
            cluster_key_types,
            table_cluster_key_id: cluster_key_meta.0,
        })))
    }

    pub fn select_granule_ranges(
        &self,
        block_meta: &BlockMeta,
        granule_index: &GranuleIndexLayout,
        input: &[Range<usize>],
    ) -> Result<Vec<Range<usize>>> {
        match self.prefetch_mins(block_meta, granule_index)? {
            Some(pending) => Ok(self.prune_with_mins(pending, input)?.0),
            None => Ok(input.to_vec()),
        }
    }

    /// Issue the mins file requests for one block without waiting for them.
    /// Returns `Ok(None)` when sparse pruning does not apply to this block.
    pub(crate) fn prefetch_mins(
        &self,
        block_meta: &BlockMeta,
        granule_index: &GranuleIndexLayout,
    ) -> Result<Option<SparsePrunePrefetch>> {
        self.prefetch_mins_with_stats(block_meta, granule_index, None)
    }

    pub(crate) fn prefetch_mins_with_stats(
        &self,
        block_meta: &BlockMeta,
        granule_index: &GranuleIndexLayout,
        lock_stats: Option<Arc<CacheLockStats>>,
    ) -> Result<Option<SparsePrunePrefetch>> {
        let Some(cluster_stats) = block_meta.cluster_stats.as_ref() else {
            return Ok(None);
        };

        if self.table_cluster_key_id != cluster_stats.cluster_key_id {
            return Ok(None);
        }

        let num_granules = num_granules_of(
            block_meta.row_count as usize,
            granule_index.granule_rows as usize,
        );
        if num_granules == 0 {
            return Ok(None);
        }

        let Some(mins_layout) = granule_index.mins.as_ref() else {
            return Ok(None);
        };

        let mins = PrefetchedGranuleMins::prefetch_with_stats(
            &self.dal,
            &self.read_settings,
            mins_layout,
            self.cluster_key_types.len(),
            num_granules,
            lock_stats,
        )?;
        Ok(Some(SparsePrunePrefetch {
            mins,
            block_max: Scalar::Tuple(cluster_stats.max().clone()),
        }))
    }

    /// Await the mins issued by [`Self::prefetch_mins`], decode them and apply
    /// the cluster-key predicate to `input`.
    pub(crate) fn prune_with_mins(
        &self,
        pending: SparsePrunePrefetch,
        input: &[Range<usize>],
    ) -> Result<(Vec<Range<usize>>, SparseGranulePruneProfile)> {
        let load_start = Instant::now();
        let granule_mins = pending.mins.read(&self.cluster_key_types)?;
        let load = load_start.elapsed();

        let evaluate_start = Instant::now();
        let ranges = self.evaluator.apply(&granule_mins, &pending.block_max)?;
        let ranges = intersect_ranges(input, &ranges);
        let evaluate = evaluate_start.elapsed();
        Ok((ranges, SparseGranulePruneProfile { load, evaluate }))
    }
}

/// Evaluates a source-column predicate against synthetic columns representing cluster-key
/// expressions. It supports both predicates written directly on the key expression and predicates
/// on source columns by projecting their domains through the key expression.
trait GranuleMinValues {
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn value(&self, index: usize) -> Scalar;
}

impl GranuleMinValues for GranuleMins {
    fn len(&self) -> usize {
        self.len()
    }

    fn value(&self, index: usize) -> Scalar {
        self.value(index)
    }
}

#[cfg(test)]
impl GranuleMinValues for Vec<Scalar> {
    fn len(&self) -> usize {
        Vec::len(self)
    }

    fn value(&self, index: usize) -> Scalar {
        self[index].clone()
    }
}

struct GranulePredicateEvaluator {
    filter: Expr<String>,
    key_columns: Vec<(String, DataType)>,
    projected_key_domains: Vec<Option<Domain>>,
    /// Discrete values per key column from `eq`/`IN` conjuncts. Domain folding
    /// only sees an `IN` list's min/max envelope, so hole pruning needs these.
    point_constraints: Vec<Option<Vec<Scalar>>>,
    source_predicate_impossible: bool,
    touches_cluster_key: bool,
    func_ctx: FunctionContext,
}

impl GranulePredicateEvaluator {
    fn try_create(
        func_ctx: FunctionContext,
        cluster_keys: Vec<Expr<String>>,
        filter: Expr<String>,
    ) -> Result<Self> {
        let filter_columns = filter.column_refs();
        let key_columns = cluster_keys
            .iter()
            .enumerate()
            .map(|(index, expr)| {
                (
                    format!("{}{}", GRANULE_KEY_COLUMN_PREFIX, index),
                    expr.data_type().clone(),
                )
            })
            .collect::<Vec<_>>();
        let touches_cluster_key = cluster_keys.iter().any(|key| {
            key.column_refs()
                .keys()
                .any(|name| filter_columns.contains_key(name))
        });

        let replacements = cluster_keys
            .iter()
            .zip(key_columns.iter())
            .collect::<Vec<_>>();
        let mut rewriter = KeyExpressionRewriter { replacements };
        let filter = match visit_expr(&filter, &mut rewriter)? {
            Some(filter) => filter,
            None => filter,
        };

        let input_domains = ConstantFolder::full_input_domains(&filter);
        let filter = eliminate_cast(&filter, input_domains, &func_ctx).unwrap_or(filter);

        let (source_domains, source_predicate_impossible) = source_predicate_domains(&filter);
        let projected_key_domains = cluster_keys
            .iter()
            .map(|key| project_source_domains(key, &source_domains, &func_ctx))
            .collect();
        let point_constraints = extract_point_constraints(&filter, &key_columns);

        Ok(Self {
            filter,
            key_columns,
            projected_key_domains,
            point_constraints,
            source_predicate_impossible,
            touches_cluster_key,
            func_ctx,
        })
    }

    fn touches_cluster_key(&self) -> bool {
        self.touches_cluster_key
    }

    fn apply<M: GranuleMinValues + ?Sized>(
        &self,
        min_values: &M,
        max_value: &Scalar,
    ) -> Result<Vec<Range<usize>>> {
        if min_values.is_empty() {
            return Ok(vec![]);
        }
        if self.point_constraints.iter().all(Option::is_none) {
            return self.apply_envelope(min_values, max_value);
        }
        self.apply_discrete(min_values, max_value)
    }

    /// Probe inwards from both ends; exact for predicates whose survivors form
    /// one contiguous run.
    fn apply_envelope<M: GranuleMinValues + ?Sized>(
        &self,
        min_values: &M,
        max_value: &Scalar,
    ) -> Result<Vec<Range<usize>>> {
        let mut start = 0;
        let mut end = min_values.len() - 1;
        while start <= end {
            let lower = min_values.value(start);
            let upper = (start + 1 < min_values.len()).then(|| min_values.value(start + 1));
            if self.eval_single_granule(&lower, upper.as_ref().unwrap_or(max_value))? {
                break;
            }
            start += 1;
        }

        while end >= start {
            let lower = min_values.value(end);
            let upper = (end + 1 < min_values.len()).then(|| min_values.value(end + 1));
            if self.eval_single_granule(&lower, upper.as_ref().unwrap_or(max_value))? {
                break;
            }
            end -= 1;
        }

        #[allow(clippy::single_range_in_vec_init)]
        if start > end {
            Ok(vec![])
        } else {
            Ok(vec![start..end + 1])
        }
    }

    fn apply_discrete<M: GranuleMinValues + ?Sized>(
        &self,
        min_values: &M,
        max_value: &Scalar,
    ) -> Result<Vec<Range<usize>>> {
        let mut ranges: Vec<Range<usize>> = Vec::new();
        for index in 0..min_values.len() {
            let lower = min_values.value(index);
            let upper = (index + 1 < min_values.len()).then(|| min_values.value(index + 1));
            if !self.eval_single_granule(&lower, upper.as_ref().unwrap_or(max_value))? {
                continue;
            }
            match ranges.last_mut() {
                Some(last) if last.end == index => last.end = index + 1,
                _ => ranges.push(index..index + 1),
            }
        }
        Ok(ranges)
    }

    fn eval_single_granule(&self, min_value: &Scalar, max_value: &Scalar) -> Result<bool> {
        if self.source_predicate_impossible {
            return Ok(false);
        }

        let Some(min_values) = min_value.as_tuple() else {
            return Ok(true);
        };
        let Some(max_values) = max_value.as_tuple() else {
            return Ok(true);
        };
        if min_values.len() < self.key_columns.len() || max_values.len() < self.key_columns.len() {
            return Ok(true);
        }

        // A lexicographic tuple interval only bounds a key component while all
        // preceding components are equal.
        let mut bounded = Vec::with_capacity(self.key_columns.len());
        for (index, (min, max)) in min_values
            .iter()
            .zip(max_values.iter())
            .take(self.key_columns.len())
            .enumerate()
        {
            bounded.push((index, min, max));
            if min != max {
                break;
            }
        }

        for (index, min, max) in &bounded {
            let Some(points) = &self.point_constraints[*index] else {
                continue;
            };
            if !points_hit_interval(points, min, max) {
                return Ok(false);
            }
        }

        let mut page_domains = Vec::new();
        for (index, min, max) in &bounded {
            let stat = ColumnStatistics::new((*min).clone(), (*max).clone(), 0, 0, None);
            let domain = statistics_to_domain(vec![&stat], &self.key_columns[*index].1);
            page_domains.push((*index, domain));
        }

        // First handle predicates written on source columns, for example
        // `start_time >= ... AND start_time < ...` for key `to_yyyymmdd(start_time)`, or
        // `trace_id = ...` for key `substring(trace_id, 1, 40)`.
        for (index, page_domain) in &page_domains {
            if let Some(projected) = &self.projected_key_domains[*index]
                && domains_disjoint(projected, page_domain)
            {
                return Ok(false);
            }
        }

        // Then handle predicates already written in terms of the cluster-key expression by
        // replacing that expression with its synthetic granule-key column.
        let mut input_domains = ConstantFolder::full_input_domains(&self.filter);
        for (index, page_domain) in page_domains {
            input_domains.insert(self.key_columns[index].0.clone(), page_domain);
        }
        let (folded, _) = ConstantFolder::fold_with_domain(
            Cow::Borrowed(&self.filter),
            &input_domains,
            &self.func_ctx,
            &BUILTIN_FUNCTIONS,
        );
        Ok(!matches!(
            folded.as_ref(),
            Expr::Constant(Constant {
                scalar: Scalar::Boolean(false),
                ..
            })
        ))
    }
}

fn key_expressions_equal(left: &Expr<String>, right: &Expr<String>) -> bool {
    match (left, right) {
        (Expr::Constant(left), Expr::Constant(right)) => {
            left.scalar == right.scalar && left.data_type == right.data_type
        }
        (Expr::ColumnRef(left), Expr::ColumnRef(right)) => {
            left.id == right.id && left.data_type == right.data_type
        }
        (Expr::Cast(left), Expr::Cast(right)) => {
            left.is_try == right.is_try
                && left.dest_type == right.dest_type
                && key_expressions_equal(&left.expr, &right.expr)
        }
        (Expr::FunctionCall(left), Expr::FunctionCall(right)) => {
            left.id == right.id
                && left.generics == right.generics
                && left.return_type == right.return_type
                && left.args.len() == right.args.len()
                && left
                    .args
                    .iter()
                    .zip(&right.args)
                    .all(|(left, right)| key_expressions_equal(left, right))
        }
        (Expr::LambdaFunctionCall(left), Expr::LambdaFunctionCall(right)) => {
            left.name == right.name
                && left.return_type == right.return_type
                && left.lambda_expr == right.lambda_expr
                && left.lambda_display == right.lambda_display
                && left.args.len() == right.args.len()
                && left
                    .args
                    .iter()
                    .zip(&right.args)
                    .all(|(left, right)| key_expressions_equal(left, right))
        }
        _ => false,
    }
}

struct KeyExpressionRewriter<'a> {
    replacements: Vec<(&'a Expr<String>, &'a (String, DataType))>,
}

impl KeyExpressionRewriter<'_> {
    fn replace(&self, expr: &Expr<String>) -> Option<Expr<String>> {
        self.replacements
            .iter()
            .find(|(key, _)| key_expressions_equal(key, expr))
            .map(|(_, (name, data_type))| {
                Expr::ColumnRef(ColumnRef {
                    span: None,
                    id: name.clone(),
                    data_type: data_type.clone(),
                    display_name: name.clone(),
                })
            })
    }
}

impl ExprVisitor<String> for KeyExpressionRewriter<'_> {
    fn enter_column_ref(
        &mut self,
        column: &ColumnRef<String>,
    ) -> std::result::Result<Option<Expr<String>>, Self::Error> {
        Ok(self.replace(&Expr::ColumnRef(column.clone())))
    }

    fn enter_cast(
        &mut self,
        cast: &Cast<String>,
    ) -> std::result::Result<Option<Expr<String>>, Self::Error> {
        if let Some(expr) = self.replace(&Expr::Cast(cast.clone())) {
            Ok(Some(expr))
        } else {
            Self::visit_cast(cast, self)
        }
    }

    fn enter_function_call(
        &mut self,
        call: &FunctionCall<String>,
    ) -> std::result::Result<Option<Expr<String>>, Self::Error> {
        if let Some(expr) = self.replace(&Expr::FunctionCall(call.clone())) {
            Ok(Some(expr))
        } else {
            Self::visit_function_call(call, self)
        }
    }
}

fn project_source_domains(
    key: &Expr<String>,
    source_domains: &HashMap<String, Domain>,
    func_ctx: &FunctionContext,
) -> Option<Domain> {
    if !is_monotonic_key_expr(key, source_domains, func_ctx) {
        return None;
    }

    let mut input_domains = ConstantFolder::full_input_domains(key);
    for (name, domain) in source_domains {
        if input_domains.contains_key(name) {
            input_domains.insert(name.clone(), domain.clone());
        }
    }

    ConstantFolder::fold_with_domain(
        Cow::Borrowed(key),
        &input_domains,
        func_ctx,
        &BUILTIN_FUNCTIONS,
    )
    .1
}

fn is_monotonic_key_expr(
    expr: &Expr<String>,
    source_domains: &HashMap<String, Domain>,
    func_ctx: &FunctionContext,
) -> bool {
    match expr {
        Expr::ColumnRef(column) => source_domains.contains_key(&column.id),
        Expr::FunctionCall(call) => {
            let input_domains = call
                .args
                .iter()
                .map(|arg| {
                    let mut domains = ConstantFolder::full_input_domains(arg);
                    for (name, domain) in source_domains {
                        if domains.contains_key(name) {
                            domains.insert(name.clone(), domain.clone());
                        }
                    }
                    ConstantFolder::fold_with_domain(
                        Cow::Borrowed(arg),
                        &domains,
                        func_ctx,
                        &BUILTIN_FUNCTIONS,
                    )
                    .1
                    .unwrap_or_else(|| Domain::full(arg.data_type()))
                })
                .collect::<Vec<_>>();
            let Some(property) = BUILTIN_FUNCTIONS.get_property(&call.function.signature.name)
            else {
                return false;
            };
            let monotonic_arg = property
                .monotonicity_check
                .and_then(|check| check(func_ctx, &input_domains))
                .or_else(|| {
                    (call.args.len() == 1
                        && (property.monotonicity
                            || property
                                .monotonicity_by_type
                                .contains(call.args[0].data_type())))
                    .then_some(0)
                });
            monotonic_arg
                .and_then(|index| call.args.get(index))
                .is_some_and(|arg| is_monotonic_key_expr(arg, source_domains, func_ctx))
        }
        _ => false,
    }
}

fn extract_point_constraints(
    filter: &Expr<String>,
    key_columns: &[(String, DataType)],
) -> Vec<Option<Vec<Scalar>>> {
    let mut result: Vec<Option<Vec<Scalar>>> = vec![None; key_columns.len()];
    let mut conjuncts = Vec::new();
    collect_conjuncts(filter, &mut conjuncts);
    for conjunct in conjuncts {
        let Some((index, mut points)) = conjunct_point_set(conjunct, key_columns) else {
            continue;
        };
        points.sort();
        points.dedup();
        let merged = match result[index].take() {
            Some(existing) => intersect_sorted_points(&existing, &points),
            None => points,
        };
        result[index] = Some(merged);
    }
    result
}

fn collect_conjuncts<'a>(expr: &'a Expr<String>, conjuncts: &mut Vec<&'a Expr<String>>) {
    if let Expr::FunctionCall(call) = expr {
        match call.function.signature.name.as_str() {
            "and" | "and_filters" => {
                for arg in &call.args {
                    collect_conjuncts(arg, conjuncts);
                }
                return;
            }
            "is_true" if call.args.len() == 1 => {
                collect_conjuncts(&call.args[0], conjuncts);
                return;
            }
            _ => {}
        }
    }
    conjuncts.push(expr);
}

fn conjunct_point_set(
    conjunct: &Expr<String>,
    key_columns: &[(String, DataType)],
) -> Option<(usize, Vec<Scalar>)> {
    let Expr::FunctionCall(call) = conjunct else {
        return None;
    };
    match call.function.signature.name.as_str() {
        "eq" => {
            let (index, point) = eq_key_point(call, key_columns)?;
            Some((index, vec![point]))
        }
        "contains" => contains_key_points(call, key_columns),
        "or" | "or_filters" => {
            let mut disjuncts = Vec::new();
            collect_disjuncts(conjunct, &mut disjuncts);
            let mut key_index = None;
            let mut points = Vec::new();
            for disjunct in disjuncts {
                let Expr::FunctionCall(call) = disjunct else {
                    return None;
                };
                if call.function.signature.name != "eq" {
                    return None;
                }
                let (index, point) = eq_key_point(call, key_columns)?;
                if *key_index.get_or_insert(index) != index {
                    return None;
                }
                points.push(point);
            }
            let index = key_index?;
            Some((index, points))
        }
        _ => None,
    }
}

fn collect_disjuncts<'a>(expr: &'a Expr<String>, disjuncts: &mut Vec<&'a Expr<String>>) {
    if let Expr::FunctionCall(call) = expr
        && matches!(call.function.signature.name.as_str(), "or" | "or_filters")
    {
        for arg in &call.args {
            collect_disjuncts(arg, disjuncts);
        }
        return;
    }
    disjuncts.push(expr);
}

fn eq_key_point(
    call: &FunctionCall<String>,
    key_columns: &[(String, DataType)],
) -> Option<(usize, Scalar)> {
    let constraint = RangeConstraint::try_from_function_call(call)?;
    if constraint.operator != "eq" || constraint.constant.is_null() {
        return None;
    }
    let index = key_columns
        .iter()
        .position(|(name, _)| *name == constraint.column_id)?;
    Some((index, constraint.constant))
}

fn contains_key_points(
    call: &FunctionCall<String>,
    key_columns: &[(String, DataType)],
) -> Option<(usize, Vec<Scalar>)> {
    let [array, probe] = call.args.as_slice() else {
        return None;
    };
    let Expr::Constant(Constant {
        scalar: Scalar::Array(items),
        ..
    }) = array
    else {
        return None;
    };
    let probe = probe.as_column_ref()?;
    let index = key_columns.iter().position(|(name, _)| *name == probe.id)?;
    let mut points = Vec::with_capacity(items.len());
    for item in items.iter() {
        let point = item.to_owned();
        // NULL list entries can never satisfy the equality.
        if point.is_null() {
            continue;
        }
        points.push(point);
    }
    Some((index, points))
}

fn intersect_sorted_points(left: &[Scalar], right: &[Scalar]) -> Vec<Scalar> {
    let mut result = Vec::new();
    let (mut l, mut r) = (0, 0);
    while l < left.len() && r < right.len() {
        match left[l].cmp(&right[r]) {
            Ordering::Less => l += 1,
            Ordering::Greater => r += 1,
            Ordering::Equal => {
                result.push(left[l].clone());
                l += 1;
                r += 1;
            }
        }
    }
    result
}

fn points_hit_interval(points: &[Scalar], min: &Scalar, max: &Scalar) -> bool {
    if min.is_null() || max.is_null() {
        return true;
    }
    let Some(first) = points.first() else {
        return false;
    };
    if !same_scalar_kind(first, min) || !same_scalar_kind(first, max) {
        return true;
    }
    let start = points.partition_point(|point| point < min);
    start < points.len() && &points[start] <= max
}

/// `Scalar` ordering across different variants (or number widths) is positional,
/// not semantic, so point pruning only compares identical kinds.
fn same_scalar_kind(left: &Scalar, right: &Scalar) -> bool {
    match (left, right) {
        (Scalar::Number(left), Scalar::Number(right)) => discriminant(left) == discriminant(right),
        (Scalar::Decimal(left), Scalar::Decimal(right)) => {
            discriminant(left) == discriminant(right) && left.size() == right.size()
        }
        _ => discriminant(left) == discriminant(right),
    }
}

fn source_predicate_domains(filter: &Expr<String>) -> (HashMap<String, Domain>, bool) {
    let mut constraints = HashMap::<String, Vec<RangeConstraint<String>>>::new();
    collect_conjunctive_ranges(filter, &mut constraints);

    let mut domains = HashMap::new();
    for (name, constraints) in constraints {
        match constraints_to_domain(&constraints) {
            ConstraintDomain::Domain(domain) => {
                domains.insert(name, domain);
            }
            ConstraintDomain::Empty => return (HashMap::new(), true),
            ConstraintDomain::Unknown => {}
        }
    }
    (domains, false)
}

fn collect_conjunctive_ranges(
    expr: &Expr<String>,
    ranges: &mut HashMap<String, Vec<RangeConstraint<String>>>,
) {
    let Expr::FunctionCall(call) = expr else {
        return;
    };
    match call.function.signature.name.as_str() {
        "and" | "and_filters" => {
            for arg in &call.args {
                collect_conjunctive_ranges(arg, ranges);
            }
        }
        "is_true" if call.args.len() == 1 => collect_conjunctive_ranges(&call.args[0], ranges),
        _ => {
            let Some(constraint) = RangeConstraint::try_from_function_call(call) else {
                return;
            };
            if constraint.operator != "noteq" {
                ranges
                    .entry(constraint.column_id.clone())
                    .or_default()
                    .push(constraint);
            }
        }
    }
}

enum ConstraintDomain {
    Domain(Domain),
    Empty,
    Unknown,
}

fn constraints_to_domain(constraints: &[RangeConstraint<String>]) -> ConstraintDomain {
    let Some(first) = constraints.first() else {
        return ConstraintDomain::Unknown;
    };
    if constraints
        .iter()
        .any(|constraint| constraint.data_type != first.data_type || constraint.constant.is_null())
    {
        return ConstraintDomain::Unknown;
    }

    let data_type = &first.data_type;
    // A constant whose scalar kind differs from the column type (e.g. a String
    // literal surviving an implicit cast on an Int32 column) can neither be
    // ordered against the column bounds nor form a `ColumnStatistics`
    // interval. The former `Domain::from_datum` path returned `Err` for such
    // constants; fail open the same way.
    let value_type = data_type.remove_nullable();
    if constraints
        .iter()
        .any(|constraint| constraint.constant.as_ref().infer_data_type() != value_type)
    {
        return ConstraintDomain::Unknown;
    }
    let (full_min, full_max) = Domain::full(&value_type).to_minmax();
    let mut lower = (!full_min.is_null()).then_some(full_min);
    let mut upper = (!full_max.is_null()).then_some(full_max);
    for constraint in constraints {
        match constraint.operator.as_str() {
            "eq" => {
                lower = Some(lower.map_or_else(
                    || constraint.constant.clone(),
                    |value| value.max(constraint.constant.clone()),
                ));
                upper = Some(upper.map_or_else(
                    || constraint.constant.clone(),
                    |value| value.min(constraint.constant.clone()),
                ));
            }
            "gt" => {
                let bound = discrete_successor(&constraint.constant)
                    .unwrap_or_else(|| constraint.constant.clone());
                lower = Some(match lower {
                    Some(value) => value.max(bound),
                    None => bound,
                });
            }
            "gte" => {
                lower = Some(lower.map_or_else(
                    || constraint.constant.clone(),
                    |value| value.max(constraint.constant.clone()),
                ));
            }
            "lt" => {
                let bound = discrete_predecessor(&constraint.constant)
                    .unwrap_or_else(|| constraint.constant.clone());
                upper = Some(match upper {
                    Some(value) => value.min(bound),
                    None => bound,
                });
            }
            "lte" => {
                upper = Some(upper.map_or_else(
                    || constraint.constant.clone(),
                    |value| value.min(constraint.constant.clone()),
                ));
            }
            _ => {}
        }
    }
    let (Some(lower), Some(upper)) = (lower, upper) else {
        return ConstraintDomain::Unknown;
    };
    if lower > upper {
        return ConstraintDomain::Empty;
    }

    let stat = ColumnStatistics::new(lower, upper, 0, 0, None);
    ConstraintDomain::Domain(statistics_to_domain(vec![&stat], data_type))
}

fn discrete_successor(value: &Scalar) -> Option<Scalar> {
    match value {
        Scalar::Timestamp(value) => value.checked_add(1).map(Scalar::Timestamp),
        Scalar::Date(value) => value.checked_add(1).map(Scalar::Date),
        Scalar::Number(NumberScalar::UInt8(value)) => value
            .checked_add(1)
            .map(|value| Scalar::Number(NumberScalar::UInt8(value))),
        Scalar::Number(NumberScalar::UInt16(value)) => value
            .checked_add(1)
            .map(|value| Scalar::Number(NumberScalar::UInt16(value))),
        Scalar::Number(NumberScalar::UInt32(value)) => value
            .checked_add(1)
            .map(|value| Scalar::Number(NumberScalar::UInt32(value))),
        Scalar::Number(NumberScalar::UInt64(value)) => value
            .checked_add(1)
            .map(|value| Scalar::Number(NumberScalar::UInt64(value))),
        Scalar::Number(NumberScalar::Int8(value)) => value
            .checked_add(1)
            .map(|value| Scalar::Number(NumberScalar::Int8(value))),
        Scalar::Number(NumberScalar::Int16(value)) => value
            .checked_add(1)
            .map(|value| Scalar::Number(NumberScalar::Int16(value))),
        Scalar::Number(NumberScalar::Int32(value)) => value
            .checked_add(1)
            .map(|value| Scalar::Number(NumberScalar::Int32(value))),
        Scalar::Number(NumberScalar::Int64(value)) => value
            .checked_add(1)
            .map(|value| Scalar::Number(NumberScalar::Int64(value))),
        _ => None,
    }
}

fn discrete_predecessor(value: &Scalar) -> Option<Scalar> {
    match value {
        Scalar::Timestamp(value) => value.checked_sub(1).map(Scalar::Timestamp),
        Scalar::Date(value) => value.checked_sub(1).map(Scalar::Date),
        Scalar::Number(NumberScalar::UInt8(value)) => value
            .checked_sub(1)
            .map(|value| Scalar::Number(NumberScalar::UInt8(value))),
        Scalar::Number(NumberScalar::UInt16(value)) => value
            .checked_sub(1)
            .map(|value| Scalar::Number(NumberScalar::UInt16(value))),
        Scalar::Number(NumberScalar::UInt32(value)) => value
            .checked_sub(1)
            .map(|value| Scalar::Number(NumberScalar::UInt32(value))),
        Scalar::Number(NumberScalar::UInt64(value)) => value
            .checked_sub(1)
            .map(|value| Scalar::Number(NumberScalar::UInt64(value))),
        Scalar::Number(NumberScalar::Int8(value)) => value
            .checked_sub(1)
            .map(|value| Scalar::Number(NumberScalar::Int8(value))),
        Scalar::Number(NumberScalar::Int16(value)) => value
            .checked_sub(1)
            .map(|value| Scalar::Number(NumberScalar::Int16(value))),
        Scalar::Number(NumberScalar::Int32(value)) => value
            .checked_sub(1)
            .map(|value| Scalar::Number(NumberScalar::Int32(value))),
        Scalar::Number(NumberScalar::Int64(value)) => value
            .checked_sub(1)
            .map(|value| Scalar::Number(NumberScalar::Int64(value))),
        _ => None,
    }
}

fn domains_disjoint(left: &Domain, right: &Domain) -> bool {
    let (left_min, left_max) = left.to_minmax();
    let (right_min, right_max) = right.to_minmax();
    if left_min.is_null() || left_max.is_null() || right_min.is_null() || right_max.is_null() {
        return false;
    }
    left_max < right_min || right_max < left_min
}

fn intersect_ranges(left: &[Range<usize>], right: &[Range<usize>]) -> Vec<Range<usize>> {
    let mut result = Vec::new();
    let (mut l, mut r) = (0, 0);
    while l < left.len() && r < right.len() {
        let start = left[l].start.max(right[r].start);
        let end = left[l].end.min(right[r].end);
        if start < end {
            result.push(start..end);
        }
        if left[l].end <= right[r].end {
            l += 1;
        } else {
            r += 1;
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use databend_common_expression::FromData;
    use databend_common_expression::type_check::check_function;
    use databend_common_expression::types::Int64Type;
    use databend_common_expression::types::NumberDataType;

    use super::*;

    fn column(name: &str, data_type: DataType) -> Expr<String> {
        Expr::ColumnRef(ColumnRef {
            span: None,
            id: name.to_string(),
            data_type,
            display_name: name.to_string(),
        })
    }

    fn constant(scalar: Scalar, data_type: DataType) -> Expr<String> {
        Expr::Constant(Constant {
            span: None,
            scalar,
            data_type,
        })
    }

    fn call(name: &str, args: Vec<Expr<String>>) -> Expr<String> {
        check_function(None, name, &[], &args, &BUILTIN_FUNCTIONS).unwrap()
    }

    fn tuple(values: Vec<Scalar>) -> Scalar {
        Scalar::Tuple(values)
    }

    #[test]
    fn test_projects_timestamp_range_through_to_yyyymmdd() {
        const DAY: i64 = 86_400_000_000;
        let start_time = column("start_time", DataType::Timestamp);
        let key = call("to_yyyymmdd", vec![start_time.clone()]);
        let filter = call("and_filters", vec![
            call("gte", vec![
                start_time.clone(),
                constant(Scalar::Timestamp(0), DataType::Timestamp),
            ]),
            call("lt", vec![
                start_time,
                constant(Scalar::Timestamp(DAY), DataType::Timestamp),
            ]),
        ]);
        let evaluator =
            GranulePredicateEvaluator::try_create(FunctionContext::default(), vec![key], filter)
                .unwrap();

        let mins = vec![
            tuple(vec![Scalar::Number(19700101_u32.into())]),
            tuple(vec![Scalar::Number(19700102_u32.into())]),
            tuple(vec![Scalar::Number(19700103_u32.into())]),
        ];
        let max = tuple(vec![Scalar::Number(19700104_u32.into())]);
        assert_eq!(evaluator.apply(&mins, &max).unwrap(), vec![0..1]);
    }

    #[test]
    fn test_projects_nullable_timestamp_range_through_to_yyyymmdd() {
        const DAY: i64 = 86_400_000_000;
        let timestamp_type = DataType::Timestamp.wrap_nullable();
        let start_time = column("start_time", timestamp_type.clone());
        let key = call("to_yyyymmdd", vec![start_time.clone()]);
        let filter = call("and_filters", vec![
            call("gte", vec![
                start_time.clone(),
                constant(Scalar::Timestamp(0), DataType::Timestamp),
            ]),
            call("lt", vec![
                start_time,
                constant(Scalar::Timestamp(DAY), DataType::Timestamp),
            ]),
        ]);
        let evaluator =
            GranulePredicateEvaluator::try_create(FunctionContext::default(), vec![key], filter)
                .unwrap();

        let mins = vec![
            tuple(vec![Scalar::Number(19700101_u32.into())]),
            tuple(vec![Scalar::Number(19700102_u32.into())]),
            tuple(vec![Scalar::Number(19700103_u32.into())]),
        ];
        let max = tuple(vec![Scalar::Number(19700104_u32.into())]);
        assert_eq!(evaluator.apply(&mins, &max).unwrap(), vec![0..1]);
    }

    #[test]
    fn test_projects_trace_id_equality_through_substring_prefix() {
        let trace_id = column("trace_id", DataType::String);
        let key = call("substr", vec![
            trace_id.clone(),
            constant(
                Scalar::Number(1_i64.into()),
                DataType::Number(NumberDataType::Int64),
            ),
            constant(
                Scalar::Number(40_u64.into()),
                DataType::Number(NumberDataType::UInt64),
            ),
        ]);
        let wanted = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-tail";
        let filter = call("eq", vec![
            trace_id,
            constant(Scalar::String(wanted.to_string()), DataType::String),
        ]);
        let evaluator =
            GranulePredicateEvaluator::try_create(FunctionContext::default(), vec![key], filter)
                .unwrap();

        let mins = vec![
            tuple(vec![Scalar::String(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            )]),
            tuple(vec![Scalar::String(
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string(),
            )]),
            tuple(vec![Scalar::String(
                "cccccccccccccccccccccccccccccccccccccccc".to_string(),
            )]),
        ];
        let max = tuple(vec![Scalar::String(
            "dddddddddddddddddddddddddddddddddddddddd".to_string(),
        )]);
        assert_eq!(evaluator.apply(&mins, &max).unwrap(), vec![0..2]);
    }

    #[test]
    fn test_composite_suffix_prunes_only_when_leading_key_is_equal() {
        let start_time = column("start_time", DataType::Timestamp);
        let trace_id = column("trace_id", DataType::String);
        let day_key = call("to_yyyymmdd", vec![start_time]);
        let prefix_key = call("substr", vec![
            trace_id.clone(),
            constant(
                Scalar::Number(1_i64.into()),
                DataType::Number(NumberDataType::Int64),
            ),
            constant(
                Scalar::Number(40_u64.into()),
                DataType::Number(NumberDataType::UInt64),
            ),
        ]);
        let filter = call("eq", vec![
            trace_id,
            constant(
                Scalar::String("cccccccccccccccccccccccccccccccccccccccc-tail".to_string()),
                DataType::String,
            ),
        ]);
        let evaluator = GranulePredicateEvaluator::try_create(
            FunctionContext::default(),
            vec![day_key, prefix_key],
            filter,
        )
        .unwrap();

        let lower = tuple(vec![
            Scalar::Number(20240101_u32.into()),
            Scalar::String("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string()),
        ]);
        let equal_leading_upper = tuple(vec![
            Scalar::Number(20240101_u32.into()),
            Scalar::String("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string()),
        ]);
        assert!(
            !evaluator
                .eval_single_granule(&lower, &equal_leading_upper)
                .unwrap()
        );

        let different_leading_upper = tuple(vec![
            Scalar::Number(20240102_u32.into()),
            Scalar::String("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string()),
        ]);
        assert!(
            evaluator
                .eval_single_granule(&lower, &different_leading_upper)
                .unwrap()
        );
    }

    #[test]
    fn test_non_prefix_substring_fails_open() {
        let trace_id = column("trace_id", DataType::String);
        let key = call("substr", vec![
            trace_id.clone(),
            constant(
                Scalar::Number(2_i64.into()),
                DataType::Number(NumberDataType::Int64),
            ),
            constant(
                Scalar::Number(40_u64.into()),
                DataType::Number(NumberDataType::UInt64),
            ),
        ]);
        let filter = call("eq", vec![
            trace_id,
            constant(Scalar::String("bbbb-tail".to_string()), DataType::String),
        ]);
        let evaluator =
            GranulePredicateEvaluator::try_create(FunctionContext::default(), vec![key], filter)
                .unwrap();

        let mins = vec![
            tuple(vec![Scalar::String("aaaa".to_string())]),
            tuple(vec![Scalar::String("bbbb".to_string())]),
            tuple(vec![Scalar::String("cccc".to_string())]),
        ];
        let max = tuple(vec![Scalar::String("dddd".to_string())]);
        assert_eq!(evaluator.apply(&mins, &max).unwrap(), vec![0..3]);
    }

    #[test]
    fn test_projects_date_range_through_start_of_month() {
        let event_date = column("event_date", DataType::Date);
        let key = call("to_start_of_month", vec![event_date.clone()]);
        let filter = call("and_filters", vec![
            call("gte", vec![
                event_date.clone(),
                constant(Scalar::Date(31), DataType::Date),
            ]),
            call("lt", vec![
                event_date,
                constant(Scalar::Date(59), DataType::Date),
            ]),
        ]);
        let evaluator =
            GranulePredicateEvaluator::try_create(FunctionContext::default(), vec![key], filter)
                .unwrap();

        let mins = vec![
            tuple(vec![Scalar::Date(0)]),
            tuple(vec![Scalar::Date(31)]),
            tuple(vec![Scalar::Date(59)]),
        ];
        let max = tuple(vec![Scalar::Date(90)]);
        // Sparse mins describe the preceding granule with the next min as a conservative inclusive
        // upper bound, so the granule before the exact month boundary is retained as well.
        assert_eq!(evaluator.apply(&mins, &max).unwrap(), vec![0..2]);
    }

    #[test]
    fn test_direct_key_expression_predicate() {
        let start_time = column("start_time", DataType::Timestamp);
        let key = call("to_yyyymmdd", vec![start_time]);
        let filter = call("eq", vec![
            key.clone(),
            constant(
                Scalar::Number(20250102_u32.into()),
                DataType::Number(NumberDataType::UInt32),
            ),
        ]);
        let evaluator =
            GranulePredicateEvaluator::try_create(FunctionContext::default(), vec![key], filter)
                .unwrap();

        let mins = vec![
            tuple(vec![Scalar::Number(20250101_u32.into())]),
            tuple(vec![Scalar::Number(20250102_u32.into())]),
            tuple(vec![Scalar::Number(20250103_u32.into())]),
        ];
        let max = tuple(vec![Scalar::Number(20250104_u32.into())]);
        assert_eq!(evaluator.apply(&mins, &max).unwrap(), vec![0..2]);
    }

    #[test]
    fn test_implicit_string_literal_prunes_integer_key() {
        let key_id = column(
            "key_id",
            DataType::Number(NumberDataType::Int32).wrap_nullable(),
        );
        let literal = constant(Scalar::String("3167".to_string()), DataType::String);
        let filters = [
            call("is_true", vec![call("eq", vec![
                key_id.clone(),
                literal.clone(),
            ])]),
            call("is_true", vec![call("eq", vec![literal, key_id.clone()])]),
        ];

        for filter in filters {
            let evaluator = GranulePredicateEvaluator::try_create(
                FunctionContext::default(),
                vec![key_id.clone()],
                filter,
            )
            .unwrap();

            // The implicit String literal inside `is_true` is normalized to the
            // integer key type, so exact point pruning remains available.
            let mut conjuncts = Vec::new();
            collect_conjuncts(&evaluator.filter, &mut conjuncts);
            let [Expr::FunctionCall(call)] = conjuncts.as_slice() else {
                panic!(
                    "expected one equality after cast elimination, got {:?}",
                    evaluator.filter
                );
            };
            let constraint = RangeConstraint::try_from_function_call(call).unwrap();
            assert_eq!(constraint.constant, Scalar::Number(3167_i32.into()));

            let mins = vec![
                tuple(vec![Scalar::Number(1000_i32.into())]),
                tuple(vec![Scalar::Number(3000_i32.into())]),
                tuple(vec![Scalar::Number(4000_i32.into())]),
            ];
            let max = tuple(vec![Scalar::Number(5000_i32.into())]);
            assert_eq!(evaluator.apply(&mins, &max).unwrap(), vec![1..2]);
        }
    }

    #[test]
    fn test_invalid_string_literal_fails_open_for_integer_key() {
        let key_id = column(
            "key_id",
            DataType::Number(NumberDataType::Int32).wrap_nullable(),
        );

        for literal in ["not-an-int", "2147483648"] {
            let filter = call("eq", vec![
                key_id.clone(),
                constant(Scalar::String(literal.to_string()), DataType::String),
            ]);
            let evaluator = GranulePredicateEvaluator::try_create(
                FunctionContext::default(),
                vec![key_id.clone()],
                filter,
            )
            .unwrap();

            let mins = vec![
                tuple(vec![Scalar::Number(1000_i32.into())]),
                tuple(vec![Scalar::Number(3000_i32.into())]),
                tuple(vec![Scalar::Number(4000_i32.into())]),
            ];
            let max = tuple(vec![Scalar::Number(5000_i32.into())]);
            assert_eq!(evaluator.apply(&mins, &max).unwrap(), vec![0..3]);
        }
    }

    fn int64_array(values: Vec<i64>) -> Expr<String> {
        constant(
            Scalar::Array(Int64Type::from_data(values)),
            DataType::Array(Box::new(DataType::Number(NumberDataType::Int64))),
        )
    }

    fn int64_point(value: i64) -> Expr<String> {
        constant(
            Scalar::Number(value.into()),
            DataType::Number(NumberDataType::Int64),
        )
    }

    fn int64_tuple(values: Vec<i64>) -> Scalar {
        tuple(
            values
                .into_iter()
                .map(|v| Scalar::Number(v.into()))
                .collect(),
        )
    }

    #[test]
    fn test_in_list_contains_prunes_interior_granules() {
        let account = column("account", DataType::Number(NumberDataType::Int64));
        let filter = call("contains", vec![
            int64_array(vec![150, 460]),
            account.clone(),
        ]);
        let evaluator = GranulePredicateEvaluator::try_create(
            FunctionContext::default(),
            vec![account],
            filter,
        )
        .unwrap();

        let mins = vec![
            int64_tuple(vec![0]),
            int64_tuple(vec![100]),
            int64_tuple(vec![200]),
            int64_tuple(vec![300]),
            int64_tuple(vec![400]),
        ];
        let max = int64_tuple(vec![500]);
        assert_eq!(evaluator.apply(&mins, &max).unwrap(), vec![1..2, 4..5]);
    }

    #[test]
    fn test_in_list_respects_leading_key_prefix_rule() {
        let site = column("site", DataType::Number(NumberDataType::Int64));
        let account = column("account", DataType::Number(NumberDataType::Int64));
        let filter = call("and", vec![
            call("eq", vec![site.clone(), int64_point(5)]),
            call("contains", vec![
                int64_array(vec![150, 999]),
                account.clone(),
            ]),
        ]);
        let evaluator = GranulePredicateEvaluator::try_create(
            FunctionContext::default(),
            vec![site, account],
            filter,
        )
        .unwrap();

        let mins = vec![
            int64_tuple(vec![5, 0]),
            int64_tuple(vec![5, 100]),
            int64_tuple(vec![5, 200]),
            int64_tuple(vec![5, 300]),
            int64_tuple(vec![6, 0]),
        ];
        let max = int64_tuple(vec![6, 50]);
        // Granule 2 is a pruned interior hole; granule 3 spans the leading-key
        // boundary and is conservatively kept.
        assert_eq!(evaluator.apply(&mins, &max).unwrap(), vec![1..2, 3..4]);
    }

    #[test]
    fn test_or_of_equalities_prunes_interior_granules() {
        let account = column("account", DataType::Number(NumberDataType::Int64));
        let filter = call("or", vec![
            call("eq", vec![account.clone(), int64_point(100)]),
            call("eq", vec![account.clone(), int64_point(400)]),
        ]);
        let evaluator = GranulePredicateEvaluator::try_create(
            FunctionContext::default(),
            vec![account],
            filter,
        )
        .unwrap();

        let mins = vec![
            int64_tuple(vec![0]),
            int64_tuple(vec![100]),
            int64_tuple(vec![200]),
            int64_tuple(vec![300]),
            int64_tuple(vec![400]),
        ];
        let max = int64_tuple(vec![500]);
        assert_eq!(evaluator.apply(&mins, &max).unwrap(), vec![0..2, 3..5]);
    }

    #[test]
    fn test_conflicting_point_conjuncts_intersect() {
        let account = column("account", DataType::Number(NumberDataType::Int64));
        let filter = call("and", vec![
            call("eq", vec![account.clone(), int64_point(150)]),
            call("contains", vec![
                int64_array(vec![150, 460]),
                account.clone(),
            ]),
        ]);
        let evaluator = GranulePredicateEvaluator::try_create(
            FunctionContext::default(),
            vec![account],
            filter,
        )
        .unwrap();

        let mins = vec![
            int64_tuple(vec![0]),
            int64_tuple(vec![100]),
            int64_tuple(vec![200]),
            int64_tuple(vec![300]),
            int64_tuple(vec![400]),
        ];
        let max = int64_tuple(vec![500]);
        assert_eq!(evaluator.apply(&mins, &max).unwrap(), vec![1..2]);
    }

    #[test]
    fn test_cast_probe_falls_back_to_envelope() {
        // The cast around the probe column blocks point extraction, so the
        // interior hole is kept.
        let account = column("account", DataType::Number(NumberDataType::Int32));
        let filter = call("contains", vec![
            int64_array(vec![150, 350]),
            account.clone(),
        ]);
        let evaluator = GranulePredicateEvaluator::try_create(
            FunctionContext::default(),
            vec![account],
            filter,
        )
        .unwrap();
        assert!(evaluator.point_constraints.iter().all(Option::is_none));

        let mins = vec![
            tuple(vec![Scalar::Number(0_i32.into())]),
            tuple(vec![Scalar::Number(100_i32.into())]),
            tuple(vec![Scalar::Number(200_i32.into())]),
            tuple(vec![Scalar::Number(300_i32.into())]),
        ];
        let max = tuple(vec![Scalar::Number(400_i32.into())]);
        assert_eq!(evaluator.apply(&mins, &max).unwrap(), vec![1..4]);
    }

    #[test]
    fn test_points_hit_interval_guards_scalar_kinds() {
        let points = vec![Scalar::Number(150_i64.into())];
        assert!(points_hit_interval(
            &points,
            &Scalar::Number(100_i64.into()),
            &Scalar::Number(200_i64.into()),
        ));
        assert!(!points_hit_interval(
            &points,
            &Scalar::Number(200_i64.into()),
            &Scalar::Number(300_i64.into()),
        ));
        // Mismatched numeric kinds: conservatively keep.
        assert!(points_hit_interval(
            &points,
            &Scalar::Number(200_u64.into()),
            &Scalar::Number(300_u64.into()),
        ));
        assert!(points_hit_interval(
            &points,
            &Scalar::Null,
            &Scalar::Number(300_i64.into()),
        ));
        assert!(!points_hit_interval(
            &[],
            &Scalar::Number(100_i64.into()),
            &Scalar::Number(200_i64.into()),
        ));
    }

    #[test]
    fn test_heterogeneous_constraint_constant_fails_open() {
        // Regression for the upstream-main merge: `Domain::from_datum` was
        // replaced by `ColumnStatistics::new`, whose same-type assert panicked
        // ("must have same type, min: '3167', max: 2147483647") when an
        // implicit cast left a String literal against an Int32 column, e.g.
        // `int_col = '3167'`. Such constraints must fail open like the old
        // `from_datum` path did.
        let heterogeneous = RangeConstraint {
            column_id: "key_id".to_string(),
            data_type: DataType::Number(NumberDataType::Int32).wrap_nullable(),
            operator: "eq".to_string(),
            constant: Scalar::String("3167".to_string()),
            is_flipped: false,
        };
        assert!(matches!(
            constraints_to_domain(std::slice::from_ref(&heterogeneous)),
            ConstraintDomain::Unknown
        ));

        // Matching constants keep producing a pruning domain.
        let homogeneous = RangeConstraint {
            column_id: "key_id".to_string(),
            data_type: DataType::Number(NumberDataType::Int32).wrap_nullable(),
            operator: "eq".to_string(),
            constant: Scalar::Number(3167_i32.into()),
            is_flipped: false,
        };
        assert!(matches!(
            constraints_to_domain(std::slice::from_ref(&homogeneous)),
            ConstraintDomain::Domain(_)
        ));

        // So does the common `string_col = 'literal'` shape.
        let string_eq = RangeConstraint {
            column_id: "key_id".to_string(),
            data_type: DataType::String,
            operator: "eq".to_string(),
            constant: Scalar::String("118".to_string()),
            is_flipped: false,
        };
        assert!(matches!(
            constraints_to_domain(std::slice::from_ref(&string_eq)),
            ConstraintDomain::Domain(_)
        ));
    }

    #[test]
    fn test_intersect_ranges() {
        let left = vec![0..3, 5..9];
        let right = vec![1..6, 7..8, 10..12];
        assert_eq!(intersect_ranges(&left, &right), vec![1..3, 5..6, 7..8]);
    }
}
