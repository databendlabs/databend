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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Constant;
use databend_common_expression::ConstantFolder;
use databend_common_expression::Domain;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::StatEvaluator;
use databend_common_expression::stat_distribution::ArgStat;
use databend_common_expression::stat_distribution::NdvEstimate;
use databend_common_expression::stat_distribution::StatCardinality;
use databend_common_expression::stat_distribution::StatCount;
use databend_common_expression::types::DataType;
use databend_common_expression::types::nullable::NullableDomain;
use databend_common_functions::BUILTIN_FUNCTIONS;

use super::constraint::ConstraintContext;
use super::constraint::ValueConstraint;
use super::constraint::clear_for_empty_result;
use crate::ColumnBinding;
use crate::ColumnSet;
use crate::Symbol;
use crate::optimizer::ir::ColumnStat;
use crate::optimizer::ir::ColumnStatSet;
use crate::optimizer::ir::CountMinSketchSet;
use crate::optimizer::ir::TopNSet;
use crate::plans::ComparisonOp;
use crate::plans::FunctionCall;
use crate::plans::ScalarExpr;

/// A default selectivity factor for a predicate
/// that we cannot estimate the selectivity for it.
/// This factor comes from the paper
/// "Access Path Selection in a Relational Database Management System"
pub const DEFAULT_SELECTIVITY: f64 = 1f64 / 5f64;
pub const UNKNOWN_COL_STATS_FILTER_SEL_LOWER_BOUND: f64 = 0.5_f64;
pub const MAX_SELECTIVITY: f64 = 1f64;
const HISTOGRAM_ROW_COUNT_TOLERANCE: f64 = 1e-9;

/// Some constants for like predicate selectivity estimation.
const FIXED_CHAR_SEL: f64 = 0.5;
const ANY_CHAR_SEL: f64 = 0.9; // not 1, since it won't match end-of-string
const FULL_WILDCARD_SEL: f64 = 2.0;

pub struct SelectivityEstimator {
    cardinality: StatCardinality,
    column_stats: ColumnStatSet,
    top_n: TopNSet,
    count_min_sketch: CountMinSketchSet,
    overrides: ColumnStatSet,
}

impl SelectivityEstimator {
    pub fn new(input_stat: ColumnStatSet, cardinality: StatCardinality) -> Self {
        Self {
            cardinality,
            column_stats: input_stat,
            top_n: TopNSet::new(),
            count_min_sketch: CountMinSketchSet::new(),
            overrides: ColumnStatSet::new(),
        }
    }

    pub fn with_top_n(mut self, top_n: TopNSet) -> Self {
        self.top_n = top_n;
        self
    }

    pub fn with_count_min_sketch(mut self, count_min_sketch: CountMinSketchSet) -> Self {
        self.count_min_sketch = count_min_sketch;
        self
    }

    fn merged_column_stats(&self) -> ColumnStatSet {
        let mut merged = self.column_stats.clone();
        merged.extend(self.overrides.clone());
        merged
    }

    pub fn column_stats(&self) -> ColumnStatSet {
        self.merged_column_stats()
    }

    pub fn into_column_stats(self) -> ColumnStatSet {
        if self.overrides.is_empty() {
            return self.column_stats;
        }
        let mut merged = self.column_stats;
        merged.extend(self.overrides);
        merged
    }

    pub fn apply(&mut self, predicates: &[ScalarExpr]) -> Result<f64> {
        if self.cardinality == StatCardinality::Exact(0) {
            self.clear_column_stats_for_empty_result();
            return Ok(0.0);
        }

        let scalar_expr = match predicates {
            [pred] => pred.clone(),
            predicates => ScalarExpr::FunctionCall(FunctionCall {
                span: None,
                func_name: "and_filters".to_string(),
                params: vec![],
                arguments: predicates.to_vec(),
            }),
        };
        let expr = scalar_expr.as_expr()?;
        let input_domains = self.build_input_domains(&expr)?;
        let (expr, output_domain) = ConstantFolder::fold_with_domain(
            &expr,
            &input_domains,
            &FunctionContext::default(),
            &BUILTIN_FUNCTIONS,
        );

        // ConstantFolder owns expression/domain reasoning: boolean shortcuts and
        // contradictions visible from input column domains. It can still leave
        // SQL-truthy constants such as `WHERE 1`, so handle constant predicates
        // here before falling through to estimation rules.
        if let Expr::Constant(constant) = &expr {
            return match constant_filter_truthiness(&constant.scalar) {
                Some(true) => Ok(self.cardinality.value()),
                Some(false) => {
                    self.clear_column_stats_for_empty_result();
                    Ok(0.0)
                }
                None => {
                    // Non-boolean constants can survive folding in legacy SQL
                    // truthiness paths. Leave the exact interpretation to
                    // execution and use the normal unknown-filter fallback.
                    Ok(self
                        .apply_selectivity_to_column_stats(Selectivity::Unknown, &ColumnSet::new()))
                }
            };
        }
        if output_domain.as_ref().is_some_and(|domain| match domain {
            Domain::Boolean(domain)
            | Domain::Nullable(NullableDomain {
                value: Some(box Domain::Boolean(domain)),
                ..
            }) => !domain.has_true,
            _ => false,
        }) {
            self.clear_column_stats_for_empty_result();
            return Ok(0.0);
        }

        let mut visitor = SelectivityVisitor {
            cardinality: self.cardinality,
            selectivity: Selectivity::Unknown,
            constraint_context: ConstraintContext::And,
            column_stats: &self.column_stats,
            top_n: &self.top_n,
            count_min_sketch: &self.count_min_sketch,
            constraints: ValueConstraintState::default(),
        };
        visitor.visit_expr(&expr)?;

        let SelectivityVisitor {
            selectivity,
            constraints,
            ..
        } = visitor;
        let not_null_columns = constraints.not_null_columns();
        self.overrides = constraints.apply_to_column_stats(&self.column_stats)?;
        let output_cardinality =
            self.apply_selectivity_to_column_stats(selectivity, &not_null_columns);

        Ok(output_cardinality)
    }

    fn build_input_domains(
        &self,
        expr: &Expr<ColumnBinding>,
    ) -> Result<HashMap<ColumnBinding, Domain>> {
        expr.column_refs()
            .into_iter()
            .map(|(binding, data_type)| {
                let Some(column_stat) = self.column_stats.get(&binding.index) else {
                    return Ok((binding, Domain::full(&data_type)));
                };

                if matches!(data_type, DataType::Nullable(_))
                    && let StatCardinality::Exact(cardinality) = self.cardinality
                    && column_stat.null_count == StatCount::Exact(cardinality)
                {
                    return Ok((
                        binding,
                        Domain::Nullable(NullableDomain {
                            has_null: true,
                            value: None,
                        }),
                    ));
                }

                if !matches!(
                    data_type.remove_nullable(),
                    DataType::Boolean
                        | DataType::String
                        | DataType::Number(_)
                        | DataType::Decimal(_)
                        | DataType::Date
                        | DataType::Timestamp
                ) {
                    return Ok((binding, Domain::full(&data_type)));
                }

                match Domain::from_datum(
                    &data_type,
                    column_stat.min.clone(),
                    column_stat.max.clone(),
                    column_stat.null_count.upper() > 0.0,
                ) {
                    Ok(domain) => Ok((binding, domain)),
                    Err(msg) => {
                        log::warn!(
                            data_type:?,
                            column_stat:?,
                            msg;
                            "Failed to build input domain"
                        );
                        Ok((binding, Domain::full(&data_type)))
                    }
                }
            })
            .collect()
    }

    fn clear_column_stats_for_empty_result(&mut self) {
        for (index, column_stat) in &self.column_stats {
            let mut column_stat = column_stat.clone();
            clear_for_empty_result(&mut column_stat);
            self.overrides.insert(*index, column_stat);
        }
    }

    // Apply the predicate estimate to column stats and return output rows.
    fn apply_selectivity_to_column_stats(
        &mut self,
        selectivity: Selectivity,
        not_null_columns: &ColumnSet,
    ) -> f64 {
        let input_cardinality = self.cardinality.value();
        let selectivity = match selectivity {
            Selectivity::Unknown => DEFAULT_SELECTIVITY,
            Selectivity::LowerBound => UNKNOWN_COL_STATS_FILTER_SEL_LOWER_BOUND,
            Selectivity::Zero => {
                self.clear_column_stats_for_empty_result();
                return 0.0;
            }
            Selectivity::All => return input_cardinality,
            Selectivity::N(n) => n,
        };

        if selectivity == MAX_SELECTIVITY {
            return input_cardinality;
        }
        if selectivity == 0.0 {
            return 0.0;
        }

        let estimated_cardinality = input_cardinality * selectivity;
        let mut final_cardinality = estimated_cardinality;
        for (index, column_stat) in &self.column_stats {
            if let Some(override_stat) = self.overrides.get_mut(index) {
                if not_null_columns.contains(index) {
                    override_stat.null_count = StatCount::exact(0);
                    override_stat.ndv = override_stat.ndv.reduce(estimated_cardinality);
                    if let Some(aligned_cardinality) =
                        align_histogram_with_cardinality(override_stat, estimated_cardinality)
                    {
                        final_cardinality = final_cardinality.min(aligned_cardinality);
                    }
                    continue;
                }
                // Value constraints have already been materialized into
                // `override_stat`. If a constrained histogram is still
                // available, use it as the row-mass boundary for the output;
                // otherwise only apply the remaining global selectivity to the
                // coarse stats.
                if override_stat.histogram.is_none() {
                    if override_stat.min != override_stat.max {
                        let input_non_null =
                            non_null_values(input_cardinality, column_stat.null_count);
                        override_stat.ndv = override_stat.ndv.min(
                            column_stat
                                .ndv
                                .reduce_by_selectivity(input_non_null, selectivity),
                        );
                        override_stat.null_count =
                            override_stat.null_count.reduce_by_selectivity(selectivity);
                    }
                } else if let Some(aligned_cardinality) =
                    align_histogram_with_cardinality(override_stat, estimated_cardinality)
                {
                    final_cardinality = final_cardinality.min(aligned_cardinality);
                }
                continue;
            }
            let mut column_stat = column_stat.clone();
            let input_non_null = non_null_values(input_cardinality, column_stat.null_count);
            column_stat.null_count = column_stat.null_count.reduce_by_selectivity(selectivity);
            if let Some(histogram) = &mut column_stat.histogram {
                let ndv_upper = column_stat.ndv.upper;
                histogram.scale_counts(selectivity);
                column_stat.ndv = scaled_histogram_ndv(ndv_upper, histogram.ndv());
                if column_stat.ndv.expected.is_some_and(|ndv| ndv <= 2.0) {
                    column_stat.histogram = None;
                }
            } else {
                column_stat.ndv = column_stat
                    .ndv
                    .reduce_by_selectivity(input_non_null, selectivity);
            }

            self.overrides.insert(*index, column_stat);
        }

        final_cardinality
    }
}

fn scaled_histogram_ndv(original_upper: f64, histogram_ndv: NdvEstimate) -> NdvEstimate {
    let upper = histogram_ndv.upper.min(original_upper);
    match histogram_ndv.expected {
        Some(expected) => NdvEstimate::new(expected.min(upper), upper),
        None => NdvEstimate::upper_bound(upper),
    }
}

fn non_null_values(cardinality: f64, null_count: StatCount) -> f64 {
    (cardinality - null_count.expected())
        .max(0.0)
        .min(cardinality)
}

// Align a histogram before it is consumed as a row distribution.
//
// Callers pass total cardinality. Histograms only describe non-null values, so
// this function applies `null_count` internally instead of letting each caller
// decide how to derive the histogram row target.
//
// Only oversized histograms are scaled down. Scaling is an unknown-value filter:
// it aligns row mass but does not prove which values survived, so NDV is derived
// from histogram row scaling while preserving the previous upper cap.
fn align_histogram_with_cardinality(column_stat: &mut ColumnStat, cardinality: f64) -> Option<f64> {
    if !cardinality.is_finite() || cardinality < 0.0 {
        return None;
    }
    let null_count = column_stat.null_count.expected().min(cardinality).max(0.0);
    let target_num_values = cardinality - null_count;

    let current_num_values = column_stat.histogram.as_ref()?.num_values();
    if current_num_values <= 0.0 {
        return None;
    }
    if current_num_values <= target_num_values * (1.0 + HISTOGRAM_ROW_COUNT_TOLERANCE) {
        return Some((current_num_values + null_count).min(cardinality));
    }

    let factor = target_num_values / current_num_values;
    let ndv_upper = column_stat.ndv.upper;
    if let Some(histogram) = &mut column_stat.histogram {
        histogram.scale_counts(factor);
        column_stat.ndv = scaled_histogram_ndv(ndv_upper, histogram.ndv());
    }
    Some(cardinality)
}

fn constant_filter_truthiness(scalar: &Scalar) -> Option<bool> {
    match scalar {
        Scalar::Null => Some(false),
        Scalar::Boolean(value) => Some(*value),
        _ => scalar
            .clone()
            .to_datum()
            .and_then(|datum| datum.as_double().ok())
            .map(|value| value != 0.0),
    }
}

// SelectivityVisitor consumes the expression after ConstantFolder has applied
// expression/domain reasoning. Deterministic predicate truth, boolean
// short-circuiting, and contradictions visible from input domains should already
// be represented as constants or expression domains here.
//
// The visitor estimates the remaining predicates and records ValueConstraints
// only as column-statistics propagation hints for AND-context predicates. Those
// constraints may refine bounds, null counts, NDV limits, and histograms, but
// they must not become a second deterministic solver for predicate truth or
// filter cardinality.
#[derive(Clone)]
struct SelectivityVisitor<'a> {
    cardinality: StatCardinality,
    selectivity: Selectivity,
    constraint_context: ConstraintContext,
    column_stats: &'a ColumnStatSet,
    top_n: &'a TopNSet,
    count_min_sketch: &'a CountMinSketchSet,
    constraints: ValueConstraintState,
}

#[derive(Clone, Default)]
struct ValueConstraintState {
    pending: HashMap<Symbol, Vec<ValueConstraint>>,
}

struct MaterializedColumnStats {
    column_stats: ColumnStatSet,
    cardinality: f64,
}

impl ValueConstraintState {
    fn not_null_columns(&self) -> ColumnSet {
        self.pending
            .iter()
            .filter_map(|(index, constraints)| {
                constraints
                    .iter()
                    .any(|constraint| matches!(constraint, ValueConstraint::NotNull))
                    .then_some(*index)
            })
            .collect()
    }

    fn apply_to_column_stat(&self, index: Symbol, column_stat: &ColumnStat) -> Result<ColumnStat> {
        let Some(constraints) = self.pending.get(&index) else {
            return Ok(column_stat.clone());
        };
        ValueConstraint::apply_all(column_stat, constraints)
    }

    fn add(
        &mut self,
        column_stats: &ColumnStatSet,
        index: Symbol,
        constraint: ValueConstraint,
    ) -> Result<()> {
        if !column_stats.contains_key(&index) {
            return Ok(());
        }
        let mut constraints = self.pending.get(&index).cloned().unwrap_or_default();
        constraints.push(constraint);
        self.pending.insert(index, constraints);
        Ok(())
    }

    fn apply_to_column_stats(&self, column_stats: &ColumnStatSet) -> Result<ColumnStatSet> {
        let mut constrained_stats = ColumnStatSet::new();
        for index in self.pending.keys() {
            let Some(column_stat) = column_stats.get(index) else {
                continue;
            };
            let column_stat = self.apply_to_column_stat(*index, column_stat)?;
            constrained_stats.insert(*index, column_stat);
        }
        Ok(constrained_stats)
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub enum Selectivity {
    #[default]
    Unknown,
    LowerBound,
    // A deterministic false predicate produced by constants, boolean logic, or
    // exact local rules. This is stronger than an estimated `N(0.0)`.
    Zero,
    // A deterministic true predicate produced by constants, boolean logic, or
    // exact local rules. This is stronger than an estimated `N(1.0)`.
    All,
    N(f64),
}

type ExprCall = databend_common_expression::FunctionCall<ColumnBinding>;

impl Selectivity {
    fn checked_estimate(value: f64) -> Result<Self> {
        if value.is_finite() && (0.0..=1.0).contains(&value) {
            return Ok(Selectivity::N(value));
        }

        let msg = format!("invalid selectivity estimate: {value:?}");
        log::warn!(msg; "Invalid selectivity estimate");
        Ok(Selectivity::Unknown)
    }
}

fn constrained_column_cardinality(
    input_stat: &ColumnStat,
    constrained_stat: &ColumnStat,
    input_cardinality: f64,
) -> Option<f64> {
    if constrained_stat.ndv.upper == 0.0 {
        return Some(0.0);
    }
    if input_cardinality <= 0.0 {
        return Some(0.0);
    }
    if let Some(histogram) = &constrained_stat.histogram {
        return Some(histogram.num_values() + constrained_stat.null_count.expected());
    }
    let input_ndv = input_stat.ndv.expected?;
    let constrained_ndv = constrained_stat.ndv.expected?;
    if input_ndv <= 0.0 {
        return None;
    }

    let input_non_null = non_null_values(input_cardinality, input_stat.null_count);
    Some(constrained_stat.null_count.expected() + input_non_null * (constrained_ndv / input_ndv))
}

impl SelectivityVisitor<'_> {
    fn materialize_column_stats(
        &self,
        expr: &Expr<ColumnBinding>,
    ) -> Result<Option<MaterializedColumnStats>> {
        let column_refs = expr.column_refs();
        if column_refs.is_empty() {
            return Ok(None);
        }

        let mut column_stats = ColumnStatSet::new();
        let mut cardinality = self.cardinality.value();
        for (binding, _) in column_refs {
            if column_stats.contains_key(&binding.index) {
                continue;
            }
            let Some(column_stat) = self.column_stats.get(&binding.index) else {
                return Ok(None);
            };
            let mut input_stat = column_stat.clone();
            align_histogram_with_cardinality(&mut input_stat, self.cardinality.value());
            let constrained_stat = self
                .constraints
                .apply_to_column_stat(binding.index, &input_stat)?;
            if let Some(constrained_cardinality) = constrained_column_cardinality(
                &input_stat,
                &constrained_stat,
                self.cardinality.value(),
            ) {
                cardinality = cardinality.min(constrained_cardinality);
            }
            column_stats.insert(binding.index, constrained_stat);
        }

        Ok(Some(MaterializedColumnStats {
            column_stats,
            cardinality,
        }))
    }

    fn build_input_stats<'s>(
        &'s self,
        expr: &Expr<ColumnBinding>,
        column_stats: &'s ColumnStatSet,
    ) -> Result<Option<HashMap<ColumnBinding, ArgStat<'s>>>> {
        let column_refs = expr.column_refs();
        if column_refs.is_empty() {
            return Ok(None);
        }

        let mut input_stats = HashMap::with_capacity(column_refs.len());
        for (binding, data_type) in column_refs {
            let Some(column_stat) = column_stats.get(&binding.index) else {
                return Ok(None);
            };

            match data_type.remove_nullable() {
                DataType::Boolean
                | DataType::Binary
                | DataType::String
                | DataType::Number(_)
                | DataType::Decimal(_)
                | DataType::Date
                | DataType::Timestamp => (),
                _ => return Ok(None),
            }

            match column_stat.to_arg_stat(&data_type) {
                Ok(arg_stat) => {
                    input_stats.insert(binding, arg_stat);
                }
                Err(msg) => {
                    return if cfg!(debug_assertions) {
                        Err(ErrorCode::Internal(format!(
                            "Failed to_arg_stat {msg} {:?} {:?}",
                            column_stat, data_type
                        )))
                    } else {
                        log::warn!(data_type:?, msg; "to_arg_stat failed");
                        Ok(None)
                    };
                }
            }
        }

        Ok(Some(input_stats))
    }

    fn derive_materialized_constraint_selectivity(
        &self,
        expr: &Expr<ColumnBinding>,
    ) -> Result<Selectivity> {
        let Some(materialized) = self.materialize_column_stats(expr)? else {
            return Ok(Selectivity::Unknown);
        };
        Selectivity::checked_estimate(materialized.cardinality / self.cardinality.value())
    }

    fn compute_comparison(&mut self, op: ComparisonOp, func: &ExprCall) -> Result<Selectivity> {
        let left = &func.args[0];
        let right = &func.args[1];
        match (left, right) {
            (Expr::ColumnRef(column_ref), Expr::Constant(constant))
            | (Expr::Constant(constant), Expr::ColumnRef(column_ref)) => {
                let column_index = column_ref.id.index;
                if !self.column_stats.contains_key(&column_index) {
                    // The column is derived column, give a small selectivity currently.
                    // Need to improve it later.
                    // Another case: column is from system table, such as numbers. We shouldn't use numbers() table to test cardinality estimation.
                    return Ok(Selectivity::LowerBound);
                }
                let column_stat = &self.column_stats[&column_index];
                let op = if left.is_constant() { op.reverse() } else { op };

                let can_apply_constant_constraint = {
                    use DataType::*;
                    matches!(
                        (
                            column_ref.data_type.remove_nullable(),
                            constant.data_type.remove_nullable(),
                        ),
                        (Number(_), Number(_))
                            | (Boolean, Boolean)
                            | (String, String)
                            | (Binary, Binary)
                            | (Date, Date)
                            | (Timestamp, Timestamp)
                            | (Decimal(_), Decimal(_))
                    )
                };
                if !can_apply_constant_constraint {
                    return self.derive_function_selectivity(func);
                }
                let Some(const_datum) = constant.scalar.clone().to_datum() else {
                    return self.derive_function_selectivity(func);
                };

                let distorted_range = matches!(
                    op,
                    ComparisonOp::GT | ComparisonOp::GTE | ComparisonOp::LT | ComparisonOp::LTE
                ) && column_stat
                    .histogram
                    .as_ref()
                    .is_some_and(|histogram| histogram.is_range_distorted());
                if matches!(self.constraint_context, ConstraintContext::And) {
                    self.constraints.add(
                        self.column_stats,
                        column_index,
                        ValueConstraint::from_comparison(op, const_datum.clone()),
                    )?;
                    if let Some(selectivity) = self.derive_frequency_equality_selectivity(
                        column_index,
                        op,
                        &constant.scalar,
                    )? {
                        return Ok(selectivity);
                    }
                    if distorted_range {
                        return Ok(Selectivity::LowerBound);
                    }
                    return match self.derive_function_selectivity(func)? {
                        Selectivity::Unknown => self.derive_materialized_constraint_selectivity(
                            &Expr::FunctionCall(func.clone()),
                        ),
                        selectivity => Ok(selectivity),
                    };
                }
                return if distorted_range {
                    Ok(Selectivity::LowerBound)
                } else if let Some(selectivity) =
                    self.derive_frequency_equality_selectivity(column_index, op, &constant.scalar)?
                {
                    Ok(selectivity)
                } else {
                    self.derive_function_selectivity(func)
                };
            }
            _ => (),
        }

        self.derive_function_selectivity(func)
    }

    fn derive_frequency_equality_selectivity(
        &self,
        column_index: Symbol,
        op: ComparisonOp,
        scalar: &Scalar,
    ) -> Result<Option<Selectivity>> {
        let exact_top_n_hit = self
            .top_n
            .get(&column_index)
            .and_then(|top_n| top_n.get_entry(scalar))
            .is_some_and(|entry| entry.error == 0);
        if exact_top_n_hit
            && let Some(selectivity) =
                self.derive_top_n_equality_selectivity(column_index, op, scalar)?
        {
            return Ok(Some(selectivity));
        }

        if let Some(selectivity) =
            self.derive_count_min_sketch_equality_selectivity(column_index, op, scalar)?
        {
            return Ok(Some(selectivity));
        }

        self.derive_top_n_equality_selectivity(column_index, op, scalar)
    }

    fn derive_top_n_equality_selectivity(
        &self,
        column_index: Symbol,
        op: ComparisonOp,
        scalar: &Scalar,
    ) -> Result<Option<Selectivity>> {
        if !matches!(op, ComparisonOp::Equal | ComparisonOp::NotEqual) {
            return Ok(None);
        }
        let Some(top_n) = self.top_n.get(&column_index) else {
            return Ok(None);
        };
        let Some(entry) = top_n.get_entry(scalar) else {
            return Ok(None);
        };
        let cardinality = self.cardinality.value();
        if cardinality == 0.0 {
            return Ok(Some(Selectivity::N(0.0)));
        }
        let Some(column_stat) = self.column_stats.get(&column_index) else {
            return Ok(None);
        };
        let non_null_cardinality = non_null_values(cardinality, column_stat.null_count);
        let upper_count = (entry.count as f64).min(non_null_cardinality);
        let lower_count = (entry.count.saturating_sub(entry.error) as f64).min(upper_count);
        if entry.error > 0
            && let Some(ndv) = column_stat.ndv.expected
            && ndv > 0.0
            && lower_count <= non_null_cardinality / ndv
        {
            return Ok(None);
        }
        let matched = if matches!(op, ComparisonOp::NotEqual) {
            non_null_cardinality - lower_count
        } else {
            upper_count
        };
        Selectivity::checked_estimate(matched / cardinality).map(Some)
    }

    fn derive_count_min_sketch_equality_selectivity(
        &self,
        column_index: Symbol,
        op: ComparisonOp,
        scalar: &Scalar,
    ) -> Result<Option<Selectivity>> {
        if !matches!(op, ComparisonOp::Equal | ComparisonOp::NotEqual) {
            return Ok(None);
        }
        let Some(count_min_sketch) = self.count_min_sketch.get(&column_index) else {
            return Ok(None);
        };
        let Some(column_stat) = self.column_stats.get(&column_index) else {
            return Ok(None);
        };
        let cardinality = self.cardinality.value();
        if cardinality == 0.0 {
            return Ok(Some(Selectivity::N(0.0)));
        }
        let non_null_cardinality = non_null_values(cardinality, column_stat.null_count);
        if non_null_cardinality == 0.0 {
            return Ok(Some(Selectivity::N(0.0)));
        }
        let Some(ndv) = column_stat.ndv.expected else {
            return Ok(None);
        };
        if ndv <= 0.0 {
            return Ok(None);
        }
        let Some(estimated_count) = count_min_sketch.estimate(scalar) else {
            return Ok(None);
        };

        let upper_count = (estimated_count as f64).min(non_null_cardinality);
        let error_bound = count_min_sketch.error_bound(non_null_cardinality.ceil() as u64) as f64;
        let lower_count = (upper_count - error_bound).max(0.0);
        let average_count = non_null_cardinality / ndv;
        if lower_count <= average_count {
            return Ok(None);
        }

        let matched = if matches!(op, ComparisonOp::NotEqual) {
            non_null_cardinality - lower_count
        } else {
            upper_count
        };
        Selectivity::checked_estimate(matched / cardinality).map(Some)
    }

    fn derive_function_selectivity(&self, func: &ExprCall) -> Result<Selectivity> {
        let cardinality = match self.cardinality {
            StatCardinality::Estimate(0.0) => {
                return Ok(Selectivity::N(0.0));
            }
            cardinality => cardinality.value(),
        };
        let expr = Expr::FunctionCall(func.clone());
        let Some(materialized) = self.materialize_column_stats(&expr)? else {
            return Ok(Selectivity::Unknown);
        };
        let MaterializedColumnStats {
            mut column_stats,
            cardinality: materialized_cardinality,
        } = materialized;
        let stat_cardinality_value = column_stats
            .values()
            .map(|stat| stat.null_count.expected())
            .fold(materialized_cardinality, f64::max);
        let stat_cardinality = if stat_cardinality_value > cardinality {
            StatCardinality::estimate(stat_cardinality_value)
        } else if stat_cardinality_value == cardinality {
            self.cardinality
        } else {
            StatCardinality::estimate(stat_cardinality_value)
        };
        // Function statistics consume histograms as non-null distributions, so
        // normalize row mass at this boundary before exposing ArgStat.
        for stat in column_stats.values_mut() {
            align_histogram_with_cardinality(stat, stat_cardinality_value);
        }
        let Some(input_stats) = self.build_input_stats(&expr, &column_stats)? else {
            return Ok(Selectivity::Unknown);
        };

        let Some(stat) = StatEvaluator::run(
            &expr,
            &FunctionContext::default(),
            &BUILTIN_FUNCTIONS,
            stat_cardinality,
            &input_stats,
        )?
        else {
            return Ok(Selectivity::Unknown);
        };

        let stat = stat.as_ref();
        let Some(distr) = stat.boolean_distribution() else {
            return Ok(Selectivity::Unknown);
        };
        Selectivity::checked_estimate(distr.true_count.expected / cardinality)
    }

    // The method uses probability predication to compute like selectivity.
    // The core idea is from postgresql.
    fn compute_like(&mut self, func: &ExprCall) -> Result<Selectivity> {
        let Expr::Constant(Constant {
            scalar: Scalar::String(patt),
            ..
        }) = &func.args[1]
        else {
            return Ok(Selectivity::Unknown);
        };
        let mut sel = 1.0_f64;

        // Skip any leading %; it's already factored into initial sel
        let mut chars = patt.chars().peekable();
        if matches!(chars.peek(), Some(&'%') | Some(&'_')) {
            chars.next(); // consume the leading %
        }

        while let Some(c) = chars.next() {
            match c {
                '%' => sel *= FULL_WILDCARD_SEL,
                '_' => sel *= ANY_CHAR_SEL,
                '\\' => {
                    if chars.peek().is_some() {
                        chars.next();
                    }
                    sel *= FIXED_CHAR_SEL;
                }
                _ => sel *= FIXED_CHAR_SEL,
            }
        }

        // Could get sel > 1 if multiple wildcards
        if sel > 1.0 {
            sel = 1.0;
        }
        Ok(Selectivity::N(sel))
    }

    fn compute_is_not_null(&mut self, expr: &Expr<ColumnBinding>) -> Result<Selectivity> {
        let Expr::ColumnRef(column_ref) = expr else {
            return Ok(Selectivity::Unknown);
        };
        let Some(column_stat) = self.column_stats.get(&column_ref.id.index) else {
            return Ok(Selectivity::Unknown);
        };
        let column_stat = self
            .constraints
            .apply_to_column_stat(column_ref.id.index, column_stat)?;
        match self.cardinality {
            StatCardinality::Estimate(0.0) => Ok(Selectivity::N(0.0)),
            StatCardinality::Exact(cardinality) => {
                let cardinality = cardinality as f64;
                Selectivity::checked_estimate(
                    (cardinality - column_stat.null_count.expected()) / cardinality,
                )
            }
            StatCardinality::Estimate(cardinality) => Selectivity::checked_estimate(
                (cardinality - column_stat.null_count.expected()) / cardinality,
            ),
        }
    }

    fn spawn_child(&self, constraint_context: ConstraintContext) -> SelectivityVisitor<'_> {
        SelectivityVisitor {
            cardinality: self.cardinality,
            selectivity: Selectivity::Unknown,
            constraint_context,
            column_stats: self.column_stats,
            top_n: self.top_n,
            count_min_sketch: self.count_min_sketch,
            constraints: self.constraints.clone(),
        }
    }

    fn visit_expr(&mut self, expr: &Expr<ColumnBinding>) -> Result<()> {
        match expr {
            Expr::Constant(constant) => {
                // Top-level constants are consumed in SelectivityEstimator::apply.
                // Nested constants can still appear after domain folding, so keep
                // their boolean identity for logical composition.
                self.selectivity = constant_filter_truthiness(&constant.scalar)
                    .map(|value| {
                        if value {
                            Selectivity::All
                        } else {
                            Selectivity::Zero
                        }
                    })
                    .unwrap_or(Selectivity::Unknown);
                Ok(())
            }
            Expr::ColumnRef(_) => {
                self.selectivity = Selectivity::LowerBound;
                Ok(())
            }
            Expr::Cast(cast) => self.visit_expr(&cast.expr),
            Expr::FunctionCall(func) => self.visit_function_call(func),
            Expr::LambdaFunctionCall(_) => {
                self.selectivity = Selectivity::Unknown;
                Ok(())
            }
        }
    }

    fn visit_function_call(&mut self, func: &ExprCall) -> Result<()> {
        let func_name = func.function.signature.name.as_str();
        match func_name {
            "and_filters" => {
                // Logical composition here combines estimates for predicates that
                // survived constant folding. It should not duplicate expression-only
                // boolean simplification such as `false AND x`.
                let mut has_unknown = false;
                let mut has_lower_bound = false;
                let mut has_zero = false;
                let mut has_n = false;
                let mut acc = 1.0_f64;
                for arg in &func.args {
                    let mut sub_visitor = self.spawn_child(ConstraintContext::And);
                    sub_visitor.visit_expr(arg)?;
                    let SelectivityVisitor {
                        selectivity,
                        constraints,
                        ..
                    } = sub_visitor;
                    match selectivity {
                        Selectivity::Unknown => has_unknown = true,
                        Selectivity::LowerBound => has_lower_bound = true,
                        Selectivity::Zero => {
                            has_zero = true;
                            acc = 0.0;
                        }
                        Selectivity::All => {}
                        Selectivity::N(n) => {
                            has_n = true;
                            // Constraints are accumulated across AND children,
                            // but each child estimate is still measured against
                            // the original input rows. Multiplying estimates
                            // for predicates on different columns would assume
                            // those columns are independent; without that proof,
                            // keep the narrowest single estimate.
                            acc = acc.min(n);
                        }
                    }
                    self.constraints = constraints;
                }

                self.selectivity = if has_zero {
                    Selectivity::Zero
                } else if !has_unknown && !has_lower_bound && !has_n {
                    Selectivity::All
                } else if (!has_unknown && !has_lower_bound) || acc < DEFAULT_SELECTIVITY {
                    Selectivity::N(acc)
                } else if has_unknown {
                    Selectivity::Unknown
                } else if has_lower_bound {
                    Selectivity::LowerBound
                } else {
                    Selectivity::Unknown
                };
            }

            "or_filters" => {
                // Keep constant false as `Zero` for boolean composition, but keep
                // numeric `N(0.0)` as an estimate that does not prove emptiness.
                let mut has_unknown = false;
                let mut has_lower_bound = false;
                let mut has_zero = false;
                let mut has_all = false;
                let mut acc = 0.0_f64;
                let mut has_numeric_selectivity = false;
                for arg in &func.args {
                    let mut sub_visitor = self.spawn_child(ConstraintContext::Or);
                    sub_visitor.visit_expr(arg)?;
                    match sub_visitor.selectivity {
                        Selectivity::Unknown => has_unknown = true,
                        Selectivity::LowerBound => has_lower_bound = true,
                        Selectivity::Zero => has_zero = true,
                        Selectivity::All => has_all = true,
                        Selectivity::N(n) => {
                            has_numeric_selectivity = true;
                            acc += (1.0 - acc) * n;
                        }
                    }
                }
                self.selectivity = if has_all {
                    Selectivity::All
                } else if has_zero && !has_numeric_selectivity && !has_unknown && !has_lower_bound {
                    Selectivity::Zero
                } else if (!has_unknown || acc > DEFAULT_SELECTIVITY) && !has_lower_bound
                    || acc > UNKNOWN_COL_STATS_FILTER_SEL_LOWER_BOUND
                {
                    if has_numeric_selectivity {
                        Selectivity::N(acc)
                    } else {
                        Selectivity::Unknown
                    }
                } else if has_lower_bound {
                    Selectivity::LowerBound
                } else {
                    Selectivity::Unknown
                }
            }

            "not" => {
                let mut sub_visitor = self.spawn_child(ConstraintContext::Not);
                sub_visitor.visit_expr(&func.args[0])?;
                self.selectivity = match sub_visitor.selectivity {
                    Selectivity::Zero => Selectivity::All,
                    Selectivity::All => Selectivity::Zero,
                    Selectivity::N(n) => Selectivity::N(1.0 - n),
                    selectivity => selectivity,
                };
            }

            "like" => {
                self.selectivity = self.compute_like(func)?;
            }

            "is_not_null" => {
                self.selectivity = self.compute_is_not_null(&func.args[0])?;
                if matches!(self.constraint_context, ConstraintContext::And)
                    && let Expr::ColumnRef(column_ref) = &func.args[0]
                {
                    self.constraints.add(
                        self.column_stats,
                        column_ref.id.index,
                        ValueConstraint::NotNull,
                    )?;
                }
            }

            _ => {
                if let Some(op) = ComparisonOp::try_from_func_name(func_name) {
                    self.selectivity = self.compute_comparison(op, func)?;
                } else {
                    self.selectivity = Selectivity::Unknown;
                }
            }
        }

        Ok(())
    }
}
