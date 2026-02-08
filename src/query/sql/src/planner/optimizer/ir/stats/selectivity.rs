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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Constant;
use databend_common_expression::ConstantFolder;
use databend_common_expression::Domain;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::StatEvaluator;
use databend_common_expression::function_stat::ArgStat;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberScalar;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_statistics::DEFAULT_HISTOGRAM_BUCKETS;
use databend_common_statistics::Datum;
use databend_common_statistics::Histogram;

use crate::ColumnBinding;
use crate::IndexType;
use crate::optimizer::ir::ColumnStat;
use crate::optimizer::ir::ColumnStatSet;
use crate::optimizer::ir::HistogramBuilder;
use crate::optimizer::ir::Ndv;
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

/// Some constants for like predicate selectivity estimation.
const FIXED_CHAR_SEL: f64 = 0.5;
const ANY_CHAR_SEL: f64 = 0.9; // not 1, since it won't match end-of-string
const FULL_WILDCARD_SEL: f64 = 2.0;

pub struct SelectivityEstimator {
    pub cardinality: f64,
    column_stats: ColumnStatSet,
    overrides: ColumnStatSet,
}

impl SelectivityEstimator {
    pub fn new(input_stat: ColumnStatSet, cardinality: f64) -> Self {
        Self {
            cardinality,
            column_stats: input_stat,
            overrides: ColumnStatSet::new(),
        }
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
        let (expr, _) =
            ConstantFolder::fold(&expr, &FunctionContext::default(), &BUILTIN_FUNCTIONS);
        let mut visitor = SelectivityVisitor {
            cardinality: self.cardinality,
            selectivity: Selectivity::Unknown,
            column_stats: &self.column_stats,
            overrides: ColumnStatSet::new(),
        };
        visitor.visit_expr(&expr)?;
        let selectivity = match visitor.selectivity {
            Selectivity::Unknown => DEFAULT_SELECTIVITY,
            Selectivity::LowerBound => UNKNOWN_COL_STATS_FILTER_SEL_LOWER_BOUND,
            Selectivity::N(n) => n,
        };
        self.overrides = visitor.overrides;
        self.update_other_statistic_by_selectivity(selectivity);

        Ok(self.cardinality * selectivity)
    }

    // Update other columns' statistic according to selectivity.
    pub fn update_other_statistic_by_selectivity(&mut self, selectivity: f64) {
        if selectivity == MAX_SELECTIVITY {
            return;
        }

        for (index, column_stat) in &self.column_stats {
            if self.overrides.contains_key(index) {
                continue;
            }
            let mut column_stat = column_stat.clone();
            column_stat.ndv = column_stat.ndv.reduce_by_selectivity(selectivity);
            column_stat.null_count = (column_stat.null_count as f64 * selectivity).ceil() as u64;

            if let Some(histogram) = &mut column_stat.histogram {
                if histogram.accuracy {
                    // If selectivity < 0.2, most buckets are invalid and
                    // the accuracy histogram can be discarded.
                    // Todo: find a better way to update histogram.
                    if selectivity < 0.2 {
                        column_stat.histogram = None;
                    }
                } else if column_stat.ndv.value() as u64 <= 2 {
                    column_stat.histogram = None;
                } else {
                    for bucket in histogram.buckets.iter_mut() {
                        bucket.update(selectivity);
                    }
                }
            }

            self.overrides.insert(*index, column_stat);
        }
    }
}

#[derive(Clone)]
struct SelectivityVisitor<'a> {
    cardinality: f64,
    selectivity: Selectivity,
    column_stats: &'a ColumnStatSet,
    overrides: ColumnStatSet,
}

#[derive(Debug, Clone, Copy, Default, enum_as_inner::EnumAsInner)]
pub enum Selectivity {
    #[default]
    Unknown,
    LowerBound,
    N(f64),
}

type ExprCall = databend_common_expression::FunctionCall<ColumnBinding>;

impl SelectivityVisitor<'_> {
    fn build_input_stats<'s>(
        &'s self,
        expr: &Expr<ColumnBinding>,
    ) -> Result<Option<HashMap<ColumnBinding, ArgStat<'s>>>> {
        let column_refs = expr.column_refs();
        if column_refs.is_empty() {
            return Ok(None);
        }

        let mut input_stats = HashMap::with_capacity(column_refs.len());
        for (binding, data_type) in column_refs {
            let Some(column_stat) = self.get_column_stat(binding.index) else {
                return Ok(None);
            };

            match data_type.remove_nullable() {
                DataType::Boolean
                | DataType::Binary
                | DataType::String
                | DataType::Number(_)
                | DataType::Decimal(_) => (),
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

    fn derive_expr_stat(&self, expr: &Expr<ColumnBinding>) -> Result<Option<ColumnStat>> {
        let Some(input_stats) = self.build_input_stats(expr)? else {
            return Ok(None);
        };
        let Some(stat) = StatEvaluator::run(
            expr,
            &FunctionContext::default(),
            &BUILTIN_FUNCTIONS,
            self.cardinality,
            &input_stats,
        )?
        else {
            return Ok(None);
        };

        let stat = stat.as_ref();
        Ok(try {
            let (min, max) = match &stat.domain {
                Domain::Nullable(domain) => {
                    debug_assert_eq!(!domain.has_null, stat.null_count == 0);
                    domain.value.as_ref()?.to_minmax()
                }
                domain => {
                    debug_assert_eq!(stat.null_count, 0);
                    domain.to_minmax()
                }
            };
            ColumnStat {
                min: min.to_datum()?,
                max: max.to_datum()?,
                ndv: stat.ndv,
                null_count: stat.null_count,
                histogram: stat.histogram.cloned(),
            }
        })
    }

    fn compute_comparison(&mut self, op: ComparisonOp, func: &ExprCall) -> Result<Selectivity> {
        let left = &func.args[0];
        let right = &func.args[1];
        match (left, right) {
            (Expr::ColumnRef(column_ref), Expr::Constant(constant))
            | (Expr::Constant(constant), Expr::ColumnRef(column_ref)) => {
                let column_index = column_ref.id.index;
                let Some(_) = self.get_column_stat(column_index) else {
                    // The column is derived column, give a small selectivity currently.
                    // Need to improve it later.
                    // Another case: column is from system table, such as numbers. We shouldn't use numbers() table to test cardinality estimation.
                    return Ok(Selectivity::LowerBound);
                };
                let Some(const_datum) = constant.scalar.clone().to_datum() else {
                    return Ok(Selectivity::Unknown);
                };
                let column_stat = self
                    .ensure_column_stat(column_index)
                    .expect("checked above");
                return Self::compute_comparison_with_stat(
                    column_stat,
                    constant,
                    if left.is_constant() { op.reverse() } else { op },
                    const_datum,
                    column_ref.data_type.remove_nullable().is_integer(),
                );
            }
            (Expr::FunctionCall(func), Expr::Constant(val))
                if op == ComparisonOp::Equal && func.function.signature.name == "modulo" =>
            {
                if let Expr::Constant(mod_num) = &func.args[1]
                    && let Some(mod_num) = mod_num.scalar.clone().to_datum()
                {
                    let mod_num = mod_num.as_double()?;
                    if mod_num == 0.0 {
                        return Err(ErrorCode::SemanticError("modulus by zero".to_string()));
                    }
                    return if let Some(remainder) = val.scalar.clone().to_datum()
                        && remainder.as_double()? >= mod_num
                    {
                        Ok(Selectivity::N(0.0))
                    } else {
                        Ok(Selectivity::N(1.0 / mod_num))
                    };
                }
            }
            (expr, Expr::Constant(constant)) => {
                return self.compute_comparison_expr(expr, constant, op);
            }
            (Expr::Constant(constant), expr) => {
                return self.compute_comparison_expr(expr, constant, op.reverse());
            }
            _ => (),
        }

        Ok(Selectivity::Unknown)
    }

    fn compute_comparison_expr(
        &mut self,
        expr: &Expr<ColumnBinding>,
        constant: &Constant,
        op: ComparisonOp,
    ) -> Result<Selectivity> {
        let Some(const_datum) = constant.scalar.clone().to_datum() else {
            return Ok(Selectivity::Unknown);
        };
        let Some(mut column_stat) = self.derive_expr_stat(expr)? else {
            return Ok(Selectivity::Unknown);
        };

        Self::compute_comparison_with_stat(
            &mut column_stat,
            constant,
            op,
            const_datum,
            expr.data_type().remove_nullable().is_integer(),
        )
    }

    fn compute_comparison_with_stat(
        column_stat: &mut ColumnStat,
        constant: &Constant,
        op: ComparisonOp,
        const_datum: Datum,
        column_is_integer: bool,
    ) -> Result<Selectivity> {
        match op {
            ComparisonOp::Equal => {
                let selectivity = evaluate_equal(column_stat, false, constant);
                *column_stat = ColumnStat::from_const(const_datum);
                Ok(selectivity)
            }
            ComparisonOp::NotEqual => {
                let selectivity = evaluate_equal(column_stat, true, constant);
                if let Selectivity::N(n) = selectivity {
                    update_statistic(
                        column_stat,
                        column_stat.min.clone(),
                        column_stat.max.clone(),
                        n,
                    )?;
                }
                Ok(selectivity)
            }
            _ => match &column_stat.histogram {
                Some(histogram) => {
                    let selectivity =
                        Self::compute_histogram_comparison(histogram, op, &const_datum)?;
                    if let Selectivity::N(n) = selectivity {
                        let (new_min, new_max) = match op {
                            ComparisonOp::GT | ComparisonOp::GTE => {
                                (const_datum.clone(), column_stat.max.clone())
                            }
                            ComparisonOp::LT | ComparisonOp::LTE => {
                                (column_stat.min.clone(), const_datum.clone())
                            }
                            _ => unreachable!(),
                        };
                        update_statistic(column_stat, new_min, new_max, n)?;
                    }
                    Ok(selectivity)
                }
                None => {
                    if column_is_integer {
                        Self::compute_ndv_comparison(column_stat, op, &const_datum)
                    } else {
                        Ok(Selectivity::Unknown)
                    }
                }
            },
        }
    }

    fn compute_ndv_comparison(
        column_stat: &mut ColumnStat,
        comparison_op: ComparisonOp,
        const_datum: &Datum,
    ) -> Result<Selectivity> {
        let min = column_stat.min.as_double()?;
        let max = column_stat.max.as_double()?;
        let ndv = column_stat.ndv;
        let numeric_literal = const_datum.as_double()?;

        let cmp_min = numeric_literal.total_cmp(&min);
        let cmp_max = numeric_literal.total_cmp(&max);

        use Ordering::*;
        let selectivity = match (comparison_op, cmp_min, cmp_max) {
            (ComparisonOp::LT, Less | Equal, _) => 0.0,
            (ComparisonOp::LTE, Less, _) => 0.0,
            (ComparisonOp::LTE, Equal, _) => {
                *column_stat = ColumnStat::from_const(const_datum.clone());
                return Ok(Selectivity::equal_selectivity(ndv, false));
            }
            (ComparisonOp::LT | ComparisonOp::LTE, Greater, Greater) => 1.0,
            (ComparisonOp::LT, Greater, Equal) => {
                let selectivity = Selectivity::equal_selectivity(ndv, true);
                if let Selectivity::N(n) = selectivity {
                    update_statistic(
                        column_stat,
                        column_stat.min.clone(),
                        column_stat.max.clone(),
                        n,
                    )?;
                }
                return Ok(selectivity);
            }
            (ComparisonOp::LT | ComparisonOp::LTE, _, _) => {
                let n = (numeric_literal - min + 1.0) / (max - min + 1.0);
                update_statistic(column_stat, column_stat.min.clone(), const_datum.clone(), n)?;
                return Ok(Selectivity::N(n));
            }

            (ComparisonOp::GT, _, Greater | Equal) => 0.0,
            (ComparisonOp::GTE, _, Greater) => 0.0,
            (ComparisonOp::GTE, Less | Equal, _) => 1.0,
            (ComparisonOp::GT, Less, _) => 1.0,
            (ComparisonOp::GT, Equal, _) => {
                let selectivity = Selectivity::equal_selectivity(ndv, true);
                if let Selectivity::N(n) = selectivity {
                    update_statistic(
                        column_stat,
                        column_stat.min.clone(),
                        column_stat.max.clone(),
                        n,
                    )?;
                }
                return Ok(selectivity);
            }
            (ComparisonOp::GTE, _, Equal) => {
                *column_stat = ColumnStat::from_const(const_datum.clone());
                return Ok(Selectivity::equal_selectivity(ndv, false));
            }
            (ComparisonOp::GT | ComparisonOp::GTE, _, _) => {
                let n = (max - numeric_literal + 1.0) / (max - min + 1.0);
                update_statistic(column_stat, const_datum.clone(), column_stat.max.clone(), n)?;
                return Ok(Selectivity::N(n));
            }

            _ => unreachable!(),
        };
        if selectivity == 0.0 {
            column_stat.ndv = column_stat.ndv.reduce_by_selectivity(0.0);
        }
        Ok(Selectivity::N(selectivity))
    }

    fn compute_histogram_comparison(
        histogram: &Histogram,
        comparison_op: ComparisonOp,
        const_datum: &Datum,
    ) -> Result<Selectivity> {
        let mut num_selected = 0.0;
        for bucket in histogram.buckets_iter() {
            let lower_bound = bucket.lower_bound();
            let upper_bound = bucket.upper_bound();

            if !const_datum.type_comparable(lower_bound) {
                return Ok(Selectivity::Unknown);
            }

            let const_gte_upper_bound = matches!(
                const_datum.compare(upper_bound)?,
                Ordering::Greater | Ordering::Equal
            );
            let (no_overlap, complete_overlap) = match comparison_op {
                ComparisonOp::LT => (
                    matches!(
                        const_datum.compare(lower_bound)?,
                        Ordering::Less | Ordering::Equal
                    ),
                    const_gte_upper_bound,
                ),
                ComparisonOp::LTE => (
                    matches!(const_datum.compare(lower_bound)?, Ordering::Less),
                    const_gte_upper_bound,
                ),
                ComparisonOp::GT => (
                    const_gte_upper_bound,
                    matches!(const_datum.compare(lower_bound)?, Ordering::Less),
                ),
                ComparisonOp::GTE => (
                    const_gte_upper_bound,
                    matches!(
                        const_datum.compare(lower_bound)?,
                        Ordering::Less | Ordering::Equal
                    ),
                ),
                _ => unreachable!(),
            };

            if complete_overlap {
                num_selected += bucket.num_values();
            } else if !no_overlap && const_datum.is_numeric() {
                let ndv = bucket.num_distinct();
                let lower_bound = lower_bound.as_double()?;
                let upper_bound = upper_bound.as_double()?;
                let const_value = const_datum.as_double()?;

                let bucket_range = upper_bound - lower_bound;
                let bucket_selectivity = match comparison_op {
                    ComparisonOp::LT => (const_value - lower_bound) / bucket_range,
                    ComparisonOp::LTE => {
                        if const_value == lower_bound {
                            1.0 / ndv
                        } else {
                            (const_value - lower_bound + 1.0) / bucket_range
                        }
                    }
                    ComparisonOp::GT => {
                        if const_value == lower_bound {
                            1.0 - 1.0 / ndv
                        } else {
                            (upper_bound - const_value - 1.0).max(0.0) / bucket_range
                        }
                    }
                    ComparisonOp::GTE => (upper_bound - const_value) / bucket_range,
                    _ => unreachable!(),
                };
                num_selected += bucket.num_values() * bucket_selectivity;
            }
        }

        Ok(Selectivity::N(num_selected / histogram.num_values()))
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
        let Some(column_stat) = self.get_column_stat(column_ref.id.index) else {
            return Ok(Selectivity::Unknown);
        };
        if self.cardinality == 0.0 {
            return Ok(Selectivity::N(0.0));
        }
        Ok(Selectivity::N(
            (self.cardinality - column_stat.null_count as f64) / self.cardinality,
        ))
    }

    fn get_column_stat(&self, index: IndexType) -> Option<&ColumnStat> {
        self.overrides
            .get(&index)
            .or_else(|| self.column_stats.get(&index))
    }

    fn ensure_column_stat(&mut self, index: IndexType) -> Option<&mut ColumnStat> {
        if !self.overrides.contains_key(&index) {
            let stat = self.column_stats.get(&index)?.clone();
            self.overrides.insert(index, stat);
        }
        self.overrides.get_mut(&index)
    }

    fn spawn_child(&self) -> SelectivityVisitor<'_> {
        SelectivityVisitor {
            cardinality: self.cardinality,
            selectivity: Selectivity::Unknown,
            column_stats: self.column_stats,
            overrides: self.overrides.clone(),
        }
    }

    fn visit_expr(&mut self, expr: &Expr<ColumnBinding>) -> Result<()> {
        match expr {
            Expr::Constant(constant) => {
                self.selectivity = if is_true_constant_predicate(constant) {
                    Selectivity::N(1.0)
                } else {
                    Selectivity::N(0.0)
                };
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
                let mut has_unknown = false;
                let mut has_lower_bound = false;
                let mut acc = 1.0_f64;
                for arg in &func.args {
                    let mut sub_visitor = self.spawn_child();
                    sub_visitor.visit_expr(arg)?;
                    match sub_visitor.selectivity {
                        Selectivity::Unknown => has_unknown = true,
                        Selectivity::LowerBound => has_lower_bound = true,
                        Selectivity::N(n) => acc = acc.min(n),
                    }
                    self.overrides.extend(sub_visitor.overrides);
                }

                self.selectivity =
                    if (!has_unknown && !has_lower_bound) || acc < DEFAULT_SELECTIVITY {
                        Selectivity::N(acc)
                    } else if has_unknown {
                        Selectivity::Unknown
                    } else if has_lower_bound {
                        Selectivity::LowerBound
                    } else {
                        Selectivity::Unknown
                    }
            }

            "or_filters" => {
                let mut has_unknown = false;
                let mut has_lower_bound = false;
                let mut acc = 0.0_f64;
                for arg in &func.args {
                    let mut sub_visitor = self.spawn_child();
                    sub_visitor.visit_expr(arg)?;
                    match sub_visitor.selectivity {
                        Selectivity::Unknown => has_unknown = true,
                        Selectivity::LowerBound => has_lower_bound = true,
                        Selectivity::N(n) => acc += (1.0 - acc) * n,
                    }
                }
                self.selectivity = if (!has_unknown || acc > DEFAULT_SELECTIVITY)
                    && !has_lower_bound
                    || acc > UNKNOWN_COL_STATS_FILTER_SEL_LOWER_BOUND
                {
                    Selectivity::N(acc)
                } else if has_lower_bound {
                    Selectivity::LowerBound
                } else {
                    Selectivity::Unknown
                }
            }

            "not" => {
                let mut sub_visitor = self.spawn_child();
                sub_visitor.visit_expr(&func.args[0])?;
                self.selectivity = match sub_visitor.selectivity {
                    Selectivity::N(n) => Selectivity::N(1.0 - n),
                    selectivity => selectivity,
                };
            }

            "like" => {
                self.selectivity = self.compute_like(func)?;
            }

            "is_not_null" => {
                self.selectivity = self.compute_is_not_null(&func.args[0])?;
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

// TODO(andylokandy): match on non-null boolean only once we have constant folding in the optimizer.
fn is_true_constant_predicate(constant: &Constant) -> bool {
    match &constant.scalar {
        Scalar::Null => false,
        Scalar::Boolean(v) => *v,
        Scalar::Number(NumberScalar::Int64(v)) => *v != 0,
        Scalar::Number(NumberScalar::UInt64(v)) => *v != 0,
        Scalar::Number(NumberScalar::Float64(v)) => *v != 0.0,
        _ => true,
    }
}

fn evaluate_equal(column_stat: &ColumnStat, not_eq: bool, constant: &Constant) -> Selectivity {
    match &constant.scalar {
        Scalar::Null => return Selectivity::N(if not_eq { 1.0 } else { 0.0 }),
        value => {
            if let Some(constant) = value.clone().to_datum()
                && (matches!(constant.compare(&column_stat.min), Ok(Ordering::Less))
                    || matches!(constant.compare(&column_stat.max), Ok(Ordering::Greater)))
            {
                return Selectivity::N(if not_eq { 1.0 } else { 0.0 });
            }
        }
    }

    Selectivity::equal_selectivity(column_stat.ndv, not_eq)
}

fn update_statistic(
    column_stat: &mut ColumnStat,
    new_min: Datum,
    new_max: Datum,
    selectivity: f64,
) -> Result<()> {
    column_stat.ndv = column_stat.ndv.reduce_by_selectivity(selectivity);
    column_stat.min = new_min.clone();
    column_stat.max = new_max.clone();
    column_stat.null_count = (column_stat.null_count as f64 * selectivity).ceil() as u64;

    if let Some(histogram) = &column_stat.histogram {
        // If selectivity < 0.2, most buckets are invalid and
        // the accuracy histogram can be discarded.
        // Todo: support unfixed buckets number for histogram and prune the histogram.
        if !histogram.accuracy || selectivity < 0.2 {
            let num_values = histogram.num_values();
            let new_num_values = (num_values * selectivity).ceil() as u64;
            let new_ndv = column_stat.ndv.value() as u64;
            column_stat.histogram = if new_ndv <= 2 {
                None
            } else {
                Some(HistogramBuilder::from_ndv(
                    new_ndv,
                    new_num_values.max(new_ndv),
                    Some((new_min, new_max)),
                    DEFAULT_HISTOGRAM_BUCKETS,
                )?)
            }
        }
    }

    Ok(())
}

impl Selectivity {
    pub fn equal_selectivity(ndv: Ndv, not: bool) -> Self {
        let v = ndv.value();
        if v == 0.0 {
            Selectivity::N(0.0)
        } else {
            let selectivity = if not { 1.0 - 1.0 / v } else { 1.0 / v };
            match ndv {
                Ndv::Stat(_) => Selectivity::N(selectivity),
                Ndv::Max(_) => Selectivity::LowerBound,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use databend_common_exception::Result;
    use databend_common_expression::RawExpr;
    use databend_common_expression::types::ArgType;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::UInt64Type;
    use databend_common_functions::test_utils::parse_raw_expr;
    use goldenfile::Mint;

    use super::*;
    use crate::ColumnBindingBuilder;
    use crate::Visibility;
    use crate::optimizer::ir::Ndv;
    use crate::plans::BoundColumnRef;
    use crate::plans::CastExpr;
    use crate::plans::ConstantExpr;
    use crate::plans::ScalarExpr;

    fn run_test(
        file: &mut impl Write,
        expr_text: &str,
        columns: &[(&str, DataType)],
        column_stats: ColumnStatSet,
    ) -> Result<()> {
        writeln!(file, "expr          : {expr_text}")?;

        let in_stats = column_stats_to_string(&column_stats);
        let raw_expr = parse_raw_expr(expr_text, columns);
        let expr = raw_expr_to_scalar(&raw_expr, columns);
        let cardinality = 100.0;
        let mut estimator = SelectivityEstimator::new(column_stats, cardinality);
        let estimated_rows = estimator.apply(&[expr])?;
        let out_stats = estimator.column_stats();

        writeln!(file, "cardinality   : {cardinality}")?;
        writeln!(file, "estimated     : {estimated_rows}")?;
        writeln!(file, "in stats      :\n{in_stats}")?;
        writeln!(
            file,
            "out stats     :\n{}",
            column_stats_to_string(&out_stats)
        )?;

        writeln!(file)?;
        Ok(())
    }

    fn column_stats_to_string(column_stats: &ColumnStatSet) -> String {
        let mut keys = column_stats.keys().copied().collect::<Vec<_>>();
        keys.sort();

        keys.iter()
            .map(|i| format!("{i} {:?}", column_stats[i]))
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn raw_expr_to_scalar(raw_expr: &RawExpr, columns: &[(&str, DataType)]) -> ScalarExpr {
        match raw_expr {
            RawExpr::Constant { scalar, .. } => ScalarExpr::ConstantExpr(ConstantExpr {
                span: None,
                value: scalar.clone(),
            }),
            RawExpr::ColumnRef { id, .. } => {
                let index = *id;
                let (name, data_type) = &columns[index];
                let column = ColumnBindingBuilder::new(
                    name.to_string(),
                    index,
                    Box::new(data_type.clone()),
                    Visibility::Visible,
                )
                .build();
                ScalarExpr::BoundColumnRef(BoundColumnRef { span: None, column })
            }
            RawExpr::Cast {
                expr,
                dest_type,
                is_try,
                ..
            } => ScalarExpr::CastExpr(CastExpr {
                span: None,
                is_try: *is_try,
                argument: Box::new(raw_expr_to_scalar(expr, columns)),
                target_type: Box::new(dest_type.clone()),
            }),
            RawExpr::FunctionCall {
                name, args, params, ..
            } => ScalarExpr::FunctionCall(FunctionCall {
                span: None,
                func_name: name.clone(),
                params: params.clone(),
                arguments: args
                    .iter()
                    .map(|arg| raw_expr_to_scalar(arg, columns))
                    .collect(),
            }),
            RawExpr::LambdaFunctionCall { .. } => {
                unreachable!("lambda expressions are not used in tests")
            }
        }
    }

    #[test]
    fn test_selectivity() -> Result<()> {
        let mut mint = Mint::new("tests/ut/testdata");
        let file = &mut mint.new_goldenfile("selectivity.txt").unwrap();

        test_comparison(file)?;
        test_logic(file)?;
        test_mod(file)?;
        test_like(file)?;

        Ok(())
    }

    fn test_comparison(file: &mut impl Write) -> Result<()> {
        let column_stats = ColumnStatSet::from_iter([
            (0, ColumnStat {
                min: Datum::UInt(10),
                max: Datum::UInt(20),
                ndv: Ndv::Stat(10.0),
                null_count: 0,
                histogram: None,
            }),
            (1, ColumnStat {
                min: Datum::UInt(10),
                max: Datum::UInt(20),
                ndv: Ndv::Stat(10.0),
                null_count: 10,
                histogram: None,
            }),
        ]);
        let columns = &[("a", UInt64Type::data_type())];

        run_test(file, "a = 5", columns, column_stats.clone())?;
        run_test(file, "a = 15", columns, column_stats.clone())?;

        run_test(file, "a != 5", columns, column_stats.clone())?;
        run_test(file, "a != 15", columns, column_stats.clone())?;

        run_test(file, "a > 5", columns, column_stats.clone())?;
        run_test(file, "a > 10", columns, column_stats.clone())?;
        run_test(file, "a > 17", columns, column_stats.clone())?;
        run_test(file, "a > 20", columns, column_stats.clone())?;
        run_test(file, "a > 25", columns, column_stats.clone())?;

        run_test(file, "a >= 5", columns, column_stats.clone())?;
        run_test(file, "a >= 10", columns, column_stats.clone())?;
        run_test(file, "a >= 17", columns, column_stats.clone())?;
        run_test(file, "a >= 20", columns, column_stats.clone())?;
        run_test(file, "a >= 25", columns, column_stats.clone())?;

        run_test(file, "a < 5", columns, column_stats.clone())?;
        run_test(file, "a < 10", columns, column_stats.clone())?;
        run_test(file, "a < 17", columns, column_stats.clone())?;
        run_test(file, "a < 20", columns, column_stats.clone())?;
        run_test(file, "a < 25", columns, column_stats.clone())?;

        run_test(file, "a <= 5", columns, column_stats.clone())?;
        run_test(file, "a <= 10", columns, column_stats.clone())?;
        run_test(file, "a <= 17", columns, column_stats.clone())?;
        run_test(file, "a <= 20", columns, column_stats.clone())?;
        run_test(file, "a <= 25", columns, column_stats.clone())?;

        run_test(file, "a + 1 = 15", columns, column_stats.clone())?;

        Ok(())
    }

    fn test_logic(file: &mut impl Write) -> Result<()> {
        let column_stats = ColumnStatSet::from_iter([
            (0, ColumnStat {
                min: Datum::UInt(0),
                max: Datum::UInt(9),
                ndv: Ndv::Stat(10.0),
                null_count: 0,
                histogram: None,
            }),
            (1, ColumnStat {
                min: Datum::UInt(0),
                max: Datum::UInt(9),
                ndv: Ndv::Stat(10.0),
                null_count: 10,
                histogram: None,
            }),
        ]);
        let columns = &[("a", UInt64Type::data_type())];

        run_test(
            file,
            "and_filters(a = 5, a > 3)",
            columns,
            column_stats.clone(),
        )?;

        run_test(
            file,
            "or_filters(a = 5, a = 6)",
            columns,
            column_stats.clone(),
        )?;

        run_test(file, "not(a = 5)", columns, column_stats.clone())?;

        run_test(
            file,
            "is_not_null(b)",
            &[
                ("a", UInt64Type::data_type()),
                ("b", UInt64Type::data_type().wrap_nullable()),
            ],
            column_stats.clone(),
        )?;
        Ok(())
    }

    fn test_mod(file: &mut impl Write) -> Result<()> {
        let column_stats = ColumnStatSet::from_iter([
            (0, ColumnStat {
                min: Datum::UInt(0),
                max: Datum::UInt(9),
                ndv: Ndv::Stat(10.0),
                null_count: 0,
                histogram: None,
            }),
            (1, ColumnStat {
                min: Datum::UInt(0),
                max: Datum::UInt(9),
                ndv: Ndv::Stat(10.0),
                null_count: 10,
                histogram: None,
            }),
        ]);
        let columns = &[("a", UInt64Type::data_type())];

        run_test(file, "a % 4 = 1", columns, column_stats.clone())?;
        run_test(file, "a % 4 = 5", columns, column_stats.clone())?;
        Ok(())
    }

    fn test_like(file: &mut impl Write) -> Result<()> {
        let columns = &[("s", DataType::String)];
        let column_stats = ColumnStatSet::from_iter([(0, ColumnStat {
            min: Datum::Bytes("aa".as_bytes().to_vec()),
            max: Datum::Bytes("zz".as_bytes().to_vec()),
            ndv: Ndv::Stat(52.0),
            null_count: 0,
            histogram: None,
        })]);
        run_test(file, "s like 'ab%'", columns, column_stats.clone())?;
        run_test(file, "s like '%ab_'", columns, column_stats.clone())?;

        Ok(())
    }
}
