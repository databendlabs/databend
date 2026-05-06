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
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::StatEvaluator;
use databend_common_expression::stat_distribution::ArgStat;
use databend_common_expression::stat_distribution::StatCardinality;
use databend_common_expression::stat_distribution::StatCount;
use databend_common_expression::stat_distribution::StatEstimate;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberScalar;
use databend_common_functions::BUILTIN_FUNCTIONS;

use super::constraint::ColumnStatUpdate;
use super::constraint::ValueConstraint;
use crate::ColumnBinding;
use crate::Symbol;
use crate::optimizer::ir::ColumnStat;
use crate::optimizer::ir::ColumnStatSet;
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
    cardinality: StatCardinality,
    column_stats: ColumnStatSet,
    overrides: ColumnStatSet,
}

impl SelectivityEstimator {
    pub fn new(input_stat: ColumnStatSet, cardinality: StatCardinality) -> Self {
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

        let SelectivityVisitor {
            selectivity,
            overrides,
            ..
        } = visitor;
        self.overrides = overrides;
        let selectivity = self.update_other_statistic_by_selectivity(selectivity);

        Ok(self.cardinality.value() * selectivity)
    }

    // Update other columns' statistic according to selectivity.
    pub fn update_other_statistic_by_selectivity(&mut self, selectivity: Selectivity) -> f64 {
        let selectivity = match selectivity {
            Selectivity::Unknown => DEFAULT_SELECTIVITY,
            Selectivity::LowerBound => UNKNOWN_COL_STATS_FILTER_SEL_LOWER_BOUND,
            Selectivity::Zero => {
                for (index, column_stat) in &self.column_stats {
                    let mut column_stat = column_stat.clone();
                    column_stat.ndv = StatEstimate::exact(0.0);
                    column_stat.null_count = StatCount::exact(0);
                    column_stat.histogram = None;
                    self.overrides.insert(*index, column_stat);
                }
                return 0.0;
            }
            Selectivity::N(n) => n,
        };

        if selectivity == MAX_SELECTIVITY {
            return selectivity;
        }
        if selectivity == 0.0 {
            return selectivity;
        }

        for (index, column_stat) in &self.column_stats {
            if self.overrides.contains_key(index) {
                continue;
            }
            let mut column_stat = column_stat.clone();
            ColumnStatUpdate {
                column_stat: &mut column_stat,
            }
            .apply_unconstrained_filter(selectivity);

            self.overrides.insert(*index, column_stat);
        }

        selectivity
    }
}

#[derive(Clone)]
struct SelectivityVisitor<'a> {
    cardinality: StatCardinality,
    selectivity: Selectivity,
    column_stats: &'a ColumnStatSet,
    overrides: ColumnStatSet,
}

#[derive(Debug, Clone, Copy, Default, enum_as_inner::EnumAsInner)]
pub enum Selectivity {
    #[default]
    Unknown,
    LowerBound,
    // Trusted empty-result signal. This is intentionally separate from a
    // numeric estimate of 0.0 so downstream statistics are cleared only when
    // the predicate is known to produce no rows.
    Zero,
    N(f64),
}

type ExprCall = databend_common_expression::FunctionCall<ColumnBinding>;

impl Selectivity {
    fn checked_estimate(value: f64) -> Result<Self> {
        if value.is_finite() && (0.0..=1.0).contains(&value) {
            return Ok(Selectivity::N(value));
        }

        let msg = format!("invalid selectivity estimate: {value:?}");
        if cfg!(debug_assertions) {
            Err(ErrorCode::Internal(msg))
        } else {
            log::warn!(msg; "Invalid selectivity estimate");
            Ok(Selectivity::Unknown)
        }
    }
}

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

    fn compute_comparison(&mut self, op: ComparisonOp, func: &ExprCall) -> Result<Selectivity> {
        let left = &func.args[0];
        let right = &func.args[1];
        match (left, right) {
            (Expr::ColumnRef(column_ref), Expr::Constant(constant))
            | (Expr::Constant(constant), Expr::ColumnRef(column_ref)) => {
                let column_index = column_ref.id.index;
                let Some(mut column_stat) = self.get_column_stat(column_index).cloned() else {
                    // The column is derived column, give a small selectivity currently.
                    // Need to improve it later.
                    // Another case: column is from system table, such as numbers. We shouldn't use numbers() table to test cardinality estimation.
                    return Ok(Selectivity::LowerBound);
                };
                let Some(const_datum) = constant.scalar.clone().to_datum() else {
                    return Ok(Selectivity::Unknown);
                };
                let op = if left.is_constant() { op.reverse() } else { op };
                let is_zero = {
                    use std::cmp::Ordering::*;

                    match op {
                        ComparisonOp::Equal => {
                            matches!(const_datum.compare(&column_stat.min)?, Less)
                                || matches!(const_datum.compare(&column_stat.max)?, Greater)
                        }
                        ComparisonOp::NotEqual => false,
                        ComparisonOp::GT => {
                            matches!(const_datum.compare(&column_stat.max)?, Greater | Equal)
                        }
                        ComparisonOp::GTE => {
                            matches!(const_datum.compare(&column_stat.max)?, Greater)
                        }
                        ComparisonOp::LT => {
                            matches!(const_datum.compare(&column_stat.min)?, Less | Equal)
                        }
                        ComparisonOp::LTE => {
                            matches!(const_datum.compare(&column_stat.min)?, Less)
                        }
                    }
                };
                let selectivity = if is_zero {
                    Selectivity::Zero
                } else {
                    self.derive_function_selectivity(func)?
                };
                let constraint = ValueConstraint::from_comparison(op, const_datum);
                ColumnStatUpdate {
                    column_stat: &mut column_stat,
                }
                .apply_constraint(&constraint, selectivity)?;
                self.overrides.insert(column_index, column_stat);
                return Ok(selectivity);
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
                        Ok(Selectivity::Zero)
                    } else {
                        Selectivity::checked_estimate(1.0 / mod_num)
                    };
                }
            }
            _ => (),
        }

        self.derive_function_selectivity(func)
    }

    fn derive_function_selectivity(&self, func: &ExprCall) -> Result<Selectivity> {
        let cardinality = match self.cardinality {
            StatCardinality::Exact(0) => return Ok(Selectivity::Zero),
            StatCardinality::Estimate(0.0) => return Ok(Selectivity::N(0.0)),
            cardinality => cardinality.value(),
        };
        let Some(input_stats) = self.build_input_stats(&Expr::FunctionCall(func.clone()))? else {
            return Ok(Selectivity::Unknown);
        };
        let Some(stat) = StatEvaluator::run(
            &Expr::FunctionCall(func.clone()),
            &FunctionContext::default(),
            &BUILTIN_FUNCTIONS,
            self.cardinality,
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
        let Some(column_stat) = self.get_column_stat(column_ref.id.index) else {
            return Ok(Selectivity::Unknown);
        };
        match self.cardinality {
            StatCardinality::Exact(0) => Ok(Selectivity::Zero),
            StatCardinality::Estimate(0.0) => Ok(Selectivity::N(0.0)),
            StatCardinality::Exact(cardinality) => {
                if column_stat.null_count.exact_value() == Some(cardinality) {
                    return Ok(Selectivity::Zero);
                }
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

    fn get_column_stat(&self, index: Symbol) -> Option<&ColumnStat> {
        self.overrides
            .get(&index)
            .or_else(|| self.column_stats.get(&index))
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
                    Selectivity::Zero
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
                let mut has_trusted_zero = false;
                let mut acc = 1.0_f64;
                for arg in &func.args {
                    let mut sub_visitor = self.spawn_child();
                    sub_visitor.visit_expr(arg)?;
                    match sub_visitor.selectivity {
                        Selectivity::Unknown => has_unknown = true,
                        Selectivity::LowerBound => has_lower_bound = true,
                        Selectivity::Zero => {
                            has_trusted_zero = true;
                            acc = 0.0;
                        }
                        Selectivity::N(n) => acc = acc.min(n),
                    }
                    self.overrides.extend(sub_visitor.overrides);
                }

                self.selectivity =
                    if (!has_unknown && !has_lower_bound) || acc < DEFAULT_SELECTIVITY {
                        if has_trusted_zero {
                            Selectivity::Zero
                        } else {
                            Selectivity::N(acc)
                        }
                    } else if has_unknown {
                        Selectivity::Unknown
                    } else if has_lower_bound {
                        Selectivity::LowerBound
                    } else {
                        Selectivity::Unknown
                    };
            }

            "or_filters" => {
                let mut has_unknown = false;
                let mut has_lower_bound = false;
                let mut acc = 0.0_f64;
                let mut has_numeric_selectivity = false;
                for arg in &func.args {
                    let mut sub_visitor = self.spawn_child();
                    sub_visitor.visit_expr(arg)?;
                    match sub_visitor.selectivity {
                        Selectivity::Unknown => has_unknown = true,
                        Selectivity::LowerBound => has_lower_bound = true,
                        Selectivity::Zero => {}
                        Selectivity::N(n) => {
                            has_numeric_selectivity = true;
                            acc += (1.0 - acc) * n;
                        }
                    }
                }
                self.selectivity = if (!has_unknown || acc > DEFAULT_SELECTIVITY)
                    && !has_lower_bound
                    || acc > UNKNOWN_COL_STATS_FILTER_SEL_LOWER_BOUND
                {
                    if has_numeric_selectivity {
                        Selectivity::N(acc)
                    } else {
                        Selectivity::Zero
                    }
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
                    Selectivity::Zero => Selectivity::N(1.0),
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

#[cfg(test)]
mod tests {
    use databend_common_expression::Scalar;
    use databend_common_expression::stat_distribution::StatCount;
    use databend_common_expression::stat_distribution::StatEstimate;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::NumberScalar;
    use databend_common_expression::types::decimal::DecimalScalar;
    use databend_common_expression::types::decimal::DecimalSize;
    use databend_common_statistics::DEFAULT_HISTOGRAM_BUCKETS;
    use databend_common_statistics::Datum;

    use super::*;
    use crate::ColumnBindingBuilder;
    use crate::Visibility;
    use crate::optimizer::ir::HistogramBuilder;
    use crate::plans::BoundColumnRef;
    use crate::plans::ConstantExpr;
    use crate::plans::FunctionCall;

    #[test]
    fn test_date_comparison_uses_column_statistics() -> Result<()> {
        let column_index = Symbol::new(0);
        let mut column_stats = ColumnStatSet::new();
        column_stats.insert(column_index, ColumnStat {
            min: Datum::Int(20),
            max: Datum::Int(30),
            ndv: StatEstimate::exact(11.0),
            null_count: StatCount::exact(0),
            histogram: None,
        });

        let predicate = ScalarExpr::FunctionCall(FunctionCall {
            span: None,
            func_name: "eq".to_string(),
            params: vec![],
            arguments: vec![
                ScalarExpr::BoundColumnRef(BoundColumnRef {
                    span: None,
                    column: ColumnBindingBuilder::new(
                        "d".to_string(),
                        column_index,
                        Box::new(DataType::Date),
                        Visibility::Visible,
                    )
                    .build(),
                }),
                ScalarExpr::TypedConstantExpr(
                    ConstantExpr {
                        span: None,
                        value: Scalar::Date(10),
                    },
                    DataType::Date,
                ),
            ],
        });

        let mut estimator =
            SelectivityEstimator::new(column_stats, StatCardinality::estimate(100.0));

        assert_eq!(estimator.apply(&[predicate])?, 0.0);
        let column_stats = estimator.into_column_stats();
        let column_stat = &column_stats[&column_index];
        assert_eq!(column_stat.ndv, StatEstimate::exact(0.0));
        assert_eq!(column_stat.null_count, StatCount::exact(0));
        Ok(())
    }

    #[test]
    fn test_string_comparison_uses_function_statistics() -> Result<()> {
        let column_index = Symbol::new(0);
        let mut column_stats = ColumnStatSet::new();
        column_stats.insert(column_index, ColumnStat {
            min: Datum::Bytes(b"b".to_vec()),
            max: Datum::Bytes(b"d".to_vec()),
            ndv: StatEstimate::exact(3.0),
            null_count: StatCount::exact(0),
            histogram: None,
        });

        let predicate = string_comparison_predicate(column_index, ComparisonOp::Equal, "a");
        let mut estimator =
            SelectivityEstimator::new(column_stats.clone(), StatCardinality::estimate(30.0));
        assert_eq!(estimator.apply(&[predicate])?, 0.0);
        let derived = estimator.into_column_stats();
        assert_eq!(derived[&column_index].ndv, StatEstimate::exact(0.0));

        let predicate = string_comparison_predicate(column_index, ComparisonOp::Equal, "c");
        let mut estimator =
            SelectivityEstimator::new(column_stats, StatCardinality::estimate(30.0));
        assert_eq!(estimator.apply(&[predicate])?, 10.0);
        Ok(())
    }

    #[test]
    fn test_decimal_comparison_uses_function_statistics() -> Result<()> {
        let column_index = Symbol::new(0);
        let decimal_size = DecimalSize::new(10, 2).unwrap();
        let mut column_stats = ColumnStatSet::new();
        column_stats.insert(column_index, ColumnStat {
            min: Datum::Float(1.0.into()),
            max: Datum::Float(3.0.into()),
            ndv: StatEstimate::exact(3.0),
            null_count: StatCount::exact(0),
            histogram: None,
        });

        let predicate =
            decimal_comparison_predicate(column_index, ComparisonOp::Equal, 400, decimal_size);
        let mut estimator =
            SelectivityEstimator::new(column_stats.clone(), StatCardinality::estimate(30.0));
        assert_eq!(estimator.apply(&[predicate])?, 0.0);

        let predicate =
            decimal_comparison_predicate(column_index, ComparisonOp::Equal, 200, decimal_size);
        let mut estimator =
            SelectivityEstimator::new(column_stats, StatCardinality::estimate(30.0));
        assert_eq!(estimator.apply(&[predicate])?, 10.0);
        Ok(())
    }

    #[test]
    fn test_uint_histogram_comparison_keeps_tail_selectivity() -> Result<()> {
        let column_index = Symbol::new(0);
        let mut column_stats = ColumnStatSet::new();
        column_stats.insert(column_index, ColumnStat {
            min: Datum::UInt(0),
            max: Datum::UInt(737),
            ndv: StatEstimate::exact(738.0),
            null_count: StatCount::exact(0),
            histogram: Some(
                HistogramBuilder::from_ndv(
                    738,
                    738,
                    Some((Datum::UInt(0), Datum::UInt(737))),
                    DEFAULT_HISTOGRAM_BUCKETS,
                )
                .unwrap(),
            ),
        });

        let predicate = uint_comparison_predicate(column_index, ComparisonOp::GT, 731);
        let mut estimator =
            SelectivityEstimator::new(column_stats.clone(), StatCardinality::estimate(738.0));

        assert!((estimator.apply(&[predicate])? - 6.0).abs() < 1e-9);

        let predicate = uint_comparison_predicate(column_index, ComparisonOp::GT, 700);
        let mut estimator =
            SelectivityEstimator::new(column_stats.clone(), StatCardinality::estimate(738.0));

        assert!((estimator.apply(&[predicate])? - 37.0).abs() < 1e-9);

        let predicate = uint_comparison_predicate(column_index, ComparisonOp::GT, 737);
        let mut estimator =
            SelectivityEstimator::new(column_stats, StatCardinality::estimate(738.0));

        assert_eq!(estimator.apply(&[predicate])?, 0.0);
        Ok(())
    }

    #[test]
    fn test_unsatisfiable_range_clears_column_distribution() -> Result<()> {
        let column_index = Symbol::new(0);
        let mut column_stats = ColumnStatSet::new();
        column_stats.insert(column_index, ColumnStat {
            min: Datum::UInt(0),
            max: Datum::UInt(5),
            ndv: StatEstimate::exact(6.0),
            null_count: StatCount::exact(2),
            histogram: Some(
                HistogramBuilder::from_ndv(
                    6,
                    6,
                    Some((Datum::UInt(0), Datum::UInt(5))),
                    DEFAULT_HISTOGRAM_BUCKETS,
                )
                .unwrap(),
            ),
        });

        let predicate = nullable_uint_comparison_predicate(column_index, ComparisonOp::GT, 10);
        let mut estimator = SelectivityEstimator::new(column_stats, StatCardinality::estimate(8.0));

        assert_eq!(estimator.apply(&[predicate])?, 0.0);
        let column_stats = estimator.into_column_stats();
        let column_stat = &column_stats[&column_index];
        assert_eq!(column_stat.ndv, StatEstimate::exact(0.0));
        assert_eq!(column_stat.null_count, StatCount::exact(0));
        assert!(column_stat.histogram.is_none());
        Ok(())
    }

    #[test]
    fn test_estimated_zero_selectivity_does_not_clear_column_distribution() -> Result<()> {
        let column_index = Symbol::new(0);
        let mut column_stats = ColumnStatSet::new();
        column_stats.insert(column_index, ColumnStat {
            min: Datum::UInt(0),
            max: Datum::UInt(5),
            ndv: StatEstimate::exact(6.0),
            null_count: StatCount::estimate(8.0, 8.0),
            histogram: Some(
                HistogramBuilder::from_ndv(
                    6,
                    6,
                    Some((Datum::UInt(0), Datum::UInt(5))),
                    DEFAULT_HISTOGRAM_BUCKETS,
                )
                .unwrap(),
            ),
        });

        let predicate = nullable_uint_is_not_null_predicate(column_index);
        let mut estimator = SelectivityEstimator::new(column_stats, StatCardinality::exact(8));

        assert_eq!(estimator.apply(&[predicate])?, 0.0);
        let column_stats = estimator.into_column_stats();
        let column_stat = &column_stats[&column_index];
        assert_ne!(column_stat.ndv, StatEstimate::exact(0.0));
        assert_ne!(column_stat.null_count, StatCount::exact(0));
        Ok(())
    }

    #[test]
    fn test_estimated_zero_cardinality_does_not_create_trusted_zero() -> Result<()> {
        let column_index = Symbol::new(0);
        let mut column_stats = ColumnStatSet::new();
        column_stats.insert(column_index, ColumnStat {
            min: Datum::UInt(0),
            max: Datum::UInt(5),
            ndv: StatEstimate::exact(6.0),
            null_count: StatCount::exact(2),
            histogram: Some(
                HistogramBuilder::from_ndv(
                    6,
                    6,
                    Some((Datum::UInt(0), Datum::UInt(5))),
                    DEFAULT_HISTOGRAM_BUCKETS,
                )
                .unwrap(),
            ),
        });

        let predicate = nullable_uint_is_not_null_predicate(column_index);
        let mut estimator = SelectivityEstimator::new(column_stats, StatCardinality::estimate(0.0));

        assert_eq!(estimator.apply(&[predicate])?, 0.0);
        let column_stats = estimator.into_column_stats();
        let column_stat = &column_stats[&column_index];
        assert_ne!(column_stat.ndv, StatEstimate::exact(0.0));
        assert_ne!(column_stat.null_count, StatCount::exact(0));
        Ok(())
    }

    fn uint_comparison_predicate(column_index: Symbol, op: ComparisonOp, value: u64) -> ScalarExpr {
        ScalarExpr::FunctionCall(FunctionCall {
            span: None,
            func_name: op.to_func_name().to_string(),
            params: vec![],
            arguments: vec![
                ScalarExpr::BoundColumnRef(BoundColumnRef {
                    span: None,
                    column: ColumnBindingBuilder::new(
                        "number".to_string(),
                        column_index,
                        Box::new(DataType::Number(NumberDataType::UInt64)),
                        Visibility::Visible,
                    )
                    .build(),
                }),
                ScalarExpr::TypedConstantExpr(
                    ConstantExpr {
                        span: None,
                        value: Scalar::Number(NumberScalar::UInt64(value)),
                    },
                    DataType::Number(NumberDataType::UInt64),
                ),
            ],
        })
    }

    fn nullable_uint_comparison_predicate(
        column_index: Symbol,
        op: ComparisonOp,
        value: u64,
    ) -> ScalarExpr {
        ScalarExpr::FunctionCall(FunctionCall {
            span: None,
            func_name: op.to_func_name().to_string(),
            params: vec![],
            arguments: vec![
                ScalarExpr::BoundColumnRef(BoundColumnRef {
                    span: None,
                    column: ColumnBindingBuilder::new(
                        "number".to_string(),
                        column_index,
                        Box::new(DataType::Nullable(Box::new(DataType::Number(
                            NumberDataType::UInt64,
                        )))),
                        Visibility::Visible,
                    )
                    .build(),
                }),
                ScalarExpr::TypedConstantExpr(
                    ConstantExpr {
                        span: None,
                        value: Scalar::Number(NumberScalar::UInt64(value)),
                    },
                    DataType::Number(NumberDataType::UInt64),
                ),
            ],
        })
    }

    fn nullable_uint_is_not_null_predicate(column_index: Symbol) -> ScalarExpr {
        ScalarExpr::FunctionCall(FunctionCall {
            span: None,
            func_name: "is_not_null".to_string(),
            params: vec![],
            arguments: vec![ScalarExpr::BoundColumnRef(BoundColumnRef {
                span: None,
                column: ColumnBindingBuilder::new(
                    "number".to_string(),
                    column_index,
                    Box::new(DataType::Nullable(Box::new(DataType::Number(
                        NumberDataType::UInt64,
                    )))),
                    Visibility::Visible,
                )
                .build(),
            })],
        })
    }

    fn string_comparison_predicate(
        column_index: Symbol,
        op: ComparisonOp,
        value: &str,
    ) -> ScalarExpr {
        ScalarExpr::FunctionCall(FunctionCall {
            span: None,
            func_name: op.to_func_name().to_string(),
            params: vec![],
            arguments: vec![
                ScalarExpr::BoundColumnRef(BoundColumnRef {
                    span: None,
                    column: ColumnBindingBuilder::new(
                        "s".to_string(),
                        column_index,
                        Box::new(DataType::String),
                        Visibility::Visible,
                    )
                    .build(),
                }),
                ScalarExpr::TypedConstantExpr(
                    ConstantExpr {
                        span: None,
                        value: Scalar::String(value.to_string()),
                    },
                    DataType::String,
                ),
            ],
        })
    }

    fn decimal_comparison_predicate(
        column_index: Symbol,
        op: ComparisonOp,
        value: i128,
        decimal_size: DecimalSize,
    ) -> ScalarExpr {
        ScalarExpr::FunctionCall(FunctionCall {
            span: None,
            func_name: op.to_func_name().to_string(),
            params: vec![],
            arguments: vec![
                ScalarExpr::BoundColumnRef(BoundColumnRef {
                    span: None,
                    column: ColumnBindingBuilder::new(
                        "dec".to_string(),
                        column_index,
                        Box::new(DataType::Decimal(decimal_size)),
                        Visibility::Visible,
                    )
                    .build(),
                }),
                ScalarExpr::TypedConstantExpr(
                    ConstantExpr {
                        span: None,
                        value: Scalar::Decimal(DecimalScalar::Decimal128(value, decimal_size)),
                    },
                    DataType::Decimal(decimal_size),
                ),
            ],
        })
    }
}
