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
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberScalar;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_statistics::DEFAULT_HISTOGRAM_BUCKETS;
use databend_common_statistics::Datum;

use crate::ColumnBinding;
use crate::Symbol;
use crate::optimizer::ir::ColumnStat;
use crate::optimizer::ir::ColumnStatSet;
use crate::optimizer::ir::HistogramBuilder;
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
                if histogram.accuracy() {
                    // If selectivity < 0.2, most buckets are invalid and
                    // the accuracy histogram can be discarded.
                    // Todo: find a better way to update histogram.
                    if selectivity < 0.2 {
                        column_stat.histogram = None;
                    }
                } else if column_stat.ndv.value() as u64 <= 2 {
                    column_stat.histogram = None;
                } else {
                    histogram.scale_counts(selectivity);
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
                let Some(_) = self.get_column_stat(column_index) else {
                    // The column is derived column, give a small selectivity currently.
                    // Need to improve it later.
                    // Another case: column is from system table, such as numbers. We shouldn't use numbers() table to test cardinality estimation.
                    return Ok(Selectivity::LowerBound);
                };
                let Some(const_datum) = constant.scalar.clone().to_datum() else {
                    return Ok(Selectivity::Unknown);
                };
                let op = if left.is_constant() { op.reverse() } else { op };
                let selectivity = self.derive_function_selectivity(func)?;
                let column_stat = self
                    .ensure_column_stat(column_index)
                    .expect("checked above");
                Self::update_comparison_column_stat(column_stat, op, const_datum, selectivity)?;
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
                        Ok(Selectivity::N(0.0))
                    } else {
                        Ok(Selectivity::N(1.0 / mod_num))
                    };
                }
            }
            (Expr::FunctionCall(_), Expr::Constant(_))
            | (Expr::Constant(_), Expr::FunctionCall(_)) => {
                return self.derive_function_selectivity(func);
            }
            _ => (),
        }

        Ok(Selectivity::Unknown)
    }

    fn derive_function_selectivity(&self, func: &ExprCall) -> Result<Selectivity> {
        if self.cardinality == 0.0 {
            return Ok(Selectivity::N(0.0));
        }
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
        Ok(Selectivity::N(distr.true_count.expected / self.cardinality))
    }

    fn update_comparison_column_stat(
        column_stat: &mut ColumnStat,
        op: ComparisonOp,
        const_datum: Datum,
        selectivity: Selectivity,
    ) -> Result<()> {
        match op {
            ComparisonOp::Equal => {
                *column_stat = ColumnStat::from_const(const_datum);
                Ok(())
            }
            ComparisonOp::NotEqual => {
                if let Selectivity::N(n) = selectivity {
                    update_statistic(
                        column_stat,
                        column_stat.min.clone(),
                        column_stat.max.clone(),
                        n,
                    )?;
                }
                Ok(())
            }
            _ => {
                match selectivity {
                    Selectivity::N(0.0) => {
                        column_stat.ndv = column_stat.ndv.reduce_by_selectivity(0.0);
                    }
                    Selectivity::N(n) if n < 1.0 => {
                        if let Some((new_min, new_max)) =
                            comparison_range_bounds(column_stat, &const_datum, op)?
                        {
                            update_statistic(column_stat, new_min, new_max, n)?;
                        }
                    }
                    _ => {}
                }
                Ok(())
            }
        }
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

    fn get_column_stat(&self, index: Symbol) -> Option<&ColumnStat> {
        self.overrides
            .get(&index)
            .or_else(|| self.column_stats.get(&index))
    }

    fn ensure_column_stat(&mut self, index: Symbol) -> Option<&mut ColumnStat> {
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

fn comparison_range_bounds(
    column_stat: &ColumnStat,
    const_datum: &Datum,
    op: ComparisonOp,
) -> Result<Option<(Datum, Datum)>> {
    let (new_min, new_max) = match op {
        ComparisonOp::GT | ComparisonOp::GTE => (
            Datum::max(Some(column_stat.min.clone()), Some(const_datum.clone())),
            Some(column_stat.max.clone()),
        ),
        ComparisonOp::LT | ComparisonOp::LTE => (
            Some(column_stat.min.clone()),
            Datum::min(Some(column_stat.max.clone()), Some(const_datum.clone())),
        ),
        _ => unreachable!(),
    };
    let (Some(new_min), Some(new_max)) = (new_min, new_max) else {
        return Ok(None);
    };
    if new_min.compare(&new_max)? == std::cmp::Ordering::Greater {
        return Ok(None);
    }

    Ok(Some((new_min, new_max)))
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
        if !histogram.accuracy() || selectivity < 0.2 {
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

#[cfg(test)]
mod tests {
    use databend_common_expression::Scalar;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::NumberScalar;

    use super::*;
    use crate::ColumnBindingBuilder;
    use crate::Visibility;
    use crate::optimizer::ir::Ndv;
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
            ndv: Ndv::Stat(11.0),
            null_count: 0,
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

        let mut estimator = SelectivityEstimator::new(column_stats, 100.0);

        assert_eq!(estimator.apply(&[predicate])?, 0.0);
        Ok(())
    }

    #[test]
    fn test_uint_histogram_comparison_keeps_tail_selectivity() -> Result<()> {
        let column_index = Symbol::new(0);
        let mut column_stats = ColumnStatSet::new();
        column_stats.insert(column_index, ColumnStat {
            min: Datum::UInt(0),
            max: Datum::UInt(737),
            ndv: Ndv::Stat(738.0),
            null_count: 0,
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

        let predicate = uint_comparison_predicate(column_index, ComparisonOp::GT, 700);
        let mut estimator = SelectivityEstimator::new(column_stats.clone(), 738.0);

        assert!(estimator.apply(&[predicate])? > 0.0);

        let predicate = uint_comparison_predicate(column_index, ComparisonOp::GT, 737);
        let mut estimator = SelectivityEstimator::new(column_stats, 738.0);

        assert_eq!(estimator.apply(&[predicate])?, 0.0);
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
}
