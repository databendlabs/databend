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

use std::cmp::max;
use std::cmp::Ordering;
use std::collections::HashSet;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::type_check;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::ConstantFolder;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_storage::{Datum, DEFAULT_HISTOGRAM_BUCKETS};
use databend_common_storage::F64;

use crate::optimizer::histogram_from_ndv;
use crate::optimizer::ColumnStat;
use crate::optimizer::Statistics;
use crate::optimizer::DEFAULT_HISTOGRAM_BUCKETS;
use crate::plans::BoundColumnRef;
use crate::plans::ComparisonOp;
use crate::plans::ConstantExpr;
use crate::plans::FunctionCall;
use crate::plans::ScalarExpr;
use crate::IndexType;

/// A default selectivity factor for a predicate
/// that we cannot estimate the selectivity for it.
/// This factor comes from the paper
/// "Access Path Selection in a Relational Database Management System"
pub const DEFAULT_SELECTIVITY: f64 = 1f64 / 5f64;
pub const SMALL_SELECTIVITY: f64 = 1f64 / 2500f64;
pub const MAX_SELECTIVITY: f64 = 1f64;

/// Some constants for like predicate selectivity estimation.
const FIXED_CHAR_SEL: f64 = 0.5;
const ANY_CHAR_SEL: f64 = 0.9; // not 1, since it won't match end-of-string
const FULL_WILDCARD_SEL: f64 = 2.0;

pub struct SelectivityEstimator<'a> {
    pub input_stat: &'a mut Statistics,
    pub updated_column_indexes: HashSet<IndexType>,
}

impl<'a> SelectivityEstimator<'a> {
    pub fn new(input_stat: &'a mut Statistics, updated_column_indexes: HashSet<IndexType>) -> Self {
        Self {
            input_stat,
            updated_column_indexes,
        }
    }

    /// Compute the selectivity of a predicate.
    pub fn compute_selectivity(&mut self, predicate: &ScalarExpr, update: bool) -> Result<f64> {
        Ok(match predicate {
            ScalarExpr::BoundColumnRef(_) => {
                // If a column ref is on top of a predicate, e.g.
                // `SELECT * FROM t WHERE c1`, the selectivity is 1.
                1.0
            }

            ScalarExpr::ConstantExpr(constant) => {
                if is_true_constant_predicate(constant) {
                    1.0
                } else {
                    0.0
                }
            }

            ScalarExpr::FunctionCall(func) if func.func_name == "and" => {
                let left_selectivity = self.compute_selectivity(&func.arguments[0], update)?;
                let right_selectivity = self.compute_selectivity(&func.arguments[1], update)?;
                left_selectivity.min(right_selectivity)
            }

            ScalarExpr::FunctionCall(func) if func.func_name == "or" => {
                let left_selectivity = self.compute_selectivity(&func.arguments[0], false)?;
                let right_selectivity = self.compute_selectivity(&func.arguments[1], false)?;
                left_selectivity + right_selectivity - left_selectivity * right_selectivity
            }

            ScalarExpr::FunctionCall(func) if func.func_name == "not" => {
                let argument_selectivity = self.compute_selectivity(&func.arguments[0], false)?;
                1.0 - argument_selectivity
            }

            ScalarExpr::FunctionCall(func) => {
                if func.func_name.eq("like") {
                    return self.compute_like_selectivity(func);
                }
                if let Some(op) = ComparisonOp::try_from_func_name(&func.func_name) {
                    return self.compute_selectivity_comparison_expr(
                        op,
                        &func.arguments[0],
                        &func.arguments[1],
                        update,
                    );
                }

                DEFAULT_SELECTIVITY
            }

            _ => DEFAULT_SELECTIVITY,
        })
    }

    // The method uses probability predication to compute like selectivity.
    // The core idea is from postgresql.
    fn compute_like_selectivity(&mut self, func: &FunctionCall) -> Result<f64> {
        let right = &func.arguments[1];
        if let ScalarExpr::ConstantExpr(ConstantExpr {
            value: Scalar::String(patt),
            ..
        }) = right
        {
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
            Ok(sel)
        } else {
            Ok(DEFAULT_SELECTIVITY)
        }
    }

    fn compute_selectivity_comparison_expr(
        &mut self,
        mut op: ComparisonOp,
        left: &ScalarExpr,
        right: &ScalarExpr,
        update: bool,
    ) -> Result<f64> {
        match (left, right) {
            (ScalarExpr::BoundColumnRef(column_ref), ScalarExpr::ConstantExpr(constant))
            | (ScalarExpr::ConstantExpr(constant), ScalarExpr::BoundColumnRef(column_ref)) => {
                // Check if there is available histogram for the column.
                let column_stat = if let Some(stat) = self
                    .input_stat
                    .column_stats
                    .get_mut(&column_ref.column.index)
                {
                    stat
                } else {
                    // The column is derived column, give a small selectivity currently.
                    // Need to improve it later.
                    // Another case: column is from system table, such as numbers. We shouldn't use numbers() table to test cardinality estimation.
                    return Ok(SMALL_SELECTIVITY);
                };
                let const_datum = if let Some(datum) = Datum::from_scalar(constant.value.clone()) {
                    datum
                } else {
                    return Ok(DEFAULT_SELECTIVITY);
                };

                return match op {
                    ComparisonOp::Equal => {
                        // For equal predicate, we just use cardinality of a single
                        // value to estimate the selectivity. This assumes that
                        // the column is in a uniform distribution.
                        let selectivity = evaluate_equal(column_stat, constant);
                        if update {
                            update_statistic(
                                column_stat,
                                const_datum.clone(),
                                const_datum,
                                selectivity,
                            )?;
                            self.updated_column_indexes.insert(column_ref.column.index);
                        }
                        Ok(selectivity)
                    }
                    ComparisonOp::NotEqual => {
                        // For not equal predicate, we treat it as opposite of equal predicate.
                        let selectivity = 1.0 - evaluate_equal(column_stat, constant);
                        if update {
                            update_statistic(
                                column_stat,
                                column_stat.min.clone(),
                                column_stat.max.clone(),
                                selectivity,
                            )?;
                            self.updated_column_indexes.insert(column_ref.column.index);
                        }
                        Ok(selectivity)
                    }
                    _ => {
                        if let ScalarExpr::ConstantExpr(_) = left {
                            op = op.reverse();
                        }
                        Self::compute_binary_comparison_selectivity(
                            &op,
                            &const_datum,
                            update,
                            column_ref,
                            column_stat,
                            &mut self.updated_column_indexes,
                        )
                    }
                };
            }
            (ScalarExpr::ConstantExpr(_), ScalarExpr::ConstantExpr(_)) => {
                // TODO: constant folding in the optimizer.
                let scalar_expr = ScalarExpr::FunctionCall(FunctionCall {
                    span: None,
                    func_name: op.to_func_name().to_string(),
                    params: vec![],
                    arguments: vec![left.clone(), right.clone()],
                });
                let raw_expr = scalar_expr.as_raw_expr();
                let expr = type_check::check(&raw_expr, &BUILTIN_FUNCTIONS)?;
                let (expr, _) =
                    ConstantFolder::fold(&expr, &FunctionContext::default(), &BUILTIN_FUNCTIONS);
                if let Expr::Constant {
                    scalar: Scalar::Boolean(v),
                    ..
                } = expr
                {
                    return if v { Ok(1.0) } else { Ok(0.0) };
                }
            }
            (ScalarExpr::FunctionCall(func), ScalarExpr::ConstantExpr(val)) => {
                if op == ComparisonOp::Equal && func.func_name == "modulo" {
                    let mod_number = &func.arguments[1];
                    if let ScalarExpr::ConstantExpr(mod_num) = mod_number {
                        let mod_num = Datum::from_scalar(mod_num.value.clone());
                        if let Some(mod_num) = mod_num {
                            let mod_num = mod_num.to_double()?;
                            if mod_num == 0.0 {
                                return Err(ErrorCode::SemanticError(
                                    "modulus by zero".to_string(),
                                ));
                            }
                            if let Some(remainder) = Datum::from_scalar(val.value.clone()) {
                                let remainder = remainder.to_double()?;
                                if remainder >= mod_num {
                                    return Ok(0.0);
                                }
                            }
                            return Ok(1.0 / mod_num);
                        }
                    }
                }
            }
            _ => (),
        }

        Ok(DEFAULT_SELECTIVITY)
    }

    // Update other columns' statistic according to selectivity.
    pub fn update_other_statistic_by_selectivity(&mut self, selectivity: f64) {
        for (index, column_stat) in self.input_stat.column_stats.iter_mut() {
            if !self.updated_column_indexes.contains(index) {
                let new_ndv = (column_stat.ndv * selectivity).ceil();
                column_stat.ndv = new_ndv;
                if let Some(histogram) = &mut column_stat.histogram {
                    if new_ndv as u64 <= 2 {
                        column_stat.histogram = None;
                    } else {
                        for bucket in histogram.buckets.iter_mut() {
                            bucket.update(selectivity);
                        }
                    }
                }
            }
        }
    }

    fn compute_binary_comparison_selectivity(
        comparison_op: &ComparisonOp,
        const_datum: &Datum,
        update: bool,
        column_ref: &BoundColumnRef,
        column_stat: &mut ColumnStat,
        updated_column_indexes: &mut HashSet<IndexType>,
    ) -> Result<f64> {
        let col_hist = column_stat.histogram.as_ref();

        if col_hist.is_none() && const_datum.is_numeric() {
            let min = column_stat.min.to_double()?;
            let max = column_stat.max.to_double()?;
            let ndv = column_stat.ndv;
            let numeric_literal = const_datum.to_double()?;

            let (no_overlap, complete_overlap) = match comparison_op {
                ComparisonOp::LT => (numeric_literal <= min, numeric_literal > max),
                ComparisonOp::LTE => (numeric_literal < min, numeric_literal >= max),
                ComparisonOp::GT => (numeric_literal >= max, numeric_literal < min),
                ComparisonOp::GTE => (numeric_literal > max, numeric_literal <= min),
                _ => unreachable!(),
            };

            let percent = if no_overlap {
                0.0
            } else if complete_overlap {
                1.0
            } else {
                match comparison_op {
                    ComparisonOp::LT => {
                        if numeric_literal == max {
                            1.0 - 1.0 / ndv
                        } else {
                            (numeric_literal - min) / (max - min)
                        }
                    }
                    ComparisonOp::LTE => {
                        if numeric_literal == min {
                            1.0 / ndv
                        } else {
                            (numeric_literal - min) / (max - min)
                        }
                    }
                    ComparisonOp::GT => {
                        if numeric_literal == min {
                            1.0 - 1.0 / ndv
                        } else {
                            (max - numeric_literal) / (max - min)
                        }
                    }
                    ComparisonOp::GTE => {
                        if numeric_literal == max {
                            1.0 / ndv
                        } else {
                            (max - numeric_literal) / (max - min)
                        }
                    }
                    _ => unreachable!(),
                }
            };

            if update {
                let new_min = if matches!(comparison_op, ComparisonOp::GT | ComparisonOp::GTE) {
                    const_datum.clone()
                } else {
                    column_stat.min.clone()
                };

                let new_max = if matches!(comparison_op, ComparisonOp::LT | ComparisonOp::LTE) {
                    const_datum.clone()
                } else {
                    column_stat.max.clone()
                };

                update_statistic(column_stat, new_min, new_max, percent)?;
                updated_column_indexes.insert(column_ref.column.index);
            }

            return Ok(percent);
        }

        if col_hist.is_none() {
            return Ok(DEFAULT_SELECTIVITY);
        }
        let col_hist = col_hist.unwrap();
        let (mut num_greater, new_min, new_max) = match comparison_op {
            ComparisonOp::GT | ComparisonOp::GTE => {
                let new_min = const_datum.clone();
                let new_max = column_stat.max.clone();
                (0.0, new_min, new_max)
            }
            ComparisonOp::LT | ComparisonOp::LTE => {
                let new_max = const_datum.clone();
                let new_min = column_stat.min.clone();
                (0.0, new_min, new_max)
            }
            _ => unreachable!(),
        };

        for bucket in col_hist.buckets_iter() {
            if let Ok(ord) = bucket.upper_bound().compare(const_datum) {
                match comparison_op {
                    ComparisonOp::GT => {
                        if ord == Ordering::Less || ord == Ordering::Equal {
                            num_greater += bucket.num_values();
                        } else {
                            break;
                        }
                    }
                    ComparisonOp::GTE => {
                        if ord == Ordering::Less {
                            num_greater += bucket.num_values();
                        } else {
                            break;
                        }
                    }
                    ComparisonOp::LT => {
                        if ord == Ordering::Less {
                            num_greater += bucket.num_values();
                        } else {
                            break;
                        }
                    }
                    ComparisonOp::LTE => {
                        if ord == Ordering::Less || ord == Ordering::Equal {
                            num_greater += bucket.num_values();
                        } else {
                            break;
                        }
                    }
                    _ => unreachable!(),
                }
            } else {
                return Ok(DEFAULT_SELECTIVITY);
            }
        }

        let selectivity = match comparison_op {
            ComparisonOp::GT | ComparisonOp::GTE => 1.0 - num_greater / col_hist.num_values(),
            ComparisonOp::LT | ComparisonOp::LTE => num_greater / col_hist.num_values(),
            _ => unreachable!(),
        };

        if update {
            update_statistic(column_stat, new_min, new_max, selectivity)?;
            updated_column_indexes.insert(column_ref.column.index);
        }

        Ok(selectivity)
    }
}

// TODO(andylokandy): match on non-null boolean only once we have constant folding in the optimizer.
fn is_true_constant_predicate(constant: &ConstantExpr) -> bool {
    match &constant.value {
        Scalar::Null => false,
        Scalar::Boolean(v) => *v,
        Scalar::Number(NumberScalar::Int64(v)) => *v != 0,
        Scalar::Number(NumberScalar::UInt64(v)) => *v != 0,
        Scalar::Number(NumberScalar::Float64(v)) => *v != 0.0,
        _ => true,
    }
}

fn evaluate_equal(column_stat: &ColumnStat, constant: &ConstantExpr) -> f64 {
    let constant_datum = Datum::from_scalar(constant.value.clone());
    match constant.value.as_ref().infer_data_type() {
        DataType::Null => 0.0,
        DataType::Number(number) => match number {
            NumberDataType::UInt8
            | NumberDataType::UInt16
            | NumberDataType::UInt32
            | NumberDataType::UInt64
            | NumberDataType::Int8
            | NumberDataType::Int16
            | NumberDataType::Int32
            | NumberDataType::Int64
            | NumberDataType::Float32
            | NumberDataType::Float64 => compare_equal(&constant_datum, column_stat),
        },
        DataType::Boolean | DataType::Binary | DataType::String => {
            compare_equal(&constant_datum, column_stat)
        }
        _ => {
            if column_stat.ndv == 0.0 {
                0.0
            } else {
                1.0 / column_stat.ndv
            }
        }
    }
}

fn compare_equal(datum: &Option<Datum>, column_stat: &ColumnStat) -> f64 {
    let col_min = &column_stat.min;
    let col_max = &column_stat.max;
    if let Some(constant_datum) = datum {
        if col_min.type_comparable(constant_datum) {
            // Safe to unwrap, because type is comparable.
            if constant_datum.compare(col_min).unwrap() == Ordering::Less
                || constant_datum.compare(col_max).unwrap() == Ordering::Greater
            {
                return 0.0;
            }
        }
    }

    if column_stat.ndv == 0.0 {
        0.0
    } else {
        1.0 / column_stat.ndv
    }
}

fn update_statistic(
    column_stat: &mut ColumnStat,
    mut new_min: Datum,
    mut new_max: Datum,
    selectivity: f64,
) -> Result<()> {
    let new_ndv = (column_stat.ndv * selectivity).ceil();
    column_stat.ndv = new_ndv;
    if matches!(
        new_min,
        Datum::Bool(_) | Datum::Int(_) | Datum::UInt(_) | Datum::Float(_)
    ) {
        new_min = Datum::Float(F64::from(new_min.to_double()?));
        new_max = Datum::Float(F64::from(new_max.to_double()?));
    }
    column_stat.min = new_min.clone();
    column_stat.max = new_max.clone();
    if let Some(histogram) = &column_stat.histogram {
        let num_values = histogram.num_values();
        let new_num_values = (num_values * selectivity).ceil() as u64;
        let new_ndv = new_ndv as u64;
        if new_ndv <= 2 {
            column_stat.histogram = None;
            return Ok(());
        }
        column_stat.histogram = Some(histogram_from_ndv(
            new_ndv,
            max(new_num_values, new_ndv),
            Some((new_min, new_max)),
            DEFAULT_HISTOGRAM_BUCKETS,
        )?);
    }
    Ok(())
}
