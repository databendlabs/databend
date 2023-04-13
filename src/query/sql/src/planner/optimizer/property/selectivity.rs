// Copyright 2022 Datafuse Labs.
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

use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::types::NumberScalar;
use common_expression::Scalar;

use crate::optimizer::ColumnStat;
use crate::optimizer::Datum;
use crate::optimizer::Histogram;
use crate::optimizer::Statistics;
use crate::plans::ComparisonOp;
use crate::plans::ConstantExpr;
use crate::plans::ScalarExpr;

/// A default selectivity factor for a predicate
/// that we cannot estimate the selectivity for it.
/// This factor comes from the paper
/// "Access Path Selection in a Relational Database Management System"
pub const DEFAULT_SELECTIVITY: f64 = 1f64 / 3f64;
pub const MAX_SELECTIVITY: f64 = 1f64;

pub struct SelectivityEstimator<'a> {
    input_stat: &'a Statistics,
}

impl<'a> SelectivityEstimator<'a> {
    pub fn new(input_stat: &'a Statistics) -> Self {
        Self { input_stat }
    }

    /// Compute the selectivity of a predicate.
    pub fn compute_selectivity(&self, predicate: &ScalarExpr) -> f64 {
        match predicate {
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
                let left_selectivity = self.compute_selectivity(&func.arguments[0]);
                let right_selectivity = self.compute_selectivity(&func.arguments[1]);
                left_selectivity * right_selectivity
            }

            ScalarExpr::FunctionCall(func) if func.func_name == "or" => {
                let left_selectivity = self.compute_selectivity(&func.arguments[0]);
                let right_selectivity = self.compute_selectivity(&func.arguments[1]);
                left_selectivity + right_selectivity - left_selectivity * right_selectivity
            }

            ScalarExpr::FunctionCall(func) if func.func_name == "not" => {
                let argument_selectivity = self.compute_selectivity(&func.arguments[0]);
                1.0 - argument_selectivity
            }

            ScalarExpr::FunctionCall(func) => {
                if let Some(op) = ComparisonOp::try_from_func_name(&func.func_name) {
                    return self.compute_selectivity_comparison_expr(
                        op,
                        &func.arguments[0],
                        &func.arguments[1],
                    );
                }

                DEFAULT_SELECTIVITY
            }

            _ => DEFAULT_SELECTIVITY,
        }
    }

    fn compute_selectivity_comparison_expr(
        &self,
        op: ComparisonOp,
        left: &ScalarExpr,
        right: &ScalarExpr,
    ) -> f64 {
        if let (ScalarExpr::BoundColumnRef(column_ref), ScalarExpr::ConstantExpr(constant)) =
            (left, right)
        {
            // Check if there is available histogram for the column.
            let column_stat =
                if let Some(stat) = self.input_stat.column_stats.get(&column_ref.column.index) {
                    stat
                } else {
                    return DEFAULT_SELECTIVITY;
                };
            let col_hist = if let Some(hist) = column_stat.histogram.as_ref() {
                hist
            } else {
                return DEFAULT_SELECTIVITY;
            };
            let const_datum = if let Some(datum) = Datum::from_scalar(&constant.value) {
                datum
            } else {
                return DEFAULT_SELECTIVITY;
            };

            match op {
                ComparisonOp::Equal => {
                    // For equal predicate, we just use cardinality of a single
                    // value to estimate the selectivity. This assumes that
                    // the column is in a uniform distribution.
                    return evaluate_equal(col_hist, column_stat, constant);
                }
                ComparisonOp::NotEqual => {
                    // For not equal predicate, we treat it as opposite of equal predicate.
                    return 1.0 - evaluate_equal(col_hist, column_stat, constant);
                }
                ComparisonOp::GT => {
                    // For greater than predicate, we use the number of values
                    // that are greater than the constant value to estimate the
                    // selectivity.
                    let mut num_greater = 0.0;
                    for bucket in col_hist.buckets_iter() {
                        if let Ok(ord) = bucket.upper_bound().compare(&const_datum) {
                            if ord == Ordering::Less || ord == Ordering::Equal {
                                num_greater += bucket.num_values();
                            } else {
                                break;
                            }
                        } else {
                            return DEFAULT_SELECTIVITY;
                        }
                    }
                    return 1.0 - num_greater / col_hist.num_values();
                }
                ComparisonOp::LT => {
                    // For less than predicate, we treat it as opposite of
                    // greater than predicate.
                    let mut num_greater = 0.0;
                    for bucket in col_hist.buckets_iter() {
                        if let Ok(ord) = bucket.upper_bound().compare(&const_datum) {
                            if ord == Ordering::Less {
                                num_greater += bucket.num_values();
                            } else {
                                break;
                            }
                        } else {
                            return DEFAULT_SELECTIVITY;
                        }
                    }
                    return num_greater / col_hist.num_values();
                }
                ComparisonOp::GTE => {
                    // Greater than or equal to predicate is similar to greater than predicate.
                    let mut num_greater = 0.0;
                    for bucket in col_hist.buckets_iter() {
                        if let Ok(ord) = bucket.upper_bound().compare(&const_datum) {
                            if ord == Ordering::Less || ord == Ordering::Equal {
                                num_greater += bucket.num_values();
                            } else {
                                break;
                            }
                        } else {
                            return DEFAULT_SELECTIVITY;
                        }
                    }
                    return 1.0 - num_greater / col_hist.num_values();
                }
                ComparisonOp::LTE => {
                    // Less than or equal to predicate is similar to less than predicate.
                    let mut num_greater = 0.0;
                    for bucket in col_hist.buckets_iter() {
                        if let Ok(ord) = bucket.upper_bound().compare(&const_datum) {
                            if ord == Ordering::Less || ord == Ordering::Equal {
                                num_greater += bucket.num_values();
                            } else {
                                break;
                            }
                        } else {
                            return DEFAULT_SELECTIVITY;
                        }
                    }
                    return num_greater / col_hist.num_values();
                }
            }
        }

        DEFAULT_SELECTIVITY
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

fn evaluate_equal(col_hist: &Histogram, column_stat: &ColumnStat, constant: &ConstantExpr) -> f64 {
    let constant_datum = Datum::from_scalar(&constant.value);
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
            | NumberDataType::Float64 => compare_equal(&constant_datum, col_hist, column_stat),
        },
        DataType::Boolean | DataType::String => {
            compare_equal(&constant_datum, col_hist, column_stat)
        }
        _ => 1.0 / col_hist.num_distinct_values(),
    }
}

fn compare_equal(datum: &Option<Datum>, col_hist: &Histogram, column_stat: &ColumnStat) -> f64 {
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

    1.0 / col_hist.num_distinct_values()
}
