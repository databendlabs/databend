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

use common_expression::Literal;

use crate::optimizer::Datum;
use crate::optimizer::Statistics;
use crate::plans::ComparisonExpr;
use crate::plans::ComparisonOp;
use crate::plans::ConstantExpr;
use crate::plans::Scalar;

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
    pub fn compute_selectivity(&self, predicate: &Scalar) -> f64 {
        match predicate {
            Scalar::BoundColumnRef(_) => {
                // If a column ref is on top of a predicate, e.g.
                // `SELECT * FROM t WHERE c1`, the selectivity is 1.
                return 1.0;
            }

            Scalar::ConstantExpr(constant) => {
                if is_true_constant_predicate(constant) {
                    return 1.0;
                } else {
                    return 0.0;
                }
            }

            Scalar::AndExpr(and_expr) => {
                let left_selectivity = self.compute_selectivity(&and_expr.left);
                let right_selectivity = self.compute_selectivity(&and_expr.right);
                return left_selectivity * right_selectivity;
            }

            Scalar::OrExpr(or_expr) => {
                let left_selectivity = self.compute_selectivity(&or_expr.left);
                let right_selectivity = self.compute_selectivity(&or_expr.right);
                return f64::min(left_selectivity, right_selectivity);
            }

            Scalar::ComparisonExpr(comp_expr) => {
                return self.compute_selectivity_comparison_expr(comp_expr);
            }

            _ => {}
        }

        DEFAULT_SELECTIVITY
    }

    fn compute_selectivity_comparison_expr(&self, comp_expr: &ComparisonExpr) -> f64 {
        if let (Scalar::BoundColumnRef(column_ref), Scalar::ConstantExpr(constant)) =
            (comp_expr.left.as_ref(), comp_expr.right.as_ref())
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
            let const_datum = if let Some(datum) = Datum::from_literal(&constant.value) {
                datum
            } else {
                return DEFAULT_SELECTIVITY;
            };

            match &comp_expr.op {
                ComparisonOp::Equal => {
                    // For equal predicate, we just use cardinality of a single
                    // value to estimate the selectivity. This assumes that
                    // the column is in a uniform distribution.
                    return 1.0 / col_hist.num_distinct_values();
                }
                ComparisonOp::NotEqual => {
                    // For not equal predicate, we treat it as opposite of equal predicate.
                    return 1.0 - 1.0 / col_hist.num_distinct_values();
                }
                ComparisonOp::GT => {
                    // For greater than predicate, we use the number of values
                    // that are greater than the constant value to estimate the
                    // selectivity.
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

fn is_true_constant_predicate(constant: &ConstantExpr) -> bool {
    match &constant.value {
        Literal::Null => false,
        Literal::Boolean(v) => *v,
        Literal::Int64(v) => *v != 0,
        Literal::UInt64(v) => *v != 0,
        Literal::Float64(v) => *v != 0.0,
        _ => true,
    }
}
