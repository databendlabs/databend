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

use std::ops::Bound;

use databend_common_exception::Result;
use databend_common_statistics::Datum;
use databend_common_statistics::HistogramBounds;
use databend_common_statistics::HistogramRangeBounds;
use databend_common_statistics::StatCount;
use databend_common_statistics::StatEstimate;

use crate::optimizer::ir::ColumnStat;
use crate::plans::ComparisonOp;

// A value constraint materializes a surviving AND-context predicate into column
// bounds, null counts, NDV limits, and histograms when that can be represented
// by column statistics. If a predicate cannot be represented as column stats,
// keep the stats conservative rather than proving facts here.
#[derive(Clone)]
pub(super) enum ValueConstraint {
    Eq(Datum),
    // `!=` is not a range rewrite.
    NotEq,
    NotNull,
    Range {
        lower: Bound<Datum>,
        upper: Bound<Datum>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ConstraintContext {
    And,
    Or,
    Not,
}

impl ValueConstraint {
    pub(super) fn from_comparison(op: ComparisonOp, datum: Datum) -> Self {
        match op {
            ComparisonOp::Equal => ValueConstraint::Eq(datum),
            ComparisonOp::NotEqual => ValueConstraint::NotEq,
            ComparisonOp::GT => ValueConstraint::Range {
                lower: Bound::Excluded(datum),
                upper: Bound::Unbounded,
            },
            ComparisonOp::GTE => ValueConstraint::Range {
                lower: Bound::Included(datum),
                upper: Bound::Unbounded,
            },
            ComparisonOp::LT => ValueConstraint::Range {
                lower: Bound::Unbounded,
                upper: Bound::Excluded(datum),
            },
            ComparisonOp::LTE => ValueConstraint::Range {
                lower: Bound::Unbounded,
                upper: Bound::Included(datum),
            },
        }
    }

    pub(super) fn apply(&self, column_stat: &mut ColumnStat) -> Result<()> {
        match self {
            ValueConstraint::NotNull => {
                column_stat.null_count = StatCount::exact(0);
            }
            ValueConstraint::Eq(datum) => {
                let bounds = HistogramBounds::new(column_stat.min.clone(), column_stat.max.clone());
                if contains_bounds_datum(&bounds, datum)? {
                    *column_stat = ColumnStat::from_const(datum.clone());
                } else {
                    clear_for_empty_result(column_stat);
                }
            }
            ValueConstraint::NotEq => {}
            ValueConstraint::Range { lower, upper } => {
                let bounds = HistogramBounds::from_range_constraint(
                    &column_stat.min,
                    &column_stat.max,
                    lower,
                    upper,
                )?;
                match bounds {
                    HistogramRangeBounds::Bounds(bounds) => {
                        apply_range_bounds(
                            column_stat,
                            bounds.lower_bound().clone(),
                            bounds.upper_bound().clone(),
                        )?;
                    }
                    HistogramRangeBounds::Empty => {
                        clear_for_empty_result(column_stat);
                    }
                    HistogramRangeBounds::Imprecise => {}
                }
            }
        }
        Ok(())
    }

    pub(super) fn apply_all(
        input_stat: &ColumnStat,
        constraints: &[ValueConstraint],
    ) -> Result<ColumnStat> {
        let mut column_stat = input_stat.clone();

        for constraint in constraints {
            constraint.apply(&mut column_stat)?;
        }

        Ok(column_stat)
    }
}

fn contains_bounds_datum(bounds: &HistogramBounds, datum: &Datum) -> Result<bool> {
    Ok(
        bounds.lower_bound().compare(datum)? != std::cmp::Ordering::Greater
            && bounds.upper_bound().compare(datum)? != std::cmp::Ordering::Less,
    )
}

fn apply_range_bounds(column_stat: &mut ColumnStat, new_min: Datum, new_max: Datum) -> Result<()> {
    column_stat.min = new_min.clone();
    column_stat.max = new_max.clone();
    column_stat.null_count = StatCount::exact(0);
    if let Some(ndv_upper) = finite_range_ndv_upper(&new_min, &new_max) {
        column_stat.ndv = column_stat.ndv.reduce(ndv_upper);
    }

    if let Some(histogram) = &column_stat.histogram {
        let restricted_histogram = histogram.restrict_to_bounds(&new_min, &new_max)?;
        if let Some(histogram) = &restricted_histogram {
            column_stat.refine_ndv_from_histogram(histogram);
        }
        column_stat.histogram = restricted_histogram;
    }

    Ok(())
}

fn finite_range_ndv_upper(min: &Datum, max: &Datum) -> Option<f64> {
    if min == max {
        return Some(1.0);
    }
    match (min, max) {
        (Datum::Bool(false), Datum::Bool(true)) => Some(2.0),
        (Datum::Int(min), Datum::Int(max)) => max
            .checked_sub(*min)
            .and_then(|diff| diff.checked_add(1))
            .map(|value| value as f64),
        (Datum::UInt(min), Datum::UInt(max)) => max
            .checked_sub(*min)
            .and_then(|diff| diff.checked_add(1))
            .map(|value| value as f64),
        _ => None,
    }
}

pub(super) fn clear_for_empty_result(column_stat: &mut ColumnStat) {
    column_stat.ndv = StatEstimate::exact(0.0);
    column_stat.null_count = StatCount::exact(0);
    column_stat.histogram = None;
}
