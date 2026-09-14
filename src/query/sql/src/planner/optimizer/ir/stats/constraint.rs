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
use databend_common_statistics::NdvEstimate;
use databend_common_statistics::StatBounds;
use databend_common_statistics::StatCount;
use databend_common_statistics::StatRangeBounds;

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
            op => {
                let (lower, upper) = op.range_bounds(datum).unwrap();
                ValueConstraint::Range { lower, upper }
            }
        }
    }

    pub(super) fn apply(&self, column_stat: &mut ColumnStat) -> Result<()> {
        match self {
            ValueConstraint::NotNull => {
                column_stat.set_null_count(StatCount::exact(0));
            }
            ValueConstraint::Eq(datum) => {
                if column_stat
                    .bounds()
                    .is_some_and(|bounds| bounds.contains_datum(datum))
                {
                    *column_stat = ColumnStat::from_const(datum.clone());
                } else {
                    clear_for_empty_result(column_stat);
                }
            }
            ValueConstraint::NotEq => {}
            ValueConstraint::Range { lower, upper } => {
                let Some(bounds) = column_stat.bounds() else {
                    clear_for_empty_result(column_stat);
                    return Ok(());
                };
                let bounds = bounds.restrict_by_range(lower, upper);
                match bounds {
                    StatRangeBounds::Bounds(bounds) => {
                        apply_range_bounds(column_stat, bounds)?;
                    }
                    StatRangeBounds::Empty => {
                        clear_for_empty_result(column_stat);
                    }
                    StatRangeBounds::Imprecise => {}
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

fn apply_range_bounds(column_stat: &mut ColumnStat, bounds: StatBounds) -> Result<()> {
    if let ColumnStat::AllNull { null_count } = column_stat {
        *null_count = StatCount::exact(0);
        return Ok(());
    }
    column_stat
        .restrict_to_bounds(bounds)
        .map_err(databend_common_exception::ErrorCode::Internal)?;
    column_stat.set_null_count(StatCount::exact(0));

    Ok(())
}

pub(super) fn clear_for_empty_result(column_stat: &mut ColumnStat) {
    column_stat.set_ndv(NdvEstimate::exact(0.0));
    column_stat.set_null_count(StatCount::exact(0));
    column_stat.clear_histogram();
}
