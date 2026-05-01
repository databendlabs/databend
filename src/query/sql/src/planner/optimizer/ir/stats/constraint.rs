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
use databend_common_statistics::DEFAULT_HISTOGRAM_BUCKETS;
use databend_common_statistics::Datum;
use databend_common_statistics::HistogramBuilder;

use crate::optimizer::ir::ColumnStat;
use crate::plans::ComparisonOp;

pub(super) enum ValueConstraint {
    Eq(Datum),
    NotEq,
    Range {
        lower: Bound<Datum>,
        upper: Bound<Datum>,
    },
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

    fn range_bounds(&self, column_stat: &ColumnStat) -> Result<Option<(Datum, Datum)>> {
        let ValueConstraint::Range { lower, upper } = self else {
            unreachable!()
        };

        let new_min = match lower {
            Bound::Unbounded => Some(column_stat.min.clone()),
            Bound::Included(datum) | Bound::Excluded(datum) => {
                Datum::max(Some(column_stat.min.clone()), Some(datum.clone()))
            }
        };
        let new_max = match upper {
            Bound::Unbounded => Some(column_stat.max.clone()),
            Bound::Included(datum) | Bound::Excluded(datum) => {
                Datum::min(Some(column_stat.max.clone()), Some(datum.clone()))
            }
        };

        let (Some(new_min), Some(new_max)) = (new_min, new_max) else {
            return Ok(None);
        };
        if new_min.compare(&new_max)? == std::cmp::Ordering::Greater {
            return Ok(None);
        }

        Ok(Some((new_min, new_max)))
    }
}

pub(super) struct ColumnStatUpdate<'a> {
    pub(super) column_stat: &'a mut ColumnStat,
}

impl<'a> ColumnStatUpdate<'a> {
    pub(super) fn apply_constraint(
        &mut self,
        constraint: &ValueConstraint,
        selectivity: Option<f64>,
    ) -> Result<()> {
        match constraint {
            ValueConstraint::Eq(datum) => {
                *self.column_stat = ColumnStat::from_const(datum.clone());
            }
            ValueConstraint::NotEq => {
                if let Some(selectivity) = selectivity {
                    self.restrict_to_bounds(
                        self.column_stat.min.clone(),
                        self.column_stat.max.clone(),
                        selectivity,
                    )?;
                }
            }
            ValueConstraint::Range { .. } => match selectivity {
                Some(0.0) => {
                    self.column_stat.ndv = self.column_stat.ndv.reduce_by_selectivity(0.0);
                }
                Some(selectivity) if selectivity < 1.0 => {
                    if let Some((new_min, new_max)) = constraint.range_bounds(self.column_stat)? {
                        self.restrict_to_bounds(new_min, new_max, selectivity)?;
                    }
                }
                _ => {}
            },
        }

        Ok(())
    }

    pub(super) fn apply_unconstrained_filter(&mut self, selectivity: f64) {
        self.column_stat.ndv = self.column_stat.ndv.reduce_by_selectivity(selectivity);
        self.column_stat.null_count = self
            .column_stat
            .null_count
            .reduce_by_selectivity(selectivity);

        if let Some(histogram) = &mut self.column_stat.histogram {
            if histogram.accuracy() {
                // If selectivity < 0.2, most buckets are invalid and
                // the accuracy histogram can be discarded.
                // Todo: find a better way to update histogram.
                if selectivity < 0.2 {
                    self.column_stat.histogram = None;
                }
            } else if self.column_stat.ndv.expected as u64 <= 2 {
                self.column_stat.histogram = None;
            } else {
                histogram.scale_counts(selectivity);
            }
        }
    }

    fn restrict_to_bounds(
        &mut self,
        new_min: Datum,
        new_max: Datum,
        selectivity: f64,
    ) -> Result<()> {
        self.column_stat.ndv = self.column_stat.ndv.reduce_by_selectivity(selectivity);
        self.column_stat.min = new_min.clone();
        self.column_stat.max = new_max.clone();
        self.column_stat.null_count = self
            .column_stat
            .null_count
            .reduce_by_selectivity(selectivity);

        if let Some(histogram) = &self.column_stat.histogram {
            // If selectivity < 0.2, most buckets are invalid and
            // the accuracy histogram can be discarded.
            // Todo: support unfixed buckets number for histogram and prune the histogram.
            if !histogram.accuracy() || selectivity < 0.2 {
                let num_values = histogram.num_values();
                let new_num_values = (num_values * selectivity).ceil() as u64;
                let new_ndv = self.column_stat.ndv.expected as u64;
                self.column_stat.histogram = if new_ndv <= 2 {
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
}
