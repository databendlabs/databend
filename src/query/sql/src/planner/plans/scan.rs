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
use std::collections::HashSet;
use std::sync::Arc;

use databend_common_ast::ast::SampleConfig;
use databend_common_catalog::plan::InvertedIndexInfo;
use databend_common_catalog::statistics::BasicColumnStatistics;
use databend_common_catalog::table::TableStatistics;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableSchemaRef;
use databend_common_storage::Datum;
use databend_common_storage::Histogram;
use databend_common_storage::DEFAULT_HISTOGRAM_BUCKETS;
use databend_storages_common_table_meta::table::ChangeType;

use super::ScalarItem;
use crate::optimizer::ir::ColumnStat;
use crate::optimizer::ir::ColumnStatSet;
use crate::optimizer::ir::Distribution;
use crate::optimizer::ir::HistogramBuilder;
use crate::optimizer::ir::PhysicalProperty;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::RelationalProperty;
use crate::optimizer::ir::RequiredProperty;
use crate::optimizer::ir::SelectivityEstimator;
use crate::optimizer::ir::StatInfo;
use crate::optimizer::ir::Statistics as OpStatistics;
use crate::optimizer::ir::MAX_SELECTIVITY;
use crate::plans::Operator;
use crate::plans::RelOp;
use crate::plans::ScalarExpr;
use crate::plans::SortItem;
use crate::ColumnSet;
use crate::IndexType;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Prewhere {
    // columns needed to be output after prewhere scan
    pub output_columns: ColumnSet,
    // columns needed to conduct prewhere filter
    pub prewhere_columns: ColumnSet,
    // prewhere filter predicates
    pub predicates: Vec<ScalarExpr>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AggIndexInfo {
    pub index_id: u64,
    pub schema: TableSchemaRef,
    pub selection: Vec<ScalarItem>,
    pub predicates: Vec<ScalarExpr>,
    pub is_agg: bool,
    pub num_agg_funcs: usize,
}

impl AggIndexInfo {
    pub fn used_columns(&self) -> ColumnSet {
        let mut used_columns = ColumnSet::new();
        for item in self.selection.iter() {
            used_columns.extend(item.scalar.used_columns());
        }
        for pred in self.predicates.iter() {
            used_columns.extend(pred.used_columns());
        }
        used_columns
    }
}

#[derive(Clone, Debug, Default)]
pub struct Statistics {
    // statistics will be ignored in comparison and hashing
    pub table_stats: Option<TableStatistics>,
    // statistics will be ignored in comparison and hashing
    pub column_stats: HashMap<IndexType, Option<BasicColumnStatistics>>,
    pub histograms: HashMap<IndexType, Option<Histogram>>,
}

#[derive(Clone, Debug, Default)]
pub struct Scan {
    pub table_index: IndexType,
    pub columns: ColumnSet,
    pub push_down_predicates: Option<Vec<ScalarExpr>>,
    pub limit: Option<usize>,
    pub order_by: Option<Vec<SortItem>>,
    pub prewhere: Option<Prewhere>,
    pub agg_index: Option<AggIndexInfo>,
    pub change_type: Option<ChangeType>,
    // Whether to update stream columns.
    pub update_stream_columns: bool,
    pub inverted_index: Option<InvertedIndexInfo>,
    // Lazy row fetch.
    pub is_lazy_table: bool,
    pub sample: Option<SampleConfig>,
    pub scan_id: usize,

    pub statistics: Arc<Statistics>,
}

impl Scan {
    pub fn prune_columns(&self, columns: ColumnSet, prewhere: Option<Prewhere>) -> Self {
        let column_stats = self
            .statistics
            .column_stats
            .iter()
            .filter(|(col, _)| columns.contains(*col))
            .map(|(col, stat)| (*col, stat.clone()))
            .collect();

        let histograms = self
            .statistics
            .histograms
            .iter()
            .filter(|(col, _)| columns.contains(*col))
            .map(|(col, hist)| (*col, hist.clone()))
            .collect();

        Scan {
            table_index: self.table_index,
            columns,
            push_down_predicates: self.push_down_predicates.clone(),
            limit: self.limit,
            order_by: self.order_by.clone(),
            statistics: Arc::new(Statistics {
                table_stats: self.statistics.table_stats,
                column_stats,
                histograms,
            }),
            prewhere,
            agg_index: self.agg_index.clone(),
            change_type: self.change_type.clone(),
            update_stream_columns: self.update_stream_columns,
            inverted_index: self.inverted_index.clone(),
            is_lazy_table: self.is_lazy_table,
            sample: self.sample.clone(),
            scan_id: self.scan_id,
        }
    }

    pub fn set_update_stream_columns(&mut self, update_stream_columns: bool) {
        self.update_stream_columns = update_stream_columns;
    }

    fn used_columns(&self) -> ColumnSet {
        let mut used_columns = ColumnSet::new();
        if let Some(preds) = &self.push_down_predicates {
            for pred in preds.iter() {
                used_columns.extend(pred.used_columns());
            }
        }
        if let Some(prewhere) = &self.prewhere {
            used_columns.extend(prewhere.prewhere_columns.iter());
        }

        used_columns.extend(self.columns.iter());
        used_columns
    }
}

impl PartialEq for Scan {
    fn eq(&self, other: &Self) -> bool {
        self.table_index == other.table_index
            && self.columns == other.columns
            && self.push_down_predicates == other.push_down_predicates
    }
}

impl Eq for Scan {}

impl std::hash::Hash for Scan {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.table_index.hash(state);
        for column in self.columns.iter() {
            column.hash(state);
        }
        self.push_down_predicates.hash(state);
    }
}

impl Operator for Scan {
    fn rel_op(&self) -> RelOp {
        RelOp::Scan
    }

    fn arity(&self) -> usize {
        0
    }

    fn derive_relational_prop(&self, _rel_expr: &RelExpr) -> Result<Arc<RelationalProperty>> {
        Ok(Arc::new(RelationalProperty {
            output_columns: self.columns.clone(),
            outer_columns: Default::default(),
            used_columns: self.used_columns(),
            orderings: vec![],
            partition_orderings: None,
        }))
    }

    fn derive_physical_prop(&self, _rel_expr: &RelExpr) -> Result<PhysicalProperty> {
        Ok(PhysicalProperty {
            distribution: Distribution::Random,
        })
    }

    fn derive_stats(&self, _rel_expr: &RelExpr) -> Result<Arc<StatInfo>> {
        let used_columns = self.used_columns();

        let num_rows = self
            .statistics
            .table_stats
            .as_ref()
            .map(|s| s.num_rows.unwrap_or(0))
            .unwrap_or(0);

        let mut column_stats: ColumnStatSet = Default::default();
        for (k, v) in &self.statistics.column_stats {
            // No need to cal histogram for unused columns
            if !used_columns.contains(k) {
                continue;
            }
            if let Some(col_stat) = v.clone() {
                // Safe to unwrap: min, max are all `Some(_)`.
                let Some(min) = col_stat.min.clone() else {
                    continue;
                };
                let Some(max) = col_stat.max.clone() else {
                    continue;
                };
                // ndv could be `None`, we will use `num_rows - null_count` as ndv instead.
                //
                // NOTE: don't touch the original num_rows, since it will be used in other places.
                let mut ndv = col_stat
                    .ndv
                    .unwrap_or_else(|| num_rows.saturating_sub(col_stat.null_count));

                // Alter ndv based on min and max if the datum is uint or int.
                match (&max, &min) {
                    (Datum::UInt(m), Datum::UInt(n)) if m >= n => ndv = ndv.min(m - n + 1),
                    (Datum::Int(m), Datum::Int(n)) if m >= n => {
                        ndv = ndv.min(m.saturating_add(1).saturating_sub(*n) as u64)
                    }
                    _ => {
                        if max == min {
                            ndv = 1
                        }
                    }
                };

                let histogram = if let Some(histogram) = self.statistics.histograms.get(k)
                    && histogram.is_some()
                {
                    histogram.clone()
                } else {
                    let num_rows = num_rows.saturating_sub(col_stat.null_count);
                    let ndv = std::cmp::min(num_rows, ndv);
                    if num_rows != 0 {
                        HistogramBuilder::from_ndv(
                            ndv,
                            num_rows,
                            Some((min.clone(), max.clone())),
                            DEFAULT_HISTOGRAM_BUCKETS,
                        )
                        .ok()
                    } else {
                        None
                    }
                };
                let column_stat = ColumnStat {
                    min,
                    max,
                    ndv: ndv as f64,
                    null_count: col_stat.null_count,
                    histogram,
                };
                column_stats.insert(*k as IndexType, column_stat);
            }
        }

        let precise_cardinality = self
            .statistics
            .table_stats
            .as_ref()
            .and_then(|stat| stat.num_rows);

        let cardinality = match (precise_cardinality, &self.prewhere) {
            (Some(precise_cardinality), Some(ref prewhere)) => {
                let mut statistics = OpStatistics {
                    precise_cardinality: Some(precise_cardinality),
                    column_stats,
                };
                // Derive cardinality
                let mut sb = SelectivityEstimator::new(
                    &mut statistics,
                    precise_cardinality as f64,
                    HashSet::new(),
                );
                let mut selectivity = MAX_SELECTIVITY;
                for pred in prewhere.predicates.iter() {
                    // Compute selectivity for each conjunction
                    selectivity = selectivity.min(sb.compute_selectivity(pred, true)?);
                }
                // Update other columns's statistic according to selectivity.
                sb.update_other_statistic_by_selectivity(selectivity);
                column_stats = statistics.column_stats;
                (precise_cardinality as f64) * selectivity
            }
            (Some(precise_cardinality), None) => precise_cardinality as f64,
            (_, _) => 0.0,
        };

        // If prewhere is not none, we can't get precise cardinality
        let precise_cardinality = if self.prewhere.is_none() && self.sample.is_none() {
            precise_cardinality
        } else {
            None
        };
        Ok(Arc::new(StatInfo {
            cardinality,
            statistics: OpStatistics {
                precise_cardinality,
                column_stats,
            },
        }))
    }

    // Won't be invoked at all, since `PhysicalScan` is leaf node
    fn compute_required_prop_child(
        &self,
        _ctx: Arc<dyn TableContext>,
        _rel_expr: &RelExpr,
        _child_index: usize,
        _required: &RequiredProperty,
    ) -> Result<RequiredProperty> {
        Err(ErrorCode::Internal(
            "Cannot compute required property for children of scan".to_string(),
        ))
    }
}
