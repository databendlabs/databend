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
use std::sync::Arc;

use databend_common_ast::ast::SampleConfig;
use databend_common_catalog::plan::InvertedIndexInfo;
use databend_common_catalog::plan::VectorIndexInfo;
use databend_common_catalog::statistics::BasicColumnStatistics;
use databend_common_catalog::table::TableStatistics;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableSchemaRef;
use databend_common_statistics::DEFAULT_HISTOGRAM_BUCKETS;
use databend_common_statistics::Datum;
use databend_common_statistics::Histogram;
use databend_storages_common_table_meta::table::ChangeType;

use super::ScalarItem;
use crate::ColumnSet;
use crate::IndexType;
use crate::Symbol;
use crate::optimizer::ir::ColumnStat;
use crate::optimizer::ir::ColumnStatSet;
use crate::optimizer::ir::Distribution;
use crate::optimizer::ir::HistogramBuilder;
use crate::optimizer::ir::Ndv;
use crate::optimizer::ir::PhysicalProperty;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::RelationalProperty;
use crate::optimizer::ir::RequiredProperty;
use crate::optimizer::ir::SelectivityEstimator;
use crate::optimizer::ir::StatInfo;
use crate::optimizer::ir::Statistics as OpStatistics;
use crate::plans::Operator;
use crate::plans::RelOp;
use crate::plans::ScalarExpr;
use crate::plans::SortItem;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
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
    pub column_stats: HashMap<Symbol, Option<BasicColumnStatistics>>,
    pub histograms: HashMap<Symbol, Option<Histogram>>,
}

#[derive(Clone, Debug, Default)]
pub struct Scan {
    pub table_index: IndexType,
    pub columns: ColumnSet,
    pub push_down_predicates: Option<Vec<ScalarExpr>>,
    /// Row Access Policy predicates. Set during binding when a table has an
    /// associated RAP. Carried through the optimizer to physical plan building,
    /// where they are constant-folded and enforced by a Filter [SECURE] pipeline
    /// operator. Not pushed down to storage.
    pub secure_predicates: Option<Vec<ScalarExpr>>,
    pub limit: Option<usize>,
    pub order_by: Option<Vec<SortItem>>,
    pub prewhere: Option<Prewhere>,
    pub agg_index: Option<AggIndexInfo>,
    pub change_type: Option<ChangeType>,
    // Whether to update stream columns.
    pub update_stream_columns: bool,
    pub inverted_index: Option<InvertedIndexInfo>,
    pub vector_index: Option<VectorIndexInfo>,
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
            secure_predicates: self.secure_predicates.clone(),
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
            vector_index: self.vector_index.clone(),
            is_lazy_table: self.is_lazy_table,
            sample: self.sample.clone(),
            scan_id: self.scan_id,
        }
    }

    /// Create a derived scan for decorrelation with new columns and scan_id.
    /// Only query-semantic read-path metadata (sample, indexes, security quals) is preserved.
    /// Everything else is explicitly defaulted — the derived scan sits on the
    /// build side of a decorrelated SEMI/ANTI join, not the mutation target.
    pub fn derive_decorrelated_scan(&self, columns: ColumnSet, scan_id: usize) -> Self {
        Scan {
            table_index: self.table_index,
            columns,
            scan_id,
            // Read-path metadata: preserve from the original scan.
            sample: self.sample.clone(),
            inverted_index: self.inverted_index.clone(),
            vector_index: self.vector_index.clone(),
            // Security fields: must be preserved (binding-phase, not optimizer annotation).
            // Note: column references inside secure_predicates are NOT remapped here.
            // The caller (clone_outer_scan) is responsible for remap if needed.
            secure_predicates: self.secure_predicates.clone(),
            // Everything else: explicit default.
            change_type: None,
            update_stream_columns: false,
            is_lazy_table: false,
            push_down_predicates: None,
            limit: None,
            order_by: None,
            prewhere: None,
            agg_index: None,
            statistics: Arc::new(Statistics::default()),
        }
    }

    pub fn set_update_stream_columns(&mut self, update_stream_columns: bool) {
        self.update_stream_columns = update_stream_columns;
    }

    pub(crate) fn used_columns(&self) -> ColumnSet {
        let mut used_columns = ColumnSet::new();
        if let Some(preds) = &self.push_down_predicates {
            for pred in preds.iter() {
                used_columns.extend(pred.used_columns());
            }
        }
        if let Some(preds) = &self.secure_predicates {
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

fn reduce_ndv_by_datum_range(ndv: Ndv, min: &Datum, max: &Datum) -> Ndv {
    match (max, min) {
        (Datum::UInt(m), Datum::UInt(n)) if m >= n => {
            ndv.reduce(m.saturating_sub(*n).saturating_add(1) as _)
        }
        (Datum::Int(m), Datum::Int(n)) if m >= n => {
            ndv.reduce(m.saturating_add(1).saturating_sub(*n) as _)
        }
        _ if max == min => Ndv::Stat(1.0),
        _ => ndv,
    }
}

impl PartialEq for Scan {
    fn eq(&self, other: &Self) -> bool {
        self.table_index == other.table_index
            && self.columns == other.columns
            && self.push_down_predicates == other.push_down_predicates
            && self.secure_predicates == other.secure_predicates
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
        self.secure_predicates.hash(state);
    }
}

impl Operator for Scan {
    fn rel_op(&self) -> RelOp {
        RelOp::Scan
    }

    fn scalar_expr_iter(&self) -> Box<dyn Iterator<Item = &ScalarExpr> + '_> {
        let push_down_iter = self.push_down_predicates.iter().flatten();

        let secure_predicates_iter = self.secure_predicates.iter().flatten();

        let prewhere_iter = self
            .prewhere
            .iter()
            .flat_map(|prewhere| prewhere.predicates.iter());

        let agg_index_pred_iter = self
            .agg_index
            .iter()
            .flat_map(|agg_index| agg_index.predicates.iter());

        let agg_index_selection_iter = self
            .agg_index
            .iter()
            .flat_map(|agg_index| agg_index.selection.iter())
            .map(|selection| &selection.scalar);

        // Chain all iterators together
        Box::new(
            push_down_iter
                .chain(secure_predicates_iter)
                .chain(prewhere_iter)
                .chain(agg_index_pred_iter)
                .chain(agg_index_selection_iter),
        )
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
            .and_then(|s| s.num_rows);

        let mut column_stats: ColumnStatSet = Default::default();
        for (k, v) in &self.statistics.column_stats {
            // No need to cal histogram for unused columns
            if !used_columns.contains(k) {
                continue;
            }
            if let Some(col_stat) = v {
                let Some(min) = col_stat.min.clone() else {
                    continue;
                };
                let Some(max) = col_stat.max.clone() else {
                    continue;
                };

                // NOTE: don't touch the original num_rows, since it will be used in other places.
                let ndv = match col_stat.ndv {
                    Some(ndv) => Ndv::Stat(ndv as f64),
                    None => Ndv::Max(
                        num_rows
                            .and_then(|n| n.checked_sub(col_stat.null_count))
                            .unwrap_or(u64::MAX) as _,
                    ),
                };

                // Alter ndv based on min and max if the datum is uint or int.
                let ndv = reduce_ndv_by_datum_range(ndv, &min, &max);

                let histogram = if let Some(histogram) = self.statistics.histograms.get(k)
                    && histogram.is_some()
                {
                    histogram.clone()
                } else {
                    num_rows.and_then(|num_rows| {
                        let num_rows = num_rows.saturating_sub(col_stat.null_count);
                        if num_rows == 0 {
                            return None;
                        }
                        let Ndv::Stat(ndv) = ndv else { return None };
                        HistogramBuilder::from_ndv(
                            ndv as _,
                            num_rows,
                            Some((min.clone(), max.clone())),
                            DEFAULT_HISTOGRAM_BUCKETS,
                        )
                        .ok()
                    })
                };
                column_stats.insert(*k, ColumnStat {
                    min,
                    max,
                    ndv,
                    null_count: col_stat.null_count,
                    histogram,
                });
            }
        }

        let precise_cardinality = self
            .statistics
            .table_stats
            .as_ref()
            .and_then(|stat| stat.num_rows);

        let cardinality = match (precise_cardinality, &self.prewhere) {
            (Some(precise_cardinality), Some(prewhere)) => {
                // Derive cardinality
                let mut sb = SelectivityEstimator::new(column_stats, precise_cardinality as f64);
                let cardinality = sb.apply(&prewhere.predicates)?;
                column_stats = sb.into_column_stats();
                cardinality
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

        // SECURITY: When row access policy is active, apply selectivity from
        // secure predicates first (for reasonable cardinality estimation and
        // join ordering), then suppress column statistics to prevent data
        // leakage through statistical inference.
        if self.secure_predicates.is_some() {
            let cardinality = match &self.secure_predicates {
                Some(preds) if !preds.is_empty() => {
                    SelectivityEstimator::new(column_stats, cardinality).apply(preds)?
                }
                _ => cardinality,
            };
            return Ok(Arc::new(StatInfo {
                cardinality,
                statistics: OpStatistics {
                    precise_cardinality: None,
                    column_stats: Default::default(),
                },
            }));
        }

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reduce_ndv_by_uint_full_range_saturates() {
        let reduced = reduce_ndv_by_datum_range(
            Ndv::Stat((u64::MAX as f64) + 1.0),
            &Datum::UInt(0),
            &Datum::UInt(u64::MAX),
        );

        assert_eq!(reduced.value(), u64::MAX as f64);
    }

    #[test]
    fn test_derive_scan_preserves_bind_time_metadata() {
        let original = Scan {
            table_index: 42,
            columns: [Symbol::new(1), Symbol::new(2), Symbol::new(3)]
                .into_iter()
                .collect(),
            scan_id: 10,
            sample: Some(SampleConfig {
                row_level: None,
                block_level: Some(50.0),
            }),
            change_type: Some(ChangeType::Append),
            update_stream_columns: true,
            is_lazy_table: true,
            secure_predicates: Some(vec![]),
            // Optimizer-phase fields set to non-default to verify they get reset.
            limit: Some(100),
            order_by: Some(vec![]),
            ..Default::default()
        };

        let new_columns = [Symbol::new(10), Symbol::new(20), Symbol::new(30)]
            .into_iter()
            .collect();
        let derived = original.derive_decorrelated_scan(new_columns, 99);

        // New columns and scan_id must be replaced.
        assert_eq!(
            derived.columns,
            [Symbol::new(10), Symbol::new(20), Symbol::new(30)]
                .into_iter()
                .collect()
        );
        assert_eq!(derived.scan_id, 99);
        assert_eq!(derived.table_index, 42);

        // Query-semantic read-path metadata must be preserved.
        assert_eq!(derived.sample, original.sample);

        // Security fields must be preserved.
        assert_eq!(derived.secure_predicates, Some(vec![]));

        // Everything else must be reset to default.
        assert_eq!(derived.change_type, None);
        assert!(!derived.update_stream_columns);
        assert!(!derived.is_lazy_table);

        // Optimizer-phase fields must be reset.
        assert!(derived.push_down_predicates.is_none());
        assert!(derived.limit.is_none());
        assert!(derived.order_by.is_none());
        assert!(derived.prewhere.is_none());
        assert!(derived.agg_index.is_none());
    }
}
