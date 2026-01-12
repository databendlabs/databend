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

use std::sync::Arc;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::ColumnSet;
use crate::IndexType;
use crate::optimizer::ir::Distribution;
use crate::optimizer::ir::PhysicalProperty;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::RelationalProperty;
use crate::optimizer::ir::RequiredProperty;
use crate::optimizer::ir::SExpr;
use crate::optimizer::ir::StatInfo;
use crate::optimizer::ir::Statistics;
use crate::plans::Operator;
use crate::plans::RelOp;
use crate::plans::RelOperator;

/// DummyTableScan is a virtual scan that returns a single row (1) to carry constant values.
/// It's used when optimizer folds expressions (e.g., count(*) optimization, scalar subquery folding).
///
/// ## Query Result Cache Issue
///
/// DummyTableScan internally uses `system.one` table to produce a single row. Originally,
/// `system.one` had `result_can_be_cached() = false`, which caused queries containing
/// DummyTableScan to be excluded from query result cache entirely.
///
/// Example: `SELECT * FROM t WHERE a > (SELECT MIN(a) FROM t)`
/// - The scalar subquery `(SELECT MIN(a) FROM t)` gets optimized to DummyTableScan
/// - Without proper handling, this query cannot use result cache
///
/// ## Solution
///
/// We made `system.one` cacheable, but this creates a new problem: when table `t` changes,
/// the cached result should be invalidated. However, DummyTableScan only references
/// `system.one`, not `t`, so the cache wouldn't know to invalidate.
///
/// The `source_table_indexes` field solves this by tracking which tables' data was used
/// to compute the constant values. During physical plan building, we use these indexes
/// to add cache invalidation keys for the source tables, ensuring proper cache invalidation.
///
/// Note: The query result cache stores these keys under the historical name
/// `partitions_shas`. For normal table scans we use a partition SHA256; for Fuse tables
/// (e.g. when folded into DummyTableScan) we may use snapshot_location (or a stable
/// sentinel for empty tables) as the invalidation key.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
pub struct DummyTableScan {
    /// Table indexes that this DummyTableScan depends on.
    /// When building the physical plan, we add cache invalidation keys for these tables
    /// to ensure query result cache is invalidated when source data changes.
    pub source_table_indexes: Vec<IndexType>,
}

impl DummyTableScan {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_source_tables(source_table_indexes: Vec<IndexType>) -> Self {
        Self {
            source_table_indexes,
        }
    }

    /// Collect all table indexes from an SExpr tree.
    /// Used to track which tables a DummyTableScan depends on for cache invalidation.
    pub fn collect_source_tables(s_expr: &SExpr) -> Vec<IndexType> {
        let mut table_indexes = Vec::new();
        Self::collect_source_tables_recursive(s_expr, &mut table_indexes);
        table_indexes.sort();
        table_indexes.dedup();
        table_indexes
    }

    fn collect_source_tables_recursive(s_expr: &SExpr, table_indexes: &mut Vec<IndexType>) {
        match s_expr.plan() {
            RelOperator::Scan(scan) => {
                table_indexes.push(scan.table_index);
            }
            // MaterializedCTERef stores its definition in the `def` field, not as a child.
            // We must traverse it to capture tables referenced within the CTE.
            // Example: WITH cte AS (SELECT * FROM t) SELECT * FROM t2 WHERE x > (SELECT COUNT(*) FROM cte)
            // Without this, only t2 would be tracked, and changes to t wouldn't invalidate the cache.
            RelOperator::MaterializedCTERef(cte_ref) => {
                Self::collect_source_tables_recursive(&cte_ref.def, table_indexes);
            }
            _ => {}
        }
        for child in s_expr.children() {
            Self::collect_source_tables_recursive(child, table_indexes);
        }
    }

    pub fn used_columns(&self) -> Result<ColumnSet> {
        Ok(ColumnSet::new())
    }
}

impl Operator for DummyTableScan {
    fn rel_op(&self) -> RelOp {
        RelOp::DummyTableScan
    }

    fn arity(&self) -> usize {
        0
    }

    fn derive_relational_prop(&self, _rel_expr: &RelExpr) -> Result<Arc<RelationalProperty>> {
        Ok(Arc::new(RelationalProperty {
            output_columns: ColumnSet::new(),
            outer_columns: ColumnSet::new(),
            used_columns: ColumnSet::new(),
            orderings: vec![],
            partition_orderings: None,
        }))
    }

    fn derive_physical_prop(&self, _rel_expr: &RelExpr) -> Result<PhysicalProperty> {
        Ok(PhysicalProperty {
            distribution: Distribution::Serial,
        })
    }

    fn derive_stats(&self, _rel_expr: &RelExpr) -> Result<Arc<StatInfo>> {
        Ok(Arc::new(StatInfo {
            cardinality: 1.0,
            statistics: Statistics {
                precise_cardinality: Some(1),
                column_stats: Default::default(),
            },
        }))
    }

    fn compute_required_prop_child(
        &self,
        _ctx: Arc<dyn TableContext>,
        _rel_expr: &RelExpr,
        _child_index: usize,
        _required: &RequiredProperty,
    ) -> Result<RequiredProperty> {
        Err(ErrorCode::Internal(
            "Cannot compute required property for DummyTableScan".to_string(),
        ))
    }
}
