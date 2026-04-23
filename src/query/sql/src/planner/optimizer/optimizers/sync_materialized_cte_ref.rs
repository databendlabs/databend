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

use databend_common_exception::Result;

use crate::optimizer::Optimizer;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::SExpr;
use crate::optimizer::ir::StatInfo;
use crate::optimizer::ir::Statistics;
use crate::plans::MaterializedCTERef;
use crate::plans::RelOperator;

pub struct SyncMaterializedCTERefOptimizer {
    cte_stats: HashMap<String, Arc<StatInfo>>,
}

impl Default for SyncMaterializedCTERefOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

impl SyncMaterializedCTERefOptimizer {
    pub fn new() -> Self {
        Self {
            cte_stats: HashMap::new(),
        }
    }

    pub fn optimize_sync(&mut self, s_expr: &SExpr) -> Result<SExpr> {
        self.cte_stats.clear();
        self.collect_cte_stats(s_expr)?;
        let (s_expr, _) = self.sync_cte_ref_stats(s_expr)?;
        Ok(s_expr)
    }

    #[recursive::recursive]
    fn collect_cte_stats(&mut self, s_expr: &SExpr) -> Result<()> {
        if let RelOperator::MaterializedCTE(cte) = s_expr.plan() {
            let stat_info = RelExpr::with_s_expr(s_expr.child(0)?).derive_cardinality()?;
            self.cte_stats.insert(cte.cte_name.clone(), stat_info);
        }

        for child in s_expr.children() {
            self.collect_cte_stats(child)?;
        }

        Ok(())
    }

    fn remap_stat_info(
        cte_ref: &MaterializedCTERef,
        producer_stat_info: &Arc<StatInfo>,
    ) -> Arc<StatInfo> {
        let producer_to_ref = cte_ref
            .column_mapping
            .iter()
            .map(|(ref_col, producer_col)| (*producer_col, *ref_col))
            .collect::<HashMap<_, _>>();
        let column_stats = producer_stat_info
            .statistics
            .column_stats
            .iter()
            .filter_map(|(producer_col, stat)| {
                producer_to_ref
                    .get(producer_col)
                    .map(|ref_col| (*ref_col, stat.clone()))
            })
            .collect();

        Arc::new(StatInfo {
            cardinality: producer_stat_info.cardinality,
            statistics: Statistics {
                precise_cardinality: producer_stat_info.statistics.precise_cardinality,
                column_stats,
            },
        })
    }

    #[recursive::recursive]
    fn sync_cte_ref_stats(&self, s_expr: &SExpr) -> Result<(SExpr, bool)> {
        let mut changed = false;
        let mut new_children = Vec::with_capacity(s_expr.arity());
        for child in s_expr.children() {
            let (new_child, child_changed) = self.sync_cte_ref_stats(child)?;
            changed |= child_changed;
            new_children.push(Arc::new(new_child));
        }

        let mut result = if changed {
            s_expr.replace_children(new_children)
        } else {
            s_expr.clone()
        };

        if let RelOperator::MaterializedCTERef(cte_ref) = result.plan() {
            if let Some(producer_stat_info) = self.cte_stats.get(&cte_ref.cte_name) {
                let mut new_cte_ref = cte_ref.clone();
                new_cte_ref.stat_info = Some(Self::remap_stat_info(cte_ref, producer_stat_info));
                result =
                    result.replace_plan(Arc::new(RelOperator::MaterializedCTERef(new_cte_ref)));
                changed = true;
            }
        }

        Ok((result, changed))
    }
}

#[async_trait::async_trait]
impl Optimizer for SyncMaterializedCTERefOptimizer {
    fn name(&self) -> String {
        "SyncMaterializedCTERefOptimizer".to_string()
    }

    async fn optimize(&mut self, s_expr: &SExpr) -> Result<SExpr> {
        self.optimize_sync(s_expr)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use databend_common_expression::Scalar;

    use super::*;
    use crate::plans::ConstantExpr;
    use crate::plans::DummyTableScan;
    use crate::plans::Filter;
    use crate::plans::MaterializedCTE;
    use crate::plans::MaterializedCTERef;
    use crate::plans::Sequence;

    fn bool_constant(value: bool) -> crate::ScalarExpr {
        ConstantExpr {
            span: None,
            value: Scalar::Boolean(value),
        }
        .into()
    }

    #[test]
    fn test_sync_materialized_cte_ref_updates_consumer_stats() {
        let old_def = SExpr::create_leaf(DummyTableScan::new());
        let new_def = SExpr::create_unary(
            Filter {
                predicates: vec![bool_constant(false)],
            },
            Arc::new(old_def.clone()),
        );

        let producer = SExpr::create_unary(
            MaterializedCTE::new("cte".to_string(), None),
            Arc::new(new_def),
        );
        let consumer = SExpr::create_leaf(RelOperator::MaterializedCTERef(MaterializedCTERef {
            cte_name: "cte".to_string(),
            output_columns: vec![],
            def: old_def.clone(),
            column_mapping: HashMap::new(),
            stat_info: None,
        }));
        let query = SExpr::create_unary(
            Filter {
                predicates: vec![bool_constant(false)],
            },
            Arc::new(consumer),
        );
        let root = SExpr::create_binary(Sequence, Arc::new(producer), Arc::new(query));

        let optimized = SyncMaterializedCTERefOptimizer::new()
            .optimize_sync(&root)
            .unwrap();

        let query = optimized.child(1).unwrap();
        let consumer = query.child(0).unwrap();
        let RelOperator::MaterializedCTERef(cte_ref) = consumer.plan() else {
            panic!("expected materialized cte ref");
        };

        assert_eq!(cte_ref.def, old_def);
        assert_eq!(cte_ref.stat_info.as_ref().unwrap().cardinality, 0.0);
    }

    #[test]
    fn test_sync_materialized_cte_ref_keeps_unmatched_consumer_stats() {
        let old_def = SExpr::create_leaf(DummyTableScan::new());
        let producer = SExpr::create_unary(
            MaterializedCTE::new("cte".to_string(), None),
            Arc::new(old_def.clone()),
        );
        let consumer = SExpr::create_leaf(RelOperator::MaterializedCTERef(MaterializedCTERef {
            cte_name: "other_cte".to_string(),
            output_columns: vec![],
            def: old_def.clone(),
            column_mapping: HashMap::new(),
            stat_info: None,
        }));
        let root = SExpr::create_binary(Sequence, Arc::new(producer), Arc::new(consumer));

        let optimized = SyncMaterializedCTERefOptimizer::new()
            .optimize_sync(&root)
            .unwrap();

        let consumer = optimized.child(1).unwrap();
        let RelOperator::MaterializedCTERef(cte_ref) = consumer.plan() else {
            panic!("expected materialized cte ref");
        };

        assert_eq!(cte_ref.def, old_def);
        assert!(cte_ref.stat_info.is_none());
    }
}
