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

use super::Cost;
use super::CostModel;
use crate::optimizer::MExpr;
use crate::optimizer::Memo;
use crate::plans::ConstantTableScan;
use crate::plans::Exchange;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::plans::RelOperator;
use crate::plans::Scan;

#[derive(Default)]
pub struct DefaultCostModel {
    compute_per_row: f64,
    hash_table_per_row: f64,
    aggregate_per_row: f64,
    network_per_row: f64,

    /// The number of peers in the cluster to
    /// exchange data with.
    cluster_peers: usize,

    /// Degree of parallelism on each node.
    degree_of_parallelism: usize,
}

impl CostModel for DefaultCostModel {
    fn compute_cost(&self, memo: &Memo, m_expr: &MExpr) -> Result<Cost> {
        self.compute_cost_impl(memo, m_expr)
    }
}

impl DefaultCostModel {
    pub fn new(ctx: Arc<dyn TableContext>) -> Result<Self> {
        let settings = ctx.get_settings();
        let hash_table_per_row = settings.get_cost_factor_hash_table_per_row()? as f64;
        let aggregate_per_row = settings.get_cost_factor_aggregate_per_row()? as f64;
        let network_per_row = settings.get_cost_factor_network_per_row()? as f64;
        Ok(DefaultCostModel {
            compute_per_row: 1.0,
            hash_table_per_row,
            aggregate_per_row,
            network_per_row,
            cluster_peers: 1,
            degree_of_parallelism: 8,
        })
    }

    pub fn with_cluster_peers(mut self, cluster_peers: usize) -> Self {
        self.cluster_peers = cluster_peers;
        self
    }

    pub fn with_degree_of_parallelism(mut self, inter_node_parallelism: usize) -> Self {
        self.degree_of_parallelism = inter_node_parallelism;
        self
    }

    fn compute_cost_impl(&self, memo: &Memo, m_expr: &MExpr) -> Result<Cost> {
        match m_expr.plan.as_ref() {
            RelOperator::Scan(plan) => self.compute_cost_scan(memo, m_expr, plan),
            RelOperator::ConstantTableScan(plan) => self.compute_cost_constant_scan(plan),
            RelOperator::DummyTableScan(_) | RelOperator::CteScan(_) => Ok(Cost(0.0)),
            RelOperator::Join(plan) => self.compute_cost_join(memo, m_expr, plan),
            RelOperator::UnionAll(_) => self.compute_cost_union_all(memo, m_expr),
            RelOperator::Aggregate(_) => self.compute_aggregate(memo, m_expr),
            RelOperator::MaterializedCte(_) => self.compute_materialized_cte(memo, m_expr),

            RelOperator::EvalScalar(_)
            | RelOperator::Filter(_)
            | RelOperator::Window(_)
            | RelOperator::Sort(_)
            | RelOperator::ProjectSet(_)
            | RelOperator::Udf(_)
            | RelOperator::Limit(_) => self.compute_cost_unary_common_operator(memo, m_expr),

            RelOperator::Exchange(_) => self.compute_cost_exchange(memo, m_expr),

            _ => Err(ErrorCode::Internal("Cannot compute cost from logical plan")),
        }
    }

    fn compute_cost_scan(&self, memo: &Memo, m_expr: &MExpr, _plan: &Scan) -> Result<Cost> {
        // Since we don't have alternations(e.g. index scan) for table scan for now, we just ignore
        // the I/O cost and treat `PhysicalScan` as normal computation.
        let group = memo.group(m_expr.group_index)?;
        let cost = group.stat_info.cardinality * self.compute_per_row;
        Ok(Cost(cost))
    }

    fn compute_cost_constant_scan(&self, plan: &ConstantTableScan) -> Result<Cost> {
        let cost = plan.num_rows as f64 * self.compute_per_row;
        Ok(Cost(cost))
    }

    fn compute_cost_join(&self, memo: &Memo, m_expr: &MExpr, plan: &Join) -> Result<Cost> {
        let build_group = m_expr.child_group(memo, 1)?;
        let probe_group = m_expr.child_group(memo, 0)?;
        let build_card = build_group.stat_info.cardinality;
        let probe_card = probe_group.stat_info.cardinality;

        let mut cost = build_card * self.hash_table_per_row + probe_card * self.compute_per_row;

        if matches!(plan.join_type, JoinType::RightAnti | JoinType::RightSemi) {
            // Due to implementation reasons, right semi join is more expensive than left semi join
            // So if join type is right anti or right semi, cost needs multiply three (an approximate value)
            cost *= 3.0;
        }
        Ok(Cost(cost))
    }

    fn compute_materialized_cte(&self, memo: &Memo, m_expr: &MExpr) -> Result<Cost> {
        let left_group = m_expr.child_group(memo, 0)?;
        let cost = left_group.stat_info.cardinality * self.compute_per_row;
        Ok(Cost(cost))
    }

    /// Compute cost for the unary operators that perform simple computation(e.g. `Project`, `Filter`, `EvalScalar`).
    ///
    /// TODO(leiysky): Since we don't have alternation for `Aggregate` for now, we just
    /// treat `Aggregate` as normal computation.
    fn compute_cost_unary_common_operator(&self, memo: &Memo, m_expr: &MExpr) -> Result<Cost> {
        let group = m_expr.child_group(memo, 0)?;
        let card = group.stat_info.cardinality;
        let cost = card * self.compute_per_row;
        Ok(Cost(cost))
    }

    fn compute_cost_union_all(&self, memo: &Memo, m_expr: &MExpr) -> Result<Cost> {
        let left_group = m_expr.child_group(memo, 0)?;
        let right_group = m_expr.child_group(memo, 1)?;
        let card = left_group.stat_info.cardinality + right_group.stat_info.cardinality;
        let cost = card * self.compute_per_row;
        Ok(Cost(cost))
    }

    fn compute_aggregate(&self, memo: &Memo, m_expr: &MExpr) -> Result<Cost> {
        let group = m_expr.child_group(memo, 0)?;
        let card = group.stat_info.cardinality;
        let cost = card * self.aggregate_per_row;
        Ok(Cost(cost))
    }

    fn compute_cost_exchange(&self, memo: &Memo, m_expr: &MExpr) -> Result<Cost> {
        let exchange: Exchange = (*m_expr.plan.clone()).clone().try_into()?;
        let group = memo.group(m_expr.group_index)?;
        let cost = match exchange {
            Exchange::Hash(_) => {
                group.stat_info.cardinality * self.network_per_row
                    + group.stat_info.cardinality * self.compute_per_row
            }
            Exchange::Merge | Exchange::MergeSort => {
                // Merge is essentially a very expensive operation cause it will break the parallelism.
                // Thus we give it a very high cost so we can avoid it as much as possible.
                group.stat_info.cardinality * self.network_per_row
                    + group.stat_info.cardinality
                        * self.compute_per_row
                        * self.cluster_peers as f64
                        * self.degree_of_parallelism as f64
                        * 100.0
            }
            Exchange::Broadcast => {
                group.stat_info.cardinality * self.network_per_row * (self.cluster_peers - 1) as f64
            }
        };
        Ok(Cost(cost))
    }
}
