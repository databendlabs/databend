// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::DataSchema;
use common_datavalues::DataValue;
use common_exception::Result;
use common_planners::AggregatorPartialPlan;
use common_planners::EmptyPlan;
use common_planners::Expression;
use common_planners::PlanNode;
use common_planners::ReadDataSourcePlan;
use common_planners::StageKind;
use common_planners::StagePlan;

use crate::optimizers::Optimizer;
use crate::sessions::FuseQueryContextRef;

pub struct ScattersOptimizer {
    ctx: FuseQueryContextRef,
}

enum OptimizeKind {
    Local,
    Scattered,
}

impl ScattersOptimizer {
    pub fn create(ctx: FuseQueryContextRef) -> Self {
        ScattersOptimizer { ctx }
    }

    fn converge_stage_if_scattered(
        &mut self,
        plan: &PlanNode,
        status: &mut Vec<OptimizeKind>,
    ) -> Result<PlanNode> {
        match status.pop() {
            None => {
                status.push(OptimizeKind::Local);
                Ok(plan.clone())
            }
            Some(OptimizeKind::Local) => {
                status.push(OptimizeKind::Local);
                Ok(plan.clone())
            }
            Some(OptimizeKind::Scattered) => {
                status.push(OptimizeKind::Local);
                Ok(PlanNode::Stage(StagePlan {
                    kind: StageKind::Convergent,
                    scatters_expr: Expression::Literal(DataValue::UInt64(Some(0))),
                    input: Arc::new(plan.clone()),
                }))
            }
        }
    }

    fn optimize_read_plan(
        &mut self,
        plan: &ReadDataSourcePlan,
        status: &mut Vec<OptimizeKind>,
    ) -> Result<PlanNode> {
        let read_table = self
            .ctx
            .get_datasource()
            .get_table(plan.db.as_str(), plan.table.as_str())?;

        let settings = self.ctx.get_settings();
        let rows_threshold = settings.get_min_distributed_rows()? as usize;
        let bytes_threshold = settings.get_min_distributed_bytes()? as usize;
        if read_table.is_local()
            && (plan.statistics.read_rows >= rows_threshold
                || plan.statistics.read_bytes >= bytes_threshold)
        {
            // TODO: Need implement blockNumber function.
            // We use the blockNumber function as the scatter expr.
            // This will be DataBlock based round robin scheduling.
            status.push(OptimizeKind::Scattered);
            return Ok(PlanNode::Stage(StagePlan {
                kind: StageKind::Expansive,
                scatters_expr: Expression::ScalarFunction {
                    op: String::from("blockNumber"),
                    args: vec![],
                },
                input: Arc::new(PlanNode::ReadSource(plan.clone())),
            }));
        }

        if read_table.is_local() {
            status.push(OptimizeKind::Local);
        } else {
            status.push(OptimizeKind::Scattered);
        }

        // Keep local mode, there are two cases
        // 1. The data is small enough that we just need to continue to run on standalone mode.
        // 2. The table is already clustered. We just need to repartition for the cluster mode.
        Ok(PlanNode::ReadSource(plan.clone()))
    }

    fn optimize_aggregator(
        &mut self,
        plan: &AggregatorPartialPlan,
        input: PlanNode,
        status: &mut Vec<OptimizeKind>,
    ) -> Result<PlanNode> {
        if let Some(OptimizeKind::Local) = status.pop() {
            // Keep running in standalone mode
            // TODO We can estimate and evaluate the amount of data to decide whether to shuffle again
            // For example, when ReadSourcePlan = 500MB
            // We can evaluate 500MB * 0.8(it can be get by history) after FilterPlan
            status.push(OptimizeKind::Local);
            return Ok(PlanNode::AggregatorPartial(AggregatorPartialPlan {
                group_expr: plan.group_expr.clone(),
                aggr_expr: plan.aggr_expr.clone(),
                schema: plan.schema.clone(),
                input: Arc::new(input),
            }));
        }

        match plan.group_expr.len() {
            0 => {
                // If no group by we convergent it in local node
                status.push(OptimizeKind::Local);
                Ok(PlanNode::Stage(StagePlan {
                    kind: StageKind::Convergent,
                    scatters_expr: Expression::Literal(DataValue::UInt64(Some(0))),
                    input: Arc::new(PlanNode::AggregatorPartial(AggregatorPartialPlan {
                        group_expr: plan.group_expr.clone(),
                        aggr_expr: plan.aggr_expr.clone(),
                        schema: plan.schema.clone(),
                        input: Arc::new(input),
                    })),
                }))
            }
            _ => {
                // Keep running in cluster mode
                status.push(OptimizeKind::Scattered);
                Ok(PlanNode::Stage(StagePlan {
                    kind: StageKind::Normal,
                    scatters_expr: Expression::ScalarFunction {
                        op: String::from("sipHash"),
                        args: vec![Expression::Column(String::from("_group_by_key"))],
                    },
                    input: Arc::new(PlanNode::AggregatorPartial(AggregatorPartialPlan {
                        group_expr: plan.group_expr.clone(),
                        aggr_expr: plan.aggr_expr.clone(),
                        schema: plan.schema.clone(),
                        input: Arc::new(input),
                    })),
                }))
            }
        }
    }
}

#[async_trait::async_trait]
impl Optimizer for ScattersOptimizer {
    fn name(&self) -> &str {
        "Scatters"
    }

    async fn optimize(&mut self, plan: &PlanNode) -> Result<PlanNode> {
        if self.ctx.try_get_executors().await?.is_empty() {
            // Standalone mode.
            return Ok(plan.clone());
        }

        let mut status_rpn = vec![];
        let mut rewritten_node = PlanNode::Empty(EmptyPlan {
            schema: Arc::new(DataSchema::empty()),
        });

        plan.walk_postorder(|node| -> Result<bool> {
            match node {
                PlanNode::ReadSource(plan) => {
                    rewritten_node = self.optimize_read_plan(plan, &mut status_rpn)?
                }
                PlanNode::AggregatorPartial(plan) => {
                    rewritten_node =
                        self.optimize_aggregator(plan, rewritten_node.clone(), &mut status_rpn)?
                }
                PlanNode::Sort(plan) => {
                    let mut new_node = PlanNode::Sort(plan.clone());
                    new_node.set_inputs(vec![
                        &self.converge_stage_if_scattered(&rewritten_node, &mut status_rpn)?
                    ])?;
                    rewritten_node = new_node;
                }
                PlanNode::Limit(plan) => {
                    let mut new_node = PlanNode::Limit(plan.clone());
                    new_node.set_inputs(vec![
                        &self.converge_stage_if_scattered(&rewritten_node, &mut status_rpn)?
                    ])?;
                    rewritten_node = new_node;
                }
                _ => {
                    let mut clone_node = node.clone();
                    clone_node.set_inputs(vec![&rewritten_node])?;
                    rewritten_node = clone_node;
                }
            };

            Ok(true)
        })?;

        // We need to converge at the end
        self.converge_stage_if_scattered(&rewritten_node, &mut status_rpn)
    }
}
