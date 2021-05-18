use crate::sessions::FuseQueryContextRef;
use crate::optimizers::IOptimizer;
use common_planners::{PlanNode, EmptyPlan, StagePlan, ReadDataSourcePlan, AggregatorPartialPlan, StageKind, Statistics, ExpressionAction};
use common_exception::Result;
use common_datavalues::{DataSchema, DataValue};
use std::sync::Arc;

pub struct ScattersOptimizer {
    ctx: FuseQueryContextRef
}

enum OptimizeKind {
    Local,
    Scattered,
}

impl ScattersOptimizer {
    pub fn create(ctx: FuseQueryContextRef) -> Self {
        ScattersOptimizer {
            ctx
        }
    }

    fn optimize_read_plan(&mut self, plan: &ReadDataSourcePlan, status: &mut Vec<OptimizeKind>) -> Result<PlanNode> {
        let read_table = self.ctx.get_datasource()
            .get_table(plan.db.as_str(), plan.table.as_str())?;

        let rows_threshold = self.ctx.get_min_distributed_rows()? as usize;
        let bytes_threshold = self.ctx.get_min_distributed_bytes()? as usize;
        if read_table.is_local() && (plan.statistics.read_rows >= rows_threshold || plan.statistics.read_bytes >= bytes_threshold) {
            // TODO: Need implement blockNumber function.
            // We use the blockNumber function as the scatter expr.
            // This will be DataBlock based round robin scheduling.
            status.push(OptimizeKind::Scattered);
            return Ok(PlanNode::Stage(StagePlan {
                kind: StageKind::Expansive,
                scatters_expr: ExpressionAction::Function {
                    op: String::from("blockNumber"),
                    args: vec![],
                },
                input: Arc::new(PlanNode::ReadSource(ReadDataSourcePlan {
                    db: plan.db.clone(),
                    table: plan.table.clone(),
                    schema: plan.schema.clone(),
                    partitions: plan.partitions.clone(),
                    statistics: plan.statistics.clone(),
                    description: plan.description.clone(),
                })),
            }));
        }

        if read_table.is_local() {
            status.push(OptimizeKind::Local);
        } else {
            status.push(OptimizeKind::Scattered);
        }

        // Keep local mode, there are two cases
        // 1. The data is small enough that we just need to continue to run on standalone mode.
        // 2. The table is already clustered. We just need to rebuild the partition for the cluster mode.
        Ok(PlanNode::ReadSource(ReadDataSourcePlan {
            db: plan.db.clone(),
            table: plan.table.clone(),
            schema: plan.schema.clone(),
            partitions: plan.partitions.clone(),
            statistics: plan.statistics.clone(),
            description: plan.description.clone(),
        }))
    }

    fn optimize_aggregator(&mut self, plan: &AggregatorPartialPlan, input: PlanNode, status: &mut Vec<OptimizeKind>) -> Result<PlanNode> {
        if let Some(OptimizeKind::Local) = status.pop() {
            // Keep running in standalone mode
            status.push(OptimizeKind::Local);
            return Ok(PlanNode::AggregatorPartial(AggregatorPartialPlan {
                group_expr: plan.group_expr.clone(),
                aggr_expr: plan.aggr_expr.clone(),
                input: Arc::new(input),
            }));
        }

        match plan.group_expr.len() {
            0 => {
                // For the final state, we need to aggregate the data
                status.push(OptimizeKind::Local);
                Ok(PlanNode::Stage(StagePlan {
                    kind: StageKind::Convergent,
                    scatters_expr: ExpressionAction::Literal(DataValue::UInt64(Some(0))),
                    input: Arc::new(PlanNode::AggregatorPartial(AggregatorPartialPlan {
                        group_expr: plan.group_expr.clone(),
                        aggr_expr: plan.aggr_expr.clone(),
                        input: Arc::new(input),
                    })),
                }))
            },
            _ => {
                // Keep running in cluster mode
                status.push(OptimizeKind::Scattered);
                Ok(PlanNode::Stage(StagePlan {
                    kind: StageKind::Normal,
                    scatters_expr: plan.group_expr[0].clone(),
                    input: Arc::new(PlanNode::AggregatorPartial(AggregatorPartialPlan {
                        group_expr: plan.group_expr.clone(),
                        aggr_expr: plan.aggr_expr.clone(),
                        input: Arc::new(input),
                    })),
                }))
            }
        }
    }
}

impl IOptimizer for ScattersOptimizer {
    fn name(&self) -> &str {
        "Scatters"
    }

    fn optimize(&mut self, plan: &PlanNode) -> Result<PlanNode> {
        if self.ctx.try_get_cluster()?.is_empty()? {
            // Standalone mode.
            return Ok(plan.clone());
        }

        let mut status_rpn = vec![];
        let mut rewritten_node = PlanNode::Empty(EmptyPlan {
            schema: Arc::new(DataSchema::empty())
        });

        plan.walk_postorder(|node| -> Result<bool> {
            match node {
                PlanNode::ReadSource(plan) =>
                    rewritten_node = self.optimize_read_plan(plan, &mut status_rpn)?,
                PlanNode::AggregatorPartial(plan) =>
                    rewritten_node = self.optimize_aggregator(plan, rewritten_node.clone(), &mut status_rpn)?,
                _ => {
                    let mut clone_node = node.clone();
                    clone_node.set_input(&rewritten_node);
                    rewritten_node = clone_node;
                }
            };

            Ok(true)
        })?;

        // We need to converge at the end
        match status_rpn.pop() {
            None => Ok(rewritten_node),
            Some(OptimizeKind::Local) => Ok(rewritten_node),
            Some(OptimizeKind::Scattered) => {
                Ok(PlanNode::Stage(StagePlan {
                    kind: StageKind::Convergent,
                    scatters_expr: ExpressionAction::Literal(DataValue::UInt64(Some(0))),
                    input: Arc::new(rewritten_node),
                }))
            }
        }
    }
}

