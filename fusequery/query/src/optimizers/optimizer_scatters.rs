use crate::sessions::FuseQueryContextRef;
use crate::optimizers::IOptimizer;
use common_planners::{PlanNode, EmptyPlan, StagePlan, ReadDataSourcePlan, AggregatorPartialPlan, StageKind, Statistics, ExpressionAction};
use common_exception::Result;
use common_datavalues::DataSchema;
use std::sync::Arc;

pub struct ScattersOptimizer {
    ctx: FuseQueryContextRef
}

impl ScattersOptimizer {
    pub fn create(ctx: FuseQueryContextRef) -> Self {
        ScattersOptimizer {
            ctx
        }
    }

    fn optimize_read_plan(&mut self, plan: &ReadDataSourcePlan) -> Result<PlanNode> {
        let read_table = self.ctx.get_datasource()
            .get_table(plan.db.as_str(), plan.table.as_str())?;

        // TODO: Get threshold from settings
        if read_table.is_local() && (plan.statistics.read_rows > 10000000 || plan.statistics.read_bytes > 10000000000000) {
            // TODO: Need implement blockNumber function.
            // We use the blockNumber function as the scatter expr.
            // This will be DataBlock based round robin scheduling.
            return Ok(PlanNode::Stage(StagePlan {
                kind: StageKind::Normal,
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

    fn optimize_aggregator(&mut self, plan: &AggregatorPartialPlan, input: PlanNode) -> Result<PlanNode> {
        match plan.group_expr.len() {
            0 => Ok(PlanNode::AggregatorPartial(AggregatorPartialPlan {
                group_expr: plan.group_expr.clone(),
                aggr_expr: plan.aggr_expr.clone(),
                input: Arc::new(input),
            })),
            _ => {
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
        let mut rewritten_node = PlanNode::Empty(EmptyPlan {
            schema: Arc::new(DataSchema::empty())
        });

        plan.walk_postorder(|node| -> Result<bool> {
            match node {
                PlanNode::ReadSource(plan) =>
                    rewritten_node = self.optimize_read_plan(plan)?,
                PlanNode::AggregatorPartial(plan) =>
                    rewritten_node = self.optimize_aggregator(plan, rewritten_node)?,
                _ => {
                    let mut clone_node = node.clone();
                    clone_node.set_input(&rewritten_node);
                    rewritten_node = clone_node;
                }
            };

            Ok(true)
        })?;

        Ok(rewritten_node)
    }
}

