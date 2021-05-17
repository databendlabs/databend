use crate::sessions::FuseQueryContextRef;
use crate::optimizers::IOptimizer;
use common_planners::{PlanNode, EmptyPlan, StagePlan};
use common_exception::Result;
use common_datavalues::DataSchema;
use std::sync::Arc;

pub struct ScattersOptimizer;

impl ScattersOptimizer {
    pub fn create(_ctx: FuseQueryContextRef) -> Self {
        ScattersOptimizer {}
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
                PlanNode::AggregatorPartial(plan) => {
                    rewritten_node = PlanNode::Stage(StagePlan {
                        input: Arc::new(node.clone()),
                        scatters_expr: plan.group_expr[0].clone()
                    })
                }
                _ => {
                    let mut clone_node = node.clone();
                    clone_node.set_input(&rewritten_node)?;
                    rewritten_node = clone_node;
                }
            };

            Ok(true)
        })?;

        Ok(rewritten_node)
    }
}

