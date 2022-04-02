use std::fmt::Debug;
use std::sync::Arc;
use common_exception::Result;
use common_planners::{BroadcastPlan, PlanNode, StagePlan};
use crate::interpreters::plan_schedulers::query_fragment_dag::QueryFragmentDAG;
use crate::sessions::QueryContext;

pub trait QueryFragment: Debug {
    fn traverse_node(&self, ctx: &mut QueryFragmentDAG) -> Result<()>;
}

#[derive(Debug)]
pub struct ShuffleQueryFragment {
    input: Arc<PlanNode>,
}

impl ShuffleQueryFragment {
    pub fn create(plan: &StagePlan) -> Box<dyn QueryFragment> {
        Box::new(ShuffleQueryFragment { input: plan.input.clone() })
    }
}

impl QueryFragment for ShuffleQueryFragment {
    fn traverse_node(&self, ctx: &mut QueryFragmentDAG) -> Result<()> {
        ctx.add_shuffle_node(&self.input)
    }
}

#[derive(Debug)]
pub struct BroadcastQueryFragment {
    input: Arc<PlanNode>,
}

impl BroadcastQueryFragment {
    pub fn create(plan: &BroadcastPlan) -> Box<dyn QueryFragment> {
        Box::new(BroadcastQueryFragment { input: plan.input.clone() })
    }
}

impl QueryFragment for BroadcastQueryFragment {
    fn traverse_node(&self, ctx: &mut QueryFragmentDAG) -> Result<()> {
        ctx.add_broadcast_node(&self.input)
    }
}
