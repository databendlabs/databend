use common_exception::Result;
use common_planners::{BroadcastPlan, PlanVisitor, StagePlan};
use crate::interpreters::plan_schedulers::query_fragment::QueryFragment;

struct QueryFragmentBuilder {
    queries_fragment: Vec<Box<dyn QueryFragment>>,
}

impl PlanVisitor for QueryFragmentBuilder {
    fn visit_stage(&mut self, plan: &StagePlan) -> Result<()> {
        todo!()
    }

    fn visit_broadcast(&mut self, plan: &BroadcastPlan) -> Result<()> {
        todo!()
    }
}
