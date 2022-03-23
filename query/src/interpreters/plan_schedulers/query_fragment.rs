use std::sync::Arc;
use common_exception::Result;
use common_planners::PlanNode;
use crate::sessions::QueryContext;

enum Mode {}

// pub trait ScheduleContext {
//     fn add_
// }

// TODO:
pub trait QueryFragment {
    fn traverse(&self, ctx: &mut dyn ScheduleContext) -> Result<Mode>;
}

struct ShuffleQueryFragment {
    input: Arc<PlanNode>,
}

impl QueryFragment for ShuffleQueryFragment {
    fn traverse(&self, ctx: Arc<QueryContext>) -> Result<Mode> {
        todo!()
    }
}

struct BroadcastQueryFragment {}

impl QueryFragment for BroadcastQueryFragment {
    fn traverse(&self, ctx: &mut dyn ScheduleContext) -> Result<Mode> {
        todo!()
    }
}

