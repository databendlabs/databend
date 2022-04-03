use std::collections::VecDeque;
use std::sync::Arc;
use common_exception::{ErrorCode, Result};
use common_planners::PlanNode;
use crate::sessions::QueryContext;

// Query plan fragment with destination
pub struct QueryFragmentAction {
    node: PlanNode,
    destination_id: String,
}

impl QueryFragmentAction {
    pub fn create(destination_id: String, node: PlanNode) -> QueryFragmentAction {
        QueryFragmentAction { node, destination_id }
    }
}

pub struct QueryFragmentActions {
    fragment_actions: Vec<QueryFragmentAction>,
}

impl QueryFragmentActions {
    pub fn create() -> QueryFragmentActions {
        QueryFragmentActions { fragment_actions: vec![] }
    }

    pub fn add_action(&mut self, action: QueryFragmentAction) {
        self.fragment_actions.push(action)
    }
}

pub struct QueryFragmentsActions {
    ctx: Arc<QueryContext>,
    fragments_actions: Vec<QueryFragmentActions>,
}

impl QueryFragmentsActions {
    pub fn create(ctx: Arc<QueryContext>) -> QueryFragmentsActions {
        QueryFragmentsActions { ctx, fragments_actions: Vec::new() }
    }

    pub fn get_destinations_id(&self) -> Vec<String> {
        unimplemented!()
    }

    pub fn get_local_destination_id(&self) -> String {
        unimplemented!()
    }

    pub fn get_root_actions(&self) -> Result<&QueryFragmentActions> {
        match self.fragments_actions.last() {
            None => Err(ErrorCode::LogicalError("Logical error, call get_root_actions in empty QueryFragmentsActions")),
            Some(entity) => Ok(entity)
        }
    }

    pub fn add_fragment_actions(&mut self, actions: QueryFragmentActions) -> Result<()> {
        self.fragments_actions.push(actions);
        Ok(())
    }
}
