use std::sync::Arc;

use common_datavalues::DataSchemaRef;

use crate::PlanNode;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub enum BroadcastKind {
    OneNode,
    EachNode,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq)]
pub struct BroadcastPlan {
    pub input: Arc<PlanNode>,
    pub kind: BroadcastKind,
}

impl BroadcastPlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.input.schema()
    }

    pub fn set_input(&mut self, node: &PlanNode) {
        self.input = Arc::new(node.clone());
    }
}
