use std::fmt::{Debug, Formatter};
use common_planners::PlanNode;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct FragmentPacket {
    pub node: PlanNode,
    pub fragment_id: String,
}

impl FragmentPacket {
    pub fn create(node: PlanNode) -> FragmentPacket {
        FragmentPacket { node }
    }
}

impl Debug for FragmentPacket {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FragmentPacket")
            .field("node", &self.node.name())
            .finish()
    }
}

