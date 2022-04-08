use std::fmt::{Debug, Formatter};
use common_planners::PlanNode;
use crate::api::DataExchange;
use crate::interpreters::QueryFragmentAction;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct FragmentPacket {
    pub node: PlanNode,
    pub fragment_id: String,
    pub data_exchange: DataExchange,
}

impl FragmentPacket {
    pub fn create(fragment_id: String, node: PlanNode, data_exchange: DataExchange) -> FragmentPacket {
        FragmentPacket {
            node,
            fragment_id,
            data_exchange,
        }
    }
}

impl Debug for FragmentPacket {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FragmentPacket")
            .field("node", &self.node.name())
            .field("fragment_id", &self.fragment_id)
            .field("exchange", &self.data_exchange)
            .finish()
    }
}

