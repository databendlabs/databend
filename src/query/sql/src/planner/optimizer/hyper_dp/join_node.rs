use crate::optimizer::hyper_dp::join_relation::JoinRelationSet;

pub struct JoinNode {
    pub leaves: JoinRelationSet,
    pub left: JoinNode,
    pub right: JoinNode,
    pub cost: f64,
}
