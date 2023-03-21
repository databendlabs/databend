use crate::plans::RelOperator;
use crate::IndexType;

pub struct JoinRelation {
    operator: RelOperator,
    parent_op: RelOperator,
}

impl JoinRelation {
    pub fn new(operator: &RelOperator, parent_op: &RelOperator) -> Self {
        Self {
            operator: operator.clone(),
            parent_op: parent_op.clone(),
        }
    }
}

pub struct JoinRelationSet {
    relations: Vec<IndexType>,
}

impl JoinRelationSet {}
