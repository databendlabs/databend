use std::collections::HashSet;
use std::ops::Index;

use ahash::HashMap;
use common_exception::Result;

use crate::optimizer::SExpr;
use crate::plans::RelOperator;
use crate::IndexType;

pub struct JoinRelation {
    s_expr: SExpr,
    parent_s_expr: SExpr,
}

impl JoinRelation {
    pub fn new(s_expr: &SExpr, parent: &SExpr) -> Self {
        Self {
            s_expr: s_expr.clone(),
            parent_s_expr: parent.clone(),
        }
    }
}

#[derive(Default, Clone, Eq, PartialEq)]
pub struct JoinRelationSet {
    relations: Vec<IndexType>,
}

impl JoinRelationSet {
    pub fn new(relations: Vec<IndexType>) -> Self {
        Self { relations }
    }

    // Check if the set is empty
    pub fn is_empty(&self) -> bool {
        self.relations.is_empty()
    }

    // Check if two sets aren't intersecting
    pub fn is_disjoint(&self, other: &Self) -> bool {
        self.relations
            .iter()
            .all(|idx| !other.relations.contains(idx))
    }
}

struct RelationSetNode {
    relation_set: JoinRelationSet,
    // Key is relation id
    children: HashMap<IndexType, RelationSetNode>,
}
// The tree is initialized by join conditions' relation sets
// Such as condition: t1.a + t2.b == t3.b , the tree will be
//            root
//           /     \
//       [{},{1}]  [{3}, {}]
//          /
//      [{1, 2}, {}]
#[derive(Default, Clone, Default)]
pub struct RelationSetTree {
    root: RelationSetNode,
}

impl RelationSetTree {
    pub fn get_relation_set(&mut self, idx_set: &HashSet<IndexType>) -> Result<JoinRelationSet> {
        let mut relations: Vec<IndexType> = idx_set.iter().map(|idx| *idx).collect();
        // Make relations ordered
        relations.sort();
        let mut node = &mut self.root;
        for idx in relations.iter() {
            if !node.children.contains_key(idx) {
                node.children.insert(*idx, RelationSetNode::default());
            }
            node = node.children.get_mut(idx).unwrap();
        }
        if node.relation_set.is_empty() {
            node.relation_set = JoinRelationSet::new(relations);
        }
        Ok(node.relation_set.clone())
    }
}
