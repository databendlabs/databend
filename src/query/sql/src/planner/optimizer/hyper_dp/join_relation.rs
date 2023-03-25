// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashSet;

use ahash::HashMap;
use common_exception::Result;

use crate::optimizer::RelExpr;
use crate::optimizer::SExpr;
use crate::IndexType;

pub struct JoinRelation {
    s_expr: SExpr,
    // parent_s_expr: SExpr,
}

impl JoinRelation {
    pub fn new(s_expr: &SExpr, _parent: &SExpr) -> Self {
        Self {
            s_expr: s_expr.clone(),
            // parent_s_expr: parent.clone(),
        }
    }

    pub fn cost(&self) -> Result<f64> {
        let rel_expr = RelExpr::with_s_expr(&self.s_expr);
        Ok(rel_expr.derive_relational_prop()?.cardinality)
    }
}

#[derive(Default, Clone, Eq, PartialEq, Hash, Debug)]
pub struct JoinRelationSet {
    relations: Vec<IndexType>,
}

impl JoinRelationSet {
    pub fn new(relations: Vec<IndexType>) -> Self {
        Self { relations }
    }

    pub fn relations(&self) -> &[IndexType] {
        &self.relations
    }

    // Check if the set is empty
    pub fn is_empty(&self) -> bool {
        self.relations.is_empty()
    }

    // Check if the first set is subset of the second set
    pub fn is_subset(&self, other: &Self) -> bool {
        self.relations
            .iter()
            .all(|idx| other.relations.contains(idx))
    }

    // Check if two sets aren't intersecting
    pub fn is_disjoint(&self, other: &Self) -> bool {
        self.relations
            .iter()
            .all(|idx| !other.relations.contains(idx))
    }

    pub fn iter(&self) -> impl Iterator<Item = &IndexType> {
        self.relations.iter()
    }

    pub fn merge_relation_set(&self, other: &Self) -> JoinRelationSet {
        let mut res = JoinRelationSet {
            relations: self.relations.clone(),
        };
        res.relations.extend_from_slice(&other.relations);
        res.relations.sort();
        res.relations.dedup();
        res
    }
}

#[derive(Default, Clone)]
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
#[derive(Default, Clone)]
pub struct RelationSetTree {
    root: RelationSetNode,
}

impl RelationSetTree {
    pub fn get_relation_set_by_index(&mut self, idx: usize) -> Result<JoinRelationSet> {
        self.get_relation_set(&[idx as IndexType].iter().cloned().collect())
    }

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
