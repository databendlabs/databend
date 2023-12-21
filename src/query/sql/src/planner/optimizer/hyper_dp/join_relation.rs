// Copyright 2021 Datafuse Labs
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
use databend_common_exception::Result;

use crate::optimizer::RelExpr;
use crate::optimizer::SExpr;
use crate::IndexType;

pub struct JoinRelation {
    s_expr: SExpr,
}

impl JoinRelation {
    pub fn new(s_expr: &SExpr) -> Self {
        Self {
            s_expr: s_expr.clone(),
        }
    }

    pub fn s_expr(&self) -> SExpr {
        self.s_expr.clone()
    }

    pub fn cardinality(&self) -> Result<f64> {
        let rel_expr = RelExpr::with_s_expr(&self.s_expr);
        Ok(rel_expr.derive_cardinality()?.cardinality)
    }
}

#[derive(Default, Clone)]
struct RelationSetNode {
    relations: Vec<IndexType>,
    // Key is relation id
    children: HashMap<IndexType, RelationSetNode>,
}
// The tree is initialized by join conditions' relation sets
// Such as condition: t1.a + t2.b == t3.b , the tree will be
//   root: [{},{1, 3}]
//             /    \
//       [{},{2}]  [{3}, {}]
//           /
//     [{1, 2}, {}]
#[derive(Default, Clone)]
pub struct RelationSetTree {
    root: RelationSetNode,
}

impl RelationSetTree {
    pub fn get_relation_set_by_index(&mut self, idx: usize) -> Result<Vec<IndexType>> {
        self.get_relation_set(&[idx as IndexType].iter().cloned().collect())
    }

    pub fn get_relation_set(&mut self, idx_set: &HashSet<IndexType>) -> Result<Vec<IndexType>> {
        let mut relations: Vec<IndexType> = idx_set.iter().copied().collect();
        // Make relations ordered
        relations.sort();
        let mut node = &mut self.root;
        for idx in relations.iter() {
            if !node.children.contains_key(idx) {
                node.children.insert(*idx, RelationSetNode::default());
            }
            node = node.children.get_mut(idx).unwrap();
        }
        if node.relations.is_empty() {
            node.relations = relations;
        }
        Ok(node.relations.clone())
    }
}
