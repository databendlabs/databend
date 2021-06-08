// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::DataSchemaRef;

use crate::Expression;
use crate::PlanNode;
use crate::find_exists_exprs;

#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq)]
pub struct FilterPlan {
    /// The predicate expression, which must have Boolean type.
    pub predicate: Expression,
    /// The incoming logical plan
    pub input: Arc<PlanNode>,
}

impl FilterPlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.input.schema()
    }

    pub fn set_input(&mut self, node: &PlanNode) {
        self.input = Arc::new(node.clone());
    }

    pub fn get_inputs_from_expr(&self) -> Option<Vec<Arc<PlanNode>>> {
        let expr_vec = find_exists_exprs(&[self.predicate.clone()]);
        if expr_vec.len() == 0 {
            return None;
        }
        let mut vec = Vec::new();
        for e in expr_vec {
            if let Expression::Exists(p) = e {
                vec.push(p.clone());
            }
        }
        return Some(vec);
    }
}
