// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_exception::Result;

use crate::ExpressionAction;
use crate::PlanNode;

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct JoinPlan {
    /// TODO: Support outer join and semi join

    /// The conjunctions of join condition
    pub conditions: Vec<ExpressionAction>,
    pub left_input: Arc<PlanNode>,
    pub right_input: Arc<PlanNode>
}

impl JoinPlan {
    /// TODO: support duplicated column name
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(
            DataSchema::try_merge(vec![
                (*self.left_input.schema()).clone(),
                (*self.right_input.schema()).clone(),
            ])
            .unwrap()
        )
    }

    pub fn get_left_child(&self) -> Arc<PlanNode> {
        self.left_input.clone()
    }

    pub fn get_right_child(&self) -> Arc<PlanNode> {
        self.right_input.clone()
    }

    pub fn set_left_child(&mut self, child: &PlanNode) -> Result<()> {
        self.left_input = Arc::new(child.clone());
        Ok(())
    }

    pub fn set_right_child(&mut self, child: &PlanNode) -> Result<()> {
        self.right_input = Arc::new(child.clone());
        Ok(())
    }
}
