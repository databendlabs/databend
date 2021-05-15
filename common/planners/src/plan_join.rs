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
    pub left_child: Arc<PlanNode>,
    pub right_child: Arc<PlanNode>
}

impl JoinPlan {
    /// TODO: support duplicated column name
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(
            DataSchema::try_merge(vec![
                (*self.left_child.schema()).clone(),
                (*self.right_child.schema()).clone(),
            ])
            .unwrap()
        )
    }

    pub fn get_left_child(&self) -> Arc<PlanNode> {
        self.left_child.clone()
    }

    pub fn get_right_child(&self) -> Arc<PlanNode> {
        self.right_child.clone()
    }

    pub fn set_left_child(&mut self, child: &PlanNode) -> Result<()> {
        self.left_child = Arc::new(child.clone());
        Ok(())
    }

    pub fn set_right_child(&mut self, child: &PlanNode) -> Result<()> {
        self.right_child = Arc::new(child.clone());
        Ok(())
    }
}
