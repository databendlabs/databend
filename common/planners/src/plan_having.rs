// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::DataSchemaRef;

use crate::Expression;
use crate::PlanNode;

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct HavingPlan {
    /// The predicate expression, which must have Boolean type.
    pub predicate: Expression,
    /// The incoming logical plan
    pub input: Arc<PlanNode>
}

impl HavingPlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.input.schema()
    }
}
