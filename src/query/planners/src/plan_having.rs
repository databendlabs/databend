// Copyright 2021 Datafuse Labs.
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

use std::sync::Arc;

use common_datavalues::DataSchemaRef;
use common_datavalues::DataValue;

use crate::Expression;
use crate::PlanNode;

#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq)]
pub struct HavingPlan {
    /// The predicate expression, which must have Boolean type.
    pub predicate: Expression,
    /// The incoming logical plan
    pub input: Arc<PlanNode>,
    /// Output data schema
    pub schema: DataSchemaRef,
}

impl HavingPlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.schema.clone()
    }

    pub fn set_input(&mut self, node: &PlanNode) {
        self.input = Arc::new(node.clone());
    }

    pub fn is_literal_false(&self) -> bool {
        if let Expression::Literal { value, .. } = &self.predicate {
            return *value == DataValue::Boolean(false);
        }
        false
    }
}
