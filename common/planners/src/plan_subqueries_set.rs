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

use common_datavalues::DataField;
use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;

use crate::Expression;
use crate::PlanNode;

#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq)]
pub struct SubQueriesSetPlan {
    pub expressions: Vec<Expression>,
    pub input: Arc<PlanNode>,
}

impl SubQueriesSetPlan {
    pub fn schema(&self) -> DataSchemaRef {
        let schema = self.input.schema();
        let mut schema_fields = schema.fields().clone();
        for expression in &self.expressions {
            match expression {
                Expression::Subquery { name, query_plan } => {
                    let subquery_data_type = Expression::to_subquery_type(query_plan);
                    schema_fields.push(DataField::new(name, subquery_data_type));
                }
                Expression::ScalarSubquery { name, query_plan } => {
                    let subquery_data_type = Expression::to_scalar_subquery_type(query_plan);
                    schema_fields.push(DataField::new(name, subquery_data_type));
                }
                _ => panic!("Logical error, expressions must be Subquery or ScalarSubquery"),
            };
        }

        Arc::new(DataSchema::new(schema_fields))
    }

    pub fn get_inputs(&self) -> Vec<Arc<PlanNode>> {
        let mut inputs = Vec::with_capacity(self.expressions.len() + 1);
        for expression in &self.expressions {
            match expression {
                Expression::Subquery { query_plan, .. } => inputs.push(query_plan.clone()),
                Expression::ScalarSubquery { query_plan, .. } => inputs.push(query_plan.clone()),
                _ => panic!("Logical error, expressions must be Subquery or ScalarSubquery"),
            };
        }

        inputs.push(self.input.clone());
        inputs
    }

    #[allow(clippy::needless_range_loop)]
    pub fn set_inputs(&mut self, inputs: Vec<&PlanNode>) {
        assert_eq!(self.expressions.len(), inputs.len() - 1);
        for index in 0..self.expressions.len() {
            self.expressions[index] = match &self.expressions[index] {
                Expression::Subquery { name, .. } => Expression::Subquery {
                    name: name.clone(),
                    query_plan: Arc::new(inputs[index].clone()),
                },
                Expression::ScalarSubquery { name, .. } => Expression::ScalarSubquery {
                    name: name.clone(),
                    query_plan: Arc::new(inputs[index].clone()),
                },
                _ => panic!("Logical error, expressions must be Subquery or ScalarSubquery"),
            };
        }

        self.input = Arc::new(inputs[inputs.len() - 1].clone());
    }
}
