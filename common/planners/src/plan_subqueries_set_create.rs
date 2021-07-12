// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::DataField;
use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;

use crate::Expression;
use crate::PlanNode;

#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq)]
pub struct SubQueriesSetsPlan {
    pub expressions: Vec<Expression>,
    pub input: Arc<PlanNode>,
}

impl SubQueriesSetsPlan {
    pub fn schema(&self) -> DataSchemaRef {
        let schema = self.input.schema();
        let mut schema_fields = schema.fields().clone();
        for expression in &self.expressions {
            match expression {
                Expression::Subquery { name, query_plan } => {
                    let subquery_field_type = expression.to_subquery_type(query_plan);
                    schema_fields.push(DataField::new(name, subquery_field_type, false));
                }
                Expression::ScalarSubquery { name, query_plan } => {
                    let subquery_field_type = expression.to_scalar_subquery_type(query_plan);
                    schema_fields.push(DataField::new(name, subquery_field_type, false));
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
