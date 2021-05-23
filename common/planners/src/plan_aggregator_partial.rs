// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::{DataSchemaRef, DataField, DataType, DataSchema};
use common_exception::Result;

use crate::ExpressionAction;
use crate::PlanNode;

#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq)]
pub struct AggregatorPartialPlan {
    pub group_expr: Vec<ExpressionAction>,
    pub aggr_expr: Vec<ExpressionAction>,
    pub input: Arc<PlanNode>,
    pub schema: DataSchemaRef,
}

impl AggregatorPartialPlan {
    pub fn try_create(
        group_expr: Vec<ExpressionAction>,
        aggr_expr: Vec<ExpressionAction>,
        input: Arc<PlanNode>,
    ) -> Result<AggregatorPartialPlan> {
        match group_expr.len() {
            0 => {
                let mut fields = Vec::with_capacity(aggr_expr.len());
                for aggr_expression in &aggr_expr {
                    fields.push(DataField::new(
                        format!("{}", aggr_expression.to_function()?).as_str(),
                        DataType::Utf8,
                        false,
                    ));
                }

                Ok(AggregatorPartialPlan {
                    group_expr: group_expr,
                    aggr_expr: aggr_expr,
                    input: input,
                    schema: Arc::new(DataSchema::new(fields))
                })
            }
            _ => {
                let mut fields = Vec::with_capacity(aggr_expr.len() + 1);
                for aggr_expression in &aggr_expr {
                    fields.push(DataField::new(
                        format!("{}", aggr_expression.to_function()?).as_str(),
                        DataType::Utf8,
                        false,
                    ));
                }

                fields.push(DataField::new("_group_by_key", DataType::Binary, false));

                Ok(AggregatorPartialPlan {
                    group_expr: group_expr,
                    aggr_expr: aggr_expr,
                    input: input,
                    schema: Arc::new(DataSchema::new(fields))
                })
            }
        }
    }

    pub fn schema(&self) -> DataSchemaRef {
        self.schema.clone()
    }

    pub fn set_input(&mut self, node: &PlanNode) {
        self.input = Arc::new(node.clone());
    }
}
