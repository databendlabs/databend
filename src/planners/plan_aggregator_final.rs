// Copyright 2020-2021 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::sync::Arc;

use crate::datavalues::DataSchemaRef;
use crate::planners::{ExpressionPlan, PlanNode};

#[derive(Clone)]
pub struct AggregatorFinalPlan {
    pub aggr_expr: Vec<ExpressionPlan>,
    pub group_expr: Vec<ExpressionPlan>,
    pub schema: DataSchemaRef,
    pub input: Arc<PlanNode>,
}

impl AggregatorFinalPlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.schema.clone()
    }
}
