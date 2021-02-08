// Copyright 2020-2021 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::sync::Arc;

use crate::datavalues::DataSchemaRef;
use crate::planners::{ExpressionPlan, PlanNode};

#[derive(Clone)]
pub struct AggregatorPartialPlan {
    pub group_expr: Vec<ExpressionPlan>,
    pub aggr_expr: Vec<ExpressionPlan>,
    pub input: Arc<PlanNode>,
}

impl AggregatorPartialPlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.input.schema()
    }
}
