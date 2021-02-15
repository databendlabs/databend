// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use std::sync::Arc;

use crate::datavalues::DataSchemaRef;
use crate::planners::{ExpressionPlan, PlanNode};

#[derive(Clone)]
pub struct FilterPlan {
    /// The predicate expression, which must have Boolean type.
    pub predicate: ExpressionPlan,
    /// The incoming logical plan
    pub input: Arc<PlanNode>,
}

impl FilterPlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.input.schema()
    }
}
