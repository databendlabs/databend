// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use std::sync::Arc;

use crate::datavalues::DataSchemaRef;
use crate::planners::{ExpressionPlan, PlanNode};

/// Evaluates an arbitrary list of expressions (essentially a
/// SELECT with an expression list) on its input.
#[derive(Clone)]
pub struct ProjectionPlan {
    /// The list of expressions
    pub expr: Vec<ExpressionPlan>,
    /// The schema description of the output
    pub schema: DataSchemaRef,
    /// The incoming logical plan
    pub input: Arc<PlanNode>,
}

impl ProjectionPlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.schema.clone()
    }
}
