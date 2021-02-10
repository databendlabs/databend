// Copyright 2020-2021 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::sync::Arc;

use crate::datavalues::DataSchemaRef;
use crate::planners::PlanNode;

#[derive(Clone)]
pub struct StagePlan {
    pub input: Arc<PlanNode>,
}

impl StagePlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.input.schema()
    }
}
