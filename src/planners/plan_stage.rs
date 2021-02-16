// Copyright 2020-2021 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use std::sync::Arc;

use crate::datavalues::DataSchemaRef;
use crate::planners::PlanNode;

#[derive(Clone, Debug)]
pub enum StageState {
    Normal,
    Through,
    SortMerge,
    GroupByMerge,
    AggregatorMerge,
}

#[derive(Clone)]
pub struct StagePlan {
    pub uuid: String,
    pub id: usize,
    pub state: StageState,
    pub input: Arc<PlanNode>,
}

impl StagePlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.input.schema()
    }
}
