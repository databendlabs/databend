// Copyright 2020-2021 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use std::sync::Arc;

use crate::datavalues::DataSchemaRef;
use crate::error::FuseQueryResult;
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

    pub fn input(&self) -> Arc<PlanNode> {
        self.input.clone()
    }

    pub fn set_input(&mut self, input: &PlanNode) -> FuseQueryResult<()> {
        self.input = Arc::new(input.clone());
        Ok(())
    }
}
