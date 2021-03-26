// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::{DataSchema, DataSchemaRef};

use crate::PlannerResult;
use crate::{
    AggregatorFinalPlan, AggregatorPartialPlan, CreatePlan, EmptyPlan, ExplainPlan, FilterPlan,
    LimitPlan, ProjectionPlan, ReadDataSourcePlan, ScanPlan, SelectPlan, SettingPlan, StagePlan,
};

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub enum PlanNode {
    Empty(EmptyPlan),
    Stage(StagePlan),
    Projection(ProjectionPlan),
    AggregatorPartial(AggregatorPartialPlan),
    AggregatorFinal(AggregatorFinalPlan),
    Filter(FilterPlan),
    Limit(LimitPlan),
    Scan(ScanPlan),
    ReadSource(ReadDataSourcePlan),
    Explain(ExplainPlan),
    Select(SelectPlan),
    Create(CreatePlan),
    SetVariable(SettingPlan),
}

impl PlanNode {
    /// Get a reference to the logical plan's schema
    pub fn schema(&self) -> DataSchemaRef {
        match self {
            PlanNode::Empty(v) => v.schema(),
            PlanNode::Stage(v) => v.schema(),
            PlanNode::Scan(v) => v.schema(),
            PlanNode::Projection(v) => v.schema(),
            PlanNode::AggregatorPartial(v) => v.schema(),
            PlanNode::AggregatorFinal(v) => v.schema(),
            PlanNode::Filter(v) => v.schema(),
            PlanNode::Limit(v) => v.schema(),
            PlanNode::ReadSource(v) => v.schema(),
            PlanNode::Select(v) => v.schema(),
            PlanNode::Explain(v) => v.schema(),
            PlanNode::SetVariable(v) => v.schema(),
            PlanNode::Create(v) => v.schema(),
        }
    }

    pub fn name(&self) -> &str {
        match self {
            PlanNode::Empty(_) => "EmptyPlan",
            PlanNode::Stage(_) => "StagePlan",
            PlanNode::Scan(_) => "ScanPlan",
            PlanNode::Projection(_) => "ProjectionPlan",
            PlanNode::AggregatorPartial(_) => "AggregatorPartialPlan",
            PlanNode::AggregatorFinal(_) => "AggregatorFinalPlan",
            PlanNode::Filter(_) => "FilterPlan",
            PlanNode::Limit(_) => "LimitPlan",
            PlanNode::ReadSource(_) => "ReadSourcePlan",
            PlanNode::Select(_) => "SelectPlan",
            PlanNode::Explain(_) => "ExplainPlan",
            PlanNode::SetVariable(_) => "SetVariablePlan",
            PlanNode::Create(_) => "CreatePlan",
        }
    }

    pub fn input(&self) -> Arc<PlanNode> {
        match self {
            PlanNode::Stage(v) => v.input(),
            PlanNode::Projection(v) => v.input(),
            PlanNode::AggregatorPartial(v) => v.input(),
            PlanNode::AggregatorFinal(v) => v.input(),
            PlanNode::Filter(v) => v.input(),
            PlanNode::Limit(v) => v.input(),
            PlanNode::Explain(v) => v.input(),
            PlanNode::Select(v) => v.input(),

            _ => Arc::new(PlanNode::Empty(EmptyPlan {
                schema: Arc::new(DataSchema::empty()),
            })),
        }
    }

    pub fn set_input(&mut self, node: &PlanNode) -> PlannerResult<()> {
        match self {
            PlanNode::Stage(v) => v.set_input(node),
            PlanNode::Projection(v) => v.set_input(node),
            PlanNode::AggregatorPartial(v) => v.set_input(node),
            PlanNode::AggregatorFinal(v) => v.set_input(node),
            PlanNode::Filter(v) => v.set_input(node),
            PlanNode::Limit(v) => v.set_input(node),
            PlanNode::Explain(v) => v.set_input(node),
            PlanNode::Select(v) => v.set_input(node),

            _ => Ok(()),
        }
    }
}
