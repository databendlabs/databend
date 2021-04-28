// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_exception::Result;
use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;

use crate::AggregatorFinalPlan;
use crate::AggregatorPartialPlan;
use crate::CreateDatabasePlan;
use crate::CreateTablePlan;
use crate::EmptyPlan;
use crate::ExplainPlan;
use crate::FilterPlan;
use crate::LimitPlan;
use crate::ProjectionPlan;
use crate::ReadDataSourcePlan;
use crate::ScanPlan;
use crate::SelectPlan;
use crate::SettingPlan;
use crate::SortPlan;
use crate::StagePlan;
use crate::UseDatabasePlan;

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub enum PlanNode {
    Empty(EmptyPlan),
    Stage(StagePlan),
    Projection(ProjectionPlan),
    AggregatorPartial(AggregatorPartialPlan),
    AggregatorFinal(AggregatorFinalPlan),
    Filter(FilterPlan),
    Sort(SortPlan),
    Limit(LimitPlan),
    Scan(ScanPlan),
    ReadSource(ReadDataSourcePlan),
    Select(SelectPlan),
    Explain(ExplainPlan),
    CreateTable(CreateTablePlan),
    CreateDatabase(CreateDatabasePlan),
    UseDatabase(UseDatabasePlan),
    SetVariable(SettingPlan)
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
            PlanNode::CreateDatabase(v) => v.schema(),
            PlanNode::CreateTable(v) => v.schema(),
            PlanNode::SetVariable(v) => v.schema(),
            PlanNode::Sort(v) => v.schema(),
            PlanNode::UseDatabase(v) => v.schema()
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
            PlanNode::CreateTable(_) => "CreateTablePlan",
            PlanNode::CreateDatabase(_) => "CreateDatabasePlan",
            PlanNode::SetVariable(_) => "SetVariablePlan",
            PlanNode::Sort(_) => "SortPlan",
            PlanNode::UseDatabase(_) => "UseDatabasePlan"
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
            PlanNode::Sort(v) => v.input(),

            _ => Arc::new(PlanNode::Empty(EmptyPlan {
                schema: Arc::new(DataSchema::empty())
            }))
        }
    }

    pub fn set_input(&mut self, node: &PlanNode) -> Result<()> {
        match self {
            PlanNode::Stage(v) => v.set_input(node),
            PlanNode::Projection(v) => v.set_input(node),
            PlanNode::AggregatorPartial(v) => v.set_input(node),
            PlanNode::AggregatorFinal(v) => v.set_input(node),
            PlanNode::Filter(v) => v.set_input(node),
            PlanNode::Limit(v) => v.set_input(node),
            PlanNode::Explain(v) => v.set_input(node),
            PlanNode::Select(v) => v.set_input(node),
            PlanNode::Sort(v) => v.set_input(node),

            _ => Ok(())
        }
    }
}
