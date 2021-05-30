// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::DataSchemaRef;

use crate::AggregatorFinalPlan;
use crate::AggregatorPartialPlan;
use crate::CreateDatabasePlan;
use crate::CreateTablePlan;
use crate::DropDatabasePlan;
use crate::DropTablePlan;
use crate::EmptyPlan;
use crate::ExplainPlan;
use crate::ExpressionPlan;
use crate::FilterPlan;
use crate::HavingPlan;
use crate::JoinPlan;
use crate::InsertIntoPlan;
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
    Expression(ExpressionPlan),
    AggregatorPartial(AggregatorPartialPlan),
    AggregatorFinal(AggregatorFinalPlan),
    Filter(FilterPlan),
    Having(HavingPlan),
    Sort(SortPlan),
    Limit(LimitPlan),
    Scan(ScanPlan),
    Join(JoinPlan),
    ReadSource(ReadDataSourcePlan),
    Select(SelectPlan),
    Explain(ExplainPlan),
    CreateDatabase(CreateDatabasePlan),
    DropDatabase(DropDatabasePlan),
    CreateTable(CreateTablePlan),
    DropTable(DropTablePlan),
    UseDatabase(UseDatabasePlan),
    SetVariable(SettingPlan),
    InsertInto(InsertIntoPlan)
}

impl PlanNode {
    /// Get a reference to the logical plan's schema
    pub fn schema(&self) -> DataSchemaRef {
        match self {
            PlanNode::Empty(v) => v.schema(),
            PlanNode::Stage(v) => v.schema(),
            PlanNode::Scan(v) => v.schema(),
            PlanNode::Projection(v) => v.schema(),
            PlanNode::Expression(v) => v.schema(),
            PlanNode::AggregatorPartial(v) => v.schema(),
            PlanNode::AggregatorFinal(v) => v.schema(),
            PlanNode::Filter(v) => v.schema(),
            PlanNode::Having(v) => v.schema(),
            PlanNode::Join(v) => v.schema(),
            PlanNode::Limit(v) => v.schema(),
            PlanNode::ReadSource(v) => v.schema(),
            PlanNode::Select(v) => v.schema(),
            PlanNode::Explain(v) => v.schema(),
            PlanNode::CreateDatabase(v) => v.schema(),
            PlanNode::DropDatabase(v) => v.schema(),
            PlanNode::CreateTable(v) => v.schema(),
            PlanNode::DropTable(v) => v.schema(),
            PlanNode::SetVariable(v) => v.schema(),
            PlanNode::Sort(v) => v.schema(),
            PlanNode::UseDatabase(v) => v.schema(),
            PlanNode::InsertInto(v) => v.schema()
        }
    }

    pub fn name(&self) -> &str {
        match self {
            PlanNode::Empty(_) => "EmptyPlan",
            PlanNode::Stage(_) => "StagePlan",
            PlanNode::Scan(_) => "ScanPlan",
            PlanNode::Projection(_) => "ProjectionPlan",
            PlanNode::Expression(_) => "ExpressionPlan",
            PlanNode::AggregatorPartial(_) => "AggregatorPartialPlan",
            PlanNode::AggregatorFinal(_) => "AggregatorFinalPlan",
            PlanNode::Filter(_) => "FilterPlan",
            PlanNode::Having(_) => "HavingPlan",
            PlanNode::Join(_) => "JoinPlan",
            PlanNode::Limit(_) => "LimitPlan",
            PlanNode::ReadSource(_) => "ReadSourcePlan",
            PlanNode::Select(_) => "SelectPlan",
            PlanNode::Explain(_) => "ExplainPlan",
            PlanNode::CreateDatabase(_) => "CreateDatabasePlan",
            PlanNode::DropDatabase(_) => "DropDatabasePlan",
            PlanNode::CreateTable(_) => "CreateTablePlan",
            PlanNode::DropTable(_) => "DropTablePlan",
            PlanNode::SetVariable(_) => "SetVariablePlan",
            PlanNode::Sort(_) => "SortPlan",
            PlanNode::UseDatabase(_) => "UseDatabasePlan",
            PlanNode::InsertInto(_) => "InsertIntoPlan"
        }
    }

    pub fn inputs(&self) -> Vec<Arc<PlanNode>> {
        match self {
            PlanNode::Stage(v) => vec![v.input.clone()],
            PlanNode::Projection(v) => vec![v.input.clone()],
            PlanNode::Expression(v) => vec![v.input.clone()],
            PlanNode::AggregatorPartial(v) => vec![v.input.clone()],
            PlanNode::AggregatorFinal(v) => vec![v.input.clone()],
            PlanNode::Filter(v) => vec![v.input.clone()],
            PlanNode::Having(v) => vec![v.input.clone()],
            PlanNode::Limit(v) => vec![v.input.clone()],
            PlanNode::Explain(v) => vec![v.input.clone()],
            PlanNode::Select(v) => vec![v.input.clone()],
            PlanNode::Sort(v) => vec![v.input.clone()],

            _ => vec![]
        }
    }

    pub fn input(&self, n: usize) -> Arc<PlanNode> {
        self.inputs()[n].clone()
    }
}
