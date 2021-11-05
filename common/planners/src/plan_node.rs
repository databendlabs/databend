// Copyright 2020 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::plan_broadcast::BroadcastPlan;
use crate::plan_subqueries_set::SubQueriesSetPlan;
use crate::AggregatorFinalPlan;
use crate::AggregatorPartialPlan;
use crate::CreateDatabasePlan;
use crate::CreateTablePlan;
use crate::CreateUserPlan;
use crate::DescribeTablePlan;
use crate::DropDatabasePlan;
use crate::DropTablePlan;
use crate::EmptyPlan;
use crate::ExplainPlan;
use crate::ExpressionPlan;
use crate::FilterPlan;
use crate::HavingPlan;
use crate::InsertIntoPlan;
use crate::KillPlan;
use crate::LimitByPlan;
use crate::LimitPlan;
use crate::ProjectionPlan;
use crate::ReadDataSourcePlan;
use crate::RemotePlan;
use crate::SelectPlan;
use crate::SettingPlan;
use crate::ShowCreateTablePlan;
use crate::SortPlan;
use crate::StagePlan;
use crate::TruncateTablePlan;
use crate::UseDatabasePlan;

#[allow(clippy::large_enum_variant)]
#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq)]
pub enum PlanNode {
    Empty(EmptyPlan),
    Stage(StagePlan),
    Broadcast(BroadcastPlan),
    Remote(RemotePlan),
    Projection(ProjectionPlan),
    Expression(ExpressionPlan),
    AggregatorPartial(AggregatorPartialPlan),
    AggregatorFinal(AggregatorFinalPlan),
    Filter(FilterPlan),
    Having(HavingPlan),
    Sort(SortPlan),
    Limit(LimitPlan),
    LimitBy(LimitByPlan),
    ReadSource(ReadDataSourcePlan),
    Select(SelectPlan),
    Explain(ExplainPlan),
    CreateDatabase(CreateDatabasePlan),
    DropDatabase(DropDatabasePlan),
    CreateTable(CreateTablePlan),
    DescribeTable(DescribeTablePlan),
    DropTable(DropTablePlan),
    TruncateTable(TruncateTablePlan),
    UseDatabase(UseDatabasePlan),
    SetVariable(SettingPlan),
    InsertInto(InsertIntoPlan),
    ShowCreateTable(ShowCreateTablePlan),
    SubQueryExpression(SubQueriesSetPlan),
    Kill(KillPlan),
    CreateUser(CreateUserPlan),
}

impl PlanNode {
    /// Get a reference to the logical plan's schema
    pub fn schema(&self) -> DataSchemaRef {
        match self {
            PlanNode::Empty(v) => v.schema(),
            PlanNode::Stage(v) => v.schema(),
            PlanNode::Broadcast(v) => v.schema(),
            PlanNode::Remote(v) => v.schema(),
            PlanNode::Projection(v) => v.schema(),
            PlanNode::Expression(v) => v.schema(),
            PlanNode::AggregatorPartial(v) => v.schema(),
            PlanNode::AggregatorFinal(v) => v.schema(),
            PlanNode::Filter(v) => v.schema(),
            PlanNode::Having(v) => v.schema(),
            PlanNode::Limit(v) => v.schema(),
            PlanNode::LimitBy(v) => v.schema(),
            PlanNode::ReadSource(v) => v.schema(),
            PlanNode::Select(v) => v.schema(),
            PlanNode::Explain(v) => v.schema(),
            PlanNode::CreateDatabase(v) => v.schema(),
            PlanNode::DropDatabase(v) => v.schema(),
            PlanNode::CreateTable(v) => v.schema(),
            PlanNode::DropTable(v) => v.schema(),
            PlanNode::DescribeTable(v) => v.schema(),
            PlanNode::TruncateTable(v) => v.schema(),
            PlanNode::SetVariable(v) => v.schema(),
            PlanNode::Sort(v) => v.schema(),
            PlanNode::UseDatabase(v) => v.schema(),
            PlanNode::InsertInto(v) => v.schema(),
            PlanNode::ShowCreateTable(v) => v.schema(),
            PlanNode::SubQueryExpression(v) => v.schema(),
            PlanNode::Kill(v) => v.schema(),
            PlanNode::CreateUser(v) => v.schema(),
        }
    }

    pub fn name(&self) -> &str {
        match self {
            PlanNode::Empty(_) => "EmptyPlan",
            PlanNode::Stage(_) => "StagePlan",
            PlanNode::Broadcast(_) => "BroadcastPlan",
            PlanNode::Remote(_) => "RemotePlan",
            PlanNode::Projection(_) => "ProjectionPlan",
            PlanNode::Expression(_) => "ExpressionPlan",
            PlanNode::AggregatorPartial(_) => "AggregatorPartialPlan",
            PlanNode::AggregatorFinal(_) => "AggregatorFinalPlan",
            PlanNode::Filter(_) => "FilterPlan",
            PlanNode::Having(_) => "HavingPlan",
            PlanNode::Limit(_) => "LimitPlan",
            PlanNode::LimitBy(_) => "LimitByPlan",
            PlanNode::ReadSource(_) => "ReadSourcePlan",
            PlanNode::Select(_) => "SelectPlan",
            PlanNode::Explain(_) => "ExplainPlan",
            PlanNode::CreateDatabase(_) => "CreateDatabasePlan",
            PlanNode::DropDatabase(_) => "DropDatabasePlan",
            PlanNode::CreateTable(_) => "CreateTablePlan",
            PlanNode::DescribeTable(_) => "DescribeTablePlan",
            PlanNode::DropTable(_) => "DropTablePlan",
            PlanNode::TruncateTable(_) => "TruncateTablePlan",
            PlanNode::SetVariable(_) => "SetVariablePlan",
            PlanNode::Sort(_) => "SortPlan",
            PlanNode::UseDatabase(_) => "UseDatabasePlan",
            PlanNode::InsertInto(_) => "InsertIntoPlan",
            PlanNode::ShowCreateTable(_) => "ShowCreateTablePlan",
            PlanNode::SubQueryExpression(_) => "CreateSubQueriesSets",
            PlanNode::Kill(_) => "KillQuery",
            PlanNode::CreateUser(_) => "CreateUser",
        }
    }

    pub fn inputs(&self) -> Vec<Arc<PlanNode>> {
        match self {
            PlanNode::Stage(v) => vec![v.input.clone()],
            PlanNode::Broadcast(v) => vec![v.input.clone()],
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
            PlanNode::SubQueryExpression(v) => v.get_inputs(),

            _ => vec![],
        }
    }

    pub fn input(&self, n: usize) -> Arc<PlanNode> {
        self.inputs()[n].clone()
    }

    pub fn set_inputs(&mut self, inputs: Vec<&PlanNode>) -> Result<()> {
        if inputs.is_empty() {
            return Result::Err(ErrorCode::BadPlanInputs("Inputs must not be empty"));
        }

        match self {
            PlanNode::Stage(v) => v.set_input(inputs[0]),
            PlanNode::Broadcast(v) => v.set_input(inputs[0]),
            PlanNode::Projection(v) => v.set_input(inputs[0]),
            PlanNode::Expression(v) => v.set_input(inputs[0]),
            PlanNode::AggregatorPartial(v) => v.set_input(inputs[0]),
            PlanNode::AggregatorFinal(v) => v.set_input(inputs[0]),
            PlanNode::Filter(v) => v.set_input(inputs[0]),
            PlanNode::Having(v) => v.set_input(inputs[0]),
            PlanNode::Limit(v) => v.set_input(inputs[0]),
            PlanNode::Explain(v) => v.set_input(inputs[0]),
            PlanNode::Select(v) => v.set_input(inputs[0]),
            PlanNode::Sort(v) => v.set_input(inputs[0]),
            PlanNode::SubQueryExpression(v) => v.set_inputs(inputs),
            _ => {
                return Err(ErrorCode::UnImplement(format!(
                    "UnImplement set_inputs for {:?}",
                    self
                )));
            }
        }

        Ok(())
    }
}
