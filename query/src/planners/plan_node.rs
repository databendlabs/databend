// Copyright 2021 Datafuse Labs.
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

use crate::planners::plan_broadcast::BroadcastPlan;
use crate::planners::plan_subqueries_set::SubQueriesSetPlan;
use crate::planners::plan_user_stage_create::CreateUserStagePlan;
use crate::planners::plan_user_udf_alter::AlterUDFPlan;
use crate::planners::plan_user_udf_create::CreateUDFPlan;
use crate::planners::plan_user_udf_drop::DropUDFPlan;
use crate::planners::plan_user_udf_show::ShowUDFPlan;
use crate::planners::AggregatorFinalPlan;
use crate::planners::AggregatorPartialPlan;
use crate::planners::AlterUserPlan;
use crate::planners::CopyPlan;
use crate::planners::CreateDatabasePlan;
use crate::planners::CreateTablePlan;
use crate::planners::CreateUserPlan;
use crate::planners::DescribeStagePlan;
use crate::planners::DescribeTablePlan;
use crate::planners::DropDatabasePlan;
use crate::planners::DropTablePlan;
use crate::planners::DropUserPlan;
use crate::planners::DropUserStagePlan;
use crate::planners::EmptyPlan;
use crate::planners::ExplainPlan;
use crate::planners::ExpressionPlan;
use crate::planners::FilterPlan;
use crate::planners::GrantPrivilegePlan;
use crate::planners::HavingPlan;
use crate::planners::InsertPlan;
use crate::planners::KillPlan;
use crate::planners::LimitByPlan;
use crate::planners::LimitPlan;
use crate::planners::OptimizeTablePlan;
use crate::planners::ProjectionPlan;
use crate::planners::ReadDataSourcePlan;
use crate::planners::RemotePlan;
use crate::planners::RevokePrivilegePlan;
use crate::planners::SelectPlan;
use crate::planners::SettingPlan;
use crate::planners::ShowCreateDatabasePlan;
use crate::planners::ShowCreateTablePlan;
use crate::planners::ShowGrantsPlan;
use crate::planners::SinkPlan;
use crate::planners::SortPlan;
use crate::planners::StagePlan;
use crate::planners::TruncateTablePlan;
use crate::planners::UseDatabasePlan;
use crate::planners::UseTenantPlan;

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
    Sink(SinkPlan),
    Select(SelectPlan),
    Explain(ExplainPlan),
    CreateDatabase(CreateDatabasePlan),
    DropDatabase(DropDatabasePlan),
    ShowCreateDatabase(ShowCreateDatabasePlan),
    CreateTable(CreateTablePlan),
    DescribeTable(DescribeTablePlan),
    DropTable(DropTablePlan),
    OptimizeTable(OptimizeTablePlan),
    TruncateTable(TruncateTablePlan),
    UseDatabase(UseDatabasePlan),
    UseTenant(UseTenantPlan),
    SetVariable(SettingPlan),
    Insert(InsertPlan),
    Copy(CopyPlan),
    ShowCreateTable(ShowCreateTablePlan),
    SubQueryExpression(SubQueriesSetPlan),
    Kill(KillPlan),
    CreateUser(CreateUserPlan),
    AlterUser(AlterUserPlan),
    DropUser(DropUserPlan),
    GrantPrivilege(GrantPrivilegePlan),
    RevokePrivilege(RevokePrivilegePlan),
    CreateUserStage(CreateUserStagePlan),
    DropUserStage(DropUserStagePlan),
    DescribeStage(DescribeStagePlan),
    ShowGrants(ShowGrantsPlan),
    CreateUDF(CreateUDFPlan),
    DropUDF(DropUDFPlan),
    ShowUDF(ShowUDFPlan),
    AlterUDF(AlterUDFPlan),
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
            PlanNode::OptimizeTable(v) => v.schema(),
            PlanNode::DescribeStage(v) => v.schema(),
            PlanNode::TruncateTable(v) => v.schema(),
            PlanNode::SetVariable(v) => v.schema(),
            PlanNode::Sort(v) => v.schema(),
            PlanNode::UseDatabase(v) => v.schema(),
            PlanNode::UseTenant(v) => v.schema(),
            PlanNode::Insert(v) => v.schema(),
            PlanNode::ShowCreateTable(v) => v.schema(),
            PlanNode::SubQueryExpression(v) => v.schema(),
            PlanNode::Kill(v) => v.schema(),
            PlanNode::CreateUser(v) => v.schema(),
            PlanNode::AlterUser(v) => v.schema(),
            PlanNode::DropUser(v) => v.schema(),
            PlanNode::GrantPrivilege(v) => v.schema(),
            PlanNode::RevokePrivilege(v) => v.schema(),
            PlanNode::Sink(v) => v.schema(),
            PlanNode::Copy(v) => v.schema(),
            PlanNode::CreateUserStage(v) => v.schema(),
            PlanNode::DropUserStage(v) => v.schema(),
            PlanNode::ShowGrants(v) => v.schema(),
            PlanNode::ShowCreateDatabase(v) => v.schema(),
            PlanNode::CreateUDF(v) => v.schema(),
            PlanNode::DropUDF(v) => v.schema(),
            PlanNode::ShowUDF(v) => v.schema(),
            PlanNode::AlterUDF(v) => v.schema(),
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
            PlanNode::OptimizeTable(_) => "OptimizeTablePlan",
            PlanNode::DescribeStage(_) => "DescribeStagePlan",
            PlanNode::DropTable(_) => "DropTablePlan",
            PlanNode::TruncateTable(_) => "TruncateTablePlan",
            PlanNode::SetVariable(_) => "SetVariablePlan",
            PlanNode::Sort(_) => "SortPlan",
            PlanNode::UseDatabase(_) => "UseDatabasePlan",
            PlanNode::UseTenant(_) => "UseTenant",
            PlanNode::Insert(_) => "InsertPlan",
            PlanNode::ShowCreateTable(_) => "ShowCreateTablePlan",
            PlanNode::SubQueryExpression(_) => "CreateSubQueriesSets",
            PlanNode::Kill(_) => "KillQuery",
            PlanNode::CreateUser(_) => "CreateUser",
            PlanNode::AlterUser(_) => "AlterUser",
            PlanNode::DropUser(_) => "DropUser",
            PlanNode::GrantPrivilege(_) => "GrantPrivilegePlan",
            PlanNode::RevokePrivilege(_) => "RevokePrivilegePlan",
            PlanNode::Sink(_) => "SinkPlan",
            PlanNode::Copy(_) => "CopyPlan",
            PlanNode::CreateUserStage(_) => "CreateUserStagePlan",
            PlanNode::DropUserStage(_) => "DropUserStagePlan",
            PlanNode::ShowGrants(_) => "ShowGrantsPlan",
            PlanNode::ShowCreateDatabase(_) => "ShowCreateDatabasePlan",
            PlanNode::CreateUDF(_) => "CreateUDFPlan",
            PlanNode::DropUDF(_) => "DropUDFPlan",
            PlanNode::ShowUDF(_) => "ShowUDF",
            PlanNode::AlterUDF(_) => "AlterUDF",
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
            PlanNode::Sink(v) => vec![v.input.clone()],

            _ => vec![],
        }
    }

    pub fn input(&self, n: usize) -> Arc<PlanNode> {
        self.inputs()[n].clone()
    }
}
