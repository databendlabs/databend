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

use crate::plan_table_undrop::UnDropTablePlan;
use crate::plan_window_func::WindowFuncPlan;
use crate::AggregatorFinalPlan;
use crate::AggregatorPartialPlan;
use crate::AlterClusterKeyPlan;
use crate::AlterUserPlan;
use crate::AlterUserUDFPlan;
use crate::AlterViewPlan;
use crate::BroadcastPlan;
use crate::CallPlan;
use crate::CopyPlan;
use crate::CreateDatabasePlan;
use crate::CreateRolePlan;
use crate::CreateTablePlan;
use crate::CreateUserPlan;
use crate::CreateUserStagePlan;
use crate::CreateUserUDFPlan;
use crate::CreateViewPlan;
use crate::DescribeTablePlan;
use crate::DescribeUserStagePlan;
use crate::DropClusterKeyPlan;
use crate::DropDatabasePlan;
use crate::DropRolePlan;
use crate::DropTablePlan;
use crate::DropUserPlan;
use crate::DropUserStagePlan;
use crate::DropUserUDFPlan;
use crate::DropViewPlan;
use crate::EmptyPlan;
use crate::ExplainPlan;
use crate::ExpressionPlan;
use crate::FilterPlan;
use crate::GrantPrivilegePlan;
use crate::GrantRolePlan;
use crate::HavingPlan;
use crate::InsertPlan;
use crate::KillPlan;
use crate::LimitByPlan;
use crate::LimitPlan;
use crate::ListPlan;
use crate::OptimizeTablePlan;
use crate::ProjectionPlan;
use crate::ReadDataSourcePlan;
use crate::RemotePlan;
use crate::RenameDatabasePlan;
use crate::RenameTablePlan;
use crate::RevokePrivilegePlan;
use crate::RevokeRolePlan;
use crate::SelectPlan;
use crate::SettingPlan;
use crate::ShowCreateDatabasePlan;
use crate::ShowCreateTablePlan;
use crate::ShowPlan;
use crate::SinkPlan;
use crate::SortPlan;
use crate::StagePlan;
use crate::SubQueriesSetPlan;
use crate::TruncateTablePlan;
use crate::UnDropDatabasePlan;
use crate::UseDatabasePlan;

#[allow(clippy::large_enum_variant)]
#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq)]
pub enum PlanNode {
    // Base.
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
    WindowFunc(WindowFuncPlan),
    Sort(SortPlan),
    Limit(LimitPlan),
    LimitBy(LimitByPlan),
    ReadSource(ReadDataSourcePlan),
    SubQueryExpression(SubQueriesSetPlan),
    Sink(SinkPlan),

    // Explain.
    Explain(ExplainPlan),

    // Query.
    Select(SelectPlan),

    // Insert.
    Insert(InsertPlan),

    // Copy.
    Copy(CopyPlan),

    // Call.
    Call(CallPlan),

    // List
    List(ListPlan),

    // Cluster key.
    AlterClusterKey(AlterClusterKeyPlan),
    DropClusterKey(DropClusterKeyPlan),

    // Show.
    Show(ShowPlan),

    // Database.
    CreateDatabase(CreateDatabasePlan),
    DropDatabase(DropDatabasePlan),
    UnDropDatabase(UnDropDatabasePlan),
    RenameDatabase(RenameDatabasePlan),
    ShowCreateDatabase(ShowCreateDatabasePlan),

    // Table.
    CreateTable(CreateTablePlan),
    DropTable(DropTablePlan),
    UnDropTable(UnDropTablePlan),
    RenameTable(RenameTablePlan),
    TruncateTable(TruncateTablePlan),
    OptimizeTable(OptimizeTablePlan),
    DescribeTable(DescribeTablePlan),
    ShowCreateTable(ShowCreateTablePlan),

    // View.
    CreateView(CreateViewPlan),
    DropView(DropViewPlan),
    AlterView(AlterViewPlan),

    // User.
    CreateUser(CreateUserPlan),
    AlterUser(AlterUserPlan),
    DropUser(DropUserPlan),

    // Grant.
    GrantPrivilege(GrantPrivilegePlan),
    GrantRole(GrantRolePlan),

    // Revoke.
    RevokePrivilege(RevokePrivilegePlan),
    RevokeRole(RevokeRolePlan),

    // Role.
    CreateRole(CreateRolePlan),
    DropRole(DropRolePlan),

    // Stage.
    CreateUserStage(CreateUserStagePlan),
    DropUserStage(DropUserStagePlan),
    DescribeUserStage(DescribeUserStagePlan),

    // UDF.
    CreateUserUDF(CreateUserUDFPlan),
    DropUserUDF(DropUserUDFPlan),
    AlterUserUDF(AlterUserUDFPlan),

    // Use.
    UseDatabase(UseDatabasePlan),

    // Set.
    SetVariable(SettingPlan),

    // Kill.
    Kill(KillPlan),
}

impl PlanNode {
    /// Get a reference to the logical plan's schema
    pub fn schema(&self) -> DataSchemaRef {
        match self {
            // Base.
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
            PlanNode::WindowFunc(v) => v.schema(),
            PlanNode::Limit(v) => v.schema(),
            PlanNode::LimitBy(v) => v.schema(),
            PlanNode::ReadSource(v) => v.schema(),
            PlanNode::Sort(v) => v.schema(),
            PlanNode::SubQueryExpression(v) => v.schema(),
            PlanNode::Sink(v) => v.schema(),

            // Explain.
            PlanNode::Explain(v) => v.schema(),

            // Query.
            PlanNode::Select(v) => v.schema(),

            // Insert.
            PlanNode::Insert(v) => v.schema(),

            // Copy.
            PlanNode::Copy(v) => v.schema(),

            // Call.
            PlanNode::Call(v) => v.schema(),

            // Show.
            PlanNode::Show(v) => v.schema(),

            // Database.
            PlanNode::CreateDatabase(v) => v.schema(),
            PlanNode::DropDatabase(v) => v.schema(),
            PlanNode::ShowCreateDatabase(v) => v.schema(),
            PlanNode::RenameDatabase(v) => v.schema(),
            PlanNode::UnDropDatabase(v) => v.schema(),

            // Table.
            PlanNode::CreateTable(v) => v.schema(),
            PlanNode::DropTable(v) => v.schema(),
            PlanNode::UnDropTable(v) => v.schema(),
            PlanNode::RenameTable(v) => v.schema(),
            PlanNode::TruncateTable(v) => v.schema(),
            PlanNode::OptimizeTable(v) => v.schema(),
            PlanNode::DescribeTable(v) => v.schema(),
            PlanNode::ShowCreateTable(v) => v.schema(),

            // View.
            PlanNode::CreateView(v) => v.schema(),
            PlanNode::AlterView(v) => v.schema(),
            PlanNode::DropView(v) => v.schema(),

            // User.
            PlanNode::CreateUser(v) => v.schema(),
            PlanNode::AlterUser(v) => v.schema(),
            PlanNode::DropUser(v) => v.schema(),

            // Grant.
            PlanNode::GrantPrivilege(v) => v.schema(),
            PlanNode::GrantRole(v) => v.schema(),

            // Revoke.
            PlanNode::RevokePrivilege(v) => v.schema(),
            PlanNode::RevokeRole(v) => v.schema(),

            // Role.
            PlanNode::CreateRole(v) => v.schema(),
            PlanNode::DropRole(v) => v.schema(),

            // Stage.
            PlanNode::CreateUserStage(v) => v.schema(),
            PlanNode::DropUserStage(v) => v.schema(),
            PlanNode::DescribeUserStage(v) => v.schema(),

            // List
            PlanNode::List(v) => v.schema(),

            // UDF.
            PlanNode::CreateUserUDF(v) => v.schema(),
            PlanNode::DropUserUDF(v) => v.schema(),
            PlanNode::AlterUserUDF(v) => v.schema(),

            // Use.
            PlanNode::UseDatabase(v) => v.schema(),

            // Set.
            PlanNode::SetVariable(v) => v.schema(),

            // Kill.
            PlanNode::Kill(v) => v.schema(),

            // Cluster key.
            PlanNode::AlterClusterKey(v) => v.schema(),
            PlanNode::DropClusterKey(v) => v.schema(),
        }
    }

    pub fn name(&self) -> &str {
        match self {
            // Base.
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
            PlanNode::WindowFunc(_) => "WindowFuncPlan",
            PlanNode::Limit(_) => "LimitPlan",
            PlanNode::LimitBy(_) => "LimitByPlan",
            PlanNode::ReadSource(_) => "ReadSourcePlan",
            PlanNode::Sort(_) => "SortPlan",
            PlanNode::SubQueryExpression(_) => "CreateSubQueriesSets",
            PlanNode::Sink(_) => "SinkPlan",

            // Explain.
            PlanNode::Explain(_) => "ExplainPlan",

            // Select.
            PlanNode::Select(_) => "SelectPlan",

            // Insert.
            PlanNode::Insert(_) => "InsertPlan",

            // Copy.
            PlanNode::Copy(_) => "CopyPlan",

            // Call.
            PlanNode::Call(_) => "CallPlan",

            // Show.
            PlanNode::Show(_) => "ShowPlan",

            // Database.
            PlanNode::CreateDatabase(_) => "CreateDatabasePlan",
            PlanNode::DropDatabase(_) => "DropDatabasePlan",
            PlanNode::ShowCreateDatabase(_) => "ShowCreateDatabasePlan",
            PlanNode::RenameDatabase(_) => "RenameDatabase",
            PlanNode::UnDropDatabase(_) => "UnDropDatabase",

            // Table.
            PlanNode::CreateTable(_) => "CreateTablePlan",
            PlanNode::DropTable(_) => "DropTablePlan",
            PlanNode::UnDropTable(_) => "UndropTablePlan",
            PlanNode::RenameTable(_) => "RenameTablePlan",
            PlanNode::TruncateTable(_) => "TruncateTablePlan",
            PlanNode::OptimizeTable(_) => "OptimizeTablePlan",
            PlanNode::ShowCreateTable(_) => "ShowCreateTablePlan",
            PlanNode::DescribeTable(_) => "DescribeTablePlan",

            // View.
            PlanNode::CreateView(_) => "CreateViewPlan",
            PlanNode::AlterView(_) => "AlterViewPlan",
            PlanNode::DropView(_) => "DropViewPlan",

            // User.
            PlanNode::CreateUser(_) => "CreateUser",
            PlanNode::AlterUser(_) => "AlterUser",
            PlanNode::DropUser(_) => "DropUser",

            // Grant.
            PlanNode::GrantPrivilege(_) => "GrantPrivilegePlan",
            PlanNode::GrantRole(_) => "GrantRolePlan",

            // Revoke.
            PlanNode::RevokePrivilege(_) => "RevokePrivilegePlan",
            PlanNode::RevokeRole(_) => "RevokeRolePlan",

            // Role.
            PlanNode::CreateRole(_) => "CreateRole",
            PlanNode::DropRole(_) => "DropRole",

            // Stage.
            PlanNode::CreateUserStage(_) => "CreateUserStagePlan",
            PlanNode::DropUserStage(_) => "DropUserStagePlan",
            PlanNode::DescribeUserStage(_) => "DescribeUserStagePlan",

            // List
            PlanNode::List(_) => "ListPlan",

            // UDF.
            PlanNode::CreateUserUDF(_) => "CreateUserUDFPlan",
            PlanNode::DropUserUDF(_) => "DropUserUDFPlan",
            PlanNode::AlterUserUDF(_) => "AlterUserUDFPlan",

            // Use.
            PlanNode::UseDatabase(_) => "UseDatabasePlan",

            // Set.
            PlanNode::SetVariable(_) => "SetVariablePlan",

            // Kill.
            PlanNode::Kill(_) => "KillQuery",

            // Cluster key.
            PlanNode::AlterClusterKey(_) => "AlterClusterKeyPlan",
            PlanNode::DropClusterKey(_) => "DropClusterKeyPlan",
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
            PlanNode::WindowFunc(v) => vec![v.input.clone()],
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
