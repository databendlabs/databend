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

use crate::AdminUseTenantPlan;
use crate::AggregatorFinalPlan;
use crate::AggregatorPartialPlan;
use crate::AlterUserPlan;
use crate::AlterUserUDFPlan;
use crate::BroadcastPlan;
use crate::CopyPlan;
use crate::CreateDatabasePlan;
use crate::CreateTablePlan;
use crate::CreateUserPlan;
use crate::CreateUserStagePlan;
use crate::CreateUserUDFPlan;
use crate::DescribeTablePlan;
use crate::DescribeUserStagePlan;
use crate::DropDatabasePlan;
use crate::DropTablePlan;
use crate::DropUserPlan;
use crate::DropUserStagePlan;
use crate::DropUserUDFPlan;
use crate::EmptyPlan;
use crate::ExplainPlan;
use crate::ExpressionPlan;
use crate::FilterPlan;
use crate::GrantPrivilegePlan;
use crate::HavingPlan;
use crate::InsertPlan;
use crate::KillPlan;
use crate::LimitByPlan;
use crate::LimitPlan;
use crate::OptimizeTablePlan;
use crate::ProjectionPlan;
use crate::ReadDataSourcePlan;
use crate::RemotePlan;
use crate::RevokePrivilegePlan;
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

    // Show.
    Show(ShowPlan),

    // Database.
    CreateDatabase(CreateDatabasePlan),
    DropDatabase(DropDatabasePlan),
    ShowCreateDatabase(ShowCreateDatabasePlan),

    // Table.
    CreateTable(CreateTablePlan),
    DropTable(DropTablePlan),
    TruncateTable(TruncateTablePlan),
    OptimizeTable(OptimizeTablePlan),
    DescribeTable(DescribeTablePlan),
    ShowCreateTable(ShowCreateTablePlan),

    // User.
    CreateUser(CreateUserPlan),
    AlterUser(AlterUserPlan),
    DropUser(DropUserPlan),
    GrantPrivilege(GrantPrivilegePlan),
    RevokePrivilege(RevokePrivilegePlan),

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

    // Admin
    AdminUseTenant(AdminUseTenantPlan),
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

            // Show.
            PlanNode::Show(v) => v.schema(),

            // Database.
            PlanNode::CreateDatabase(v) => v.schema(),
            PlanNode::DropDatabase(v) => v.schema(),
            PlanNode::ShowCreateDatabase(v) => v.schema(),

            // Table.
            PlanNode::CreateTable(v) => v.schema(),
            PlanNode::DropTable(v) => v.schema(),
            PlanNode::TruncateTable(v) => v.schema(),
            PlanNode::OptimizeTable(v) => v.schema(),
            PlanNode::DescribeTable(v) => v.schema(),
            PlanNode::ShowCreateTable(v) => v.schema(),

            // User.
            PlanNode::CreateUser(v) => v.schema(),
            PlanNode::AlterUser(v) => v.schema(),
            PlanNode::DropUser(v) => v.schema(),
            PlanNode::GrantPrivilege(v) => v.schema(),
            PlanNode::RevokePrivilege(v) => v.schema(),

            // Stage.
            PlanNode::CreateUserStage(v) => v.schema(),
            PlanNode::DropUserStage(v) => v.schema(),
            PlanNode::DescribeUserStage(v) => v.schema(),

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

            // Admin.
            PlanNode::AdminUseTenant(v) => v.schema(),
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

            // Show.
            PlanNode::Show(_) => "ShowPlan",

            // Database.
            PlanNode::CreateDatabase(_) => "CreateDatabasePlan",
            PlanNode::DropDatabase(_) => "DropDatabasePlan",
            PlanNode::ShowCreateDatabase(_) => "ShowCreateDatabasePlan",

            // Table.
            PlanNode::CreateTable(_) => "CreateTablePlan",
            PlanNode::DropTable(_) => "DropTablePlan",
            PlanNode::TruncateTable(_) => "TruncateTablePlan",
            PlanNode::OptimizeTable(_) => "OptimizeTablePlan",
            PlanNode::ShowCreateTable(_) => "ShowCreateTablePlan",
            PlanNode::DescribeTable(_) => "DescribeTablePlan",

            // User.
            PlanNode::CreateUser(_) => "CreateUser",
            PlanNode::AlterUser(_) => "AlterUser",
            PlanNode::DropUser(_) => "DropUser",
            PlanNode::GrantPrivilege(_) => "GrantPrivilegePlan",
            PlanNode::RevokePrivilege(_) => "RevokePrivilegePlan",

            // Stage.
            PlanNode::CreateUserStage(_) => "CreateUserStagePlan",
            PlanNode::DropUserStage(_) => "DropUserStagePlan",
            PlanNode::DescribeUserStage(_) => "DescribeUserStagePlan",

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

            // Admin.
            PlanNode::AdminUseTenant(_) => "UseTenantPlan",
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
