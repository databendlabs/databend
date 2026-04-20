// Copyright 2021 Datafuse Labs
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

use crate::ast::*;
use crate::visit::VisitControl;
use crate::visit::Visitor;
use crate::visit::VisitorMut;
use crate::visit::Walk;
use crate::visit::WalkMut;

impl Walk for Statement {
    fn walk<V: Visitor + ?Sized>(
        &self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        try_control!(visitor.visit_statement(self));

        match self {
            Statement::Query(query) => try_walk!(query.walk(visitor)),
            Statement::StatementWithSettings { settings, stmt } => {
                if let Some(settings) = settings {
                    try_walk!(settings.walk(visitor));
                }
                try_walk!(stmt.walk(visitor));
            }
            Statement::Explain { query, .. } | Statement::ExplainAnalyze { query, .. } => {
                try_walk!(query.walk(visitor));
            }
            Statement::ReportIssue(_)
            | Statement::KillStmt { .. }
            | Statement::SetRole { .. }
            | Statement::SetSecondaryRoles { .. }
            | Statement::CreateCatalog(_)
            | Statement::ShowOnlineNodes(_)
            | Statement::ShowWarehouses(_)
            | Statement::ShowWorkers(_)
            | Statement::ShowWorkloadGroups(_)
            | Statement::CreateRole { .. }
            | Statement::DropRole { .. }
            | Statement::AlterRole(_)
            | Statement::DropStage { .. }
            | Statement::DescribeStage { .. }
            | Statement::RemoveStage { .. }
            | Statement::ListStage { .. }
            | Statement::CreateFileFormat { .. }
            | Statement::DropFileFormat { .. }
            | Statement::ShowFileFormats
            | Statement::CreateNetworkPolicy(_)
            | Statement::AlterNetworkPolicy(_)
            | Statement::DropNetworkPolicy(_)
            | Statement::DescNetworkPolicy(_)
            | Statement::ShowNetworkPolicies
            | Statement::CreatePasswordPolicy(_)
            | Statement::AlterPasswordPolicy(_)
            | Statement::DropPasswordPolicy(_)
            | Statement::DescPasswordPolicy(_)
            | Statement::Begin
            | Statement::Commit
            | Statement::Abort
            | Statement::CreateNotification(_)
            | Statement::AlterNotification(_)
            | Statement::DropNotification(_)
            | Statement::DescribeNotification(_)
            | Statement::SetPriority { .. }
            | Statement::System(_) => {}
            Statement::CopyIntoTable(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::CopyIntoLocation(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::Call(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::ShowSettings { show_options }
            | Statement::ShowProcessList { show_options }
            | Statement::ShowMetrics { show_options }
            | Statement::ShowEngines { show_options }
            | Statement::ShowFunctions { show_options }
            | Statement::ShowUserFunctions { show_options }
            | Statement::ShowTableFunctions { show_options }
            | Statement::ShowIndexes { show_options }
            | Statement::ShowVariables { show_options }
            | Statement::ShowUsers { show_options }
            | Statement::ShowRoles { show_options }
            | Statement::ShowStages { show_options }
            | Statement::ShowPasswordPolicies { show_options }
            | Statement::ShowProcedures { show_options }
            | Statement::ShowSequences { show_options } => {
                if let Some(show_options) = show_options {
                    try_walk!(show_options.walk(visitor));
                }
            }
            Statement::ShowLocks(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::SetStmt { settings } | Statement::UnSetStmt { settings } => {
                try_walk!(settings.walk(visitor));
            }
            Statement::Insert(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::InsertMultiTable(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::Replace(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::MergeInto(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::Delete(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::Update(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::ShowCatalogs(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::ShowCreateCatalog(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::DropCatalog(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::UseCatalog { catalog } => try_walk!(catalog.walk(visitor)),
            Statement::UseWarehouse(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::DropWarehouse(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::ResumeWarehouse(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::SuspendWarehouse(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::InspectWarehouse(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::CreateWarehouse(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::RenameWarehouse(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::AddWarehouseCluster(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::DropWarehouseCluster(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::RenameWarehouseCluster(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::AssignWarehouseNodes(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::UnassignWarehouseNodes(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::CreateWorker(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::DropWorker(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::AlterWorker(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::CreateWorkloadGroup(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::DropWorkloadGroup(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::RenameWorkloadGroup(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::SetWorkloadQuotasGroup(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::UnsetWorkloadQuotasGroup(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::ShowDatabases(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::ShowDropDatabases(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::ShowCreateDatabase(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::CreateDatabase(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::DropDatabase(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::UndropDatabase(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::AlterDatabase(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::UseDatabase { database } => try_walk!(database.walk(visitor)),
            Statement::ShowTables(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::ShowBranches(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::ShowCreateTable(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::DescribeTable(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::DropTable(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::UndropTable(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::TruncateTable(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::AnalyzeTable(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::ExistsTable(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::ShowTablesStatus(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::ShowDropTables(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::AttachTable(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::CreateTable(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::AlterTable(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::RenameTable(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::OptimizeTable(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::VacuumTable(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::VacuumDropTable(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::VacuumTemporaryFiles(_) => {}
            Statement::VacuumVirtualColumn(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::ShowStatistics(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::CreateDictionary(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::DropDictionary(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::ShowCreateDictionary(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::ShowDictionaries(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::RenameDictionary(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::ShowColumns(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::CreateView(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::AlterView(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::DropView(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::DescribeView(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::ShowViews(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::CreateStream(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::DropStream(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::DescribeStream(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::ShowStreams(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::CreateIndex(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::DropIndex(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::RefreshIndex(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::CreateTableIndex(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::DropTableIndex(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::RefreshTableIndex(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::RefreshVirtualColumn(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::ShowVirtualColumns(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::DescribeUser { .. } | Statement::DropUser { .. } => {}
            Statement::CreateUser(_) | Statement::AlterUser(_) => {}
            Statement::Grant(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::Revoke(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::ShowGrants { show_options, .. } => {
                if let Some(show_options) = show_options {
                    try_walk!(show_options.walk(visitor));
                }
            }
            Statement::ShowObjectPrivileges(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::ShowGrantsOfRole(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::CreateUDF(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::DropUDF { udf_name, .. } => try_walk!(udf_name.walk(visitor)),
            Statement::AlterUDF(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::CreateRowAccessPolicy(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::DropRowAccessPolicy(_) | Statement::DescRowAccessPolicy(_) => {}
            Statement::CreateTag(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::DropTag(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::ShowTags(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::AlterObjectTag(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::CreateStage(_) | Statement::AlterStage(_) => {}
            Statement::CreateConnection(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::DropConnection(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::DescribeConnection(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::ShowConnections(_) => {}
            Statement::Presign(_) => {}
            Statement::CreateDatamaskPolicy(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::DropDatamaskPolicy(_) | Statement::DescDatamaskPolicy(_) => {}
            Statement::CreateTask(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::AlterTask(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::ExecuteTask(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::DescribeTask(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::DropTask(_) => {}
            Statement::ShowTasks(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::CreateDynamicTable(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::CreatePipe(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::DescribePipe(_) | Statement::DropPipe(_) | Statement::AlterPipe(_) => {}
            Statement::ExecuteImmediate(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::CreateProcedure(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::DropProcedure(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::DescProcedure(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::CallProcedure(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::CreateSequence(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::DropSequence(stmt) => try_walk!(stmt.walk(visitor)),
            Statement::DescSequence { name } => try_walk!(name.walk(visitor)),
        }

        Ok(VisitControl::Continue)
    }
}

impl WalkMut for Statement {
    fn walk_mut<V: VisitorMut + ?Sized>(
        &mut self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        try_control!(visitor.visit_statement(self));

        match self {
            Statement::Query(query) => try_walk!(query.walk_mut(visitor)),
            Statement::StatementWithSettings { settings, stmt } => {
                if let Some(settings) = settings {
                    try_walk!(settings.walk_mut(visitor));
                }
                try_walk!(stmt.walk_mut(visitor));
            }
            Statement::Explain { query, .. } | Statement::ExplainAnalyze { query, .. } => {
                try_walk!(query.walk_mut(visitor));
            }
            Statement::ReportIssue(_)
            | Statement::KillStmt { .. }
            | Statement::SetRole { .. }
            | Statement::SetSecondaryRoles { .. }
            | Statement::CreateCatalog(_)
            | Statement::ShowOnlineNodes(_)
            | Statement::ShowWarehouses(_)
            | Statement::ShowWorkers(_)
            | Statement::ShowWorkloadGroups(_)
            | Statement::CreateRole { .. }
            | Statement::DropRole { .. }
            | Statement::AlterRole(_)
            | Statement::DropStage { .. }
            | Statement::DescribeStage { .. }
            | Statement::RemoveStage { .. }
            | Statement::ListStage { .. }
            | Statement::CreateFileFormat { .. }
            | Statement::DropFileFormat { .. }
            | Statement::ShowFileFormats
            | Statement::CreateNetworkPolicy(_)
            | Statement::AlterNetworkPolicy(_)
            | Statement::DropNetworkPolicy(_)
            | Statement::DescNetworkPolicy(_)
            | Statement::ShowNetworkPolicies
            | Statement::CreatePasswordPolicy(_)
            | Statement::AlterPasswordPolicy(_)
            | Statement::DropPasswordPolicy(_)
            | Statement::DescPasswordPolicy(_)
            | Statement::Begin
            | Statement::Commit
            | Statement::Abort
            | Statement::CreateNotification(_)
            | Statement::AlterNotification(_)
            | Statement::DropNotification(_)
            | Statement::DescribeNotification(_)
            | Statement::SetPriority { .. }
            | Statement::System(_) => {}
            Statement::CopyIntoTable(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::CopyIntoLocation(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::Call(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::ShowSettings { show_options }
            | Statement::ShowProcessList { show_options }
            | Statement::ShowMetrics { show_options }
            | Statement::ShowEngines { show_options }
            | Statement::ShowFunctions { show_options }
            | Statement::ShowUserFunctions { show_options }
            | Statement::ShowTableFunctions { show_options }
            | Statement::ShowIndexes { show_options }
            | Statement::ShowVariables { show_options }
            | Statement::ShowUsers { show_options }
            | Statement::ShowRoles { show_options }
            | Statement::ShowStages { show_options }
            | Statement::ShowPasswordPolicies { show_options }
            | Statement::ShowProcedures { show_options }
            | Statement::ShowSequences { show_options } => {
                if let Some(show_options) = show_options {
                    try_walk!(show_options.walk_mut(visitor));
                }
            }
            Statement::ShowLocks(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::SetStmt { settings } | Statement::UnSetStmt { settings } => {
                try_walk!(settings.walk_mut(visitor));
            }
            Statement::Insert(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::InsertMultiTable(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::Replace(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::MergeInto(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::Delete(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::Update(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::ShowCatalogs(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::ShowCreateCatalog(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::DropCatalog(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::UseCatalog { catalog } => try_walk!(catalog.walk_mut(visitor)),
            Statement::UseWarehouse(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::DropWarehouse(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::ResumeWarehouse(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::SuspendWarehouse(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::InspectWarehouse(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::CreateWarehouse(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::RenameWarehouse(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::AddWarehouseCluster(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::DropWarehouseCluster(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::RenameWarehouseCluster(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::AssignWarehouseNodes(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::UnassignWarehouseNodes(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::CreateWorker(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::DropWorker(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::AlterWorker(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::CreateWorkloadGroup(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::DropWorkloadGroup(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::RenameWorkloadGroup(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::SetWorkloadQuotasGroup(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::UnsetWorkloadQuotasGroup(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::ShowDatabases(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::ShowDropDatabases(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::ShowCreateDatabase(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::CreateDatabase(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::DropDatabase(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::UndropDatabase(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::AlterDatabase(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::UseDatabase { database } => try_walk!(database.walk_mut(visitor)),
            Statement::ShowTables(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::ShowBranches(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::ShowCreateTable(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::DescribeTable(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::DropTable(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::UndropTable(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::TruncateTable(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::AnalyzeTable(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::ExistsTable(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::ShowTablesStatus(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::ShowDropTables(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::AttachTable(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::CreateTable(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::AlterTable(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::RenameTable(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::OptimizeTable(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::VacuumTable(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::VacuumDropTable(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::VacuumTemporaryFiles(_) => {}
            Statement::VacuumVirtualColumn(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::ShowStatistics(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::CreateDictionary(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::DropDictionary(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::ShowCreateDictionary(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::ShowDictionaries(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::RenameDictionary(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::ShowColumns(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::CreateView(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::AlterView(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::DropView(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::DescribeView(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::ShowViews(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::CreateStream(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::DropStream(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::DescribeStream(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::ShowStreams(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::CreateIndex(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::DropIndex(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::RefreshIndex(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::CreateTableIndex(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::DropTableIndex(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::RefreshTableIndex(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::RefreshVirtualColumn(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::ShowVirtualColumns(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::DescribeUser { .. } | Statement::DropUser { .. } => {}
            Statement::CreateUser(_) | Statement::AlterUser(_) => {}
            Statement::Grant(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::Revoke(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::ShowGrants { show_options, .. } => {
                if let Some(show_options) = show_options {
                    try_walk!(show_options.walk_mut(visitor));
                }
            }
            Statement::ShowObjectPrivileges(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::ShowGrantsOfRole(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::CreateUDF(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::DropUDF { udf_name, .. } => try_walk!(udf_name.walk_mut(visitor)),
            Statement::AlterUDF(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::CreateRowAccessPolicy(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::DropRowAccessPolicy(_) | Statement::DescRowAccessPolicy(_) => {}
            Statement::CreateTag(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::DropTag(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::ShowTags(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::AlterObjectTag(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::CreateStage(_) | Statement::AlterStage(_) => {}
            Statement::CreateConnection(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::DropConnection(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::DescribeConnection(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::ShowConnections(_) => {}
            Statement::Presign(_) => {}
            Statement::CreateDatamaskPolicy(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::DropDatamaskPolicy(_) | Statement::DescDatamaskPolicy(_) => {}
            Statement::CreateTask(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::AlterTask(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::ExecuteTask(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::DescribeTask(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::DropTask(_) => {}
            Statement::ShowTasks(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::CreateDynamicTable(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::CreatePipe(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::DescribePipe(_) | Statement::DropPipe(_) | Statement::AlterPipe(_) => {}
            Statement::ExecuteImmediate(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::CreateProcedure(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::DropProcedure(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::DescProcedure(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::CallProcedure(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::CreateSequence(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::DropSequence(stmt) => try_walk!(stmt.walk_mut(visitor)),
            Statement::DescSequence { name } => try_walk!(name.walk_mut(visitor)),
        }

        Ok(VisitControl::Continue)
    }
}
