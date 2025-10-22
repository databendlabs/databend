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

use databend_common_ast::ast::FormatTreeNode;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use super::display::DefaultOperatorHumanizer;
use super::display::FormatOptions;
use super::display::IdHumanizer;
use super::display::MetadataIdHumanizer;
use super::display::TreeHumanizer;
use crate::optimizer::ir::SExpr;
use crate::plans::CreateTablePlan;
use crate::plans::Plan;

impl SExpr {
    pub(crate) fn to_format_tree<I: IdHumanizer>(
        &self,
        id_humanizer: &I,
    ) -> Result<FormatTreeNode> {
        let operator_humanizer = DefaultOperatorHumanizer;
        let tree_humanizer = TreeHumanizer::new(id_humanizer, &operator_humanizer);
        tree_humanizer.humanize_s_expr(self)
    }
}

impl Plan {
    pub fn format_indent(&self, options: FormatOptions) -> Result<String> {
        match self {
            Plan::Query {
                s_expr, metadata, ..
            } => {
                let metadata = &*metadata.read();
                let humanizer = MetadataIdHumanizer::new(metadata, options);
                Ok(s_expr.to_format_tree(&humanizer)?.format_pretty()?)
            }
            Plan::Explain { kind, plan, .. } => {
                let result = plan.format_indent(options)?;
                Ok(format!("{:?}:\n{}", kind, result))
            }
            Plan::ExplainAst { .. } => Ok("ExplainAst".to_string()),
            Plan::ExplainSyntax { .. } => Ok("ExplainSyntax".to_string()),
            Plan::ExplainAnalyze { .. } => Ok("ExplainAnalyze".to_string()),
            Plan::ExplainPerf { .. } => Ok("ExplainPerf".to_string()),

            Plan::ReportIssue { .. } => Ok("ReportOptimize".to_string()),

            Plan::CopyIntoTable(_) => Ok("CopyIntoTable".to_string()),
            Plan::CopyIntoLocation(_) => Ok("CopyIntoLocation".to_string()),

            // catalog
            Plan::ShowCreateCatalog(_) => Ok("ShowCreateCatalog".to_string()),
            Plan::CreateCatalog(_) => Ok("CreateCatalog".to_string()),
            Plan::DropCatalog(_) => Ok("DropCatalog".to_string()),
            Plan::UseCatalog(_) => Ok("UseCatalog".to_string()),

            // Databases
            Plan::ShowCreateDatabase(_) => Ok("ShowCreateDatabase".to_string()),
            Plan::CreateDatabase(_) => Ok("CreateDatabase".to_string()),
            Plan::DropDatabase(_) => Ok("DropDatabase".to_string()),
            Plan::UndropDatabase(_) => Ok("UndropDatabase".to_string()),
            Plan::RenameDatabase(_) => Ok("RenameDatabase".to_string()),
            Plan::RefreshDatabaseCache(_) => Ok("RefreshDatabaseCache".to_string()),

            // Tables
            Plan::CreateTable(create_table) => format_create_table(create_table, options),
            Plan::ShowCreateTable(_) => Ok("ShowCreateTable".to_string()),
            Plan::DropTable(_) => Ok("DropTable".to_string()),
            Plan::UndropTable(_) => Ok("UndropTable".to_string()),
            Plan::DescribeTable(_) => Ok("DescribeTable".to_string()),
            Plan::RenameTable(_) => Ok("RenameTable".to_string()),
            Plan::SwapTable(_) => Ok("SwapTable".to_string()),
            Plan::ModifyTableComment(_) => Ok("ModifyTableComment".to_string()),
            Plan::ModifyTableConnection(_) => Ok("ModifyTableConnection".to_string()),
            Plan::SetOptions(_) => Ok("SetOptions".to_string()),
            Plan::UnsetOptions(_) => Ok("UnsetOptions".to_string()),
            Plan::RenameTableColumn(_) => Ok("RenameTableColumn".to_string()),
            Plan::AddTableColumn(_) => Ok("AddTableColumn".to_string()),
            Plan::ModifyTableColumn(_) => Ok("ModifyTableColumn".to_string()),
            Plan::DropTableColumn(_) => Ok("DropTableColumn".to_string()),
            Plan::AddTableConstraint(_) => Ok("AddTableConstraint".to_string()),
            Plan::DropTableConstraint(_) => Ok("DropTableConstraint".to_string()),
            Plan::AlterTableClusterKey(_) => Ok("AlterTableClusterKey".to_string()),
            Plan::DropTableClusterKey(_) => Ok("DropTableClusterKey".to_string()),
            Plan::RefreshTableCache(_) => Ok("RefreshTableCache".to_string()),
            Plan::ReclusterTable(_) => Ok("ReclusterTable".to_string()),
            Plan::TruncateTable(_) => Ok("TruncateTable".to_string()),
            Plan::OptimizePurge(_) => Ok("OptimizePurge".to_string()),
            Plan::OptimizeCompactSegment(_) => Ok("OptimizeCompactSegment".to_string()),
            Plan::OptimizeCompactBlock { .. } => Ok("OptimizeCompactBlock".to_string()),
            Plan::VacuumTable(_) => Ok("VacuumTable".to_string()),
            Plan::VacuumDropTable(_) => Ok("VacuumDropTable".to_string()),
            Plan::VacuumTemporaryFiles(_) => Ok("VacuumTemporaryFiles".to_string()),
            Plan::AnalyzeTable(_) => Ok("AnalyzeTable".to_string()),
            Plan::ExistsTable(_) => Ok("ExistsTable".to_string()),
            Plan::AddTableRowAccessPolicy(_) => Ok("AddTableRowAccessPolicy".to_string()),
            Plan::DropTableRowAccessPolicy(_) => Ok("DropTableRowAccessPolicy".to_string()),
            Plan::DropAllTableRowAccessPolicies(_) => {
                Ok("DropAllTableRowAccessPolicies".to_string())
            }

            // Views
            Plan::CreateView(_) => Ok("CreateView".to_string()),
            Plan::AlterView(_) => Ok("AlterView".to_string()),
            Plan::DropView(_) => Ok("DropView".to_string()),
            Plan::DescribeView(_) => Ok("DescribeView".to_string()),

            // Streams
            Plan::CreateStream(_) => Ok("CreateStream".to_string()),
            Plan::DropStream(_) => Ok("DropStream".to_string()),

            // Dynamic Tables
            Plan::CreateDynamicTable(_) => Ok("CreateDynamicTable".to_string()),

            // Indexes
            Plan::CreateIndex(_) => Ok("CreateIndex".to_string()),
            Plan::DropIndex(_) => Ok("DropIndex".to_string()),
            Plan::RefreshIndex(_) => Ok("RefreshIndex".to_string()),
            Plan::CreateTableIndex(_) => Ok("CreateTableIndex".to_string()),
            Plan::DropTableIndex(_) => Ok("DropTableIndex".to_string()),
            Plan::RefreshTableIndex(_) => Ok("RefreshTableIndex".to_string()),

            // Virtual Columns
            Plan::RefreshVirtualColumn(_) => Ok("RefreshVirtualColumn".to_string()),

            // Insert
            Plan::Insert(_) => Ok("Insert".to_string()),
            Plan::InsertMultiTable(_) => Ok("InsertMultiTable".to_string()),
            Plan::Replace(_) => Ok("Replace".to_string()),
            Plan::DataMutation {
                s_expr, metadata, ..
            } => {
                let metadata = &*metadata.read();
                let humanizer = MetadataIdHumanizer::new(metadata, options);
                Ok(format!(
                    "MergeInto:\n{}",
                    s_expr.to_format_tree(&humanizer)?.format_pretty()?
                ))
            }

            // Stages
            Plan::CreateStage(_) => Ok("CreateStage".to_string()),
            Plan::DropStage(_) => Ok("DropStage".to_string()),
            Plan::RemoveStage(_) => Ok("RemoveStage".to_string()),

            // FileFormat
            Plan::CreateFileFormat(_) => Ok("CreateFileFormat".to_string()),
            Plan::DropFileFormat(_) => Ok("DropFileFormat".to_string()),
            Plan::ShowFileFormats(_) => Ok("ShowFileFormats".to_string()),

            // Account
            Plan::GrantRole(_) => Ok("GrantRole".to_string()),
            Plan::GrantPriv(_) => Ok("GrantPrivilege".to_string()),
            Plan::RevokePriv(_) => Ok("RevokePrivilege".to_string()),
            Plan::RevokeRole(_) => Ok("RevokeRole".to_string()),
            Plan::CreateUser(_) => Ok("CreateUser".to_string()),
            Plan::DropUser(_) => Ok("DropUser".to_string()),
            Plan::CreateUDF(_) => Ok("CreateUDF".to_string()),
            Plan::AlterUDF(_) => Ok("AlterUDF".to_string()),
            Plan::DropUDF(_) => Ok("DropUDF".to_string()),
            Plan::AlterUser(_) => Ok("AlterUser".to_string()),
            Plan::DescUser(_) => Ok("DescUser".to_string()),
            Plan::CreateRole(_) => Ok("CreateRole".to_string()),
            Plan::DropRole(_) => Ok("DropRole".to_string()),
            Plan::Presign(_) => Ok("Presign".to_string()),

            Plan::Set(_) => Ok("Set".to_string()),
            Plan::Unset(_) => Ok("Unset".to_string()),
            Plan::SetRole(_) => Ok("SetRole".to_string()),
            Plan::SetSecondaryRoles(_) => Ok("SetSecondaryRoles".to_string()),
            Plan::UseDatabase(_) => Ok("UseDatabase".to_string()),
            Plan::Kill(_) => Ok("Kill".to_string()),

            Plan::RevertTable(_) => Ok("RevertTable".to_string()),

            // data mask
            Plan::CreateDatamaskPolicy(_) => Ok("CreateDatamaskPolicy".to_string()),
            Plan::DropDatamaskPolicy(_) => Ok("DropDatamaskPolicy".to_string()),
            Plan::DescDatamaskPolicy(_) => Ok("DescDatamaskPolicy".to_string()),

            // row policy
            Plan::CreateRowAccessPolicy(_) => Ok("CreateRowAccessPolicy".to_string()),
            Plan::DropRowAccessPolicy(_) => Ok("DropRowAccessPolicy".to_string()),
            Plan::DescRowAccessPolicy(_) => Ok("DescRowAccessPolicy".to_string()),

            // network policy
            Plan::CreateNetworkPolicy(_) => Ok("CreateNetworkPolicy".to_string()),
            Plan::AlterNetworkPolicy(_) => Ok("AlterNetworkPolicy".to_string()),
            Plan::DropNetworkPolicy(_) => Ok("DropNetworkPolicy".to_string()),
            Plan::DescNetworkPolicy(_) => Ok("DescNetworkPolicy".to_string()),
            Plan::ShowNetworkPolicies(_) => Ok("ShowNetworkPolicies".to_string()),

            // password policy
            Plan::CreatePasswordPolicy(_) => Ok("CreatePasswordPolicy".to_string()),
            Plan::AlterPasswordPolicy(_) => Ok("AlterPasswordPolicy".to_string()),
            Plan::DropPasswordPolicy(_) => Ok("DropPasswordPolicy".to_string()),
            Plan::DescPasswordPolicy(_) => Ok("DescPasswordPolicy".to_string()),

            // task
            Plan::CreateTask(_) => Ok("CreateTask".to_string()),
            Plan::DropTask(_) => Ok("DropTask".to_string()),
            Plan::AlterTask(_) => Ok("AlterTask".to_string()),
            Plan::DescribeTask(_) => Ok("DescribeTask".to_string()),
            Plan::ExecuteTask(_) => Ok("ExecuteTask".to_string()),
            Plan::ShowTasks(_) => Ok("ShowTasks".to_string()),

            // task
            Plan::CreateConnection(_) => Ok("CreateConnection".to_string()),
            Plan::DescConnection(_) => Ok("DescConnection".to_string()),
            Plan::DropConnection(_) => Ok("DropConnection".to_string()),
            Plan::ShowConnections(_) => Ok("ShowConnections".to_string()),
            Plan::Begin => Ok("Begin".to_string()),
            Plan::Commit => Ok("commit".to_string()),
            Plan::Abort => Ok("Abort".to_string()),

            // Notification
            Plan::CreateNotification(_) => Ok("CreateNotification".to_string()),
            Plan::DropNotification(_) => Ok("DropNotification".to_string()),
            Plan::DescNotification(_) => Ok("DescNotification".to_string()),
            Plan::AlterNotification(_) => Ok("AlterNotification".to_string()),

            // Stored procedures
            Plan::ExecuteImmediate(_) => Ok("ExecuteImmediate".to_string()),
            Plan::CreateProcedure(_) => Ok("CreateProcedure".to_string()),
            Plan::DropProcedure(_) => Ok("DropProcedure".to_string()),
            Plan::DescProcedure(_) => Ok("DescProcedure".to_string()),
            Plan::CallProcedure(_) => Ok("CallProcedure".to_string()),
            // Plan::ShowCreateProcedure(_) => Ok("ShowCreateProcedure".to_string()),
            // Plan::RenameProcedure(_) => Ok("ProcedureDatabase".to_string()),

            // sequence
            Plan::CreateSequence(_) => Ok("CreateSequence".to_string()),
            Plan::DropSequence(_) => Ok("DropSequence".to_string()),
            Plan::DescSequence(_) => Ok("DescSequence".to_string()),

            Plan::SetPriority(_) => Ok("SetPriority".to_string()),
            Plan::System(_) => Ok("System".to_string()),

            // Dictionary
            Plan::CreateDictionary(_) => Ok("CreateDictionary".to_string()),
            Plan::DropDictionary(_) => Ok("DropDictionary".to_string()),
            Plan::ShowCreateDictionary(_) => Ok("ShowCreateDictionary".to_string()),
            Plan::RenameDictionary(_) => Ok("RenameDictionary".to_string()),
            Plan::ShowWarehouses => Ok("ShowWarehouses".to_string()),
            Plan::ShowOnlineNodes => Ok("ShowOnlineNodes".to_string()),
            Plan::DropWarehouse(_) => Ok("DropWarehouse".to_string()),
            Plan::ResumeWarehouse(_) => Ok("ResumeWarehouse".to_string()),
            Plan::SuspendWarehouse(_) => Ok("SuspendWarehouse".to_string()),
            Plan::RenameWarehouse(_) => Ok("RenameWarehouse".to_string()),
            Plan::InspectWarehouse(_) => Ok("InspectWarehouse".to_string()),
            Plan::DropWarehouseCluster(_) => Ok("DropWarehouseCluster".to_string()),
            Plan::RenameWarehouseCluster(_) => Ok("RenameWarehouseCluster".to_string()),
            Plan::CreateWarehouse(_) => Ok("CreateWarehouse".to_string()),
            Plan::UseWarehouse(_) => Ok("UseWarehouse".to_string()),
            Plan::AddWarehouseCluster(_) => Ok("AddWarehouseCluster".to_string()),
            Plan::AssignWarehouseNodes(_) => Ok("AddWarehouseClusterNode".to_string()),
            Plan::UnassignWarehouseNodes(_) => Ok("DropWarehouseClusterNode".to_string()),
            Plan::ShowWorkloadGroups => Ok("ShowWorkloadGroups".to_string()),
            Plan::CreateWorkloadGroup(_) => Ok("CreateWorkloadGroup".to_string()),
            Plan::DropWorkloadGroup(_) => Ok("DropWorkloadGroup".to_string()),
            Plan::RenameWorkloadGroup(_) => Ok("RenameWorkloadGroup".to_string()),
            Plan::SetWorkloadGroupQuotas(_) => Ok("SetWorkloadGroupQuotas".to_string()),
            Plan::UnsetWorkloadGroupQuotas(_) => Ok("UnsetWorkloadGroupQuotas".to_string()),
            Plan::AlterRole(_) => Ok("AlterRole".to_string()),
            Plan::AlterDatabase(_) => {
                todo!()
            }
        }
    }
}

fn format_create_table(create_table: &CreateTablePlan, options: FormatOptions) -> Result<String> {
    match &create_table.as_select {
        Some(plan) => match plan.as_ref() {
            Plan::Query {
                s_expr, metadata, ..
            } => {
                let metadata = &*metadata.read();
                let humanizer = MetadataIdHumanizer::new(metadata, options);
                let res = s_expr.to_format_tree(&humanizer)?;
                Ok(
                    FormatTreeNode::with_children("CreateTableAsSelect".to_string(), vec![res])
                        .format_pretty()?,
                )
            }
            _ => Err(ErrorCode::Internal("Invalid create table plan")),
        },
        None => Ok("CreateTable".to_string()),
    }
}
