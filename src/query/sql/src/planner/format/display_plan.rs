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
use itertools::Itertools;

use crate::format_scalar;
use crate::optimizer::SExpr;
use crate::plans::CreateTablePlan;
use crate::plans::Mutation;
use crate::plans::Plan;

impl Plan {
    pub fn format_indent(&self, verbose: bool) -> Result<String> {
        match self {
            Plan::Query {
                s_expr, metadata, ..
            } => {
                let metadata = &*metadata.read();
                Ok(s_expr.to_format_tree(metadata, verbose)?.format_pretty()?)
            }
            Plan::Explain { kind, plan, .. } => {
                let result = plan.format_indent(false)?;
                Ok(format!("{:?}:\n{}", kind, result))
            }
            Plan::ExplainAst { .. } => Ok("ExplainAst".to_string()),
            Plan::ExplainSyntax { .. } => Ok("ExplainSyntax".to_string()),
            Plan::ExplainAnalyze { .. } => Ok("ExplainAnalyze".to_string()),

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

            // Tables
            Plan::CreateTable(create_table) => format_create_table(create_table),
            Plan::ShowCreateTable(_) => Ok("ShowCreateTable".to_string()),
            Plan::DropTable(_) => Ok("DropTable".to_string()),
            Plan::UndropTable(_) => Ok("UndropTable".to_string()),
            Plan::DescribeTable(_) => Ok("DescribeTable".to_string()),
            Plan::RenameTable(_) => Ok("RenameTable".to_string()),
            Plan::ModifyTableComment(_) => Ok("ModifyTableComment".to_string()),
            Plan::SetOptions(_) => Ok("SetOptions".to_string()),
            Plan::UnsetOptions(_) => Ok("UnsetOptions".to_string()),
            Plan::RenameTableColumn(_) => Ok("RenameTableColumn".to_string()),
            Plan::AddTableColumn(_) => Ok("AddTableColumn".to_string()),
            Plan::ModifyTableColumn(_) => Ok("ModifyTableColumn".to_string()),
            Plan::DropTableColumn(_) => Ok("DropTableColumn".to_string()),
            Plan::AlterTableClusterKey(_) => Ok("AlterTableClusterKey".to_string()),
            Plan::DropTableClusterKey(_) => Ok("DropTableClusterKey".to_string()),
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
            Plan::CreateVirtualColumn(_) => Ok("CreateVirtualColumn".to_string()),
            Plan::AlterVirtualColumn(_) => Ok("AlterVirtualColumn".to_string()),
            Plan::DropVirtualColumn(_) => Ok("DropVirtualColumn".to_string()),
            Plan::RefreshVirtualColumn(_) => Ok("RefreshVirtualColumn".to_string()),

            // Insert
            Plan::Insert(_) => Ok("Insert".to_string()),
            Plan::InsertMultiTable(_) => Ok("InsertMultiTable".to_string()),
            Plan::Replace(_) => Ok("Replace".to_string()),
            Plan::DataMutation { s_expr, .. } => format_merge_into(s_expr),

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
        }
    }
}

fn format_create_table(create_table: &CreateTablePlan) -> Result<String> {
    match &create_table.as_select {
        Some(plan) => match plan.as_ref() {
            Plan::Query {
                s_expr, metadata, ..
            } => {
                let metadata = &*metadata.read();
                let res = s_expr.to_format_tree(metadata, false)?;
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

fn format_merge_into(s_expr: &SExpr) -> Result<String> {
    let merge_into: Mutation = s_expr.plan().clone().try_into()?;
    // add merge into target_table
    let table_index = merge_into
        .metadata
        .read()
        .get_table_index(
            Some(merge_into.database_name.as_str()),
            merge_into.table_name.as_str(),
        )
        .unwrap();

    let table_entry = merge_into.metadata.read().table(table_index).clone();
    let target_table_format = format!(
        "target_table: {}.{}.{}",
        table_entry.catalog(),
        table_entry.database(),
        table_entry.name(),
    );

    let target_build_optimization = false;
    let target_build_optimization_format = FormatTreeNode::new(format!(
        "target_build_optimization: {}",
        target_build_optimization
    ));
    let distributed_format =
        FormatTreeNode::new(format!("distributed: {}", merge_into.distributed));
    let can_try_update_column_only_format = FormatTreeNode::new(format!(
        "can_try_update_column_only: {}",
        merge_into.can_try_update_column_only
    ));
    // add matched clauses
    let mut matched_children = Vec::with_capacity(merge_into.matched_evaluators.len());
    let taregt_schema = table_entry.table().schema_with_stream();
    for evaluator in &merge_into.matched_evaluators {
        let condition_format = evaluator.condition.as_ref().map_or_else(
            || "condition: None".to_string(),
            |predicate| format!("condition: {}", format_scalar(predicate)),
        );
        if evaluator.update.is_none() {
            matched_children.push(FormatTreeNode::new(format!(
                "matched delete: [{}]",
                condition_format
            )));
        } else {
            let map = evaluator.update.as_ref().unwrap();
            let mut field_indexes: Vec<usize> =
                map.iter().map(|(field_idx, _)| *field_idx).collect();
            field_indexes.sort();
            let update_format = field_indexes
                .iter()
                .map(|field_idx| {
                    let expr = map.get(field_idx).unwrap();
                    format!(
                        "{} = {}",
                        taregt_schema.field(*field_idx).name(),
                        format_scalar(expr)
                    )
                })
                .join(",");
            matched_children.push(FormatTreeNode::new(format!(
                "matched update: [{},update set {}]",
                condition_format, update_format
            )));
        }
    }
    // add unmatched clauses
    let mut unmatched_children = Vec::with_capacity(merge_into.unmatched_evaluators.len());
    for evaluator in &merge_into.unmatched_evaluators {
        let condition_format = evaluator.condition.as_ref().map_or_else(
            || "condition: None".to_string(),
            |predicate| format!("condition: {}", format_scalar(predicate)),
        );
        let insert_schema_format = evaluator
            .source_schema
            .fields
            .iter()
            .map(|field| field.name())
            .join(",");
        let values_format = evaluator.values.iter().map(format_scalar).join(",");
        let unmatched_format = format!(
            "insert into ({}) values({})",
            insert_schema_format, values_format
        );
        unmatched_children.push(FormatTreeNode::new(format!(
            "unmatched insert: [{},{}]",
            condition_format, unmatched_format
        )));
    }
    let all_children = [
        vec![distributed_format],
        vec![target_build_optimization_format],
        vec![can_try_update_column_only_format],
        matched_children,
        unmatched_children,
    ]
    .concat();
    let res = FormatTreeNode::with_children(target_table_format, all_children).format_pretty()?;
    Ok(format!("MergeInto:\n{res}"))
}
