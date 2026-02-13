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

use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::Arc;

use databend_common_ast::ast::ExplainKind;
use databend_common_catalog::query_kind::QueryKind;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::types::DataType;
use educe::Educe;

use super::CreateDictionaryPlan;
use super::DropDictionaryPlan;
use super::ModifyTableConnectionPlan;
use super::RenameDictionaryPlan;
use super::ShowCreateDictionaryPlan;
use crate::BindContext;
use crate::MetadataRef;
use crate::binder::ExplainConfig;
use crate::optimizer::ir::SExpr;
use crate::plans::AddTableColumnPlan;
use crate::plans::AddTableConstraintPlan;
use crate::plans::AddTableRowAccessPolicyPlan;
use crate::plans::AddWarehouseClusterPlan;
use crate::plans::AlterDatabasePlan;
use crate::plans::AlterNetworkPolicyPlan;
use crate::plans::AlterNotificationPlan;
use crate::plans::AlterPasswordPolicyPlan;
use crate::plans::AlterRolePlan;
use crate::plans::AlterStagePlan;
use crate::plans::AlterTableClusterKeyPlan;
use crate::plans::AlterTaskPlan;
use crate::plans::AlterUDFPlan;
use crate::plans::AlterUserPlan;
use crate::plans::AlterViewPlan;
use crate::plans::AlterWorkerPlan;
use crate::plans::AnalyzeTablePlan;
use crate::plans::AssignWarehouseNodesPlan;
use crate::plans::CallProcedurePlan;
use crate::plans::CopyIntoTableMode;
use crate::plans::CopyIntoTablePlan;
use crate::plans::CreateCatalogPlan;
use crate::plans::CreateConnectionPlan;
use crate::plans::CreateDatabasePlan;
use crate::plans::CreateDatamaskPolicyPlan;
use crate::plans::CreateDynamicTablePlan;
use crate::plans::CreateFileFormatPlan;
use crate::plans::CreateIndexPlan;
use crate::plans::CreateNetworkPolicyPlan;
use crate::plans::CreateNotificationPlan;
use crate::plans::CreatePasswordPolicyPlan;
use crate::plans::CreateProcedurePlan;
use crate::plans::CreateRolePlan;
use crate::plans::CreateSequencePlan;
use crate::plans::CreateStagePlan;
use crate::plans::CreateStreamPlan;
use crate::plans::CreateTableIndexPlan;
use crate::plans::CreateTablePlan;
use crate::plans::CreateTableRefPlan;
use crate::plans::CreateTagPlan;
use crate::plans::CreateTaskPlan;
use crate::plans::CreateUDFPlan;
use crate::plans::CreateUserPlan;
use crate::plans::CreateViewPlan;
use crate::plans::CreateWarehousePlan;
use crate::plans::CreateWorkerPlan;
use crate::plans::CreateWorkloadGroupPlan;
use crate::plans::DescConnectionPlan;
use crate::plans::DescDatamaskPolicyPlan;
use crate::plans::DescNetworkPolicyPlan;
use crate::plans::DescNotificationPlan;
use crate::plans::DescPasswordPolicyPlan;
use crate::plans::DescProcedurePlan;
use crate::plans::DescRowAccessPolicyPlan;
use crate::plans::DescSequencePlan;
use crate::plans::DescUserPlan;
use crate::plans::DescribeTablePlan;
use crate::plans::DescribeTaskPlan;
use crate::plans::DescribeViewPlan;
use crate::plans::DropAllTableRowAccessPoliciesPlan;
use crate::plans::DropCatalogPlan;
use crate::plans::DropConnectionPlan;
use crate::plans::DropDatabasePlan;
use crate::plans::DropDatamaskPolicyPlan;
use crate::plans::DropFileFormatPlan;
use crate::plans::DropIndexPlan;
use crate::plans::DropNetworkPolicyPlan;
use crate::plans::DropNotificationPlan;
use crate::plans::DropPasswordPolicyPlan;
use crate::plans::DropProcedurePlan;
use crate::plans::DropRolePlan;
use crate::plans::DropRowAccessPolicyPlan;
use crate::plans::DropSequencePlan;
use crate::plans::DropStagePlan;
use crate::plans::DropStreamPlan;
use crate::plans::DropTableClusterKeyPlan;
use crate::plans::DropTableColumnPlan;
use crate::plans::DropTableConstraintPlan;
use crate::plans::DropTableIndexPlan;
use crate::plans::DropTablePlan;
use crate::plans::DropTableRefPlan;
use crate::plans::DropTableRowAccessPolicyPlan;
use crate::plans::DropTagPlan;
use crate::plans::DropTaskPlan;
use crate::plans::DropUDFPlan;
use crate::plans::DropUserPlan;
use crate::plans::DropViewPlan;
use crate::plans::DropWarehouseClusterPlan;
use crate::plans::DropWarehousePlan;
use crate::plans::DropWorkerPlan;
use crate::plans::DropWorkloadGroupPlan;
use crate::plans::Exchange;
use crate::plans::ExecuteImmediatePlan;
use crate::plans::ExecuteTaskPlan;
use crate::plans::ExistsTablePlan;
use crate::plans::GrantPrivilegePlan;
use crate::plans::GrantRolePlan;
use crate::plans::Insert;
use crate::plans::InsertMultiTable;
use crate::plans::InspectWarehousePlan;
use crate::plans::KillPlan;
use crate::plans::ModifyTableColumnPlan;
use crate::plans::ModifyTableCommentPlan;
use crate::plans::OptimizeCompactSegmentPlan;
use crate::plans::OptimizePurgePlan;
use crate::plans::PresignPlan;
use crate::plans::ReclusterPlan;
use crate::plans::RefreshDatabaseCachePlan;
use crate::plans::RefreshIndexPlan;
use crate::plans::RefreshTableCachePlan;
use crate::plans::RefreshTableIndexPlan;
use crate::plans::RefreshVirtualColumnPlan;
use crate::plans::RelOperator;
use crate::plans::RemoveStagePlan;
use crate::plans::RenameDatabasePlan;
use crate::plans::RenameTableColumnPlan;
use crate::plans::RenameTablePlan;
use crate::plans::RenameWarehouseClusterPlan;
use crate::plans::RenameWarehousePlan;
use crate::plans::RenameWorkloadGroupPlan;
use crate::plans::Replace;
use crate::plans::ResumeWarehousePlan;
use crate::plans::RevertTablePlan;
use crate::plans::RevokePrivilegePlan;
use crate::plans::RevokeRolePlan;
use crate::plans::SetObjectTagsPlan;
use crate::plans::SetOptionsPlan;
use crate::plans::SetPlan;
use crate::plans::SetPriorityPlan;
use crate::plans::SetRolePlan;
use crate::plans::SetSecondaryRolesPlan;
use crate::plans::SetWorkloadGroupQuotasPlan;
use crate::plans::ShowConnectionsPlan;
use crate::plans::ShowCreateCatalogPlan;
use crate::plans::ShowCreateDatabasePlan;
use crate::plans::ShowCreateTablePlan;
use crate::plans::ShowFileFormatsPlan;
use crate::plans::ShowNetworkPoliciesPlan;
use crate::plans::ShowTasksPlan;
use crate::plans::SuspendWarehousePlan;
use crate::plans::SwapTablePlan;
use crate::plans::SystemPlan;
use crate::plans::TruncateTablePlan;
use crate::plans::UnassignWarehouseNodesPlan;
use crate::plans::UndropDatabasePlan;
use crate::plans::UndropTablePlan;
use crate::plans::UnsetObjectTagsPlan;
use crate::plans::UnsetOptionsPlan;
use crate::plans::UnsetPlan;
use crate::plans::UnsetWorkloadGroupQuotasPlan;
use crate::plans::UseCatalogPlan;
use crate::plans::UseDatabasePlan;
use crate::plans::UseWarehousePlan;
use crate::plans::VacuumDropTablePlan;
use crate::plans::VacuumTablePlan;
use crate::plans::VacuumTemporaryFilesPlan;
use crate::plans::copy_into_location::CopyIntoLocationPlan;
use crate::plans::row_access_policy::CreateRowAccessPolicyPlan;
use crate::plans::worker_schema;

#[derive(Educe)]
#[educe(
    Clone(bound = false, attrs = "#[recursive::recursive]"),
    Debug(bound = false, attrs = "#[recursive::recursive]")
)]
pub enum Plan {
    // `SELECT` statement
    Query {
        s_expr: Box<SExpr>,
        metadata: MetadataRef,
        bind_context: Box<BindContext>,
        rewrite_kind: Option<RewriteKind>,
        // Use for generate query result cache key.
        formatted_ast: Option<String>,
        ignore_result: bool,
    },

    Explain {
        kind: ExplainKind,
        config: ExplainConfig,
        plan: Box<Plan>,
    },
    ExplainAst {
        formatted_string: String,
    },
    ExplainSyntax {
        formatted_sql: String,
    },
    ExplainAnalyze {
        partial: bool,
        graphical: bool,
        plan: Box<Plan>,
    },
    ExplainPerf {
        sql: String,
    },
    ReportIssue(String),

    // Call is rewrite into Query
    // Call(Box<CallPlan>),

    // Catalogs
    ShowCreateCatalog(Box<ShowCreateCatalogPlan>),
    CreateCatalog(Box<CreateCatalogPlan>),
    DropCatalog(Box<DropCatalogPlan>),
    UseCatalog(Box<UseCatalogPlan>),

    // Warehouses
    ShowOnlineNodes,
    ShowWarehouses,
    UseWarehouse(Box<UseWarehousePlan>),
    CreateWarehouse(Box<CreateWarehousePlan>),
    DropWarehouse(Box<DropWarehousePlan>),
    ResumeWarehouse(Box<ResumeWarehousePlan>),
    SuspendWarehouse(Box<SuspendWarehousePlan>),
    RenameWarehouse(Box<RenameWarehousePlan>),
    InspectWarehouse(Box<InspectWarehousePlan>),
    AddWarehouseCluster(Box<AddWarehouseClusterPlan>),
    DropWarehouseCluster(Box<DropWarehouseClusterPlan>),
    RenameWarehouseCluster(Box<RenameWarehouseClusterPlan>),
    AssignWarehouseNodes(Box<AssignWarehouseNodesPlan>),
    UnassignWarehouseNodes(Box<UnassignWarehouseNodesPlan>),

    // Workers
    ShowWorkers,
    CreateWorker(Box<CreateWorkerPlan>),
    AlterWorker(Box<AlterWorkerPlan>),
    DropWorker(Box<DropWorkerPlan>),

    // Workloads
    ShowWorkloadGroups,
    CreateWorkloadGroup(Box<CreateWorkloadGroupPlan>),
    DropWorkloadGroup(Box<DropWorkloadGroupPlan>),
    RenameWorkloadGroup(Box<RenameWorkloadGroupPlan>),
    SetWorkloadGroupQuotas(Box<SetWorkloadGroupQuotasPlan>),
    UnsetWorkloadGroupQuotas(Box<UnsetWorkloadGroupQuotasPlan>),

    // Databases
    ShowCreateDatabase(Box<ShowCreateDatabasePlan>),
    CreateDatabase(Box<CreateDatabasePlan>),
    DropDatabase(Box<DropDatabasePlan>),
    UndropDatabase(Box<UndropDatabasePlan>),
    RenameDatabase(Box<RenameDatabasePlan>),
    UseDatabase(Box<UseDatabasePlan>),
    RefreshDatabaseCache(Box<RefreshDatabaseCachePlan>),
    AlterDatabase(Box<AlterDatabasePlan>),

    // Tables
    ShowCreateTable(Box<ShowCreateTablePlan>),
    DescribeTable(Box<DescribeTablePlan>),
    CreateTable(Box<CreateTablePlan>),
    DropTable(Box<DropTablePlan>),
    UndropTable(Box<UndropTablePlan>),
    RenameTable(Box<RenameTablePlan>),
    SwapTable(Box<SwapTablePlan>),
    ModifyTableComment(Box<ModifyTableCommentPlan>),
    RenameTableColumn(Box<RenameTableColumnPlan>),
    AddTableColumn(Box<AddTableColumnPlan>),
    DropTableColumn(Box<DropTableColumnPlan>),
    ModifyTableColumn(Box<ModifyTableColumnPlan>),
    AddTableConstraint(Box<AddTableConstraintPlan>),
    DropTableConstraint(Box<DropTableConstraintPlan>),
    AlterTableClusterKey(Box<AlterTableClusterKeyPlan>),
    DropTableClusterKey(Box<DropTableClusterKeyPlan>),
    ReclusterTable(Box<ReclusterPlan>),
    RevertTable(Box<RevertTablePlan>),
    TruncateTable(Box<TruncateTablePlan>),
    VacuumTable(Box<VacuumTablePlan>),
    VacuumDropTable(Box<VacuumDropTablePlan>),
    VacuumTemporaryFiles(Box<VacuumTemporaryFilesPlan>),
    AnalyzeTable(Box<AnalyzeTablePlan>),
    ExistsTable(Box<ExistsTablePlan>),
    SetOptions(Box<SetOptionsPlan>),
    UnsetOptions(Box<UnsetOptionsPlan>),
    RefreshTableCache(Box<RefreshTableCachePlan>),
    ModifyTableConnection(Box<ModifyTableConnectionPlan>),
    AddTableRowAccessPolicy(Box<AddTableRowAccessPolicyPlan>),
    DropTableRowAccessPolicy(Box<DropTableRowAccessPolicyPlan>),
    DropAllTableRowAccessPolicies(Box<DropAllTableRowAccessPoliciesPlan>),
    CreateTableRef(Box<CreateTableRefPlan>),
    DropTableRef(Box<DropTableRefPlan>),

    // Optimize
    OptimizePurge(Box<OptimizePurgePlan>),
    OptimizeCompactSegment(Box<OptimizeCompactSegmentPlan>),
    OptimizeCompactBlock {
        s_expr: Box<SExpr>,
        need_purge: bool,
    },

    // Insert
    Insert(Box<Insert>),
    InsertMultiTable(Box<InsertMultiTable>),
    Replace(Box<Replace>),
    DataMutation {
        s_expr: Box<SExpr>,
        schema: DataSchemaRef,
        metadata: MetadataRef,
    },

    CopyIntoTable(Box<CopyIntoTablePlan>),
    CopyIntoLocation(Box<CopyIntoLocationPlan>),

    // Views
    CreateView(Box<CreateViewPlan>),
    AlterView(Box<AlterViewPlan>),
    DropView(Box<DropViewPlan>),
    DescribeView(Box<DescribeViewPlan>),

    // Streams
    CreateStream(Box<CreateStreamPlan>),
    DropStream(Box<DropStreamPlan>),

    // Indexes
    CreateIndex(Box<CreateIndexPlan>),
    DropIndex(Box<DropIndexPlan>),
    RefreshIndex(Box<RefreshIndexPlan>),
    CreateTableIndex(Box<CreateTableIndexPlan>),
    DropTableIndex(Box<DropTableIndexPlan>),
    RefreshTableIndex(Box<RefreshTableIndexPlan>),

    // Virtual Columns
    RefreshVirtualColumn(Box<RefreshVirtualColumnPlan>),

    // Account
    AlterUser(Box<AlterUserPlan>),
    CreateUser(Box<CreateUserPlan>),
    DropUser(Box<DropUserPlan>),
    DescUser(Box<DescUserPlan>),

    // UDF
    CreateUDF(Box<CreateUDFPlan>),
    AlterUDF(Box<AlterUDFPlan>),
    DropUDF(Box<DropUDFPlan>),

    // RowAccessPolicy
    CreateRowAccessPolicy(Box<CreateRowAccessPolicyPlan>),
    DropRowAccessPolicy(Box<DropRowAccessPolicyPlan>),
    DescRowAccessPolicy(Box<DescRowAccessPolicyPlan>),

    // Role
    CreateRole(Box<CreateRolePlan>),
    DropRole(Box<DropRolePlan>),
    AlterRole(Box<AlterRolePlan>),
    GrantRole(Box<GrantRolePlan>),
    GrantPriv(Box<GrantPrivilegePlan>),
    RevokePriv(Box<RevokePrivilegePlan>),
    RevokeRole(Box<RevokeRolePlan>),
    SetRole(Box<SetRolePlan>),
    SetSecondaryRoles(Box<SetSecondaryRolesPlan>),

    // FileFormat
    CreateFileFormat(Box<CreateFileFormatPlan>),
    DropFileFormat(Box<DropFileFormatPlan>),
    ShowFileFormats(Box<ShowFileFormatsPlan>),

    // Tags
    CreateTag(Box<CreateTagPlan>),
    DropTag(Box<DropTagPlan>),
    SetObjectTags(Box<SetObjectTagsPlan>),
    UnsetObjectTags(Box<UnsetObjectTagsPlan>),

    // Stages
    CreateStage(Box<CreateStagePlan>),
    AlterStage(Box<AlterStagePlan>),
    DropStage(Box<DropStagePlan>),
    RemoveStage(Box<RemoveStagePlan>),

    // Connection
    CreateConnection(Box<CreateConnectionPlan>),
    DescConnection(Box<DescConnectionPlan>),
    DropConnection(Box<DropConnectionPlan>),
    ShowConnections(Box<ShowConnectionsPlan>),

    // Presign
    Presign(Box<PresignPlan>),

    // Set
    Set(Box<SetPlan>),
    Unset(Box<UnsetPlan>),
    Kill(Box<KillPlan>),
    SetPriority(Box<SetPriorityPlan>),
    System(Box<SystemPlan>),

    // Data mask
    CreateDatamaskPolicy(Box<CreateDatamaskPolicyPlan>),
    DropDatamaskPolicy(Box<DropDatamaskPolicyPlan>),
    DescDatamaskPolicy(Box<DescDatamaskPolicyPlan>),

    // Network policy
    CreateNetworkPolicy(Box<CreateNetworkPolicyPlan>),
    AlterNetworkPolicy(Box<AlterNetworkPolicyPlan>),
    DropNetworkPolicy(Box<DropNetworkPolicyPlan>),
    DescNetworkPolicy(Box<DescNetworkPolicyPlan>),
    ShowNetworkPolicies(Box<ShowNetworkPoliciesPlan>),

    // Password policy
    CreatePasswordPolicy(Box<CreatePasswordPolicyPlan>),
    AlterPasswordPolicy(Box<AlterPasswordPolicyPlan>),
    DropPasswordPolicy(Box<DropPasswordPolicyPlan>),
    DescPasswordPolicy(Box<DescPasswordPolicyPlan>),

    // Task
    CreateTask(Box<CreateTaskPlan>),
    AlterTask(Box<AlterTaskPlan>),
    DropTask(Box<DropTaskPlan>),
    DescribeTask(Box<DescribeTaskPlan>),
    ShowTasks(Box<ShowTasksPlan>),
    ExecuteTask(Box<ExecuteTaskPlan>),

    CreateDynamicTable(Box<CreateDynamicTablePlan>),

    // Txn
    Begin,
    Commit,
    Abort,

    // Notifications
    CreateNotification(Box<CreateNotificationPlan>),
    AlterNotification(Box<AlterNotificationPlan>),
    DropNotification(Box<DropNotificationPlan>),
    DescNotification(Box<DescNotificationPlan>),

    // Stored procedures
    ExecuteImmediate(Box<ExecuteImmediatePlan>),
    // ShowCreateProcedure(Box<ShowCreateProcedurePlan>),
    DropProcedure(Box<DropProcedurePlan>),
    DescProcedure(Box<DescProcedurePlan>),
    CreateProcedure(Box<CreateProcedurePlan>),
    CallProcedure(Box<CallProcedurePlan>),
    // RenameProcedure(Box<RenameProcedurePlan>),

    // sequence
    CreateSequence(Box<CreateSequencePlan>),
    DropSequence(Box<DropSequencePlan>),
    DescSequence(Box<DescSequencePlan>),

    // Dictionary
    CreateDictionary(Box<CreateDictionaryPlan>),
    DropDictionary(Box<DropDictionaryPlan>),
    ShowCreateDictionary(Box<ShowCreateDictionaryPlan>),
    RenameDictionary(Box<RenameDictionaryPlan>),
}

#[derive(Clone, Debug)]
pub enum RewriteKind {
    ShowSettings,
    ShowVariables,
    ShowMetrics,
    ShowProcessList,
    ShowEngines,
    ShowIndexes,

    ShowLocks,

    ShowCatalogs,
    ShowDatabases,
    ShowDropDatabases,
    ShowTables(String, String),
    ShowColumns(String, String, String),
    ShowTablesStatus,
    ShowVirtualColumns,
    ShowDictionaries(String),
    ShowStatistics,

    ShowStreams(String),
    ShowTags,

    ShowFunctions,
    ShowUserFunctions,
    ShowTableFunctions,

    ShowUsers,
    ShowStages,
    DescribeStage,
    ListStage,
    ShowRoles,
    ShowPasswordPolicies,
    ShowGrants,

    Call,
    ShowProcedures,
    ShowSequences,
}

impl Plan {
    pub fn kind(&self) -> QueryKind {
        match self {
            Plan::Query { .. } => QueryKind::Query,
            Plan::CopyIntoTable(copy_plan) => match copy_plan.write_mode {
                CopyIntoTableMode::Insert { .. } => QueryKind::Insert,
                _ => QueryKind::CopyIntoTable,
            },
            Plan::Explain { .. }
            | Plan::ExplainAnalyze { .. }
            | Plan::ExplainAst { .. }
            | Plan::ExplainSyntax { .. } => QueryKind::Explain,
            Plan::Insert(_) => QueryKind::Insert,
            Plan::Replace(_)
            | Plan::DataMutation { .. }
            | Plan::OptimizePurge(_)
            | Plan::OptimizeCompactSegment(_)
            | Plan::OptimizeCompactBlock { .. } => QueryKind::Update,
            _ => QueryKind::Other,
        }
    }
}

impl Display for Plan {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}", self.kind())
    }
}

impl Plan {
    pub fn schema(&self) -> DataSchemaRef {
        match self {
            Plan::Query {
                s_expr: _,
                metadata: _,
                bind_context,
                ..
            } => bind_context.output_schema(),
            Plan::Explain { .. }
            | Plan::ExplainAst { .. }
            | Plan::ExplainSyntax { .. }
            | Plan::ExplainAnalyze { .. }
            | Plan::ExplainPerf { .. } => {
                DataSchemaRefExt::create(vec![DataField::new("explain", DataType::String)])
            }
            Plan::DataMutation { schema, .. } => schema.clone(),
            Plan::ShowCreateCatalog(plan) => plan.schema(),
            Plan::ShowCreateDatabase(plan) => plan.schema(),
            Plan::ShowCreateDictionary(plan) => plan.schema(),
            Plan::ShowCreateTable(plan) => plan.schema(),
            Plan::DescribeTable(plan) => plan.schema(),
            Plan::VacuumTable(plan) => plan.schema(),
            Plan::VacuumDropTable(plan) => plan.schema(),
            Plan::VacuumTemporaryFiles(plan) => plan.schema(),
            Plan::ExistsTable(plan) => plan.schema(),
            Plan::DescribeView(plan) => plan.schema(),
            Plan::ShowFileFormats(plan) => plan.schema(),
            Plan::Replace(plan) => plan.schema(),
            Plan::Presign(plan) => plan.schema(),
            Plan::CreateDatamaskPolicy(plan) => plan.schema(),
            Plan::DropDatamaskPolicy(plan) => plan.schema(),
            Plan::DescDatamaskPolicy(plan) => plan.schema(),
            Plan::DescNetworkPolicy(plan) => plan.schema(),
            Plan::ShowNetworkPolicies(plan) => plan.schema(),
            Plan::DescPasswordPolicy(plan) => plan.schema(),
            Plan::CopyIntoTable(plan) => plan.schema(),
            Plan::CopyIntoLocation(plan) => plan.schema(),
            Plan::CreateTask(plan) => plan.schema(),
            Plan::DescribeTask(plan) => plan.schema(),
            Plan::RefreshVirtualColumn(plan) => plan.schema(),
            Plan::ShowTasks(plan) => plan.schema(),
            Plan::ExecuteTask(plan) => plan.schema(),
            Plan::DescRowAccessPolicy(plan) => plan.schema(),
            Plan::DescNotification(plan) => plan.schema(),
            Plan::DescConnection(plan) => plan.schema(),
            Plan::ShowConnections(plan) => plan.schema(),
            Plan::ExecuteImmediate(plan) => plan.schema(),
            Plan::CallProcedure(plan) => plan.schema(),
            Plan::InsertMultiTable(plan) => plan.schema(),
            Plan::DescUser(plan) => plan.schema(),
            Plan::Insert(plan) => plan.schema(),
            Plan::InspectWarehouse(plan) => plan.schema(),
            Plan::ShowWarehouses => DataSchemaRefExt::create(vec![
                DataField::new("warehouse", DataType::String),
                DataField::new("type", DataType::String),
                DataField::new("status", DataType::String),
            ]),
            Plan::ShowWorkers => worker_schema(),
            Plan::ShowOnlineNodes => DataSchemaRefExt::create(vec![
                DataField::new("id", DataType::String),
                DataField::new("type", DataType::String),
                DataField::new("node_group", DataType::String),
                DataField::new("warehouse", DataType::String),
                DataField::new("cluster", DataType::String),
                DataField::new("version", DataType::String),
            ]),
            Plan::DescProcedure(plan) => plan.schema(),
            Plan::ShowWorkloadGroups => DataSchemaRefExt::create(vec![
                DataField::new("name", DataType::String),
                DataField::new("cpu_quota", DataType::String),
                DataField::new("memory_quota", DataType::String),
                DataField::new("query_timeout", DataType::String),
                DataField::new("max_concurrency", DataType::String),
                DataField::new("query_queued_timeout", DataType::String),
            ]),
            Plan::DescSequence(plan) => plan.schema(),
            Plan::ReportIssue { .. } => {
                DataSchemaRefExt::create(vec![DataField::new("summary", DataType::String)])
            }
            _ => Arc::new(DataSchema::empty()),
        }
    }

    pub fn has_result_set(&self) -> bool {
        !self.schema().fields().is_empty()
    }

    pub fn is_dynamic_schema(&self) -> bool {
        matches!(
            self,
            Plan::ExecuteImmediate { .. } | Plan::CallProcedure { .. }
        )
    }

    pub fn remove_exchange_for_select(&self) -> Self {
        if let Plan::Query {
            s_expr,
            metadata,
            bind_context,
            rewrite_kind,
            formatted_ast,
            ignore_result,
        } = self
        {
            if let RelOperator::Exchange(Exchange::Merge) = s_expr.plan.as_ref() {
                let s_expr = Box::new(s_expr.child(0).unwrap().clone());
                return Plan::Query {
                    s_expr,
                    metadata: metadata.clone(),
                    bind_context: bind_context.clone(),
                    rewrite_kind: rewrite_kind.clone(),
                    formatted_ast: formatted_ast.clone(),
                    ignore_result: *ignore_result,
                };
            }
        }
        self.clone()
    }

    pub fn bind_context(&self) -> Option<BindContext> {
        if let Plan::Query { bind_context, .. } = self {
            Some(*bind_context.clone())
        } else {
            None
        }
    }

    pub fn replace_query_s_expr(&self, s_expr: SExpr) -> Self {
        let Plan::Query {
            metadata,
            bind_context,
            rewrite_kind,
            formatted_ast,
            ignore_result,
            ..
        } = self
        else {
            unreachable!()
        };

        Plan::Query {
            s_expr: Box::new(s_expr),
            metadata: metadata.clone(),
            bind_context: bind_context.clone(),
            rewrite_kind: rewrite_kind.clone(),
            formatted_ast: formatted_ast.clone(),
            ignore_result: *ignore_result,
        }
    }
}
