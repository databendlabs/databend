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

use std::collections::BTreeMap;
use std::fmt::Display;
use std::fmt::Formatter;

use derive_visitor::Drive;
use derive_visitor::DriveMut;
use derive_visitor::Visitor;
use derive_visitor::VisitorMut;
use dictionary::CreateDictionaryStmt;
use dictionary::DropDictionaryStmt;
use dictionary::ShowCreateDictionaryStmt;
use itertools::Itertools;

use super::merge_into::MergeIntoStmt;
use super::*;
use crate::Span;
use crate::ast::CreateOption;
use crate::ast::ExternalAccessStmt;
use crate::ast::Identifier;
use crate::ast::Query;
use crate::ast::quote::QuotedString;
use crate::ast::statements::connection::CreateConnectionStmt;
use crate::ast::statements::pipe::CreatePipeStmt;
use crate::ast::statements::role::AlterRoleStmt;
use crate::ast::statements::settings::Settings;
use crate::ast::statements::task::CreateTaskStmt;
use crate::ast::statements::warehouse::ShowWarehousesStmt;
use crate::ast::statements::worker::ShowWorkersStmt;
use crate::ast::statements::workload::CreateWorkloadGroupStmt;
use crate::ast::statements::workload::DropWorkloadGroupStmt;
use crate::ast::statements::workload::RenameWorkloadGroupStmt;
use crate::ast::statements::workload::SetWorkloadGroupQuotasStmt;
use crate::ast::statements::workload::ShowWorkloadGroupsStmt;
use crate::ast::write_comma_separated_list;
use crate::parser::SqlSecrets;
use crate::parser::mask::hide_literals;
use crate::parser::mask::is_secret_name;
use crate::parser::mask::mask_dynamic_sql_expr;
use crate::parser::mask::mask_partial;
use crate::parser::mask::mask_program_literals;
use crate::parser::mask::url_secrets;
use crate::parser::mask_raw_sql_with_secrets;
use crate::parser::mask_url;

// SQL statement
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub enum Statement {
    Query(Box<Query>),
    StatementWithSettings {
        settings: Option<Settings>,
        stmt: Box<Statement>,
    },
    Explain {
        kind: ExplainKind,
        options: (Span, Vec<ExplainOption>),
        query: Box<Statement>,
    },
    ExplainAnalyze {
        // if partial is true, only scan/filter/join will be shown.
        partial: bool,
        graphical: bool,
        query: Box<Statement>,
    },
    ReportIssue(String),

    CopyIntoTable(CopyIntoTableStmt),
    CopyIntoLocation(CopyIntoLocationStmt),

    Call(CallStmt),

    ShowSettings {
        show_options: Option<ShowOptions>,
    },
    ShowProcessList {
        show_options: Option<ShowOptions>,
    },
    ShowMetrics {
        show_options: Option<ShowOptions>,
    },
    ShowEngines {
        show_options: Option<ShowOptions>,
    },
    ShowFunctions {
        show_options: Option<ShowOptions>,
    },
    ShowUserFunctions {
        show_options: Option<ShowOptions>,
    },
    ShowTableFunctions {
        show_options: Option<ShowOptions>,
    },
    ShowIndexes {
        show_options: Option<ShowOptions>,
    },
    ShowLocks(ShowLocksStmt),

    KillStmt {
        kill_target: KillTarget,
        object_id: String,
    },

    SetStmt {
        settings: Settings,
    },
    UnSetStmt {
        settings: Settings,
    },

    ShowVariables {
        show_options: Option<ShowOptions>,
    },

    SetRole {
        is_default: bool,
        role_name: String,
    },

    SetSecondaryRoles {
        option: SecondaryRolesOption,
    },

    Insert(InsertStmt),
    InsertMultiTable(InsertMultiTableStmt),
    Replace(ReplaceStmt),
    MergeInto(MergeIntoStmt),
    Delete(DeleteStmt),

    Update(UpdateStmt),

    // Catalogs
    ShowCatalogs(ShowCatalogsStmt),
    ShowCreateCatalog(ShowCreateCatalogStmt),
    CreateCatalog(CreateCatalogStmt),
    DropCatalog(DropCatalogStmt),
    UseCatalog {
        catalog: Identifier,
    },

    // Warehouses
    ShowOnlineNodes(ShowOnlineNodesStmt),
    UseWarehouse(UseWarehouseStmt),
    ShowWarehouses(ShowWarehousesStmt),
    DropWarehouse(DropWarehouseStmt),
    CreateWarehouse(CreateWarehouseStmt),
    RenameWarehouse(RenameWarehouseStmt),
    ResumeWarehouse(ResumeWarehouseStmt),
    SuspendWarehouse(SuspendWarehouseStmt),
    InspectWarehouse(InspectWarehouseStmt),
    AddWarehouseCluster(AddWarehouseClusterStmt),
    DropWarehouseCluster(DropWarehouseClusterStmt),
    RenameWarehouseCluster(RenameWarehouseClusterStmt),
    AssignWarehouseNodes(AssignWarehouseNodesStmt),
    UnassignWarehouseNodes(UnassignWarehouseNodesStmt),

    // Workers
    ShowWorkers(ShowWorkersStmt),
    CreateWorker(CreateWorkerStmt),
    AlterWorker(AlterWorkerStmt),
    DropWorker(DropWorkerStmt),

    // Workloads
    ShowWorkloadGroups(ShowWorkloadGroupsStmt),
    CreateWorkloadGroup(CreateWorkloadGroupStmt),
    DropWorkloadGroup(DropWorkloadGroupStmt),
    RenameWorkloadGroup(RenameWorkloadGroupStmt),
    SetWorkloadQuotasGroup(SetWorkloadGroupQuotasStmt),
    UnsetWorkloadQuotasGroup(UnsetWorkloadGroupQuotasStmt),

    // Databases
    ShowDatabases(ShowDatabasesStmt),
    ShowDropDatabases(ShowDropDatabasesStmt),
    ShowCreateDatabase(ShowCreateDatabaseStmt),
    CreateDatabase(CreateDatabaseStmt),
    DropDatabase(DropDatabaseStmt),
    UndropDatabase(UndropDatabaseStmt),
    AlterDatabase(AlterDatabaseStmt),
    UseDatabase {
        database: Identifier,
    },

    // Shares
    CreateShare(CreateShareStmt),
    DropShare(DropShareStmt),
    AlterShare(AlterShareStmt),
    GrantShare(GrantShareStmt),
    RevokeShare(RevokeShareStmt),
    ShowShares(ShowSharesStmt),
    DescShare(DescShareStmt),

    // Tables
    ShowTables(ShowTablesStmt),
    ShowCreateTable(ShowCreateTableStmt),
    DescribeTable(DescribeTableStmt),
    ShowTablesStatus(ShowTablesStatusStmt),
    ShowDropTables(ShowDropTablesStmt),
    AttachTable(AttachTableStmt),
    CreateTable(CreateTableStmt),
    DropTable(DropTableStmt),
    UndropTable(UndropTableStmt),
    AlterTable(AlterTableStmt),
    RenameTable(RenameTableStmt),
    TruncateTable(TruncateTableStmt),
    OptimizeTable(OptimizeTableStmt),
    VacuumTable(VacuumTableStmt),
    VacuumTables(VacuumTablesStmt),
    VacuumAll(VacuumAllStmt),
    VacuumDropTable(VacuumDropTableStmt),
    VacuumTemporaryFiles(VacuumTemporaryFiles),
    VacuumVirtualColumn(VacuumVirtualColumnStmt),
    AnalyzeTable(AnalyzeTableStmt),
    ExistsTable(ExistsTableStmt),
    ShowStatistics(ShowStatisticsStmt),

    // Dictionaries
    CreateDictionary(CreateDictionaryStmt),
    DropDictionary(DropDictionaryStmt),
    ShowCreateDictionary(ShowCreateDictionaryStmt),
    ShowDictionaries(ShowDictionariesStmt),
    RenameDictionary(RenameDictionaryStmt),

    // Columns
    ShowColumns(ShowColumnsStmt),

    // Views
    CreateView(CreateViewStmt),
    AlterView(AlterViewStmt),
    DropView(DropViewStmt),
    ShowViews(ShowViewsStmt),
    DescribeView(DescribeViewStmt),
    RefreshLineage(RefreshLineageStmt),

    // Materialized Views
    CreateMaterializedView(CreateMaterializedViewStmt),
    AlterMaterializedView(AlterMaterializedViewStmt),
    DropMaterializedView(DropMaterializedViewStmt),
    RefreshMaterializedView(RefreshMaterializedViewStmt),
    ShowCreateMaterializedView(ShowCreateMaterializedViewStmt),
    ShowMaterializedViews(ShowMaterializedViewsStmt),

    // Streams
    CreateStream(CreateStreamStmt),
    DropStream(DropStreamStmt),
    ShowStreams(ShowStreamsStmt),
    DescribeStream(DescribeStreamStmt),

    // Indexes
    CreateTableIndex(CreateTableIndexStmt),
    DropTableIndex(DropTableIndexStmt),
    RefreshTableIndex(RefreshTableIndexStmt),

    // VirtualColumns
    RefreshVirtualColumn(RefreshVirtualColumnStmt),
    ShowVirtualColumns(ShowVirtualColumnsStmt),

    // User
    ShowUsers {
        show_options: Option<ShowOptions>,
    },
    DescribeUser {
        user: UserIdentity,
    },
    ShowPublicKeys {
        user: UserIdentity,
    },
    CreateUser(CreateUserStmt),
    AlterUser(AlterUserStmt),
    DropUser {
        if_exists: bool,
        user: UserIdentity,
    },
    ShowRoles {
        show_options: Option<ShowOptions>,
    },
    CreateRole {
        create_option: CreateOption,
        role_name: String,
        comment: Option<String>,
    },
    DropRole {
        if_exists: bool,
        role_name: String,
    },
    AlterRole(AlterRoleStmt),
    Grant(GrantStmt),
    ShowGrants {
        principal: Option<PrincipalIdentity>,
        show_options: Option<ShowOptions>,
    },
    ShowObjectPrivileges(ShowObjectPrivilegesStmt),
    ShowGrantsOfRole(ShowGranteesOfRoleStmt),
    Revoke(RevokeStmt),

    // UDF
    CreateUDF(CreateUDFStmt),
    DropUDF {
        if_exists: bool,
        udf_name: Identifier,
    },
    AlterUDF(AlterUDFStmt),

    // RowAccessPolicy
    CreateRowAccessPolicy(CreateRowAccessPolicyStmt),
    DropRowAccessPolicy(DropRowAccessPolicyStmt),
    DescRowAccessPolicy(DescRowAccessPolicyStmt),

    // Tags
    CreateTag(CreateTagStmt),
    DropTag(DropTagStmt),
    ShowTags(ShowTagsStmt),
    AlterObjectTag(AlterObjectTagStmt),

    // Stages
    CreateStage(CreateStageStmt),
    AlterStage(AlterStageStmt),
    ShowStages {
        show_options: Option<ShowOptions>,
    },
    DropStage {
        if_exists: bool,
        stage_name: String,
    },
    DescribeStage {
        stage_name: String,
    },
    RemoveStage {
        location: String,
        pattern: String,
    },
    ListStage {
        location: String,
        pattern: Option<String>,
    },
    // Connection
    ExternalAccess(ExternalAccessStmt),
    CreateConnection(CreateConnectionStmt),
    DropConnection(DropConnectionStmt),
    DescribeConnection(DescribeConnectionStmt),
    ShowConnections(ShowConnectionsStmt),

    // UserDefinedFileFormat
    CreateFileFormat {
        create_option: CreateOption,
        name: String,
        file_format_options: FileFormatOptions,
    },
    DropFileFormat {
        if_exists: bool,
        name: String,
    },
    ShowFileFormats,
    Presign(PresignStmt),

    // data mask
    CreateDatamaskPolicy(CreateDatamaskPolicyStmt),
    DropDatamaskPolicy(DropDatamaskPolicyStmt),
    DescDatamaskPolicy(DescDatamaskPolicyStmt),

    // network policy
    CreateNetworkPolicy(CreateNetworkPolicyStmt),
    AlterNetworkPolicy(AlterNetworkPolicyStmt),
    DropNetworkPolicy(DropNetworkPolicyStmt),
    DescNetworkPolicy(DescNetworkPolicyStmt),
    ShowNetworkPolicies,

    // password policy
    CreatePasswordPolicy(CreatePasswordPolicyStmt),
    AlterPasswordPolicy(AlterPasswordPolicyStmt),
    DropPasswordPolicy(DropPasswordPolicyStmt),
    DescPasswordPolicy(DescPasswordPolicyStmt),
    ShowPasswordPolicies {
        show_options: Option<ShowOptions>,
    },

    // tasks
    CreateTask(CreateTaskStmt),
    AlterTask(AlterTaskStmt),
    ExecuteTask(ExecuteTaskStmt),
    DescribeTask(DescribeTaskStmt),
    DropTask(DropTaskStmt),
    ShowTasks(ShowTasksStmt),

    CreateDynamicTable(CreateDynamicTableStmt),
    RefreshDynamicTable(RefreshDynamicTableStmt),

    // pipes
    CreatePipe(CreatePipeStmt),
    DescribePipe(DescribePipeStmt),
    DropPipe(DropPipeStmt),
    AlterPipe(AlterPipeStmt),

    // Transactions
    Begin,
    Commit,
    Abort,

    // Notifications
    CreateNotification(CreateNotificationStmt),
    AlterNotification(AlterNotificationStmt),
    DropNotification(DropNotificationStmt),
    DescribeNotification(DescribeNotificationStmt),

    // Stored procedures
    ExecuteImmediate(ExecuteImmediateStmt),
    CreateProcedure(CreateProcedureStmt),
    DropProcedure(DropProcedureStmt),
    ShowProcedures {
        show_options: Option<ShowOptions>,
    },
    DescProcedure(DescProcedureStmt),
    CallProcedure(CallProcedureStmt),

    // Sequence
    CreateSequence(CreateSequenceStmt),
    DropSequence(DropSequenceStmt),
    ShowSequences {
        show_options: Option<ShowOptions>,
    },
    DescSequence {
        name: Identifier,
    },

    // Set priority for query
    SetPriority {
        priority: Priority,
        object_id: String,
    },

    // System actions
    System(SystemStmt),
}

/// Masks credentials that `Display` would otherwise print, including inside
/// wrappers such as `EXPLAIN` and `SETTINGS (...)`, and records their values.
#[derive(Default, VisitorMut)]
#[visitor(
    AuthOption(enter),
    CreateCatalogStmt(enter),
    NotificationWebhookOptions(enter),
    UDFDefinition(enter),
    CreateDictionaryStmt(enter),
    Connection(enter),
    CreateConnectionStmt(enter),
    AlterTableStmt(enter),
    ExecuteImmediateStmt(enter),
    CreateProcedureStmt(enter),
    CallProcedureStmt(enter),
    TaskSql(enter),
    CreateTaskStmt(enter),
    AlterTaskOptions(enter),
    Settings(enter),
    HintItem(enter),
    SelectStageOptions(enter),
    CreateTableStmt(enter),
    CreateWorkerStmt(enter),
    AlterWorkerAction(enter)
)]
struct SecretMasker(SqlSecrets);

/// Whether [`SecretMasker`] may change a statement, so that statements
/// without credentials, such as large `INSERT`s, are not cloned to be masked.
#[derive(Default, Visitor)]
#[visitor(
    AuthOption(enter),
    CreateCatalogStmt(enter),
    NotificationWebhookOptions(enter),
    UDFDefinition(enter),
    CreateDictionaryStmt(enter),
    Connection(enter),
    CreateConnectionStmt(enter),
    AlterTableStmt(enter),
    ExecuteImmediateStmt(enter),
    CreateProcedureStmt(enter),
    CallProcedureStmt(enter),
    TaskSql(enter),
    CreateTaskStmt(enter),
    AlterTaskOptions(enter),
    Settings(enter),
    HintItem(enter),
    SelectStageOptions(enter),
    CreateTableStmt(enter),
    CreateWorkerStmt(enter),
    AlterWorkerAction(enter)
)]
struct MayHoldSecrets(bool);

impl MayHoldSecrets {
    fn enter_auth_option(&mut self, _: &AuthOption) {
        self.0 = true;
    }

    fn enter_create_catalog_stmt(&mut self, _: &CreateCatalogStmt) {
        self.0 = true;
    }

    fn enter_notification_webhook_options(&mut self, _: &NotificationWebhookOptions) {
        self.0 = true;
    }

    fn enter_udf_definition(&mut self, _: &UDFDefinition) {
        self.0 = true;
    }

    fn enter_create_dictionary_stmt(&mut self, _: &CreateDictionaryStmt) {
        self.0 = true;
    }

    fn enter_connection(&mut self, connection: &Connection) {
        self.0 |= !connection.conns.is_empty();
    }

    fn enter_create_connection_stmt(&mut self, _: &CreateConnectionStmt) {
        self.0 = true;
    }

    fn enter_alter_table_stmt(&mut self, stmt: &AlterTableStmt) {
        self.0 |= match &stmt.action {
            AlterTableAction::ModifyConnection { .. } => true,
            AlterTableAction::SetOptions { set_options } => options_may_hold_secrets(set_options),
            _ => false,
        };
    }

    fn enter_select_stage_options(&mut self, options: &SelectStageOptions) {
        self.0 |= !options.connection.is_empty();
    }

    fn enter_create_table_stmt(&mut self, stmt: &CreateTableStmt) {
        self.0 |= options_may_hold_secrets(&stmt.table_options)
            || stmt
                .table_properties
                .as_ref()
                .is_some_and(options_may_hold_secrets);
    }

    fn enter_create_worker_stmt(&mut self, stmt: &CreateWorkerStmt) {
        self.0 |= options_may_hold_secrets(&stmt.options);
    }

    fn enter_alter_worker_action(&mut self, action: &AlterWorkerAction) {
        if let AlterWorkerAction::SetOptions { options } = action {
            self.0 |= options_may_hold_secrets(options);
        }
    }

    fn enter_execute_immediate_stmt(&mut self, _: &ExecuteImmediateStmt) {
        self.0 = true;
    }

    fn enter_create_procedure_stmt(&mut self, _: &CreateProcedureStmt) {
        self.0 = true;
    }

    fn enter_call_procedure_stmt(&mut self, _: &CallProcedureStmt) {
        self.0 = true;
    }

    fn enter_task_sql(&mut self, _: &TaskSql) {
        self.0 = true;
    }

    fn enter_create_task_stmt(&mut self, _: &CreateTaskStmt) {
        self.0 = true;
    }

    fn enter_alter_task_options(&mut self, _: &AlterTaskOptions) {
        self.0 = true;
    }

    fn enter_settings(&mut self, settings: &Settings) {
        self.0 |= settings
            .identifiers
            .iter()
            .any(|ident| is_secret_name(&ident.name));
    }

    fn enter_hint_item(&mut self, item: &HintItem) {
        self.0 |= is_secret_name(&item.name.name);
    }
}

/// Whether [`SecretMasker::mask_secret_options`] may change `options`.
fn options_may_hold_secrets(options: &BTreeMap<String, String>) -> bool {
    options
        .iter()
        .any(|(key, value)| is_secret_name(key) || value.contains("://"))
}

impl SecretMasker {
    fn hide(&mut self, value: &mut String, masked: String) {
        if *value != masked {
            self.0.push(value);
            self.0.extend(url_secrets(value));
            *value = masked;
        }
    }

    fn mask_secret_options(&mut self, options: &mut BTreeMap<String, String>) {
        for (key, value) in options.iter_mut() {
            let masked = if is_secret_name(key) {
                mask_partial(value)
            } else {
                mask_url(value, false)
            };
            self.hide(value, masked);
        }
    }

    fn mask_all(&mut self, options: &mut BTreeMap<String, String>) {
        for value in options.values_mut() {
            let masked = mask_partial(value);
            self.hide(value, masked);
        }
    }

    fn enter_auth_option(&mut self, auth_option: &mut AuthOption) {
        if let Some(password) = &mut auth_option.password {
            self.hide(password, "***".to_string());
        }
    }

    fn enter_create_catalog_stmt(&mut self, stmt: &mut CreateCatalogStmt) {
        self.mask_secret_options(&mut stmt.catalog_options);
    }

    fn enter_notification_webhook_options(&mut self, opts: &mut NotificationWebhookOptions) {
        if let Some(header) = &mut opts.authorization_header {
            let masked = mask_partial(header);
            self.hide(header, masked);
        }
        if let Some(url) = &mut opts.url {
            let masked = mask_url(url, true);
            self.hide(url, masked);
        }
    }

    fn enter_udf_definition(&mut self, definition: &mut UDFDefinition) {
        if let UDFDefinition::UDFServer {
            headers, address, ..
        }
        | UDFDefinition::UDAFServer {
            headers, address, ..
        }
        | UDFDefinition::UDTFServer {
            headers, address, ..
        } = definition
        {
            // Any header may authenticate the request.
            self.mask_all(headers);
            let masked = mask_url(address, false);
            self.hide(address, masked);
        }
    }

    fn enter_create_dictionary_stmt(&mut self, stmt: &mut CreateDictionaryStmt) {
        self.mask_secret_options(&mut stmt.source_options);
    }

    fn enter_connection(&mut self, connection: &mut Connection) {
        self.mask_all(&mut connection.conns);
    }

    fn enter_create_connection_stmt(&mut self, stmt: &mut CreateConnectionStmt) {
        self.mask_all(&mut stmt.storage_params);
    }

    fn enter_alter_table_stmt(&mut self, stmt: &mut AlterTableStmt) {
        match &mut stmt.action {
            AlterTableAction::ModifyConnection { new_connection } => self.mask_all(new_connection),
            AlterTableAction::SetOptions { set_options } => self.mask_secret_options(set_options),
            _ => {}
        }
    }

    // `SELECT ... FROM '<uri>' (CONNECTION => (...))`
    fn enter_select_stage_options(&mut self, options: &mut SelectStageOptions) {
        self.mask_all(&mut options.connection);
    }

    fn enter_create_table_stmt(&mut self, stmt: &mut CreateTableStmt) {
        self.mask_secret_options(&mut stmt.table_options);
        if let Some(properties) = &mut stmt.table_properties {
            self.mask_secret_options(properties);
        }
    }

    fn enter_create_worker_stmt(&mut self, stmt: &mut CreateWorkerStmt) {
        self.mask_secret_options(&mut stmt.options);
    }

    fn enter_alter_worker_action(&mut self, action: &mut AlterWorkerAction) {
        if let AlterWorkerAction::SetOptions { options } = action {
            self.mask_secret_options(options);
        }
    }

    fn enter_execute_immediate_stmt(&mut self, stmt: &mut ExecuteImmediateStmt) {
        self.0.extend(mask_dynamic_sql_expr(&mut stmt.script));
    }

    fn enter_create_procedure_stmt(&mut self, stmt: &mut CreateProcedureStmt) {
        let (script, secrets) = mask_raw_sql_with_secrets(&stmt.script);
        stmt.script = script;
        self.0.extend(secrets);
    }

    fn enter_call_procedure_stmt(&mut self, stmt: &mut CallProcedureStmt) {
        // Arguments may be credentials passed on to dynamic SQL.
        for arg in &mut stmt.args {
            self.0.extend(hide_literals(arg));
        }
    }

    fn enter_task_sql(&mut self, sql: &mut TaskSql) {
        match sql {
            TaskSql::SingleStatement(stmt) => {
                let (masked, secrets) = mask_raw_sql_with_secrets(stmt);
                *stmt = masked;
                self.0.extend(secrets);
            }
            // Statements of a block share variables, see `mask_program_literals`.
            TaskSql::ScriptBlock(stmts) => {
                for stmt in stmts {
                    let (masked, secrets) = mask_program_literals(stmt);
                    *stmt = masked;
                    self.0.extend(secrets);
                }
            }
        }
    }

    // Session parameters, e.g. `enterprise_license`.
    fn enter_create_task_stmt(&mut self, stmt: &mut CreateTaskStmt) {
        self.mask_secret_options(&mut stmt.session_parameters);
    }

    fn enter_alter_task_options(&mut self, options: &mut AlterTaskOptions) {
        if let AlterTaskOptions::Set {
            session_parameters: Some(params),
            ..
        } = options
        {
            self.mask_secret_options(params);
        }
    }

    // Settings and variables, e.g. `SET GLOBAL enterprise_license = '...'`.
    fn enter_settings(&mut self, settings: &mut Settings) {
        if let SetValues::Expr(values) = &mut settings.values {
            for (ident, value) in settings.identifiers.iter().zip(values.iter_mut()) {
                if is_secret_name(&ident.name) {
                    self.0.extend(hide_literals(value.as_mut()));
                }
            }
        }
    }

    fn enter_hint_item(&mut self, item: &mut HintItem) {
        if is_secret_name(&item.name.name) {
            self.0.extend(hide_literals(&mut item.expr));
        }
    }
}

impl Statement {
    pub fn to_mask_sql(&self) -> String {
        self.to_mask_sql_with_secrets().0
    }

    /// [`Self::to_mask_sql`], also returning the hidden values.
    ///
    /// Only values of credential-bearing nodes are masked, wherever they
    /// occur (e.g. a `CONNECTION` of a location in a query). Expressions of
    /// queries and DML are user data and keep their text, even if a column is
    /// named like a credential, e.g. `WHERE token = 'abc'`.
    pub fn to_mask_sql_with_secrets(&self) -> (String, SqlSecrets) {
        let mut detector = MayHoldSecrets::default();
        self.drive(&mut detector);
        if !detector.0 {
            return (self.to_string(), SqlSecrets::default());
        }
        let mut masker = SecretMasker::default();
        let mut stmt = self.clone();
        stmt.drive_mut(&mut masker);
        (stmt.to_string(), masker.0)
    }

    pub fn allowed_in_multi_statement(&self) -> bool {
        match self {
            Statement::Query(..)
            | Statement::Explain { .. }
            | Statement::ReportIssue { .. }
            | Statement::ExplainAnalyze { .. }
            | Statement::CopyIntoTable(..)
            | Statement::CopyIntoLocation(..)
            | Statement::Call(..)
            | Statement::ShowSettings { .. }
            | Statement::ShowProcessList { .. }
            | Statement::ShowMetrics { .. }
            | Statement::ShowEngines { .. }
            | Statement::ShowFunctions { .. }
            | Statement::ShowUserFunctions { .. }
            | Statement::ShowTableFunctions { .. }
            | Statement::ShowIndexes { .. }
            | Statement::ShowLocks(..)
            | Statement::SetPriority { .. }
            | Statement::System(..)
            | Statement::KillStmt { .. }
            | Statement::SetStmt { .. }
            | Statement::UnSetStmt { .. }
            | Statement::ShowVariables { .. }
            | Statement::SetRole { .. }
            | Statement::SetSecondaryRoles { .. }
            | Statement::Insert(..)
            | Statement::InsertMultiTable(..)
            | Statement::Replace(..)
            | Statement::MergeInto(..)
            | Statement::Delete(..)
            | Statement::Update(..)
            | Statement::AlterTable(AlterTableStmt {
                action: AlterTableAction::MaterializeTableTtl,
                ..
            })
            | Statement::ShowCatalogs(..)
            | Statement::ShowCreateCatalog(..)
            | Statement::UseCatalog { .. }
            | Statement::ShowDatabases(..)
            | Statement::ShowDropDatabases(..)
            | Statement::ShowCreateDatabase(..)
            | Statement::ShowShares(..)
            | Statement::DescShare(..)
            | Statement::UseDatabase { .. }
            | Statement::ShowCreateMaterializedView(..)
            | Statement::ShowTables(..)
            | Statement::ShowCreateTable(..)
            | Statement::DescribeTable(..)
            | Statement::ShowStatistics(..)
            | Statement::ShowTablesStatus(..)
            | Statement::ShowDropTables(..)
            | Statement::OptimizeTable(..)
            | Statement::VacuumTable(..)
            | Statement::VacuumTables(..)
            | Statement::VacuumAll(..)
            | Statement::VacuumDropTable(..)
            | Statement::VacuumTemporaryFiles(..)
            | Statement::VacuumVirtualColumn(..)
            | Statement::AnalyzeTable(..)
            | Statement::ExistsTable(..)
            | Statement::ShowCreateDictionary(..)
            | Statement::ShowDictionaries(..)
            | Statement::ShowColumns(..)
            | Statement::ShowViews(..)
            | Statement::DescribeView(..)
            | Statement::RefreshLineage(..)
            | Statement::ShowMaterializedViews(..)
            | Statement::ShowStreams(..)
            | Statement::DescribeStream(..)
            | Statement::RefreshTableIndex(..)
            | Statement::RefreshVirtualColumn(..)
            | Statement::ShowVirtualColumns(..)
            | Statement::ShowUsers { .. }
            | Statement::DescribeUser { .. }
            | Statement::ShowPublicKeys { .. }
            | Statement::ShowRoles { .. }
            | Statement::ShowGrants { .. }
            | Statement::ShowObjectPrivileges(..)
            | Statement::ShowGrantsOfRole(..)
            | Statement::ShowTags(..)
            | Statement::ShowStages { .. }
            | Statement::DescribeStage { .. }
            | Statement::RemoveStage { .. }
            | Statement::ListStage { .. }
            | Statement::DescribeConnection(..)
            | Statement::ShowConnections(..)
            | Statement::ShowFileFormats
            | Statement::Presign(..)
            | Statement::DescDatamaskPolicy(..)
            | Statement::DescRowAccessPolicy(..)
            | Statement::DescNetworkPolicy(..)
            | Statement::ShowNetworkPolicies
            | Statement::DescPasswordPolicy(..)
            | Statement::ShowPasswordPolicies { .. }
            | Statement::ExecuteTask(..)
            | Statement::DescribeTask(..)
            | Statement::ShowTasks(..)
            | Statement::DescribePipe(..)
            | Statement::Begin
            | Statement::Commit
            | Statement::Abort
            | Statement::DescribeNotification(..)
            | Statement::ExecuteImmediate(..)
            | Statement::ShowProcedures { .. }
            | Statement::ShowSequences { .. }
            | Statement::DescSequence { .. }
            | Statement::DescProcedure(..)
            | Statement::CallProcedure(..)
            | Statement::ShowWarehouses(..)
            | Statement::ShowWorkers(..)
            | Statement::ShowOnlineNodes(..)
            | Statement::InspectWarehouse(..) => true,

            Statement::CreateDatabase(..)
            | Statement::CreateShare(..)
            | Statement::DropShare(..)
            | Statement::AlterShare(..)
            | Statement::GrantShare(..)
            | Statement::RevokeShare(..)
            | Statement::CreateTable(..)
            | Statement::CreateView(..)
            | Statement::CreateStage(..)
            | Statement::AlterStage(..)
            | Statement::CreateSequence(..)
            | Statement::CreateDictionary(..)
            | Statement::ExternalAccess(..)
            | Statement::CreateConnection(..)
            | Statement::CreatePipe(..)
            | Statement::AlterTable(..)
            | Statement::AlterObjectTag(..)
            | Statement::AlterView(..)
            | Statement::AlterMaterializedView(..)
            | Statement::AlterUser(..)
            | Statement::AlterDatabase(..)
            | Statement::DropDatabase(..)
            | Statement::DropTable(..)
            | Statement::DropView(..)
            | Statement::DropMaterializedView(..)
            | Statement::CreateMaterializedView(..)
            | Statement::RefreshMaterializedView(..)
            | Statement::DropSequence(..)
            | Statement::DropDictionary(..)
            | Statement::TruncateTable(..)
            | Statement::AttachTable(..)
            | Statement::RenameTable(..)
            | Statement::CreateCatalog(..)
            | Statement::DropCatalog(..)
            | Statement::UndropDatabase(..)
            | Statement::UndropTable(..)
            | Statement::RenameDictionary(..)
            | Statement::CreateStream(..)
            | Statement::DropStream(..)
            | Statement::CreateTableIndex(..)
            | Statement::DropTableIndex(..)
            | Statement::CreateUser(..)
            | Statement::DropUser { .. }
            | Statement::CreateRole { .. }
            | Statement::DropRole { .. }
            | Statement::Grant(..)
            | Statement::Revoke(..)
            | Statement::CreateUDF(..)
            | Statement::DropUDF { .. }
            | Statement::AlterUDF(..)
            | Statement::CreateRowAccessPolicy(..)
            | Statement::DropRowAccessPolicy(..)
            | Statement::CreateTag(..)
            | Statement::DropTag(..)
            | Statement::DropStage { .. }
            | Statement::DropConnection(..)
            | Statement::CreateFileFormat { .. }
            | Statement::DropFileFormat { .. }
            | Statement::CreateDatamaskPolicy(..)
            | Statement::DropDatamaskPolicy(..)
            | Statement::CreateNetworkPolicy(..)
            | Statement::AlterNetworkPolicy(..)
            | Statement::DropNetworkPolicy(..)
            | Statement::CreatePasswordPolicy(..)
            | Statement::AlterPasswordPolicy(..)
            | Statement::DropPasswordPolicy(..)
            | Statement::CreateTask(..)
            | Statement::AlterTask(..)
            | Statement::DropTask(..)
            | Statement::CreateDynamicTable(..)
            | Statement::RefreshDynamicTable(..)
            | Statement::DropPipe(..)
            | Statement::AlterPipe(..)
            | Statement::CreateNotification(..)
            | Statement::AlterNotification(..)
            | Statement::DropNotification(..)
            | Statement::CreateProcedure(..)
            | Statement::DropProcedure(..)
            | Statement::CreateWarehouse(..)
            | Statement::UseWarehouse(..)
            | Statement::DropWarehouse(..)
            | Statement::RenameWarehouse(..)
            | Statement::AddWarehouseCluster(..)
            | Statement::DropWarehouseCluster(..)
            | Statement::RenameWarehouseCluster(..)
            | Statement::AssignWarehouseNodes(..)
            | Statement::UnassignWarehouseNodes(..)
            | Statement::ResumeWarehouse(..)
            | Statement::SuspendWarehouse(..)
            | Statement::CreateWorker(..)
            | Statement::AlterWorker(..)
            | Statement::DropWorker(..)
            | Statement::ShowWorkloadGroups(..)
            | Statement::CreateWorkloadGroup(..)
            | Statement::DropWorkloadGroup(..)
            | Statement::RenameWorkloadGroup(..)
            | Statement::SetWorkloadQuotasGroup(..)
            | Statement::UnsetWorkloadQuotasGroup(..) => false,
            Statement::AlterRole(..) => false,
            Statement::StatementWithSettings { stmt, settings: _ } => {
                stmt.allowed_in_multi_statement()
            }
        }
    }

    pub fn is_transaction_command(&self) -> bool {
        matches!(
            self,
            Statement::Commit | Statement::Abort | Statement::Begin
        )
    }
}

impl Display for Statement {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Statement::Explain {
                options: (_, options),
                kind,
                query,
            } => {
                write!(f, "EXPLAIN")?;
                if !options.is_empty() {
                    write!(
                        f,
                        "({})",
                        options
                            .iter()
                            .map(|opt| {
                                match opt {
                                    ExplainOption::Verbose => "VERBOSE",
                                    ExplainOption::Logical => "LOGICAL",
                                    ExplainOption::Optimized => "OPTIMIZED",
                                    ExplainOption::Decorrelated => "DECORRELATED",
                                }
                            })
                            .join(", ")
                    )?;
                }
                match kind {
                    ExplainKind::Ast(_) => write!(f, " AST")?,
                    ExplainKind::Syntax(_) => write!(f, " SYNTAX")?,
                    ExplainKind::Graph => write!(f, " GRAPH")?,
                    ExplainKind::Pipeline => write!(f, " PIPELINE")?,
                    ExplainKind::Fragments => write!(f, " FRAGMENTS")?,
                    ExplainKind::Raw => write!(f, " RAW")?,
                    ExplainKind::Optimized => write!(f, " Optimized")?,
                    ExplainKind::Decorrelated => write!(f, " DECORRELATED")?,
                    ExplainKind::Plan => (),
                    ExplainKind::AnalyzePlan => write!(f, " ANALYZE")?,
                    ExplainKind::Join => write!(f, " JOIN")?,
                    ExplainKind::Memo(_) => write!(f, " MEMO")?,
                    ExplainKind::Graphical => write!(f, " GRAPHICAL")?,
                    ExplainKind::Perf { event_groups } => {
                        write!(f, " PERF")?;
                        if !event_groups.is_empty() {
                            let groups_str: Vec<String> =
                                event_groups.iter().map(|g| g.join("+")).collect();
                            write!(f, "(events='{}')", groups_str.join(","))?;
                        }
                    }
                }
                write!(f, " {query}")?;
            }
            Statement::ReportIssue(sql) => {
                write!(f, "REPORT ISSUE {}", sql)?;
            }
            Statement::StatementWithSettings { settings, stmt } => {
                if let Some(setting) = settings {
                    write!(f, "SETTINGS (")?;
                    let ids = &setting.identifiers;
                    if let SetValues::Expr(values) = &setting.values {
                        let mut expr = Vec::with_capacity(ids.len());
                        for (id, value) in ids.iter().zip(values.iter()) {
                            expr.push(format!("{} = {}", id, value));
                        }
                        write_comma_separated_list(f, expr)?;
                    } else {
                        unreachable!();
                    }
                    write!(f, ") ")?;
                } else {
                    write!(f, "SETTINGS ")?;
                }
                write!(f, "{stmt}")?;
            }
            Statement::ExplainAnalyze {
                partial,
                graphical,
                query,
            } => {
                if *partial {
                    write!(f, "EXPLAIN ANALYZE PARTIAL {query}")?;
                } else if *graphical {
                    write!(f, "EXPLAIN ANALYZE GRAPHICAL {query}")?;
                } else {
                    write!(f, "EXPLAIN ANALYZE {query}")?;
                }
            }
            Statement::Query(stmt) => write!(f, "{stmt}")?,
            Statement::Insert(stmt) => write!(f, "{stmt}")?,
            Statement::InsertMultiTable(insert_multi_table) => write!(f, "{insert_multi_table}")?,
            Statement::Replace(stmt) => write!(f, "{stmt}")?,
            Statement::MergeInto(stmt) => write!(f, "{stmt}")?,
            Statement::Delete(stmt) => write!(f, "{stmt}")?,
            Statement::Update(stmt) => write!(f, "{stmt}")?,
            Statement::CopyIntoTable(stmt) => write!(f, "{stmt}")?,
            Statement::CopyIntoLocation(stmt) => write!(f, "{stmt}")?,
            Statement::ShowSettings { show_options } => {
                write!(f, "SHOW SETTINGS")?;
                if let Some(show_options) = show_options {
                    write!(f, " {show_options}")?;
                }
            }
            Statement::ShowVariables { show_options } => {
                write!(f, "SHOW VARIABLES")?;
                if let Some(show_options) = show_options {
                    write!(f, " {show_options}")?;
                }
            }
            Statement::ShowProcessList { show_options } => {
                write!(f, "SHOW PROCESSLIST")?;
                if let Some(show_options) = show_options {
                    write!(f, " {show_options}")?;
                }
            }
            Statement::ShowMetrics { show_options } => {
                write!(f, "SHOW METRICS")?;
                if let Some(show_options) = show_options {
                    write!(f, " {show_options}")?;
                }
            }
            Statement::ShowEngines { show_options } => {
                write!(f, "SHOW ENGINES")?;
                if let Some(show_options) = show_options {
                    write!(f, " {show_options}")?;
                }
            }
            Statement::ShowIndexes { show_options } => {
                write!(f, "SHOW INDEXES")?;
                if let Some(show_options) = show_options {
                    write!(f, " {show_options}")?;
                }
            }
            Statement::ShowFunctions { show_options } => {
                write!(f, "SHOW FUNCTIONS")?;
                if let Some(show_options) = show_options {
                    write!(f, " {show_options}")?;
                }
            }
            Statement::ShowUserFunctions { show_options } => {
                write!(f, "SHOW USER FUNCTIONS")?;
                if let Some(show_options) = show_options {
                    write!(f, " {show_options}")?;
                }
            }
            Statement::ShowTableFunctions { show_options } => {
                write!(f, "SHOW TABLE_FUNCTIONS")?;
                if let Some(show_options) = show_options {
                    write!(f, " {show_options}")?;
                }
            }
            Statement::ShowLocks(stmt) => write!(f, "{stmt}")?,
            Statement::KillStmt {
                kill_target,
                object_id,
            } => {
                write!(f, "KILL")?;
                match *kill_target {
                    KillTarget::Query => write!(f, " QUERY")?,
                    KillTarget::Connection => write!(f, " CONNECTION")?,
                }
                write!(f, " '{object_id}'")?;
            }
            Statement::SetStmt { settings } => write!(f, "SET {}", settings)?,
            Statement::UnSetStmt { settings } => write!(f, "UNSET {}", settings)?,
            Statement::SetRole {
                is_default,
                role_name,
            } => {
                write!(f, "SET ")?;
                if *is_default {
                    write!(f, "DEFAULT ")?;
                }
                write!(f, "ROLE '{role_name}'")?;
            }
            Statement::SetSecondaryRoles { option } => {
                write!(f, "SET SECONDARY ROLES ")?;
                match option {
                    SecondaryRolesOption::None => write!(f, "NONE")?,
                    SecondaryRolesOption::All => write!(f, "ALL")?,
                    SecondaryRolesOption::SpecifyRole(roles) => {
                        write_comma_separated_list(f, roles)?
                    }
                }
            }
            Statement::ShowCatalogs(stmt) => write!(f, "{stmt}")?,
            Statement::ShowCreateCatalog(stmt) => write!(f, "{stmt}")?,
            Statement::CreateCatalog(stmt) => write!(f, "{stmt}")?,
            Statement::DropCatalog(stmt) => write!(f, "{stmt}")?,
            Statement::UseCatalog { catalog } => write!(f, "USE CATALOG {catalog}")?,
            Statement::ShowDatabases(stmt) => write!(f, "{stmt}")?,
            Statement::ShowDropDatabases(stmt) => write!(f, "{stmt}")?,
            Statement::ShowCreateDatabase(stmt) => write!(f, "{stmt}")?,
            Statement::CreateDatabase(stmt) => write!(f, "{stmt}")?,
            Statement::DropDatabase(stmt) => write!(f, "{stmt}")?,
            Statement::UndropDatabase(stmt) => write!(f, "{stmt}")?,
            Statement::AlterDatabase(stmt) => write!(f, "{stmt}")?,
            Statement::UseDatabase { database } => write!(f, "USE {database}")?,
            Statement::CreateShare(stmt) => write!(f, "{stmt}")?,
            Statement::DropShare(stmt) => write!(f, "{stmt}")?,
            Statement::AlterShare(stmt) => write!(f, "{stmt}")?,
            Statement::GrantShare(stmt) => write!(f, "{stmt}")?,
            Statement::RevokeShare(stmt) => write!(f, "{stmt}")?,
            Statement::ShowShares(stmt) => write!(f, "{stmt}")?,
            Statement::DescShare(stmt) => write!(f, "{stmt}")?,
            Statement::ShowTables(stmt) => write!(f, "{stmt}")?,
            Statement::ShowColumns(stmt) => write!(f, "{stmt}")?,
            Statement::ShowCreateTable(stmt) => write!(f, "{stmt}")?,
            Statement::DescribeTable(stmt) => write!(f, "{stmt}")?,
            Statement::ShowTablesStatus(stmt) => write!(f, "{stmt}")?,
            Statement::ShowDropTables(stmt) => write!(f, "{stmt}")?,
            Statement::ShowStatistics(stmt) => write!(f, "{stmt}")?,
            Statement::AttachTable(stmt) => write!(f, "{stmt}")?,
            Statement::CreateTable(stmt) => write!(f, "{stmt}")?,
            Statement::DropTable(stmt) => write!(f, "{stmt}")?,
            Statement::UndropTable(stmt) => write!(f, "{stmt}")?,
            Statement::AlterTable(stmt) => write!(f, "{stmt}")?,
            Statement::RenameTable(stmt) => write!(f, "{stmt}")?,
            Statement::TruncateTable(stmt) => write!(f, "{stmt}")?,
            Statement::OptimizeTable(stmt) => write!(f, "{stmt}")?,
            Statement::VacuumTable(stmt) => write!(f, "{stmt}")?,
            Statement::VacuumTables(stmt) => write!(f, "{stmt}")?,
            Statement::VacuumAll(stmt) => write!(f, "{stmt}")?,
            Statement::VacuumDropTable(stmt) => write!(f, "{stmt}")?,
            Statement::VacuumTemporaryFiles(stmt) => write!(f, "{stmt}")?,
            Statement::VacuumVirtualColumn(stmt) => write!(f, "{stmt}")?,
            Statement::AnalyzeTable(stmt) => write!(f, "{stmt}")?,
            Statement::ExistsTable(stmt) => write!(f, "{stmt}")?,
            Statement::CreateDictionary(stmt) => write!(f, "{stmt}")?,
            Statement::DropDictionary(stmt) => write!(f, "{stmt}")?,
            Statement::ShowCreateDictionary(stmt) => write!(f, "{stmt}")?,
            Statement::ShowDictionaries(stmt) => write!(f, "{stmt}")?,
            Statement::RenameDictionary(stmt) => write!(f, "{stmt}")?,
            Statement::CreateView(stmt) => write!(f, "{stmt}")?,
            Statement::AlterView(stmt) => write!(f, "{stmt}")?,
            Statement::DropView(stmt) => write!(f, "{stmt}")?,
            Statement::ShowViews(stmt) => write!(f, "{stmt}")?,
            Statement::DescribeView(stmt) => write!(f, "{stmt}")?,
            Statement::RefreshLineage(stmt) => write!(f, "{stmt}")?,
            Statement::CreateMaterializedView(stmt) => write!(f, "{stmt}")?,
            Statement::AlterMaterializedView(stmt) => write!(f, "{stmt}")?,
            Statement::DropMaterializedView(stmt) => write!(f, "{stmt}")?,
            Statement::RefreshMaterializedView(stmt) => write!(f, "{stmt}")?,
            Statement::ShowCreateMaterializedView(stmt) => write!(f, "{stmt}")?,
            Statement::ShowMaterializedViews(stmt) => write!(f, "{stmt}")?,
            Statement::CreateStream(stmt) => write!(f, "{stmt}")?,
            Statement::DropStream(stmt) => write!(f, "{stmt}")?,
            Statement::ShowStreams(stmt) => write!(f, "{stmt}")?,
            Statement::DescribeStream(stmt) => write!(f, "{stmt}")?,
            Statement::CreateTableIndex(stmt) => write!(f, "{stmt}")?,
            Statement::DropTableIndex(stmt) => write!(f, "{stmt}")?,
            Statement::RefreshTableIndex(stmt) => write!(f, "{stmt}")?,
            Statement::RefreshVirtualColumn(stmt) => write!(f, "{stmt}")?,
            Statement::ShowVirtualColumns(stmt) => write!(f, "{stmt}")?,
            Statement::ShowUsers { show_options } => {
                write!(f, "SHOW USERS")?;
                if let Some(show_options) = show_options {
                    write!(f, " {show_options}")?;
                }
            }
            Statement::DescribeUser { user } => write!(f, "DESCRIBE USER {user}")?,
            Statement::ShowPublicKeys { user } => write!(f, "SHOW PUBLIC KEYS FOR USER {user}")?,
            Statement::ShowRoles { show_options } => {
                write!(f, "SHOW ROLES")?;
                if let Some(show_options) = show_options {
                    write!(f, " {show_options}")?;
                }
            }
            Statement::CreateUser(stmt) => write!(f, "{stmt}")?,
            Statement::AlterUser(stmt) => write!(f, "{stmt}")?,
            Statement::DropUser { if_exists, user } => {
                write!(f, "DROP USER")?;
                if *if_exists {
                    write!(f, " IF EXISTS")?;
                }
                write!(f, " {}", user)?;
            }
            Statement::CreateRole {
                create_option,
                role_name: role,
                comment,
            } => {
                write!(f, "CREATE")?;
                if let CreateOption::CreateOrReplace = create_option {
                    write!(f, " OR REPLACE")?;
                }
                write!(f, " ROLE")?;
                if let CreateOption::CreateIfNotExists = create_option {
                    write!(f, " IF NOT EXISTS")?;
                }
                write!(f, " {}", QuotedString(role, '\''))?;
                if let Some(comment) = comment {
                    write!(f, " COMMENT = '{comment}'")?;
                }
            }
            Statement::DropRole {
                if_exists,
                role_name: role,
            } => {
                write!(f, "DROP ROLE")?;
                if *if_exists {
                    write!(f, " IF EXISTS")?;
                }
                write!(f, " '{role}'")?;
            }
            Statement::Grant(stmt) => write!(f, "{stmt}")?,
            Statement::ShowGrants {
                principal,
                show_options,
            } => {
                write!(f, "SHOW GRANTS")?;
                if let Some(principal) = principal {
                    write!(f, " FOR")?;
                    write!(f, "{principal}")?;
                }
                if let Some(show_options) = show_options {
                    write!(f, " {show_options}")?;
                }
            }
            Statement::ShowObjectPrivileges(stmt) => write!(f, "{stmt}")?,
            Statement::ShowGrantsOfRole(stmt) => write!(f, "{stmt}")?,
            Statement::Revoke(stmt) => write!(f, "{stmt}")?,
            Statement::CreateUDF(stmt) => write!(f, "{stmt}")?,
            Statement::DropUDF {
                if_exists,
                udf_name,
            } => {
                write!(f, "DROP FUNCTION")?;
                if *if_exists {
                    write!(f, " IF EXISTS")?;
                }
                write!(f, " {udf_name}")?;
            }
            Statement::AlterUDF(stmt) => write!(f, "{stmt}")?,
            Statement::CreateRowAccessPolicy(stmt) => write!(f, "{stmt}")?,
            Statement::DescRowAccessPolicy(stmt) => write!(f, "{stmt}")?,
            Statement::DropRowAccessPolicy(stmt) => write!(f, "{stmt}")?,
            Statement::CreateTag(stmt) => write!(f, "{stmt}")?,
            Statement::DropTag(stmt) => write!(f, "{stmt}")?,
            Statement::ShowTags(stmt) => write!(f, "{stmt}")?,
            Statement::AlterObjectTag(stmt) => write!(f, "{stmt}")?,
            Statement::ListStage { location, pattern } => {
                write!(f, "LIST @{location}")?;
                if let Some(pattern) = pattern {
                    write!(f, " PATTERN = {}", QuotedString(pattern, '\''))?;
                }
            }
            Statement::ShowStages { show_options } => {
                write!(f, "SHOW STAGES")?;
                if let Some(show_options) = show_options {
                    write!(f, " {show_options}")?;
                }
            }
            Statement::DropStage {
                if_exists,
                stage_name,
            } => {
                write!(f, "DROP STAGE")?;
                if *if_exists {
                    write!(f, " IF EXISTS")?;
                }
                write!(f, " {stage_name}")?;
            }
            Statement::CreateStage(stmt) => write!(f, "{stmt}")?,
            Statement::AlterStage(stmt) => write!(f, "{stmt}")?,
            Statement::RemoveStage { location, pattern } => {
                write!(f, "REMOVE @{location}")?;
                if !pattern.is_empty() {
                    write!(f, " PATTERN = {}", QuotedString(pattern, '\''))?;
                }
            }
            Statement::DescribeStage { stage_name } => write!(f, "DESC STAGE {stage_name}")?,
            Statement::CreateFileFormat {
                create_option,
                name,
                file_format_options,
            } => {
                write!(f, "CREATE")?;
                if let CreateOption::CreateOrReplace = create_option {
                    write!(f, " OR REPLACE")?;
                }
                write!(f, " FILE FORMAT")?;
                if let CreateOption::CreateIfNotExists = create_option {
                    write!(f, " IF NOT EXISTS")?;
                }
                write!(f, " {name}")?;
                write!(f, " {file_format_options}")?;
            }
            Statement::DropFileFormat { if_exists, name } => {
                write!(f, "DROP FILE FORMAT")?;
                if *if_exists {
                    write!(f, " IF EXISTS")?;
                }
                write!(f, " {name}")?;
            }
            Statement::ShowFileFormats => write!(f, "SHOW FILE FORMATS")?,
            Statement::Call(stmt) => write!(f, "{stmt}")?,
            Statement::Presign(stmt) => write!(f, "{stmt}")?,
            Statement::CreateDatamaskPolicy(stmt) => write!(f, "{stmt}")?,
            Statement::DropDatamaskPolicy(stmt) => write!(f, "{stmt}")?,
            Statement::DescDatamaskPolicy(stmt) => write!(f, "{stmt}")?,
            Statement::CreateNetworkPolicy(stmt) => write!(f, "{stmt}")?,
            Statement::AlterNetworkPolicy(stmt) => write!(f, "{stmt}")?,
            Statement::DropNetworkPolicy(stmt) => write!(f, "{stmt}")?,
            Statement::DescNetworkPolicy(stmt) => write!(f, "{stmt}")?,
            Statement::ShowNetworkPolicies => write!(f, "SHOW NETWORK POLICIES")?,
            Statement::CreatePasswordPolicy(stmt) => write!(f, "{stmt}")?,
            Statement::AlterPasswordPolicy(stmt) => write!(f, "{stmt}")?,
            Statement::DropPasswordPolicy(stmt) => write!(f, "{stmt}")?,
            Statement::DescPasswordPolicy(stmt) => write!(f, "{stmt}")?,
            Statement::ShowPasswordPolicies { show_options } => {
                write!(f, "SHOW PASSWORD POLICIES")?;
                if let Some(show_options) = show_options {
                    write!(f, " {show_options}")?;
                }
            }
            Statement::CreateTask(stmt) => write!(f, "{stmt}")?,
            Statement::AlterTask(stmt) => write!(f, "{stmt}")?,
            Statement::ExecuteTask(stmt) => write!(f, "{stmt}")?,
            Statement::DropTask(stmt) => write!(f, "{stmt}")?,
            Statement::ShowTasks(stmt) => write!(f, "{stmt}")?,
            Statement::DescribeTask(stmt) => write!(f, "{stmt}")?,
            Statement::CreatePipe(stmt) => write!(f, "{stmt}")?,
            Statement::DescribePipe(stmt) => write!(f, "{stmt}")?,
            Statement::DropPipe(stmt) => write!(f, "{stmt}")?,
            Statement::AlterPipe(stmt) => write!(f, "{stmt}")?,
            Statement::ExternalAccess(stmt) => write!(f, "{stmt}")?,
            Statement::CreateConnection(stmt) => write!(f, "{stmt}")?,
            Statement::DropConnection(stmt) => write!(f, "{stmt}")?,
            Statement::DescribeConnection(stmt) => write!(f, "{stmt}")?,
            Statement::ShowConnections(stmt) => write!(f, "{stmt}")?,
            Statement::Begin => write!(f, "BEGIN")?,
            Statement::Commit => write!(f, "COMMIT")?,
            Statement::Abort => write!(f, "ABORT")?,
            Statement::CreateNotification(stmt) => write!(f, "{stmt}")?,
            Statement::AlterNotification(stmt) => write!(f, "{stmt}")?,
            Statement::DropNotification(stmt) => write!(f, "{stmt}")?,
            Statement::DescribeNotification(stmt) => write!(f, "{stmt}")?,
            Statement::ExecuteImmediate(stmt) => write!(f, "{stmt}")?,
            Statement::CreateProcedure(stmt) => write!(f, "{stmt}")?,
            Statement::DropProcedure(stmt) => write!(f, "{stmt}")?,
            Statement::DescProcedure(stmt) => write!(f, "{stmt}")?,
            Statement::ShowProcedures { show_options } => {
                write!(f, "SHOW PROCEDURES")?;
                if let Some(show_options) = show_options {
                    write!(f, " {show_options}")?;
                }
            }
            Statement::CreateSequence(stmt) => write!(f, "{stmt}")?,
            Statement::DropSequence(stmt) => write!(f, "{stmt}")?,
            Statement::ShowSequences { show_options } => {
                write!(f, "SHOW SEQUENCES")?;
                if let Some(show_options) = show_options {
                    write!(f, " {show_options}")?;
                }
            }
            Statement::DescSequence { name } => {
                write!(f, "DESC SEQUENCE {name}")?;
            }
            Statement::CreateDynamicTable(stmt) => write!(f, "{stmt}")?,
            Statement::RefreshDynamicTable(stmt) => write!(f, "{stmt}")?,
            Statement::SetPriority {
                priority,
                object_id,
            } => {
                write!(f, "SET PRIORITY")?;
                write!(f, " {priority}")?;
                write!(f, " '{object_id}'")?;
            }
            Statement::System(stmt) => write!(f, "{stmt}")?,
            Statement::CallProcedure(stmt) => write!(f, "{stmt}")?,

            Statement::ShowOnlineNodes(stmt) => write!(f, "{stmt}")?,
            Statement::ShowWarehouses(stmt) => write!(f, "{stmt}")?,
            Statement::UseWarehouse(stmt) => write!(f, "{stmt}")?,
            Statement::DropWarehouse(stmt) => write!(f, "{stmt}")?,
            Statement::CreateWarehouse(stmt) => write!(f, "{stmt}")?,
            Statement::RenameWarehouse(stmt) => write!(f, "{stmt}")?,
            Statement::ResumeWarehouse(stmt) => write!(f, "{stmt}")?,
            Statement::SuspendWarehouse(stmt) => write!(f, "{stmt}")?,
            Statement::InspectWarehouse(stmt) => write!(f, "{stmt}")?,
            Statement::AddWarehouseCluster(stmt) => write!(f, "{stmt}")?,
            Statement::DropWarehouseCluster(stmt) => write!(f, "{stmt}")?,
            Statement::RenameWarehouseCluster(stmt) => write!(f, "{stmt}")?,
            Statement::AssignWarehouseNodes(stmt) => write!(f, "{stmt}")?,
            Statement::UnassignWarehouseNodes(stmt) => write!(f, "{stmt}")?,
            Statement::ShowWorkers(stmt) => write!(f, "{stmt}")?,
            Statement::CreateWorker(stmt) => write!(f, "{stmt}")?,
            Statement::AlterWorker(stmt) => write!(f, "{stmt}")?,
            Statement::DropWorker(stmt) => write!(f, "{stmt}")?,
            Statement::ShowWorkloadGroups(stmt) => write!(f, "{stmt}")?,
            Statement::CreateWorkloadGroup(stmt) => write!(f, "{stmt}")?,
            Statement::DropWorkloadGroup(stmt) => write!(f, "{stmt}")?,
            Statement::RenameWorkloadGroup(stmt) => write!(f, "{stmt}")?,
            Statement::SetWorkloadQuotasGroup(stmt) => write!(f, "{stmt}")?,
            Statement::UnsetWorkloadQuotasGroup(stmt) => write!(f, "{stmt}")?,
            Statement::AlterRole(stmt) => write!(f, "{stmt}")?,
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct StatementWithFormat {
    pub(crate) stmt: Statement,
    pub(crate) format: Option<String>,
}

impl Display for StatementWithFormat {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}", self.stmt)?;
        if let Some(format) = &self.format {
            write!(f, " FORMAT {}", format)?;
        }
        Ok(())
    }
}
