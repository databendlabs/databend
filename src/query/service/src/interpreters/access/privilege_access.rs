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

use std::collections::HashSet;
use std::sync::Arc;

use databend_common_catalog::plan::DataSourceInfo;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::GrantObjectByID;
use databend_common_meta_app::principal::StageInfo;
use databend_common_meta_app::principal::StageType;
use databend_common_meta_app::principal::UserGrantSet;
use databend_common_meta_app::principal::UserPrivilegeType;
use databend_common_sql::optimizer::get_udf_names;
use databend_common_sql::plans::InsertInputSource;
use databend_common_sql::plans::PresignAction;
use databend_common_sql::plans::RewriteKind;
use databend_common_users::RoleCacheManager;

use crate::interpreters::access::AccessChecker;
use crate::sessions::QueryContext;
use crate::sql::plans::Plan;

pub struct PrivilegeAccess {
    ctx: Arc<QueryContext>,
}

impl PrivilegeAccess {
    pub fn create(ctx: Arc<QueryContext>) -> Box<dyn AccessChecker> {
        Box::new(PrivilegeAccess { ctx })
    }

    // PrivilegeAccess checks the privilege by names, we'd need to convert the GrantObject to
    // GrantObjectByID to check the privilege.
    // Currently we only checks ownerships by id, and other privileges by database/table names.
    // This will change, all the privileges will be checked by id.
    async fn convert_grant_object_by_id(
        &self,
        object: &GrantObject,
    ) -> Result<Option<GrantObjectByID>> {
        let tenant = self.ctx.get_tenant();

        let object = match object {
            GrantObject::Database(catalog_name, db_name) => {
                if db_name.to_lowercase() == "system" {
                    return Ok(None);
                }
                let db_id = self
                    .ctx
                    .get_catalog(catalog_name)
                    .await?
                    .get_database(&tenant, db_name)
                    .await?
                    .get_db_info()
                    .ident
                    .db_id;
                GrantObjectByID::Database {
                    catalog_name: catalog_name.clone(),
                    db_id,
                }
            }
            GrantObject::Table(catalog_name, db_name, table_name) => {
                if db_name.to_lowercase() == "system" {
                    return Ok(None);
                }
                let catalog = self.ctx.get_catalog(catalog_name).await?;
                let db_id = catalog
                    .get_database(&tenant, db_name)
                    .await?
                    .get_db_info()
                    .ident
                    .db_id;
                let table = catalog.get_table(&tenant, db_name, table_name).await?;
                let table_id = table.get_id();
                GrantObjectByID::Table {
                    catalog_name: catalog_name.clone(),
                    db_id,
                    table_id,
                }
            }
            _ => return Ok(None),
        };

        Ok(Some(object))
    }

    async fn validate_access(
        &self,
        object: &GrantObject,
        privileges: Vec<UserPrivilegeType>,
        verify_ownership: bool,
    ) -> Result<()> {
        let session = self.ctx.get_current_session();
        if verify_ownership {
            let object_by_id =
                self.convert_grant_object_by_id(object)
                    .await
                    .or_else(|e| match e.code() {
                        ErrorCode::UNKNOWN_DATABASE
                        | ErrorCode::UNKNOWN_TABLE
                        | ErrorCode::UNKNOWN_CATALOG => Ok(None),
                        _ => Err(e.add_message("error on validating access")),
                    })?;
            if let Some(object_by_id) = &object_by_id {
                let result = session.validate_ownership(object_by_id).await;
                if result.is_ok() {
                    return Ok(());
                }
            }
        }

        session.validate_privilege(object, privileges).await
    }

    async fn validate_access_stage(
        &self,
        stage_info: &StageInfo,
        privilege: UserPrivilegeType,
    ) -> Result<()> {
        // this settings might be enabled as default after we got a better confidence on it
        if !self
            .ctx
            .get_settings()
            .get_enable_experimental_rbac_check()?
        {
            return Ok(());
        }

        // skip check the temp stage from uri like `COPY INTO tbl FROM 'http://xxx'`
        if stage_info.is_temporary {
            return Ok(());
        }

        // every user can presign his own user stage like: `PRESIGN @~/tmp.txt`
        if stage_info.stage_type == StageType::User
            && stage_info.stage_name == self.ctx.get_current_user()?.name
        {
            return Ok(());
        }

        self.validate_access(
            &GrantObject::Stage(stage_info.stage_name.to_string()),
            vec![privilege],
            true,
        )
        .await
    }

    async fn check_udf_priv(&self, udf_names: HashSet<&String>) -> Result<()> {
        for udf in udf_names {
            self.validate_access(
                &GrantObject::UDF(udf.clone()),
                vec![UserPrivilegeType::Usage],
                false,
            )
            .await?;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl AccessChecker for PrivilegeAccess {
    #[async_backtrace::framed]
    async fn check(&self, ctx: &Arc<QueryContext>, plan: &Plan) -> Result<()> {
        let user = self.ctx.get_current_user()?;
        let (identity, grant_set) = (user.identity().to_string(), user.grants);
        let tenant = self.ctx.get_tenant();
        let enable_experimental_rbac_check = self
            .ctx
            .get_settings()
            .get_enable_experimental_rbac_check()?;

        match plan {
            Plan::Query {
                metadata,
                rewrite_kind,
                s_expr,
                ..
            } => {
                match rewrite_kind {
                    Some(RewriteKind::ShowDatabases)
                    | Some(RewriteKind::ShowEngines)
                    | Some(RewriteKind::ShowFunctions)
                    | Some(RewriteKind::ShowTableFunctions) => {
                        return Ok(());
                    }
                    Some(RewriteKind::ShowTables(database)) => {
                        let has_priv = has_priv(&tenant, database, None, grant_set).await?;
                        return if has_priv {
                            Ok(())
                        } else {
                            Err(ErrorCode::PermissionDenied(format!(
                                "Permission denied, user {} don't have privilege for database {}",
                                identity, database
                            )))
                        };
                    }
                    Some(RewriteKind::ShowStreams(database)) => {
                        let has_priv = has_priv(&tenant, database, None, grant_set).await?;
                        return if has_priv {
                            Ok(())
                        } else {
                            Err(ErrorCode::PermissionDenied(format!(
                                "Permission denied, user {} don't have privilege for database {}",
                                identity, database
                            )))
                        };
                    }
                    Some(RewriteKind::ShowColumns(database, table)) => {
                        let has_priv = has_priv(&tenant, database, Some(table), grant_set).await?;
                        return if has_priv {
                            Ok(())
                        } else {
                            Err(ErrorCode::PermissionDenied(format!(
                                "Permission denied, user {} don't have privilege for table {}.{}",
                                identity, database, table
                            )))
                        };
                    }
                    _ => {}
                };
                if enable_experimental_rbac_check {
                    match s_expr.get_udfs() {
                        Ok(udfs) => {
                            if !udfs.is_empty() {
                                for udf in udfs {
                                    self.validate_access(
                                        &GrantObject::UDF(udf.clone()),
                                        vec![UserPrivilegeType::Usage],
                                        false,
                                    ).await?
                                }
                            }
                        }
                        Err(err) => {
                            return Err(err.add_message("get udf error on validating access"));
                        }
                    }
                }

                let metadata = metadata.read().clone();

                for table in metadata.tables() {
                    if enable_experimental_rbac_check && table.is_source_of_stage() {
                        match table.table().get_data_source_info() {
                            DataSourceInfo::StageSource(stage_info) => {
                                self.validate_access_stage(&stage_info.stage_info, UserPrivilegeType::Read).await?;
                            }
                            DataSourceInfo::Parquet2Source(stage_info) => {
                                self.validate_access_stage(&stage_info.stage_info, UserPrivilegeType::Read).await?;
                            }
                            DataSourceInfo::ParquetSource(stage_info) => {
                                self.validate_access_stage(&stage_info.stage_info, UserPrivilegeType::Read).await?;
                            }
                            DataSourceInfo::TableSource(_) | DataSourceInfo::ResultScanSource(_) => {}
                        }
                    }
                    if table.is_source_of_view() {
                        continue;
                    }
                    self.validate_access(
                        &GrantObject::Table(
                            table.catalog().to_string(),
                            table.database().to_string(),
                            table.name().to_string(),
                        ),
                        vec![UserPrivilegeType::Select],
                        true,
                    )
                        .await?
                }
            }
            Plan::ExplainAnalyze { plan } | Plan::Explain { plan, .. } => {
                self.check(ctx, plan).await?
            }

            // Database.
            Plan::ShowCreateDatabase(plan) => {
                self.validate_access(
                    &GrantObject::Database(plan.catalog.clone(), plan.database.clone()),
                    vec![UserPrivilegeType::Select],
                    true,
                )
                    .await?
            }
            Plan::CreateUDF(_) | Plan::CreateDatabase(_) | Plan::CreateIndex(_) => {
                self.validate_access(&GrantObject::Global, vec![UserPrivilegeType::Create], true)
                    .await?;
            }
            Plan::DropDatabase(_)
            | Plan::UndropDatabase(_)
            | Plan::DropUDF(_)
            | Plan::DropIndex(_) => {
                self.validate_access(&GrantObject::Global, vec![UserPrivilegeType::Drop], true)
                    .await?;
            }
            Plan::UseDatabase(plan) => {
                // Use db is special. Should not check the privilege.
                // Just need to check user grant objects contain the db that be used.
                let database = &plan.database;
                let has_priv = has_priv(&tenant, database, None, grant_set).await?;

                return if has_priv {
                    Ok(())
                } else {
                    Err(ErrorCode::PermissionDenied(format!(
                        "Permission denied, user {} don't have privilege for database {}",
                        identity, database
                    )))
                };
            }

            // Virtual Column.
            Plan::CreateVirtualColumn(plan) => {
                self.validate_access(
                    &GrantObject::Table(
                        plan.catalog.clone(),
                        plan.database.clone(),
                        plan.table.clone(),
                    ),
                    vec![UserPrivilegeType::Create],
                    false,
                )
                    .await?;
            }
            Plan::AlterVirtualColumn(plan) => {
                self.validate_access(
                    &GrantObject::Table(
                        plan.catalog.clone(),
                        plan.database.clone(),
                        plan.table.clone(),
                    ),
                    vec![UserPrivilegeType::Alter],
                    false,
                )
                    .await?;
            }
            Plan::DropVirtualColumn(plan) => {
                self.validate_access(
                    &GrantObject::Table(
                        plan.catalog.clone(),
                        plan.database.clone(),
                        plan.table.clone(),
                    ),
                    vec![UserPrivilegeType::Drop],
                    false,
                )
                    .await?;
            }
            Plan::RefreshVirtualColumn(plan) => {
                self.validate_access(
                    &GrantObject::Table(
                        plan.catalog.clone(),
                        plan.database.clone(),
                        plan.table.clone(),
                    ),
                    vec![UserPrivilegeType::Super],
                    false,
                )
                    .await?;
            }

            // Table.
            Plan::ShowCreateTable(plan) => {
                self.validate_access(
                    &GrantObject::Table(
                        plan.catalog.clone(),
                        plan.database.clone(),
                        plan.table.clone(),
                    ),
                    vec![UserPrivilegeType::Select],
                    true,
                )
                    .await?
            }
            Plan::DescribeTable(plan) => {
                self.validate_access(
                    &GrantObject::Table(
                        plan.catalog.clone(),
                        plan.database.clone(),
                        plan.table.clone(),
                    ),
                    vec![UserPrivilegeType::Select],
                    true,
                )
                    .await?
            }
            Plan::CreateTable(plan) => {
                self.validate_access(
                    &GrantObject::Database(plan.catalog.clone(), plan.database.clone()),
                    vec![UserPrivilegeType::Create],
                    true,
                )
                    .await?;
                if let Some(query) = &plan.as_select {
                    self.check(ctx, query).await?;
                }
            }
            Plan::DropTable(plan) => {
                self.validate_access(
                    &GrantObject::Database(plan.catalog.clone(), plan.database.clone()),
                    vec![UserPrivilegeType::Drop],
                    true,
                )
                    .await?;
            }
            Plan::UndropTable(plan) => {
                self.validate_access(
                    &GrantObject::Database(plan.catalog.clone(), plan.database.clone()),
                    vec![UserPrivilegeType::Drop],
                    true,
                )
                    .await?;
            }
            Plan::RenameTable(plan) => {
                // You must have ALTER and DROP privileges for the original table,
                // and CREATE and INSERT privileges for the new table.
                self.validate_access(
                    &GrantObject::Table(
                        plan.catalog.clone(),
                        plan.database.clone(),
                        plan.table.clone(),
                    ),
                    vec![UserPrivilegeType::Alter, UserPrivilegeType::Drop],
                    true,
                )
                    .await?;
                // TODO(liyz): need only check the create privilege on the target database? the target
                // table may still not existed yet.
                self.validate_access(
                    &GrantObject::Table(
                        plan.catalog.clone(),
                        plan.new_database.clone(),
                        plan.new_table.clone(),
                    ),
                    vec![UserPrivilegeType::Create, UserPrivilegeType::Insert],
                    false,
                )
                    .await?;
            }
            Plan::SetOptions(plan) => {
                self.validate_access(
                    &GrantObject::Table(
                        plan.catalog.clone(),
                        plan.database.clone(),
                        plan.table.clone(),
                    ),
                    vec![UserPrivilegeType::Alter],
                    true,
                )
                    .await?;
            }
            Plan::AddTableColumn(plan) => {
                self.validate_access(
                    &GrantObject::Table(
                        plan.catalog.clone(),
                        plan.database.clone(),
                        plan.table.clone(),
                    ),
                    vec![UserPrivilegeType::Alter],
                    true,
                )
                    .await?;
            }
            Plan::RenameTableColumn(plan) => {
                self.validate_access(
                    &GrantObject::Table(
                        plan.catalog.clone(),
                        plan.database.clone(),
                        plan.table.clone(),
                    ),
                    vec![UserPrivilegeType::Alter],
                    true,
                )
                    .await?;
            }
            Plan::ModifyTableColumn(plan) => {
                self.validate_access(
                    &GrantObject::Table(
                        plan.catalog.clone(),
                        plan.database.clone(),
                        plan.table.clone(),
                    ),
                    vec![UserPrivilegeType::Alter],
                    true,
                )
                    .await?;
            }
            Plan::DropTableColumn(plan) => {
                self.validate_access(
                    &GrantObject::Table(
                        plan.catalog.clone(),
                        plan.database.clone(),
                        plan.table.clone(),
                    ),
                    vec![UserPrivilegeType::Alter],
                    true,
                )
                    .await?;
            }
            Plan::AlterTableClusterKey(plan) => {
                self.validate_access(
                    &GrantObject::Table(
                        plan.catalog.clone(),
                        plan.database.clone(),
                        plan.table.clone(),
                    ),
                    vec![UserPrivilegeType::Alter],
                    true,
                )
                    .await?;
            }
            Plan::DropTableClusterKey(plan) => {
                self.validate_access(
                    &GrantObject::Table(
                        plan.catalog.clone(),
                        plan.database.clone(),
                        plan.table.clone(),
                    ),
                    vec![UserPrivilegeType::Drop],
                    true,
                )
                    .await?;
            }
            Plan::ReclusterTable(plan) => {
                if enable_experimental_rbac_check {
                    if let Some(scalar) = &plan.push_downs {
                        let udf = get_udf_names(scalar)?;
                        self.check_udf_priv(udf).await?;
                    }
                }
                self.validate_access(
                    &GrantObject::Table(
                        plan.catalog.clone(),
                        plan.database.clone(),
                        plan.table.clone(),
                    ),
                    vec![UserPrivilegeType::Alter],
                    true,
                )
                    .await?;
            }
            Plan::TruncateTable(plan) => {
                self.validate_access(
                    &GrantObject::Table(
                        plan.catalog.clone(),
                        plan.database.clone(),
                        plan.table.clone(),
                    ),
                    vec![UserPrivilegeType::Delete],
                    true,
                )
                    .await?;
            }
            Plan::OptimizeTable(plan) => {
                self.validate_access(
                    &GrantObject::Table(
                        plan.catalog.clone(),
                        plan.database.clone(),
                        plan.table.clone(),
                    ),
                    vec![UserPrivilegeType::Super],
                    true,
                )
                    .await?;
            }
            Plan::VacuumTable(plan) => {
                self.validate_access(
                    &GrantObject::Table(
                        plan.catalog.clone(),
                        plan.database.clone(),
                        plan.table.clone(),
                    ),
                    vec![UserPrivilegeType::Super],
                    true,
                )
                    .await?;
            }
            Plan::VacuumDropTable(plan) => {
                self.validate_access(
                    &GrantObject::Database(plan.catalog.clone(), plan.database.clone()),
                    vec![UserPrivilegeType::Super],
                    true,
                )
                    .await?;
            }
            Plan::AnalyzeTable(plan) => {
                self.validate_access(
                    &GrantObject::Table(
                        plan.catalog.clone(),
                        plan.database.clone(),
                        plan.table.clone(),
                    ),
                    vec![UserPrivilegeType::Super],
                    true,
                )
                    .await?;
            }
            // Others.
            Plan::Insert(plan) => {
                self.validate_access(
                    &GrantObject::Table(
                        plan.catalog.clone(),
                        plan.database.clone(),
                        plan.table.clone(),
                    ),
                    vec![UserPrivilegeType::Insert],
                    true,
                )
                    .await?;
                match &plan.source {
                    InsertInputSource::SelectPlan(plan) => {
                        self.check(ctx, plan).await?;
                    }
                    InsertInputSource::Stage(plan) => {
                        self.check(ctx, plan).await?;
                    }
                    InsertInputSource::StreamingWithFormat(..)
                    | InsertInputSource::StreamingWithFileFormat {..}
                    | InsertInputSource::Values {..} => {}
                }
            }
            Plan::Replace(plan) => {
                //plan.delete_when is Expr no need to check privileges.
                self.validate_access(
                    &GrantObject::Table(
                        plan.catalog.clone(),
                        plan.database.clone(),
                        plan.table.clone(),
                    ),
                    vec![UserPrivilegeType::Insert, UserPrivilegeType::Delete],
                    true,
                )
                    .await?;
                match &plan.source {
                    InsertInputSource::SelectPlan(plan) => {
                        self.check(ctx, plan).await?;
                    }
                    InsertInputSource::Stage(plan) => {
                        self.check(ctx, plan).await?;
                    }
                    InsertInputSource::StreamingWithFormat(..)
                    | InsertInputSource::StreamingWithFileFormat {..}
                    | InsertInputSource::Values {..} => {}
                }
            }
            Plan::MergeInto(plan) => {
                if enable_experimental_rbac_check {
                    let s_expr = &plan.input;
                    match s_expr.get_udfs() {
                        Ok(udfs) => {
                            if !udfs.is_empty() {
                                for udf in udfs {
                                    self.validate_access(
                                        &GrantObject::UDF(udf.clone()),
                                        vec![UserPrivilegeType::Usage],
                                        false,
                                    ).await?
                                }
                            }
                        }
                        Err(err) => {
                            return Err(err.add_message("get udf error on validating access"));
                        }
                    }
                    let matched_evaluators = &plan.matched_evaluators;
                    let unmatched_evaluators = &plan.unmatched_evaluators;
                    for matched_evaluator in matched_evaluators {
                        if let Some(condition) = &matched_evaluator.condition {
                            let udf = get_udf_names(condition)?;
                            self.check_udf_priv(udf).await?;
                        }
                        if let Some(updates) = &matched_evaluator.update {
                            for scalar in updates.values() {
                                let udf = get_udf_names(scalar)?;
                                self.check_udf_priv(udf).await?;
                            }
                        }
                    }
                    for unmatched_evaluator in unmatched_evaluators {
                        if let Some(condition) = &unmatched_evaluator.condition {
                            let udf = get_udf_names(condition)?;
                            self.check_udf_priv(udf).await?;
                        }
                        for value in &unmatched_evaluator.values {
                            let udf = get_udf_names(value)?;
                            self.check_udf_priv(udf).await?;
                        }
                    }
                }
                self.validate_access(
                    &GrantObject::Table(
                        plan.catalog.clone(),
                        plan.database.clone(),
                        plan.table.clone(),
                    ),
                    vec![UserPrivilegeType::Insert, UserPrivilegeType::Delete],
                    true,
                )
                    .await?;
            }
            Plan::Delete(plan) => {
                if enable_experimental_rbac_check {
                    if let Some(selection) = &plan.selection {
                        let udf = get_udf_names(selection)?;
                        self.check_udf_priv(udf).await?;
                    }
                    for subquery in &plan.subquery_desc {
                        match subquery.input_expr.get_udfs() {
                            Ok(udfs) => {
                                if !udfs.is_empty() {
                                    for udf in udfs {
                                        self.validate_access(
                                            &GrantObject::UDF(udf.clone()),
                                            vec![UserPrivilegeType::Usage],
                                            false,
                                        ).await?
                                    }
                                }
                            }
                            Err(err) => {
                                return Err(err.add_message("get udf error on validating access"));
                            }
                        }
                    }
                }
                self.validate_access(
                    &GrantObject::Table(
                        plan.catalog_name.clone(),
                        plan.database_name.clone(),
                        plan.table_name.clone(),
                    ),
                    vec![UserPrivilegeType::Delete],
                    true,
                )
                    .await?;
            }
            Plan::Update(plan) => {
                if enable_experimental_rbac_check {
                    for scalar in plan.update_list.values() {
                        let udf = get_udf_names(scalar)?;
                        self.check_udf_priv(udf).await?;
                    }
                    if let Some(selection) = &plan.selection {
                        let udf = get_udf_names(selection)?;
                        self.check_udf_priv(udf).await?;
                    }
                    for subquery in &plan.subquery_desc {
                        match subquery.input_expr.get_udfs() {
                            Ok(udfs) => {
                                if !udfs.is_empty() {
                                    for udf in udfs {
                                        self.validate_access(
                                            &GrantObject::UDF(udf.clone()),
                                            vec![UserPrivilegeType::Usage],
                                            false,
                                        ).await?
                                    }
                                }
                            }
                            Err(err) => {
                                return Err(err.add_message("get udf error on validating access"));
                            }
                        }
                    }
                }
                self.validate_access(
                    &GrantObject::Table(
                        plan.catalog.clone(),
                        plan.database.clone(),
                        plan.table.clone(),
                    ),
                    vec![UserPrivilegeType::Update],
                    true,
                )
                    .await?;
            }
            Plan::CreateView(plan) => {
                self.validate_access(
                    &GrantObject::Database(plan.catalog.clone(), plan.database.clone()),
                    vec![UserPrivilegeType::Create],
                    true,
                )
                    .await?;
            }
            Plan::AlterView(plan) => {
                self.validate_access(
                    &GrantObject::Database(plan.catalog.clone(), plan.database.clone()),
                    vec![UserPrivilegeType::Alter],
                    true,
                )
                    .await?;
            }
            Plan::DropView(plan) => {
                self.validate_access(
                    &GrantObject::Database(plan.catalog.clone(), plan.database.clone()),
                    vec![UserPrivilegeType::Drop],
                    true,
                )
                    .await?;
            }
            Plan::CreateStream(plan) => {
                self.validate_access(
                    &GrantObject::Database(plan.catalog.clone(), plan.database.clone()),
                    vec![UserPrivilegeType::Create],
                    true,
                )
                    .await?;
            }
            Plan::DropStream(plan) => {
                self.validate_access(
                    &GrantObject::Database(plan.catalog.clone(), plan.database.clone()),
                    vec![UserPrivilegeType::Drop],
                    true,
                )
                    .await?;
            }
            Plan::CreateUser(_) => {
                self.validate_access(
                    &GrantObject::Global,
                    vec![UserPrivilegeType::CreateUser],
                    false,
                )
                    .await?;
            }
            Plan::DropUser(_) => {
                self.validate_access(
                    &GrantObject::Global,
                    vec![UserPrivilegeType::DropUser],
                    false,
                )
                    .await?;
            }
            Plan::CreateRole(_) => {
                self.validate_access(
                    &GrantObject::Global,
                    vec![UserPrivilegeType::CreateRole],
                    false,
                )
                    .await?;
            }
            Plan::DropRole(_) => {
                self.validate_access(
                    &GrantObject::Global,
                    vec![UserPrivilegeType::DropRole],
                    false,
                )
                    .await?;
            }
            Plan::GrantShareObject(_)
            | Plan::RevokeShareObject(_)
            | Plan::AlterShareTenants(_)
            | Plan::ShowObjectGrantPrivileges(_)
            | Plan::ShowGrantTenantsOfShare(_)
            | Plan::ShowGrants(_)
            | Plan::GrantRole(_)
            | Plan::GrantPriv(_)
            | Plan::RevokePriv(_)
            | Plan::AlterUDF(_)
            | Plan::RevokeRole(_) => {
                self.validate_access(&GrantObject::Global, vec![UserPrivilegeType::Grant], false)
                    .await?;
            }
            Plan::SetVariable(_) | Plan::UnSetVariable(_) | Plan::Kill(_) => {
                self.validate_access(&GrantObject::Global, vec![UserPrivilegeType::Super], false)
                    .await?;
            }
            Plan::AlterUser(_)
            | Plan::RenameDatabase(_)
            | Plan::RevertTable(_)
            | Plan::RefreshIndex(_) => {
                self.validate_access(&GrantObject::Global, vec![UserPrivilegeType::Alter], false)
                    .await?;
            }
            Plan::CopyIntoTable(plan) => {
                self.validate_access_stage(&plan.stage_table_info.stage_info, UserPrivilegeType::Read).await?;
                self
                    .validate_access(
                        &GrantObject::Table(
                            plan.catalog_info.catalog_name().to_string(),
                            plan.database_name.to_string(),
                            plan.table_name.to_string(),
                        ),
                        vec![UserPrivilegeType::Insert],
                        true,
                    )
                    .await?;
                if let Some(query) = &plan.query {
                    self.check(ctx, query).await?;
                }
            }
            Plan::CopyIntoLocation(plan) => {
                self.validate_access_stage(&plan.stage, UserPrivilegeType::Write).await?;
                let from = plan.from.clone();
                return self.check(ctx, &from).await;
            }
            Plan::RemoveStage(plan) => {
                self.validate_access_stage(&plan.stage, UserPrivilegeType::Write).await?;
            }
            Plan::CreateShareEndpoint(_)
            | Plan::ShowShareEndpoint(_)
            | Plan::DropShareEndpoint(_)
            | Plan::CreateShare(_)
            | Plan::DropShare(_)
            | Plan::DescShare(_)
            | Plan::ShowShares(_)
            | Plan::ShowCreateCatalog(_)
            | Plan::CreateCatalog(_)
            | Plan::DropCatalog(_)
            | Plan::CreateStage(_)
            | Plan::DropStage(_)
            | Plan::CreateFileFormat(_)
            | Plan::DropFileFormat(_)
            | Plan::ShowFileFormats(_)
            | Plan::CreateNetworkPolicy(_)
            | Plan::AlterNetworkPolicy(_)
            | Plan::DropNetworkPolicy(_)
            | Plan::DescNetworkPolicy(_)
            | Plan::ShowNetworkPolicies(_)
            | Plan::CreatePasswordPolicy(_)
            | Plan::AlterPasswordPolicy(_)
            | Plan::DropPasswordPolicy(_)
            | Plan::DescPasswordPolicy(_)
            | Plan::CreateConnection(_)
            | Plan::ShowConnections(_)
            | Plan::DescConnection(_)
            | Plan::DropConnection(_)
            | Plan::CreateTask(_)   // TODO: need to build ownership info for task
            | Plan::ShowTasks(_)    // TODO: need to build ownership info for task
            | Plan::DescribeTask(_) // TODO: need to build ownership info for task
            | Plan::ExecuteTask(_)  // TODO: need to build ownership info for task
            | Plan::DropTask(_)     // TODO: need to build ownership info for task
            | Plan::AlterTask(_) => {
                self.validate_access(&GrantObject::Global, vec![UserPrivilegeType::Super], false)
                    .await?;
            }
            Plan::CreateDatamaskPolicy(_) | Plan::DropDatamaskPolicy(_) => {
                self.validate_access(
                    &GrantObject::Global,
                    vec![UserPrivilegeType::CreateDataMask],
                    false,
                )
                    .await?;
            }
            // Note: No need to check privileges
            // SET ROLE & SHOW ROLES is a session-local statement (have same semantic with the SET ROLE in postgres), no need to check privileges
            Plan::SetRole(_) => {}
            Plan::SetSecondaryRoles(_) => {}
            Plan::ShowRoles(_) => {}
            Plan::Presign(plan) => {
                let privilege = match &plan.action {
                    PresignAction::Upload => UserPrivilegeType::Write,
                    PresignAction::Download => UserPrivilegeType::Read,
                };
                self.validate_access_stage(&plan.stage, privilege).await?;
            }
            Plan::ExplainAst { .. } => {}
            Plan::ExplainSyntax { .. } => {}
            // just used in clickhouse-sqlalchemy, no need to check
            Plan::ExistsTable(_) => {}
            Plan::DescDatamaskPolicy(_) => {}
        }

        Ok(())
    }
}

// TODO(liyz): replace it with verify_access
async fn has_priv(
    tenant: &str,
    database: &String,
    table: Option<&String>,
    grant_set: UserGrantSet,
) -> Result<bool> {
    Ok(RoleCacheManager::instance()
        .find_related_roles(tenant, &grant_set.roles())
        .await?
        .into_iter()
        .map(|role| role.grants)
        .fold(grant_set, |a, b| a | b)
        .entries()
        .iter()
        .any(|e| {
            let object = e.object();
            match object {
                GrantObject::Global => true,
                GrantObject::Database(_, ldb) => ldb == database,
                GrantObject::Table(_, ldb, ltab) => {
                    if let Some(table) = table {
                        ldb == database && ltab == table
                    } else {
                        ldb == database
                    }
                }
                _ => false,
            }
        }))
}
