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

use databend_common_base::base::GlobalInstance;
use databend_common_catalog::catalog::Catalog;
use databend_common_catalog::plan::DataSourceInfo;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_management::RoleApi;
use databend_common_management::WarehouseInfo;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::OwnershipInfo;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::principal::StageInfo;
use databend_common_meta_app::principal::StageType;
use databend_common_meta_app::principal::UserGrantSet;
use databend_common_meta_app::principal::UserPrivilegeSet;
use databend_common_meta_app::principal::UserPrivilegeType;
use databend_common_meta_app::principal::SYSTEM_TABLES_ALLOW_LIST;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_types::seq_value::SeqV;
use databend_common_sql::binder::MutationType;
use databend_common_sql::optimizer::get_udf_names;
use databend_common_sql::plans::InsertInputSource;
use databend_common_sql::plans::Mutation;
use databend_common_sql::plans::OptimizeCompactBlock;
use databend_common_sql::plans::PresignAction;
use databend_common_sql::plans::RewriteKind;
use databend_common_sql::Planner;
use databend_common_users::RoleCacheManager;
use databend_common_users::UserApiProvider;
use databend_enterprise_resources_management::ResourcesManagement;
use databend_storages_common_table_meta::table::OPT_KEY_TEMP_PREFIX;

use crate::interpreters::access::AccessChecker;
use crate::sessions::QueryContext;
use crate::sessions::Session;
use crate::sql::plans::Plan;

pub struct PrivilegeAccess {
    ctx: Arc<QueryContext>,
}

enum ObjectId {
    Database(u64),
    Table(u64, u64),
}

// table functions that need `Super` privilege
const SYSTEM_TABLE_FUNCTIONS: [&str; 2] = ["fuse_amend", "set_cache_capacity"];

impl PrivilegeAccess {
    pub fn create(ctx: Arc<QueryContext>) -> Box<dyn AccessChecker> {
        Box::new(PrivilegeAccess { ctx })
    }

    // PrivilegeAccess checks the privilege by names, we'd need to convert the GrantObject to
    // OwnerObject to check the privilege.
    // Currently we checks db/table ownerships by id, stage/udf ownerships by name.
    async fn convert_to_owner_object(
        &self,
        object: &GrantObject,
        disable_table_info_refresh: bool,
    ) -> Result<Option<OwnershipObject>> {
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
                    .database_id
                    .db_id;
                OwnershipObject::Database {
                    catalog_name: catalog_name.clone(),
                    db_id,
                }
            }
            GrantObject::Table(catalog_name, db_name, table_name) => {
                if db_name.to_lowercase() == "system" {
                    return Ok(None);
                }
                let catalog = if !disable_table_info_refresh {
                    self.ctx.get_catalog(catalog_name).await?
                } else {
                    self.ctx
                        .get_catalog(catalog_name)
                        .await?
                        .disable_table_info_refresh()?
                };
                let db_id = catalog
                    .get_database(&tenant, db_name)
                    .await?
                    .get_db_info()
                    .database_id
                    .db_id;
                let table_id = if !disable_table_info_refresh {
                    self.ctx
                        .get_table(catalog_name, db_name, table_name)
                        .await?
                        .get_id()
                } else {
                    match self.ctx.get_table(catalog_name, db_name, table_name).await {
                        Ok(table) => table.get_id(),
                        // attach table issue_16121 xx, then vacuum drop table from issue_16121 , then drop table
                        // should disable catalog
                        Err(_) => {
                            let cat = catalog.disable_table_info_refresh()?;
                            cat.get_table(&tenant, db_name, table_name).await?.get_id()
                        }
                    }
                };
                OwnershipObject::Table {
                    catalog_name: catalog_name.clone(),
                    db_id,
                    table_id,
                }
            }
            GrantObject::DatabaseById(catalog_name, db_id) => OwnershipObject::Database {
                catalog_name: catalog_name.clone(),
                db_id: *db_id,
            },
            GrantObject::TableById(catalog_name, db_id, table_id) => OwnershipObject::Table {
                catalog_name: catalog_name.clone(),
                db_id: *db_id,
                table_id: *table_id,
            },
            GrantObject::Stage(name) => OwnershipObject::Stage {
                name: name.to_string(),
            },
            GrantObject::UDF(name) => OwnershipObject::UDF {
                name: name.to_string(),
            },
            GrantObject::Warehouse(id) => OwnershipObject::Warehouse { id: id.to_string() },
            GrantObject::Global => return Ok(None),
        };

        Ok(Some(object))
    }

    async fn validate_db_access(
        &self,
        catalog_name: &str,
        db_name: &str,
        privileges: UserPrivilegeType,
        if_exists: bool,
    ) -> Result<()> {
        let tenant = self.ctx.get_tenant();
        let check_current_role_only = match privileges {
            // create table/stream need check db's Create Privilege
            UserPrivilegeType::Create => true,
            _ => false,
        };
        match self
            .validate_access(
                &GrantObject::Database(catalog_name.to_string(), db_name.to_string()),
                privileges,
                check_current_role_only,
                false,
            )
            .await
        {
            Ok(_) => {
                return Ok(());
            }
            Err(_err) => {
                let catalog = self.ctx.get_catalog(catalog_name).await?;
                match self
                    .convert_to_id(&tenant, &catalog, db_name, None, false)
                    .await
                {
                    Ok(obj) => {
                        let (db_id, _) = match obj {
                            ObjectId::Table(db_id, table_id) => (db_id, Some(table_id)),
                            ObjectId::Database(db_id) => (db_id, None),
                        };
                        if let Err(err) = self
                            .validate_access(
                                &GrantObject::DatabaseById(catalog_name.to_string(), db_id),
                                privileges,
                                check_current_role_only,
                                false,
                            )
                            .await
                        {
                            if err.code() != ErrorCode::PERMISSION_DENIED {
                                return Err(err);
                            }
                            let current_user = self.ctx.get_current_user()?;
                            let session = self.ctx.get_current_session();
                            let roles_name = if check_current_role_only {
                                // Roles name use to return err msg. If None no need to return Err
                                session
                                    .get_current_role()
                                    .map(|r| r.name)
                                    .unwrap_or_default()
                            } else {
                                session
                                    .get_all_effective_roles()
                                    .await?
                                    .iter()
                                    .map(|r| r.name.clone())
                                    .collect::<Vec<_>>()
                                    .join(",")
                            };

                            return Err(ErrorCode::PermissionDenied(format!(
                                "Permission denied: privilege [{:?}] is required on '{}'.'{}'.* for user {} with roles [{}]. \
                                Note: Please ensure that your current role have the appropriate permissions to create a new Warehouse|Database|Table|UDF|Stage.",
                                privileges,
                                catalog_name,
                                db_name,
                                &current_user.identity().display(),
                                roles_name,
                            )));
                        }
                    }
                    Err(e) => match e.code() {
                        ErrorCode::UNKNOWN_DATABASE
                        | ErrorCode::UNKNOWN_TABLE
                        | ErrorCode::ILLEGAL_STREAM
                        | ErrorCode::UNKNOWN_CATALOG
                            if if_exists =>
                        {
                            return Ok(());
                        }
                        _ => return Err(e.add_message("error on validating database access")),
                    },
                }
            }
        }
        Ok(())
    }

    async fn validate_table_access(
        &self,
        catalog_name: &str,
        db_name: &str,
        table_name: &str,
        privilege: UserPrivilegeType,
        if_exists: bool,
        disable_table_info_refresh: bool,
    ) -> Result<()> {
        // skip checking the privilege on system tables.
        if ((db_name == "system" && SYSTEM_TABLES_ALLOW_LIST.iter().any(|x| x == &table_name))
            || db_name == "information_schema")
            && privilege == UserPrivilegeType::Select
        {
            return Ok(());
        }

        if self.ctx.is_temp_table(catalog_name, db_name, table_name) {
            return Ok(());
        }

        let tenant = self.ctx.get_tenant();

        match self.ctx.get_catalog(catalog_name).await {
            Ok(catalog) => {
                if catalog.exists_table_function(table_name) {
                    return self.validate_table_function_access(table_name).await;
                }
                // to keep compatibility with the legacy privileges which granted by table name,
                // we'd both check the privileges by name and id.
                // we'll completely move to the id side in the future.
                match self
                    .validate_access(
                        &GrantObject::Table(
                            catalog_name.to_string(),
                            db_name.to_string(),
                            table_name.to_string(),
                        ),
                        privilege,
                        false,
                        disable_table_info_refresh,
                    )
                    .await
                {
                    Ok(_) => return Ok(()),
                    Err(_err) => {
                        match self
                            .convert_to_id(
                                &tenant,
                                &catalog,
                                db_name,
                                Some(table_name),
                                disable_table_info_refresh,
                            )
                            .await
                        {
                            Ok(obj) => {
                                let (db_id, table_id) = match obj {
                                    ObjectId::Table(db_id, table_id) => (db_id, Some(table_id)),
                                    ObjectId::Database(db_id) => (db_id, None),
                                };
                                // Note: validate_table_access is not used for validate Create Table privilege
                                if let Err(err) = self
                                    .validate_access(
                                        &GrantObject::TableById(
                                            catalog_name.to_string(),
                                            db_id,
                                            table_id.unwrap(),
                                        ),
                                        privilege,
                                        false,
                                        disable_table_info_refresh,
                                    )
                                    .await
                                {
                                    if err.code() != ErrorCode::PERMISSION_DENIED {
                                        return Err(err);
                                    }
                                    let current_user = self.ctx.get_current_user()?;
                                    let session = self.ctx.get_current_session();
                                    let roles_name = session
                                        .get_all_effective_roles()
                                        .await?
                                        .iter()
                                        .map(|r| r.name.clone())
                                        .collect::<Vec<_>>()
                                        .join(",");
                                    return Err(ErrorCode::PermissionDenied(format!(
                                        "Permission denied: privilege [{:?}] is required on '{}'.'{}'.'{}' for user {} with roles [{}]",
                                        privilege,
                                        catalog_name,
                                        db_name,
                                        table_name,
                                        &current_user.identity().display(),
                                        roles_name,
                                    )));
                                }
                            }
                            Err(e) => match e.code() {
                                ErrorCode::UNKNOWN_DATABASE
                                | ErrorCode::UNKNOWN_TABLE
                                | ErrorCode::ILLEGAL_STREAM
                                | ErrorCode::UNKNOWN_CATALOG
                                    if if_exists =>
                                {
                                    return Ok(());
                                }

                                _ => return Err(e.add_message("error on validating table access")),
                            },
                        }
                    }
                }
            }
            Err(error) => {
                return if error.code() == ErrorCode::UNKNOWN_CATALOG && if_exists {
                    Ok(())
                } else {
                    Err(error)
                };
            }
        }

        Ok(())
    }

    async fn validate_warehouse_ownership(
        &self,
        warehouse: String,
        current_user: String,
    ) -> Option<Result<()>> {
        let session = self.ctx.get_current_session();
        let warehouse_mgr = GlobalInstance::get::<Arc<dyn ResourcesManagement>>();

        // Only check support_forward_warehouse_request privileges
        if !warehouse_mgr.support_forward_warehouse_request() {
            return Some(Ok(()));
        }

        match warehouse_mgr.list_warehouses().await {
            Ok(warehouses) => {
                if let Some(sw) = warehouses
                    .iter()
                    .filter_map(|w| {
                        if let WarehouseInfo::SystemManaged(sw) = w {
                            Some(sw)
                        } else {
                            None
                        }
                    })
                    .find(|sw| sw.id == warehouse.clone())
                {
                    let id = sw.role_id.to_string();
                    let grant_object = GrantObject::Warehouse(id);
                    match self
                        .has_ownership(&session, &grant_object, false, false)
                        .await
                    {
                        Ok(has) => {
                            if has {
                                Some(Ok(()))
                            } else {
                                Some(Err(ErrorCode::PermissionDenied(format!(
                                    "Permission denied: Ownership is required on WAREHOUSE '{}' for user {}",
                                    warehouse,
                                    current_user
                                ))))
                            }
                        }
                        Err(e) => Some(Err(e.add_message("error on checking warehouse ownership"))),
                    }
                } else {
                    None
                }
            }
            Err(e) => Some(Err(e.add_message("error on validating warehouse ownership"))),
        }
    }

    async fn has_ownership(
        &self,
        session: &Arc<Session>,
        grant_object: &GrantObject,
        check_current_role_only: bool,
        disable_table_info_refresh: bool,
    ) -> Result<bool> {
        let owner_object = self
            .convert_to_owner_object(grant_object, disable_table_info_refresh)
            .await
            .or_else(|e| match e.code() {
                ErrorCode::UNKNOWN_DATABASE
                | ErrorCode::UNKNOWN_TABLE
                | ErrorCode::ILLEGAL_STREAM
                | ErrorCode::UNKNOWN_CATALOG => Ok(None),
                _ => Err(e.add_message("error on check has_ownership")),
            })?;
        if let Some(object) = &owner_object {
            if let OwnershipObject::Table {
                catalog_name,
                db_id,
                ..
            } = object
            {
                let database_owner = OwnershipObject::Database {
                    catalog_name: catalog_name.to_string(),
                    db_id: *db_id,
                };
                // If Table ownership check fails, check for Database ownership
                if session
                    .has_ownership(object, check_current_role_only)
                    .await?
                    || session
                        .has_ownership(&database_owner, check_current_role_only)
                        .await?
                {
                    return Ok(true);
                }
            } else if session
                .has_ownership(object, check_current_role_only)
                .await?
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn validate_access(
        &self,
        grant_object: &GrantObject,
        privilege: UserPrivilegeType,
        check_current_role_only: bool,
        disable_table_info_refresh: bool,
    ) -> Result<()> {
        let session = self.ctx.get_current_session();

        let verify_ownership = match grant_object {
            GrantObject::Database(_, _)
            | GrantObject::Table(_, _, _)
            | GrantObject::DatabaseById(_, _)
            | GrantObject::UDF(_)
            | GrantObject::Stage(_)
            | GrantObject::Warehouse(_)
            | GrantObject::TableById(_, _, _) => true,
            GrantObject::Global => false,
        };

        if verify_ownership
            && self
                .has_ownership(
                    &session,
                    grant_object,
                    check_current_role_only,
                    disable_table_info_refresh,
                )
                .await?
        {
            return Ok(());
        }

        // wrap an user-facing error message with table/db names on cases like TableByID / DatabaseByID
        match session
            .validate_privilege(grant_object, privilege, check_current_role_only)
            .await
        {
            Ok(_) => Ok(()),
            Err(err) => {
                if err.code() != ErrorCode::PERMISSION_DENIED {
                    return Err(err);
                }
                let current_user = self.ctx.get_current_user()?;

                let roles_name = if check_current_role_only {
                    // Roles name use to return err msg. If None no need to return Err
                    session
                        .get_current_role()
                        .map(|r| r.name)
                        .unwrap_or_default()
                } else {
                    session
                        .get_all_effective_roles()
                        .await?
                        .iter()
                        .map(|r| r.name.clone())
                        .collect::<Vec<_>>()
                        .join(",")
                };

                match grant_object {
                    GrantObject::TableById(_, _, _) => Err(ErrorCode::PermissionDenied("")),
                    GrantObject::DatabaseById(_, _) => Err(ErrorCode::PermissionDenied("")),
                    GrantObject::Global
                    | GrantObject::UDF(_)
                    | GrantObject::Warehouse(_)
                    | GrantObject::Stage(_)
                    | GrantObject::Database(_, _)
                    | GrantObject::Table(_, _, _) => Err(ErrorCode::PermissionDenied(format!(
                        "Permission denied: privilege [{:?}] is required on {} for user {} with roles [{}]. \
                        Note: Please ensure that your current role have the appropriate permissions to create a new Warehouse|Database|Table|UDF|Stage.",
                        privilege,
                        grant_object,
                        &current_user.identity().display(),
                        roles_name,
                    ))),
                }
            }
        }
    }

    async fn validate_stage_access(
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

        // Note: validate_stage_access is not used for validate Create Stage privilege
        self.validate_access(
            &GrantObject::Stage(stage_info.stage_name.to_string()),
            privilege,
            false,
            false,
        )
        .await
    }

    async fn validate_udf_access(&self, udf_names: HashSet<&String>) -> Result<()> {
        // Note: validate_udf_access is not used for validate Create UDF
        for udf in udf_names {
            self.validate_access(
                &GrantObject::UDF(udf.clone()),
                UserPrivilegeType::Usage,
                false,
                false,
            )
            .await?;
        }
        Ok(())
    }

    async fn validate_table_function_access(&self, table_func_name: &str) -> Result<()> {
        if SYSTEM_TABLE_FUNCTIONS.iter().any(|x| x == &table_func_name) {
            // need Super privilege to invoke system table functions
            let privilege = UserPrivilegeType::Super;
            let session = self.ctx.get_current_session();
            let current_user = self.ctx.get_current_user()?;
            session
                .validate_privilege(&GrantObject::Global, privilege, true)
                .await.map_err(|err | {

                if err.code() != ErrorCode::PERMISSION_DENIED {
                    err
                } else {
                    let role_name = session.get_current_role().map(|r|r.name).unwrap_or_default();
                    ErrorCode::PermissionDenied(format!(
                        "Permission denied: privilege [{:?}] is required to invoke table function [{}] for user {} with roles [{}]",
                        privilege,
                        table_func_name,
                        &current_user.identity().display(),
                        role_name,
                    ))
                }
            })
        } else {
            Ok(())
        }
    }

    async fn convert_to_id(
        &self,
        tenant: &Tenant,
        catalog: &Arc<dyn Catalog>,
        database_name: &str,
        table_name: Option<&str>,
        disable_table_info_refresh: bool,
    ) -> Result<ObjectId> {
        let cat = catalog.clone();
        let db_id = cat
            .get_database(tenant, database_name)
            .await?
            .get_db_info()
            .database_id
            .db_id;
        if let Some(table_name) = table_name {
            let table_id = if !disable_table_info_refresh {
                self.ctx
                    .get_table(cat.name().as_str(), database_name, table_name)
                    .await?
                    .get_id()
            } else {
                match self
                    .ctx
                    .get_table(cat.name().as_str(), database_name, table_name)
                    .await
                {
                    Ok(table) => table.get_id(),
                    // attach table issue_16121 xx, then vacuum drop table from issue_16121 , then drop table
                    // should disable catalog
                    Err(_) => cat
                        .get_table(tenant, database_name, table_name)
                        .await?
                        .get_id(),
                }
            };
            return Ok(ObjectId::Table(db_id, table_id));
        }
        Ok(ObjectId::Database(db_id))
    }

    async fn validate_insert_source(
        &self,
        ctx: &Arc<QueryContext>,
        source: &InsertInputSource,
    ) -> Result<()> {
        match source {
            InsertInputSource::SelectPlan(plan) => {
                self.check(ctx, plan).await?;
            }
            InsertInputSource::Stage(plan) => {
                self.check(ctx, plan).await?;
            }
            InsertInputSource::Values(_) => {}
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl AccessChecker for PrivilegeAccess {
    #[async_backtrace::framed]
    async fn check(&self, ctx: &Arc<QueryContext>, plan: &Plan) -> Result<()> {
        let user = self.ctx.get_current_user()?;
        if let Plan::AlterUser(plan) = plan {
            // Alter current user's password do not need to check privileges.
            if plan.user.username == user.name && plan.user_option.is_none() {
                return Ok(());
            }
        }
        // User need to change password first in two casese:
        // 1. set `MUST_CHANGE_PASSWORD` when create user or alter user password,
        //    and the user login first time.
        // 2. The password has not been changed within the maximum period
        //    specified in the password policy `MAX_AGE_DAYS`.
        let need_change = user.auth_info.get_need_change();
        if need_change {
            // If current user need change password, other operation is not allowed.
            return Err(ErrorCode::NeedChangePasswordDenied(
                "Must change password before execute other operations".to_string(),
            ));
        }
        let (identity, grant_set) = (user.identity().display().to_string(), user.grants);

        let enable_experimental_rbac_check = self
            .ctx
            .get_settings()
            .get_enable_experimental_rbac_check()?;
        let tenant = self.ctx.get_tenant();
        let ctl_name = self.ctx.get_current_catalog();

        match plan {
            Plan::Query {
                metadata,
                rewrite_kind,
                s_expr,
                ..
            } => {
                match rewrite_kind {
                    Some(RewriteKind::ShowDatabases)
                    | Some(RewriteKind::ShowDropDatabases)
                    | Some(RewriteKind::ShowEngines)
                    | Some(RewriteKind::ShowFunctions)
                    | Some(RewriteKind::ShowUserFunctions)
                    | Some(RewriteKind::ShowDictionaries(_)) => {
                        return Ok(());
                    }
                    | Some(RewriteKind::ShowTableFunctions) => {
                        return Ok(());
                    }
                    Some(RewriteKind::ShowTables(catalog, database)) => {
                        let clg = self.ctx.get_catalog(catalog).await?;
                        let (show_db_id, table_id) = match self.convert_to_id(&tenant, &clg, database, None, false).await? {
                            ObjectId::Table(db_id, table_id) => { (db_id, Some(table_id)) }
                            ObjectId::Database(db_id) => { (db_id, None) }
                        };

                        if has_priv(&tenant, database, None, show_db_id, table_id, grant_set).await? {
                            return Ok(())
                        }

                        let user_api = UserApiProvider::instance();
                        let ownerships = user_api
                            .role_api(&tenant)
                            .get_ownerships()
                            .await?;
                        let roles = self.ctx.get_all_effective_roles().await?;
                        let roles_name: Vec<String> = roles.iter().map(|role| role.name.to_string()).collect();
                        check_db_tb_ownership_access(&identity, catalog, database, show_db_id, &ownerships, &roles_name)?;
                    }
                    Some(RewriteKind::ShowStreams(database)) => {
                        let ctl = self.ctx.get_catalog(&ctl_name).await?;
                        let (show_db_id, table_id) = match self.convert_to_id(&tenant, &ctl, database, None, false).await? {
                            ObjectId::Table(db_id, table_id) => { (db_id, Some(table_id)) }
                            ObjectId::Database(db_id) => { (db_id, None) }
                        };
                        if has_priv(&tenant, database, None, show_db_id, table_id, grant_set).await? {
                            return Ok(());
                        }
                        let user_api = UserApiProvider::instance();
                        let ownerships = user_api
                            .role_api(&tenant)
                            .get_ownerships()
                            .await?;
                        let roles = self.ctx.get_all_effective_roles().await?;
                        let roles_name: Vec<String> = roles.iter().map(|role| role.name.to_string()).collect();
                        check_db_tb_ownership_access(&identity, &ctl_name, database, show_db_id, &ownerships, &roles_name)?;
                    }
                    Some(RewriteKind::ShowColumns(catalog_name, database, table)) => {
                        if self.ctx.is_temp_table(catalog_name,database,table){
                            return Ok(());
                        }
                        let session = self.ctx.get_current_session();
                        if self.has_ownership(&session, &GrantObject::Table(catalog_name.clone(), database.clone(), table.clone()), false, false).await? ||
                            self.has_ownership(&session, &GrantObject::Database(catalog_name.clone(), database.clone()), false, false).await?   {
                            return Ok(());
                        }
                        let catalog = self.ctx.get_catalog(catalog_name).await?;
                        let (db_id, table_id) = match self.convert_to_id(&tenant, &catalog, database, Some(table), false).await? {
                            ObjectId::Table(db_id, table_id) => { (db_id, Some(table_id)) }
                            ObjectId::Database(db_id) => { (db_id, None) }
                        };
                        let has_priv = has_priv(&tenant, database, Some(table), db_id, table_id, grant_set).await?;
                        return if has_priv {
                            Ok(())
                        } else {
                            Err(ErrorCode::PermissionDenied(format!(
                                "Permission denied: User {} does not have the required privileges for table '{}.{}'",
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
                                self.validate_udf_access(udfs).await?;
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
                                self.validate_stage_access(&stage_info.stage_info, UserPrivilegeType::Read).await?;
                            }
                            DataSourceInfo::ParquetSource(stage_info) => {
                                self.validate_stage_access(&stage_info.stage_info, UserPrivilegeType::Read).await?;
                            }
                            DataSourceInfo::ORCSource(stage_info) => {
                                self.validate_stage_access(&stage_info.stage_table_info.stage_info, UserPrivilegeType::Read).await?;
                            }
                            DataSourceInfo::TableSource(_) | DataSourceInfo::ResultScanSource(_) => {}
                        }
                    }
                    if table.is_source_of_view()||table.table().is_temp() {
                        continue;
                    }

                    let catalog_name = table.catalog();
                    // like this sql: copy into t from (select * from @s3); will bind a mock table with name `system.read_parquet(s3)`
                    // this is no means to check table `system.read_parquet(s3)` privilege
                    if !table.is_source_of_stage() {
                        self.validate_table_access(catalog_name, table.database(), table.name(), UserPrivilegeType::Select, false, false).await?
                    }
                }
            }
            Plan::ExplainAnalyze { plan, .. } | Plan::Explain { plan, .. } => {
                self.check(ctx, plan).await?
            }

            // Database.
            Plan::ShowCreateDatabase(plan) => {
                self.validate_db_access(&plan.catalog, &plan.database, UserPrivilegeType::Select, false).await?
            }
            Plan::CreateDatabase(_) => {
                self.validate_access(&GrantObject::Global, UserPrivilegeType::CreateDatabase, true, false)
                    .await?;
            }
            Plan::DropDatabase(plan) => {
                self.validate_db_access(&plan.catalog, &plan.database, UserPrivilegeType::Drop, plan.if_exists).await?;
            }
            Plan::UndropDatabase(_)
            | Plan::DropIndex(_)
            | Plan::DropTableIndex(_) => {
                // undroptable/db need convert name to id. But because of drop, can not find the id. Upgrade Object to Database.
                self.validate_access(&GrantObject::Global, UserPrivilegeType::Drop, false, false)
                    .await?;
            }
            Plan::CreateStage(_) => {
                self.validate_access(&GrantObject::Global, UserPrivilegeType::Super, true, false)
                    .await?;
            }
            Plan::CreateUDF(_) => {
                self.validate_access(&GrantObject::Global, UserPrivilegeType::Super, true, false)
                    .await?;
            }
            Plan::DropUDF(plan) => {
                let udf_name = &plan.udf;
                if !UserApiProvider::instance().exists_udf(&tenant, udf_name).await? && plan.if_exists {
                    return Ok(());
                }
                if enable_experimental_rbac_check {
                    let udf = HashSet::from([udf_name]);
                    self.validate_udf_access(udf).await?;
                } else {
                    self.validate_access(&GrantObject::Global, UserPrivilegeType::Drop, false, false)
                                        .await?;
                }
            }
            Plan::DropStage(plan) => {
                match UserApiProvider::instance().get_stage(&tenant, &plan.name).await {
                    Ok(stage) => {
                        if enable_experimental_rbac_check {
                            let privileges = vec![UserPrivilegeType::Read, UserPrivilegeType::Write];
                            for privilege in privileges {
                                self.validate_stage_access(&stage, privilege).await?;
                            }
                        } else {
                            self.validate_access(&GrantObject::Global, UserPrivilegeType::Super, false, false)
                                .await?;
                        }
                    }
                    Err(e) => {
                        return match e.code() {
                            ErrorCode::UNKNOWN_STAGE if plan.if_exists =>
                                {
                                    Ok(())
                                }
                            _ => Err(e.add_message("error on validating stage access")),
                        }
                    }
                }
            }
            Plan::UseDatabase(plan) => {
                let ctl = self.ctx.get_catalog(&ctl_name).await?;
                // Use db is special. Should not check the privilege.
                // Just need to check user grant objects contain the db that be used.
                let (show_db_id, _) = match self.convert_to_id(&tenant, &ctl, &plan.database, None, false).await? {
                    ObjectId::Table(db_id, table_id) => { (db_id, Some(table_id)) }
                    ObjectId::Database(db_id) => { (db_id, None) }
                };
                if has_priv(&tenant, &plan.database, None, show_db_id, None, grant_set).await? {
                    return Ok(());
                }
                let user_api = UserApiProvider::instance();
                let ownerships = user_api
                    .role_api(&tenant)
                    .get_ownerships()
                    .await?;
                let roles = self.ctx.get_all_effective_roles().await?;
                let roles_name: Vec<String> = roles.iter().map(|role| role.name.to_string()).collect();
                check_db_tb_ownership_access(&identity, &ctl_name, &plan.database, show_db_id, &ownerships, &roles_name)?;
            }

            // Virtual Column.
            Plan::CreateVirtualColumn(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Create, false, false).await?
            }
            Plan::AlterVirtualColumn(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Alter, plan.if_exists, false).await?
            }
            Plan::DropVirtualColumn(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Drop, plan.if_exists, false).await?
            }
            Plan::RefreshVirtualColumn(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Super, false, false).await?
            }

            // Table.
            Plan::ShowCreateTable(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Select, false, false).await?
            }
            Plan::DescribeTable(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Select, false, false).await?
            }
            Plan::CreateTable(plan) => {
                if !plan.options.contains_key(OPT_KEY_TEMP_PREFIX){
                    self.validate_db_access(&plan.catalog, &plan.database, UserPrivilegeType::Create, false).await?;
                }
                if let Some(query) = &plan.as_select {
                    self.check(ctx, query).await?;
                }
            }
            Plan::DropTable(plan) => {
                // For attach table
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Drop, plan.if_exists, true).await?;
            }
            Plan::UndropTable(plan) => {
                // undroptable/db need convert name to id. But because of drop, can not find the id. Upgrade Object to Database.
                self.validate_db_access(&plan.catalog, &plan.database, UserPrivilegeType::Drop, false).await?;

            }
            Plan::RenameTable(plan) => {
                if  self.ctx.is_temp_table(&plan.catalog,&plan.database, &plan.table) {
                    return Ok(());
                }
                // You must have ALTER and DROP privileges for the original table,
                // and CREATE for the new db.
                let privileges = vec![UserPrivilegeType::Alter, UserPrivilegeType::Drop];
                for privilege in privileges {
                    self.validate_table_access(&plan.catalog, &plan.database, &plan.table, privilege, plan.if_exists, false).await?;
                }
                self.validate_db_access(&plan.catalog, &plan.new_database, UserPrivilegeType::Create, false).await?;
            }
            Plan::SetOptions(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Alter, false, false).await?
            }
            Plan::UnsetOptions(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Alter, false, false).await?
            }
            Plan::AddTableColumn(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Alter, false, false).await?
            }
            Plan::RenameTableColumn(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Alter, false, false).await?
            }
            Plan::ModifyTableColumn(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Alter, false, false).await?
            }
            Plan::ModifyTableComment(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Alter, false, false).await?
            }
            Plan::DropTableColumn(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Alter, false, false).await?
            }
            Plan::AlterTableClusterKey(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Alter, false, false).await?
            }
            Plan::DropTableClusterKey(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Drop, false, false).await?
            }
            Plan::ReclusterTable(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Alter, false, false).await?
            }
            Plan::TruncateTable(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Delete, false, false).await?
            }
            Plan::OptimizePurge(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Super, false, false).await?
            },
            Plan::OptimizeCompactSegment(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Super, false, false).await?
            },
            Plan::OptimizeCompactBlock { s_expr, .. } => {
                let plan: OptimizeCompactBlock = s_expr.plan().clone().try_into()?;
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Super, false, false).await?
            },
            Plan::VacuumTable(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Super, false, false).await?
            }
            Plan::VacuumDropTable(plan) => {
                self.validate_db_access(&plan.catalog, &plan.database, UserPrivilegeType::Super, false).await?
            }
            Plan::VacuumTemporaryFiles(_) => {
                self.validate_access(&GrantObject::Global, UserPrivilegeType::Super, false, false).await?
            }
            Plan::AnalyzeTable(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Super, false, false).await?
            }
            // Dictionary
            Plan::ShowCreateDictionary(_)
            | Plan::CreateDictionary(_)
            | Plan::DropDictionary(_)
            | Plan::RenameDictionary(_) => {
                self.validate_access(&GrantObject::Global, UserPrivilegeType::Super, false, false)
                    .await?;
            }
            // Others.
            Plan::Insert(plan) => {
                let target_table_privileges = if plan.overwrite {
                    vec![UserPrivilegeType::Insert, UserPrivilegeType::Delete]
                } else {
                    vec![UserPrivilegeType::Insert]
                };
                for privilege in target_table_privileges {
                    self.validate_table_access(&plan.catalog, &plan.database, &plan.table, privilege, false, false).await?;
                }
                self.validate_insert_source(ctx, &plan.source).await?;
            }
            Plan::InsertMultiTable(plan) => {
                let target_table_privileges = if plan.overwrite {
                    vec![UserPrivilegeType::Insert, UserPrivilegeType::Delete]
                } else {
                    vec![UserPrivilegeType::Insert]
                };
                for target in plan.whens.iter().flat_map(|when|when.intos.iter()).chain(plan.opt_else.as_ref().into_iter().flat_map(|e|e.intos.iter())){
                    for privilege in target_table_privileges.clone() {
                    self.validate_table_access(&target.catalog, &target.database, &target.table, privilege, false, false).await?;
                    }
                }
                self.check(ctx, &plan.input_source).await?;
            }
            Plan::Replace(plan) => {
                //plan.delete_when is Expr no need to check privileges.
                let privileges = vec![UserPrivilegeType::Insert, UserPrivilegeType::Delete];
                for privilege in privileges {
                    self.validate_table_access(&plan.catalog, &plan.database, &plan.table, privilege, false, false).await?;
                }
                self.validate_insert_source(ctx, &plan.source).await?;
            }
            Plan::DataMutation { s_expr, .. } => {
                let plan: Mutation = s_expr.plan().clone().try_into()?;
                if enable_experimental_rbac_check {
                    let s_expr = s_expr.child(0)?;
                    match s_expr.get_udfs() {
                        Ok(udfs) => {
                            if !udfs.is_empty() {
                                self.validate_udf_access(udfs).await?;
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
                            self.validate_udf_access(udf).await?;
                        }
                        if let Some(updates) = &matched_evaluator.update {
                            for scalar in updates.values() {
                                let udf = get_udf_names(scalar)?;
                                self.validate_udf_access(udf).await?;
                            }
                        }
                    }
                    for unmatched_evaluator in unmatched_evaluators {
                        if let Some(condition) = &unmatched_evaluator.condition {
                            let udf = get_udf_names(condition)?;
                            self.validate_udf_access(udf).await?;
                        }
                        for value in &unmatched_evaluator.values {
                            let udf = get_udf_names(value)?;
                            self.validate_udf_access(udf).await?;
                        }
                    }
                }
                let privileges = match plan.mutation_type {
                    MutationType::Merge => vec![UserPrivilegeType::Insert, UserPrivilegeType::Update, UserPrivilegeType::Delete],
                    MutationType::Update => vec![UserPrivilegeType::Update],
                    MutationType::Delete => vec![UserPrivilegeType::Delete],
                };
                for privilege in privileges {
                    self.validate_table_access(&plan.catalog_name, &plan.database_name, &plan.table_name, privilege, false, false).await?;
                }
            }
            Plan::CreateView(plan) => {
                let mut planner = Planner::new(self.ctx.clone());
                let (plan, _) = planner.plan_sql(&plan.subquery).await?;
                self.check(ctx, &plan).await?
            }
            Plan::AlterView(plan) => {
                self.validate_db_access(&plan.catalog, &plan.database, UserPrivilegeType::Alter, false).await?;
                let mut planner = Planner::new(self.ctx.clone());
                let (plan, _) = planner.plan_sql(&plan.subquery).await?;
                self.check(ctx, &plan).await?
            }
            Plan::DropView(plan) => {
                self.validate_db_access(&plan.catalog, &plan.database, UserPrivilegeType::Drop, plan.if_exists).await?
            }
            Plan::DescribeView(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.view_name, UserPrivilegeType::Select, false, false).await?
            }
            Plan::CreateStream(plan) => {
                self.validate_db_access(&plan.catalog, &plan.database, UserPrivilegeType::Create, false).await?
            }
            Plan::DropStream(plan) => {
                self.validate_db_access(&plan.catalog, &plan.database, UserPrivilegeType::Drop, plan.if_exists).await?
            }
            Plan::CreateDynamicTable(plan) => {
                self.validate_db_access(&plan.catalog, &plan.database, UserPrivilegeType::Create, false).await?;
            }
            Plan::CreateUser(_) => {
                self.validate_access(
                    &GrantObject::Global,
                    UserPrivilegeType::CreateUser,
                    false,
                    false,
                )
                    .await?;
            }
            Plan::DropUser(_) => {
                self.validate_access(
                    &GrantObject::Global,
                    UserPrivilegeType::DropUser,
                    false,false
                )
                    .await?;
            }
            Plan::CreateRole(_) => {
                self.validate_access(
                    &GrantObject::Global,
                    UserPrivilegeType::CreateRole,
                    false,
                    false,
                )
                    .await?;
            }
            Plan::DropRole(_) => {
                self.validate_access(
                    &GrantObject::Global,
                    UserPrivilegeType::DropRole,
                    false, false
                )
                    .await?;
            }
            Plan::GrantRole(_)
            | Plan::GrantPriv(_)
            | Plan::RevokePriv(_)
            | Plan::RevokeRole(_) => {
                self.validate_access(&GrantObject::Global, UserPrivilegeType::Grant,false, false)
                    .await?;
            }
            Plan::Set(plan) => {
                use databend_common_ast::ast::SetType;
                if let SetType::SettingsGlobal = plan.set_type {
                    self.validate_access(&GrantObject::Global, UserPrivilegeType::Super, false, false)
                        .await?;
                }
            }
            Plan::Unset(plan) => {
                use databend_common_ast::ast::SetType;
                if let SetType::SettingsGlobal = plan.unset_type {
                    self.validate_access(&GrantObject::Global, UserPrivilegeType::Super, false, false)
                        .await?;
                }
            }
            Plan::Kill(_) | Plan::SetPriority(_) | Plan::System(_) => {
                self.validate_access(&GrantObject::Global, UserPrivilegeType::Super, false, false)
                    .await?;
            }

            Plan::RenameDatabase(_)
            | Plan::RevertTable(_)
            | Plan::AlterUDF(_)
            | Plan::RefreshIndex(_)
            | Plan::RefreshTableIndex(_)
            | Plan::AlterUser(_) => {
                self.validate_access(&GrantObject::Global, UserPrivilegeType::Alter, false, false)
                    .await?;
            }
            Plan::CopyIntoTable(plan) => {
                self.validate_stage_access(&plan.stage_table_info.stage_info, UserPrivilegeType::Read).await?;
                self.validate_table_access(plan.catalog_info.catalog_name(), &plan.database_name, &plan.table_name, UserPrivilegeType::Insert, false, false).await?;
                if let Some(query) = &plan.query {
                    self.check(ctx, query).await?;
                }
            }
            Plan::CopyIntoLocation(plan) => {
                self.validate_stage_access(&plan.stage, UserPrivilegeType::Write).await?;
                let from = plan.from.clone();
                return self.check(ctx, &from).await;
            }
            Plan::RemoveStage(plan) => {
                self.validate_stage_access(&plan.stage, UserPrivilegeType::Write).await?;
            }
            Plan::ShowCreateCatalog(_)
            | Plan::CreateCatalog(_)
            | Plan::DropCatalog(_)
            | Plan::UseCatalog(_)
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
            | Plan::CreateIndex(_)
            | Plan::CreateTableIndex(_)
            | Plan::CreateNotification(_)
            | Plan::DropNotification(_)
            | Plan::DescNotification(_)
            | Plan::AlterNotification(_)
            | Plan::DescUser(_)
            | Plan::CreateTask(_)   // TODO: need to build ownership info for task
            | Plan::ShowTasks(_)    // TODO: need to build ownership info for task
            | Plan::DescribeTask(_) // TODO: need to build ownership info for task
            | Plan::ExecuteTask(_)  // TODO: need to build ownership info for task
            | Plan::DropTask(_)     // TODO: need to build ownership info for task
            | Plan::AlterTask(_)
            | Plan::CreateSequence(_)
            | Plan::DropSequence(_) => {
                self.validate_access(&GrantObject::Global, UserPrivilegeType::Super, false, false)
                    .await?;
            }
            Plan::CreateDatamaskPolicy(_) | Plan::DropDatamaskPolicy(_) => {
                self.validate_access(
                    &GrantObject::Global,
                    UserPrivilegeType::CreateDataMask,
                    false,false
                )
                    .await?;
            }
            // Note: No need to check privileges
            // SET ROLE & SHOW ROLES is a session-local statement (have same semantic with the SET ROLE in postgres), no need to check privileges
            Plan::SetRole(_) => {}
            Plan::SetSecondaryRoles(_) => {}
            Plan::Presign(plan) => {
                let privilege = match &plan.action {
                    PresignAction::Upload => UserPrivilegeType::Write,
                    PresignAction::Download => UserPrivilegeType::Read,
                };
                self.validate_stage_access(&plan.stage, privilege).await?;
            }
            Plan::ExplainAst { .. } => {}
            Plan::ExplainSyntax { .. } => {}
            // just used in clickhouse-sqlalchemy, no need to check
            Plan::ExistsTable(_) => {}
            Plan::DescDatamaskPolicy(_) => {}
            Plan::Begin => {}
            Plan::ExecuteImmediate(_)
            | Plan::CallProcedure(_)
            | Plan::CreateProcedure(_)
            | Plan::DropProcedure(_)
            | Plan::DescProcedure(_)
            /*| Plan::ShowCreateProcedure(_)
            | Plan::RenameProcedure(_)*/ => {
                self.validate_access(&GrantObject::Global, UserPrivilegeType::Super, false, false)
                    .await?;
            }
            Plan::Commit => {}
            Plan::Abort => {}
            Plan::ShowWarehouses => {
                // check privilege in interpreter
            }
            Plan::ShowOnlineNodes => {
                // todo: now no limit
            }
            Plan::DropWarehouse(plan) => {
                self.validate_warehouse_ownership(plan.warehouse.clone(), identity).await.transpose()?;
            }
            Plan::ResumeWarehouse(plan) => {
                self.validate_warehouse_ownership(plan.warehouse.clone(), identity).await.transpose()?;
            }
            Plan::SuspendWarehouse(plan) => {
                self.validate_warehouse_ownership(plan.warehouse.clone(), identity).await.transpose()?;
            }
            Plan::RenameWarehouse(plan) => {
                self.validate_warehouse_ownership(plan.warehouse.clone(), identity).await.transpose()?;
            }
            Plan::InspectWarehouse(plan) => {
                self.validate_warehouse_ownership(plan.warehouse.clone(), identity).await.transpose()?;
            }
            Plan::DropWarehouseCluster(plan) => {
                self.validate_warehouse_ownership(plan.warehouse.clone(), identity).await.transpose()?;
            }
            Plan::RenameWarehouseCluster(plan) => {
                self.validate_warehouse_ownership(plan.warehouse.clone(), identity).await.transpose()?;
            }
            Plan::UseWarehouse(plan) => {
                self.validate_warehouse_ownership(plan.warehouse.clone(), identity).await.transpose()?;
            }
            Plan::CreateWarehouse(_) => {
                let warehouse_mgr = GlobalInstance::get::<Arc<dyn ResourcesManagement>>();
                // Only check support_forward_warehouse_request privileges
                if !warehouse_mgr.support_forward_warehouse_request() {
                    return Ok(());
                }
                // only current role has global level create warehouse privilege, it will pass
                self.validate_access(&GrantObject::Global, UserPrivilegeType::CreateWarehouse, true, false)
                    .await?;
            }
            Plan::AddWarehouseCluster(plan) => {
                self.validate_warehouse_ownership(plan.warehouse.clone(), identity).await.transpose()?;
            }
            Plan::AssignWarehouseNodes(plan) => {
                self.validate_warehouse_ownership(plan.warehouse.clone(), identity).await.transpose()?;
            }
            Plan::UnassignWarehouseNodes(plan) => {
                self.validate_warehouse_ownership(plan.warehouse.clone(), identity).await.transpose()?;
            }
        }

        Ok(())
    }
}

fn check_db_tb_ownership_access(
    identity: &String,
    catalog: &String,
    database: &String,
    show_db_id: u64,
    ownerships: &[SeqV<OwnershipInfo>],
    roles_name: &[String],
) -> Result<()> {
    // If contains account_admin even though the current role is not account_admin,
    // It also as a admin user.
    if roles_name.contains(&"account_admin".to_string()) {
        return Ok(());
    }

    for ownership in ownerships {
        if roles_name.contains(&ownership.data.role) {
            match &ownership.data.object {
                OwnershipObject::Database {
                    catalog_name,
                    db_id,
                } => {
                    if catalog_name == catalog && *db_id == show_db_id {
                        return Ok(());
                    }
                }
                OwnershipObject::Table {
                    catalog_name,
                    db_id,
                    table_id: _,
                } => {
                    if catalog_name == catalog && *db_id == show_db_id {
                        return Ok(());
                    }
                }
                OwnershipObject::UDF { .. }
                | OwnershipObject::Stage { .. }
                | OwnershipObject::Warehouse { .. } => {}
            }
        }
    }

    Err(ErrorCode::PermissionDenied(format!(
        "Permission denied: User {} does not have the required privileges for database '{}'",
        identity, database
    )))
}

// TODO(liyz): replace it with verify_access
async fn has_priv(
    tenant: &Tenant,
    db_name: &str,
    table_name: Option<&str>,
    db_id: u64,
    table_id: Option<u64>,
    grant_set: UserGrantSet,
) -> Result<bool> {
    if db_name.to_lowercase() == "information_schema" {
        return Ok(true);
    }
    if db_name.to_lowercase() == "system" {
        if let Some(table_name) = table_name {
            if SYSTEM_TABLES_ALLOW_LIST.contains(&table_name) {
                return Ok(true);
            }
        }
    }

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
                GrantObject::Global => {
                    if db_name.to_lowercase() == "system" {
                        return true;
                    }
                    e.privileges().iter().any(|privilege| {
                        UserPrivilegeSet::available_privileges_on_database(false)
                            .has_privilege(privilege)
                    })
                }
                GrantObject::Database(_, ldb) => *ldb == db_name,
                GrantObject::DatabaseById(_, ldb) => *ldb == db_id,
                GrantObject::Table(_, ldb, ltab) => {
                    if let Some(table) = table_name {
                        *ldb == db_name && *ltab == table
                    } else {
                        *ldb == db_name
                    }
                }
                GrantObject::TableById(_, ldb, ltab) => {
                    if let Some(table) = table_id {
                        *ldb == db_id && *ltab == table
                    } else {
                        *ldb == db_id
                    }
                }
                _ => false,
            }
        }))
}
