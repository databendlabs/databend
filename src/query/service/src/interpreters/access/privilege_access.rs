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

use std::collections::HashMap;
use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;

use databend_common_base::base::GlobalInstance;
use databend_common_catalog::catalog::CATALOG_DEFAULT;
use databend_common_catalog::catalog::Catalog;
use databend_common_catalog::plan::DataSourceInfo;
use databend_common_catalog::session_type::SessionType;
use databend_common_catalog::table::Table;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_management::RoleApi;
use databend_common_management::WarehouseInfo;
use databend_common_meta_api::DatamaskApi;
use databend_common_meta_api::RowAccessPolicyApi;
use databend_common_meta_app::data_mask::DataMaskNameIdent;
use databend_common_meta_app::principal::GetProcedureReq;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::OwnershipInfo;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::principal::ProcedureNameIdent;
use databend_common_meta_app::principal::SENSITIVE_SYSTEM_RESOURCE;
use databend_common_meta_app::principal::SYSTEM_TABLES_ALLOW_LIST;
use databend_common_meta_app::principal::StageInfo;
use databend_common_meta_app::principal::StageType;
use databend_common_meta_app::principal::UserGrantSet;
use databend_common_meta_app::principal::UserPrivilegeSet;
use databend_common_meta_app::principal::UserPrivilegeType;
use databend_common_meta_app::row_access_policy::RowAccessPolicyNameIdent;
use databend_common_meta_app::schema::is_materialized_view_engine;
use databend_common_meta_app::tenant::Tenant;
use databend_common_sql::Planner;
use databend_common_sql::binder::MutationType;
use databend_common_sql::plans::InsertInputSource;
use databend_common_sql::plans::MaintenanceTarget;
use databend_common_sql::plans::ModifyColumnAction;
use databend_common_sql::plans::Mutation;
use databend_common_sql::plans::OptimizeCompactBlock;
use databend_common_sql::plans::PresignAction;
use databend_common_sql::plans::RewriteKind;
use databend_common_sql::plans::TagSetObject;
use databend_common_users::BUILTIN_ROLE_ACCOUNT_ADMIN;
use databend_common_users::RoleCacheManager;
use databend_common_users::UserApiProvider;
use databend_enterprise_resources_management::ResourcesManagement;
use databend_meta_client::types::SeqV;
use databend_storages_common_table_meta::table::OPT_KEY_DATABASE_ID;
use databend_storages_common_table_meta::table::OPT_KEY_TEMP_PREFIX;
use parking_lot::Mutex;

use crate::history_tables::session::get_history_log_user;
use crate::interpreters::access::AccessChecker;
use crate::meta_service_error;
use crate::sessions::QueryContext;
use crate::sessions::Session;
use crate::sessions::TableContextAuthorization;
use crate::sessions::TableContextCluster;
use crate::sessions::TableContextSettings;
use crate::sessions::TableContextTableAccess;
use crate::sql::plans::Plan;

pub struct PrivilegeAccess {
    ctx: Arc<QueryContext>,
    cache: QueryAccessCache,
}

// PrivilegeAccess is created for one plan check, so these entries never outlive the query.
#[derive(Default)]
struct QueryAccessCache {
    database_ids: Mutex<HashMap<(String, String), u64>>,
    ownership_checks: Mutex<HashMap<(OwnershipObject, bool), bool>>,
}

impl QueryAccessCache {
    async fn get_or_load_database_id<F, Fut>(
        &self,
        catalog_name: &str,
        database_name: &str,
        load: F,
    ) -> Result<u64>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<u64>>,
    {
        let key = (catalog_name.to_string(), database_name.to_string());
        if let Some(db_id) = self.database_ids.lock().get(&key) {
            return Ok(*db_id);
        }

        let db_id = load().await?;
        self.database_ids.lock().insert(key, db_id);
        Ok(db_id)
    }

    async fn get_or_load_ownership_check<F, Fut>(
        &self,
        object: &OwnershipObject,
        check_current_role_only: bool,
        load: F,
    ) -> Result<bool>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<bool>>,
    {
        let key = (object.clone(), check_current_role_only);
        if let Some(has_ownership) = self.ownership_checks.lock().get(&key) {
            return Ok(*has_ownership);
        }

        let has_ownership = load().await?;
        self.ownership_checks.lock().insert(key, has_ownership);
        Ok(has_ownership)
    }

    fn ownership_check(
        &self,
        object: &OwnershipObject,
        check_current_role_only: bool,
    ) -> Option<bool> {
        self.ownership_checks
            .lock()
            .get(&(object.clone(), check_current_role_only))
            .copied()
    }

    fn insert_ownership_checks(&self, checks: impl IntoIterator<Item = (OwnershipObject, bool)>) {
        self.ownership_checks.lock().extend(
            checks
                .into_iter()
                .map(|(object, has_ownership)| ((object, false), has_ownership)),
        );
    }
}

enum ObjectId {
    Database(u64),
    Table(u64, u64),
}

// table functions that need `Super` privilege
const SYSTEM_TABLE_FUNCTIONS: [&str; 2] = ["fuse_amend", "set_cache_capacity"];

/// Bound each ownership MGet to avoid oversized Meta RPC payloads for queries
/// that reference many tables.
const MGET_OWNERSHIP_BATCH_SIZE: usize = 256;

type TableAccessKey<'a> = (&'a str, &'a str, &'a str, u64);

fn mark_table_access_checked<'a>(
    checked_tables: &mut HashSet<TableAccessKey<'a>>,
    catalog: &'a str,
    database: &'a str,
    table: &'a str,
    table_id: u64,
) -> bool {
    checked_tables.insert((catalog, database, table, table_id))
}

fn should_prefetch_ownerships(session_type: &SessionType) -> bool {
    !matches!(session_type, SessionType::Local)
}

#[async_trait::async_trait]
trait OwnershipPrefetchApi: Send + Sync {
    async fn mget_ownerships(
        &self,
        tenant: &Tenant,
        objects: &[OwnershipObject],
    ) -> Result<Vec<Option<OwnershipInfo>>>;

    async fn exists_role(&self, tenant: &Tenant, role: String) -> Result<bool>;
}

#[async_trait::async_trait]
impl OwnershipPrefetchApi for UserApiProvider {
    async fn mget_ownerships(
        &self,
        tenant: &Tenant,
        objects: &[OwnershipObject],
    ) -> Result<Vec<Option<OwnershipInfo>>> {
        UserApiProvider::mget_ownerships(self, tenant, objects).await
    }

    async fn exists_role(&self, tenant: &Tenant, role: String) -> Result<bool> {
        UserApiProvider::exists_role(self, tenant, role).await
    }
}

async fn prefetch_ownerships_with_api(
    cache: &QueryAccessCache,
    tenant: &Tenant,
    objects: &[OwnershipObject],
    effective_role_names: &HashSet<String>,
    user_api: &dyn OwnershipPrefetchApi,
) -> Result<()> {
    let mut unique_objects = Vec::with_capacity(objects.len());
    let mut seen_objects = HashSet::with_capacity(objects.len());
    for object in objects {
        if cache.ownership_check(object, false).is_none() && seen_objects.insert(object) {
            unique_objects.push(object.clone());
        }
    }

    let mut role_exists = HashMap::new();
    for objects in unique_objects.chunks(MGET_OWNERSHIP_BATCH_SIZE) {
        let ownerships = user_api.mget_ownerships(tenant, objects).await?;
        if ownerships.len() != objects.len() {
            return Err(ErrorCode::Internal(format!(
                "ownership MGet returned {} results for {} objects",
                ownerships.len(),
                objects.len()
            )));
        }

        let mut checks = Vec::with_capacity(objects.len());
        for (object, ownership) in objects.iter().cloned().zip(ownerships) {
            let owner_role = match ownership {
                Some(owner) => {
                    if owner.object != object {
                        return Err(ErrorCode::Internal(format!(
                            "ownership MGet returned {} for requested object {}",
                            owner.object, object
                        )));
                    }
                    let exists = match role_exists.get(&owner.role) {
                        Some(exists) => *exists,
                        None => {
                            let exists = user_api.exists_role(tenant, owner.role.clone()).await?;
                            role_exists.insert(owner.role.clone(), exists);
                            exists
                        }
                    };
                    if exists {
                        owner.role
                    } else {
                        BUILTIN_ROLE_ACCOUNT_ADMIN.to_string()
                    }
                }
                None => BUILTIN_ROLE_ACCOUNT_ADMIN.to_string(),
            };
            checks.push((object, effective_role_names.contains(&owner_role)));
        }
        cache.insert_ownership_checks(checks);
    }
    Ok(())
}

impl PrivilegeAccess {
    pub fn create(ctx: Arc<QueryContext>) -> Box<dyn AccessChecker> {
        Box::new(PrivilegeAccess {
            ctx,
            cache: QueryAccessCache::default(),
        })
    }

    async fn get_database_id(
        &self,
        tenant: &Tenant,
        catalog_name: &str,
        catalog: &Arc<dyn Catalog>,
        database_name: &str,
    ) -> Result<u64> {
        self.cache
            .get_or_load_database_id(catalog_name, database_name, || async {
                Ok(catalog
                    .get_database(tenant, database_name)
                    .await?
                    .get_db_info()
                    .database_id
                    .db_id)
            })
            .await
    }

    async fn has_ownership_cached(
        &self,
        session: &Arc<Session>,
        object: &OwnershipObject,
        check_current_role_only: bool,
    ) -> Result<bool> {
        self.cache
            .get_or_load_ownership_check(object, check_current_role_only, || async {
                session.has_ownership(object, check_current_role_only).await
            })
            .await
    }

    async fn prefetch_ownerships(&self, objects: &[OwnershipObject]) -> Result<()> {
        if objects.is_empty() {
            return Ok(());
        }

        let session = self.ctx.get_current_session();
        if !should_prefetch_ownerships(&session.get_type()) {
            return Ok(());
        }

        let effective_role_names = session
            .get_all_effective_roles()
            .await?
            .into_iter()
            .map(|role| role.name)
            .collect::<HashSet<_>>();
        prefetch_ownerships_with_api(
            &self.cache,
            &self.ctx.get_tenant(),
            objects,
            &effective_role_names,
            UserApiProvider::instance().as_ref(),
        )
        .await
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
                let catalog = self.ctx.get_catalog(catalog_name).await?;
                let db_id = self
                    .get_database_id(&tenant, catalog_name, &catalog, db_name)
                    .await?;
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
                let db_id = self
                    .get_database_id(&tenant, catalog_name, &catalog, db_name)
                    .await?;
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
            GrantObject::Connection(name) => OwnershipObject::Connection {
                name: name.to_string(),
            },
            GrantObject::Sequence(name) => OwnershipObject::Sequence {
                name: name.to_string(),
            },
            GrantObject::Procedure(procedure_id) => OwnershipObject::Procedure {
                procedure_id: *procedure_id,
            },
            GrantObject::MaskingPolicy(policy_id) => OwnershipObject::MaskingPolicy {
                policy_id: *policy_id,
            },
            GrantObject::RowAccessPolicy(policy_id) => OwnershipObject::RowAccessPolicy {
                policy_id: *policy_id,
            },
            GrantObject::Global => return Ok(None),
        };

        Ok(Some(object))
    }

    fn access_system_history(
        &self,
        catalog_name: Option<&str>,
        db_name: Option<&str>,
        stage_name: Option<&str>,
        privilege: UserPrivilegeType,
    ) -> Result<()> {
        let cluster = self.ctx.get_cluster();
        let cluster_id = cluster.get_cluster_id().unwrap_or_default();
        let tenant_id = GlobalConfig::instance().query.tenant_id.clone();
        if get_history_log_user(tenant_id.tenant_name(), &cluster_id).identity()
            == self.ctx.get_current_user()?.identity()
        {
            return Ok(());
        }
        match (catalog_name, db_name, stage_name) {
            (Some(catalog_name), Some(db_name), None) => {
                if catalog_name == CATALOG_DEFAULT
                    && db_name.eq_ignore_ascii_case(SENSITIVE_SYSTEM_RESOURCE)
                    && !matches!(
                        privilege,
                        UserPrivilegeType::Select | UserPrivilegeType::Drop
                    )
                {
                    return Err(ErrorCode::PermissionDenied(format!(
                        "Permission Denied: Operation '{:?}' on database 'default.system_history' is not allowed. This sensitive system resource only supports 'SELECT' and 'DROP'",
                        privilege
                    )));
                }
            }
            (None, None, Some(stage_name)) => {
                let config = GlobalConfig::instance();
                let sensitive_system_stage = config.log.history.stage_name.clone();

                return if stage_name.eq_ignore_ascii_case(&sensitive_system_stage) {
                    if let Some(current_role) = self.ctx.get_current_role() {
                        if current_role.name == BUILTIN_ROLE_ACCOUNT_ADMIN {
                            Ok(())
                        } else {
                            Err(ErrorCode::PermissionDenied(format!(
                                "Permission Denied: Operation '{:?}' on stage {sensitive_system_stage} is not allowed",
                                privilege
                            )))
                        }
                    } else {
                        Err(ErrorCode::PermissionDenied(format!(
                            "Permission Denied: Operation '{:?}' on stage {sensitive_system_stage} is not allowed",
                            privilege
                        )))
                    }
                } else {
                    Ok(())
                };
            }
            _ => unreachable!(),
        }

        Ok(())
    }

    async fn get_role_names_and_ownerships(
        &self,
        tenant: &Tenant,
    ) -> Result<(Vec<String>, Vec<SeqV<OwnershipInfo>>)> {
        let roles = self.ctx.get_all_effective_roles().await?;
        let roles_name = roles
            .iter()
            .map(|role| role.name.to_string())
            .collect::<Vec<_>>();

        if roles_name
            .iter()
            .any(|role_name| role_name == BUILTIN_ROLE_ACCOUNT_ADMIN)
        {
            return Ok((roles_name, Vec::new()));
        }

        let user_api = UserApiProvider::instance();
        let ownerships = user_api
            .role_api(tenant)
            .list_ownerships()
            .await
            .map_err(meta_service_error)?;
        Ok((roles_name, ownerships))
    }

    async fn validate_db_access(
        &self,
        catalog_name: &str,
        db_name: &str,
        privileges: UserPrivilegeType,
        if_exists: bool,
    ) -> Result<()> {
        self.access_system_history(Some(catalog_name), Some(db_name), None, privileges)?;
        let tenant = self.ctx.get_tenant();
        let catalog = self.ctx.get_catalog(catalog_name).await?;
        if if_exists && !catalog.exists_database(&tenant, db_name).await? {
            return Ok(());
        }
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
                                Note: Please ensure that your current role have the appropriate permissions to create a new Object",
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

        self.access_system_history(Some(catalog_name), Some(db_name), None, privilege)?;

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

    async fn validate_mv_source_access(&self, mv_table: &dyn Table) -> Result<()> {
        if !is_materialized_view_engine(mv_table.engine()) {
            return Err(ErrorCode::TableEngineNotSupported(format!(
                "'{}' is not a materialized view",
                mv_table.name()
            )));
        }

        let source_table_id = mv_table
            .get_table_info()
            .meta
            .materialized_view_source_table_id()?;
        // MV sources are restricted to persistent FUSE tables in the default catalog. Resolve the
        // current source name through catalog APIs so table renames preserve the privilege anchor.
        let source_catalog = self.ctx.get_catalog(CATALOG_DEFAULT).await?;
        let source_meta = source_catalog
            .get_table_meta_by_id(source_table_id)
            .await?
            .ok_or_else(|| {
                ErrorCode::InvalidMaterializedView(format!(
                    "source table id {source_table_id} does not exist"
                ))
            })?;
        let source_db_id = source_meta
            .data
            .options
            .get(OPT_KEY_DATABASE_ID)
            .ok_or_else(|| {
                ErrorCode::InvalidMaterializedView(format!(
                    "source table id {source_table_id} does not record its database id"
                ))
            })?
            .parse::<u64>()?;

        // Current grants and ownership are ID-based. Avoid resolving names on the common path;
        // only fall back to names for compatibility with legacy grants.
        match self
            .validate_access(
                &GrantObject::TableById(CATALOG_DEFAULT.to_string(), source_db_id, source_table_id),
                UserPrivilegeType::Select,
                false,
                false,
            )
            .await
        {
            Ok(()) => return Ok(()),
            Err(err) if err.code() == ErrorCode::PERMISSION_DENIED => {}
            Err(err) => return Err(err),
        }

        let source_db_name = source_catalog.get_db_name_by_id(source_db_id).await?;
        let source_table_name = source_catalog
            .get_table_name_by_id(source_table_id)
            .await?
            .ok_or_else(|| {
                ErrorCode::InvalidMaterializedView(format!(
                    "source table id {source_table_id} does not have a current name"
                ))
            })?;
        self.validate_table_access(
            CATALOG_DEFAULT,
            &source_db_name,
            &source_table_name,
            UserPrivilegeType::Select,
            false,
            false,
        )
        .await
    }

    async fn validate_table_index_access(
        &self,
        catalog_name: &str,
        db_name: &str,
        table_name: &str,
    ) -> Result<()> {
        self.access_system_history(
            Some(catalog_name),
            Some(db_name),
            None,
            UserPrivilegeType::Alter,
        )?;

        self.validate_table_index_alter_or_super_access(catalog_name, db_name, table_name)
            .await
    }

    async fn validate_drop_table_index_access(
        &self,
        catalog_name: &str,
        db_name: &str,
        table_name: &str,
    ) -> Result<()> {
        self.access_system_history(
            Some(catalog_name),
            Some(db_name),
            None,
            UserPrivilegeType::Drop,
        )?;

        match self
            .validate_table_index_alter_or_super_access(catalog_name, db_name, table_name)
            .await
        {
            Ok(()) => Ok(()),
            Err(err) if err.code() == ErrorCode::PERMISSION_DENIED => {
                match self
                    .validate_access(&GrantObject::Global, UserPrivilegeType::Drop, false, false)
                    .await
                {
                    Ok(()) => Ok(()),
                    Err(drop_err) if drop_err.code() == ErrorCode::PERMISSION_DENIED => Err(err),
                    Err(drop_err) => Err(drop_err),
                }
            }
            Err(err) => Err(err),
        }
    }

    async fn validate_table_index_alter_or_super_access(
        &self,
        catalog_name: &str,
        db_name: &str,
        table_name: &str,
    ) -> Result<()> {
        match self
            .validate_real_table_alter_access(catalog_name, db_name, table_name)
            .await
        {
            Ok(()) => Ok(()),
            Err(err) if err.code() == ErrorCode::PERMISSION_DENIED => {
                match self
                    .validate_access(&GrantObject::Global, UserPrivilegeType::Super, false, false)
                    .await
                {
                    Ok(()) => Ok(()),
                    Err(super_err) if super_err.code() == ErrorCode::PERMISSION_DENIED => Err(err),
                    Err(super_err) => Err(super_err),
                }
            }
            Err(err) => Err(err),
        }
    }

    async fn validate_real_table_alter_access(
        &self,
        catalog_name: &str,
        db_name: &str,
        table_name: &str,
    ) -> Result<()> {
        if self.ctx.is_temp_table(catalog_name, db_name, table_name) {
            return Ok(());
        }

        let tenant = self.ctx.get_tenant();
        let catalog = self.ctx.get_catalog(catalog_name).await?;

        match self
            .validate_access(
                &GrantObject::Table(
                    catalog_name.to_string(),
                    db_name.to_string(),
                    table_name.to_string(),
                ),
                UserPrivilegeType::Alter,
                false,
                false,
            )
            .await
        {
            Ok(()) => Ok(()),
            Err(err) if err.code() == ErrorCode::PERMISSION_DENIED => {
                match self
                    .convert_to_id(&tenant, &catalog, db_name, Some(table_name), false)
                    .await
                {
                    Ok(ObjectId::Table(db_id, table_id)) => {
                        match self
                            .validate_access(
                                &GrantObject::TableById(catalog_name.to_string(), db_id, table_id),
                                UserPrivilegeType::Alter,
                                false,
                                false,
                            )
                            .await
                        {
                            Ok(()) => Ok(()),
                            Err(err) if err.code() == ErrorCode::PERMISSION_DENIED => {
                                let current_user = self.ctx.get_current_user()?;
                                let session = self.ctx.get_current_session();
                                let roles_name = session
                                    .get_all_effective_roles()
                                    .await?
                                    .iter()
                                    .map(|r| r.name.clone())
                                    .collect::<Vec<_>>()
                                    .join(",");
                                Err(ErrorCode::PermissionDenied(format!(
                                    "Permission denied: privilege [{:?}] is required on '{}'.'{}'.'{}' for user {} with roles [{}]",
                                    UserPrivilegeType::Alter,
                                    catalog_name,
                                    db_name,
                                    table_name,
                                    &current_user.identity().display(),
                                    roles_name,
                                )))
                            }
                            Err(err) => Err(err),
                        }
                    }
                    Ok(ObjectId::Database(_)) => unreachable!("table name is provided"),
                    Err(err) => Err(err.add_message("error on validating table index access")),
                }
            }
            Err(err) => Err(err),
        }
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
                                    warehouse, current_user
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

    async fn validate_connection_access(
        &self,
        connection: String,
        privilege: UserPrivilegeType,
    ) -> Result<()> {
        if !self
            .ctx
            .get_settings()
            .get_enable_experimental_connection_privilege_check()?
        {
            return self
                .validate_access(&GrantObject::Global, UserPrivilegeType::Super, false, false)
                .await;
        }

        if self
            .validate_access(&GrantObject::Global, UserPrivilegeType::Super, false, false)
            .await
            .is_ok()
        {
            return Ok(());
        }

        self.validate_access(
            &GrantObject::Connection(connection),
            privilege,
            false,
            false,
        )
        .await
    }

    async fn validate_tag_object_access(
        &self,
        object: &TagSetObject,
        tenant: &Tenant,
    ) -> Result<()> {
        match object {
            TagSetObject::Database(target) => {
                self.validate_db_access(
                    &target.catalog,
                    &target.database,
                    UserPrivilegeType::Alter,
                    target.if_exists,
                )
                .await
            }
            TagSetObject::Table(target) => {
                self.validate_table_access(
                    &target.catalog,
                    &target.database,
                    &target.table,
                    UserPrivilegeType::Alter,
                    target.if_exists,
                    false,
                )
                .await
            }
            TagSetObject::Stage(target) => {
                match UserApiProvider::instance()
                    .get_stage(tenant, &target.stage_name)
                    .await
                {
                    Ok(stage) => {
                        self.validate_stage_access(&stage, UserPrivilegeType::Write)
                            .await
                    }
                    Err(e) => {
                        if e.code() == ErrorCode::UNKNOWN_STAGE && target.if_exists {
                            Ok(())
                        } else {
                            Err(e.add_message("error on validating stage access"))
                        }
                    }
                }
            }
            TagSetObject::User(_) | TagSetObject::Role(_) => {
                self.validate_access(&GrantObject::Global, UserPrivilegeType::Alter, false, false)
                    .await
            }
            TagSetObject::Connection(target) => {
                self.validate_connection_access(
                    target.connection_name.clone(),
                    UserPrivilegeType::AccessConnection,
                )
                .await
            }
            TagSetObject::View(target) => {
                self.validate_table_access(
                    &target.catalog,
                    &target.database,
                    &target.view,
                    UserPrivilegeType::Alter,
                    target.if_exists,
                    false,
                )
                .await
            }
            TagSetObject::Stream(target) => {
                self.validate_table_access(
                    &target.catalog,
                    &target.database,
                    &target.stream,
                    UserPrivilegeType::Alter,
                    target.if_exists,
                    false,
                )
                .await
            }
            TagSetObject::UDF(_) | TagSetObject::Procedure(_) => {
                self.validate_access(&GrantObject::Global, UserPrivilegeType::Alter, false, false)
                    .await
            }
        }
    }

    async fn validate_seq_access(&self, seq: String) -> Result<()> {
        if !self
            .ctx
            .get_settings()
            .get_enable_experimental_sequence_privilege_check()?
        {
            return self
                .validate_access(&GrantObject::Global, UserPrivilegeType::Super, false, false)
                .await;
        }

        if self
            .validate_access(&GrantObject::Global, UserPrivilegeType::Super, false, false)
            .await
            .is_ok()
        {
            return Ok(());
        }

        self.validate_access(
            &GrantObject::Sequence(seq),
            UserPrivilegeType::AccessSequence,
            false,
            false,
        )
        .await
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
            return self
                .has_owner_object(session, object, check_current_role_only)
                .await;
        }
        Ok(false)
    }

    async fn has_owner_object(
        &self,
        session: &Arc<Session>,
        object: &OwnershipObject,
        check_current_role_only: bool,
    ) -> Result<bool> {
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
            // If Table ownership check fails, check for Database ownership.
            return Ok(self
                .has_ownership_cached(session, object, check_current_role_only)
                .await?
                || self
                    .has_ownership_cached(session, &database_owner, check_current_role_only)
                    .await?);
        }

        self.has_ownership_cached(session, object, check_current_role_only)
            .await
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
            | GrantObject::Connection(_)
            | GrantObject::Sequence(_)
            | GrantObject::Procedure(_)
            | GrantObject::TableById(_, _, _)
            | GrantObject::MaskingPolicy(_)
            | GrantObject::RowAccessPolicy(_) => true,
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
                    GrantObject::Procedure(_) => Err(ErrorCode::PermissionDenied(format!(
                        "Permission denied: privilege [{:?}] is required on PROCEDURE for user {} with roles [{}]. \
                        Note: Please ensure that your current role have the appropriate permissions to create a new Object",
                        privilege,
                        &current_user.identity().display(),
                        roles_name,
                    ))),
                    GrantObject::Global
                    | GrantObject::UDF(_)
                    | GrantObject::Warehouse(_)
                    | GrantObject::Connection(_)
                    | GrantObject::Sequence(_)
                    | GrantObject::Stage(_)
                    | GrantObject::Database(_, _)
                    | GrantObject::Table(_, _, _)
                    | GrantObject::MaskingPolicy(_)
                    | GrantObject::RowAccessPolicy(_) => Err(ErrorCode::PermissionDenied(format!(
                        "Permission denied: privilege [{:?}] is required on {} for user {} with roles [{}]. \
                        Note: Please ensure that your current role have the appropriate permissions to create a new Object",
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

        // History Config has a inner stage, can not be operator by any user.
        self.access_system_history(None, None, Some(&stage_info.stage_name), privilege)?;

        // Note: validate_stage_access is not used for validate Create Stage privilege
        self.validate_access(
            &GrantObject::Stage(stage_info.stage_name.to_string()),
            privilege,
            false,
            false,
        )
        .await
    }

    async fn resolve_masking_policy_id_by_name(&self, policy_name: &str) -> Result<u64> {
        let meta_api = UserApiProvider::instance().get_meta_store_client();
        let ident = DataMaskNameIdent::new(self.ctx.get_tenant(), policy_name);
        if let Some(policy_id) = meta_api
            .get_data_mask_id(&ident)
            .await
            .map_err(meta_service_error)?
        {
            Ok(*policy_id.data)
        } else {
            Err(ErrorCode::UnknownDatamask(format!(
                "Unknown masking policy {}",
                policy_name
            )))
        }
    }

    async fn find_masking_policy_id_for_column(
        &self,
        catalog: &str,
        database: &str,
        table: &str,
        column: &str,
    ) -> Result<Option<u64>> {
        let tenant = self.ctx.get_tenant();
        let catalog = self.ctx.get_catalog(catalog).await?;
        let table_obj = catalog.get_table(&tenant, database, table).await?;
        let schema = table_obj.schema();
        if let Some((_, field)) = schema.column_with_name(column) {
            if let Some(policy) = table_obj
                .get_table_info()
                .meta
                .column_mask_policy_columns_ids
                .get(&field.column_id)
            {
                return Ok(Some(policy.policy_id));
            }
        }
        Ok(None)
    }

    async fn validate_masking_policy_access(
        &self,
        policy_id: u64,
        policy_name: &str,
    ) -> Result<()> {
        match self
            .validate_access(
                &GrantObject::Global,
                UserPrivilegeType::ApplyMaskingPolicy,
                false,
                false,
            )
            .await
        {
            Ok(_) => return Ok(()),
            Err(err) => {
                if err.code() != ErrorCode::PERMISSION_DENIED {
                    return Err(err);
                }
            }
        }

        match self
            .validate_access(
                &GrantObject::MaskingPolicy(policy_id),
                UserPrivilegeType::ApplyMaskingPolicy,
                false,
                false,
            )
            .await
        {
            Ok(_) => return Ok(()),
            Err(err) => {
                if err.code() != ErrorCode::PERMISSION_DENIED {
                    return Err(err);
                }
            }
        }

        let session = self.ctx.get_current_session();
        if self
            .has_ownership(
                &session,
                &GrantObject::MaskingPolicy(policy_id),
                false,
                false,
            )
            .await?
        {
            return Ok(());
        }

        let current_user = self.ctx.get_current_user()?;
        Err(ErrorCode::PermissionDenied(format!(
            "Permission denied: APPLY MASKING POLICY or OWNERSHIP is required on MASKING POLICY {} for user {}",
            policy_name,
            current_user.identity().display()
        )))
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

    async fn resolve_row_access_policy_id_by_name(&self, policy_name: &str) -> Result<u64> {
        let meta_api = UserApiProvider::instance().get_meta_store_client();
        let ident = RowAccessPolicyNameIdent::new(self.ctx.get_tenant(), policy_name.to_string());
        if let Some((policy_id, _)) = meta_api
            .get_row_access_policy(&ident)
            .await
            .map_err(meta_service_error)?
        {
            Ok(*policy_id.data)
        } else {
            Err(ErrorCode::UnknownRowAccessPolicy(format!(
                "Unknown row access policy {}",
                policy_name
            )))
        }
    }

    async fn find_row_access_policy_for_table(
        &self,
        catalog: &str,
        database: &str,
        table: &str,
    ) -> Result<Option<u64>> {
        let tenant = self.ctx.get_tenant();
        let catalog = self.ctx.get_catalog(catalog).await?;
        let table_obj = catalog.get_table(&tenant, database, table).await?;
        Ok(table_obj
            .get_table_info()
            .meta
            .row_access_policy_columns_ids
            .as_ref()
            .map(|p| p.policy_id))
    }

    async fn validate_row_access_policy_access(
        &self,
        policy_id: u64,
        policy_name: &str,
    ) -> Result<()> {
        match self
            .validate_access(
                &GrantObject::Global,
                UserPrivilegeType::ApplyRowAccessPolicy,
                false,
                false,
            )
            .await
        {
            Ok(_) => return Ok(()),
            Err(err) => {
                if err.code() != ErrorCode::PERMISSION_DENIED {
                    return Err(err);
                }
            }
        }

        match self
            .validate_access(
                &GrantObject::RowAccessPolicy(policy_id),
                UserPrivilegeType::ApplyRowAccessPolicy,
                false,
                false,
            )
            .await
        {
            Ok(_) => return Ok(()),
            Err(err) => {
                if err.code() != ErrorCode::PERMISSION_DENIED {
                    return Err(err);
                }
            }
        }

        let session = self.ctx.get_current_session();
        if self
            .has_ownership(
                &session,
                &GrantObject::RowAccessPolicy(policy_id),
                false,
                false,
            )
            .await?
        {
            return Ok(());
        }

        let current_user = self.ctx.get_current_user()?;
        Err(ErrorCode::PermissionDenied(format!(
            "Permission denied: APPLY ROW ACCESS POLICY or OWNERSHIP is required on ROW ACCESS POLICY {} for user {}",
            policy_name,
            current_user.identity().display()
        )))
    }

    async fn validate_procedure_access(
        &self,
        tenant: &Tenant,
        name: &ProcedureNameIdent,
    ) -> Result<()> {
        if self
            .validate_access(&GrantObject::Global, UserPrivilegeType::Super, false, false)
            .await
            .is_ok()
        {
            return Ok(());
        }

        let req = GetProcedureReq {
            inner: name.clone(),
        };

        let procedure = UserApiProvider::instance()
            .procedure_api(tenant)
            .get_procedure(&req)
            .await
            .map_err(meta_service_error)?;

        match procedure {
            Some(procedure) => {
                self.validate_access(
                    &GrantObject::Procedure(procedure.id),
                    UserPrivilegeType::AccessProcedure,
                    false,
                    false,
                )
                .await
            }
            None => Err(ErrorCode::UnknownProcedure(format!(
                "Unknown procedure {}",
                name
            ))),
        }
    }

    async fn validate_table_function_access(&self, table_func_name: &str) -> Result<()> {
        if SYSTEM_TABLE_FUNCTIONS.iter().any(|x| x == &table_func_name) {
            // need Super privilege to invoke system table functions
            let privilege = UserPrivilegeType::Super;
            let session = self.ctx.get_current_session();
            let current_user = self.ctx.get_current_user()?;
            session
                .validate_privilege(&GrantObject::Global, privilege, true)
                .await.map_err(|err| {
                if err.code() != ErrorCode::PERMISSION_DENIED {
                    err
                } else {
                    let role_name = session.get_current_role().map(|r| r.name).unwrap_or_default();
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
        let catalog_name = cat.name();
        let db_id = self
            .get_database_id(tenant, &catalog_name, &cat, database_name)
            .await?;
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
            InsertInputSource::StreamingLoad { .. } => {}
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
            if plan.user_info.name == user.name && !plan.change_user_option {
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

        let enable_seq_rbac_check = self
            .ctx
            .get_settings()
            .get_enable_experimental_sequence_privilege_check()?;
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

                        if has_priv(&tenant, database, None, show_db_id, table_id, grant_set, false).await? {
                            return Ok(());
                        }

                        let (roles_name, ownerships) =
                            self.get_role_names_and_ownerships(&tenant).await?;
                        check_db_tb_ownership_access(&identity, catalog, database, show_db_id, &ownerships, &roles_name)?;
                    }
                    Some(RewriteKind::ShowStreams(database)) => {
                        let ctl = self.ctx.get_catalog(&ctl_name).await?;
                        let (show_db_id, table_id) = match self.convert_to_id(&tenant, &ctl, database, None, false).await? {
                            ObjectId::Table(db_id, table_id) => { (db_id, Some(table_id)) }
                            ObjectId::Database(db_id) => { (db_id, None) }
                        };
                        if has_priv(&tenant, database, None, show_db_id, table_id, grant_set, true).await? {
                            return Ok(());
                        }
                        let (roles_name, ownerships) =
                            self.get_role_names_and_ownerships(&tenant).await?;
                        check_db_tb_ownership_access(&identity, &ctl_name, database, show_db_id, &ownerships, &roles_name)?;
                    }
                    Some(RewriteKind::ShowColumns(catalog_name, database, table)) => {
                        if self.ctx.is_temp_table(catalog_name, database, table) {
                            return Ok(());
                        }
                        let session = self.ctx.get_current_session();
                        if self.has_ownership(&session, &GrantObject::Table(catalog_name.clone(), database.clone(), table.clone()), false, false).await? ||
                            self.has_ownership(&session, &GrantObject::Database(catalog_name.clone(), database.clone()), false, false).await? {
                            return Ok(());
                        }
                        let catalog = self.ctx.get_catalog(catalog_name).await?;
                        let (db_id, table_id) = match self.convert_to_id(&tenant, &catalog, database, Some(table), false).await? {
                            ObjectId::Table(db_id, table_id) => { (db_id, Some(table_id)) }
                            ObjectId::Database(db_id) => { (db_id, None) }
                        };
                        let has_priv = has_priv(&tenant, database, Some(table), db_id, table_id, grant_set, false).await?;
                        return if has_priv {
                            Ok(())
                        } else {
                            Err(ErrorCode::PermissionDenied(format!(
                                "Permission denied: User {} does not have the required privileges for table '{}.{}'",
                                identity, database, table
                            )))
                        };
                    }
                    Some(RewriteKind::ShowTags) => {
                        self.validate_access(
                            &GrantObject::Global,
                            UserPrivilegeType::Super,
                            false,
                            false,
                        )
                        .await?;
                        return Ok(());
                    }
                    Some(RewriteKind::ShowSequences) => {
                        // will check privilege in show_sequences_table
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
                let mut ownership_objects = Vec::new();
                let mut prepared_ownerships = HashSet::new();
                let mut prepared_tables = HashSet::new();
                for table in metadata.tables() {
                    if table.is_source_of_view()
                        || table.is_source_of_stage()
                        || table.table().is_temp()
                    {
                        continue;
                    }

                    let catalog_name = table.catalog();
                    let database = table.database();
                    let table_name = table.name();
                    let catalog_table = table.table();
                    let table_id = catalog_table.get_id();
                    if !mark_table_access_checked(
                        &mut prepared_tables,
                        catalog_name,
                        database,
                        table_name,
                        table_id,
                    ) {
                        continue;
                    }

                    if database == "information_schema"
                        || database == "system"
                        || is_materialized_view_engine(catalog_table.engine())
                    {
                        continue;
                    }

                    let catalog = self.ctx.get_catalog(catalog_name).await?;
                    if catalog.exists_table_function(table_name) {
                        continue;
                    }
                    let db_id = self
                        .get_database_id(&tenant, catalog_name, &catalog, database)
                        .await?;
                    let database_owner = OwnershipObject::Database {
                        catalog_name: catalog_name.to_string(),
                        db_id,
                    };
                    if prepared_ownerships.insert(database_owner.clone()) {
                        ownership_objects.push(database_owner);
                    }
                    let table_owner = OwnershipObject::Table {
                        catalog_name: catalog_name.to_string(),
                        db_id,
                        table_id,
                    };
                    if prepared_ownerships.insert(table_owner.clone()) {
                        ownership_objects.push(table_owner);
                    }
                }
                self.prefetch_ownerships(&ownership_objects).await?;
                let mut checked_tables = HashSet::new();

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
                    if table.is_source_of_view() || table.table().is_temp() {
                        continue;
                    }

                    let catalog_name = table.catalog();
                    // like this sql: copy into t from (select * from @s3); will bind a mock table with name `system.read_parquet(s3)`
                    // this is no means to check table `system.read_parquet(s3)` privilege
                    if !table.is_source_of_stage() {
                        let database = table.database();
                        let table_name = table.name();
                        let catalog_table = table.table();
                        if mark_table_access_checked(
                            &mut checked_tables,
                            catalog_name,
                            database,
                            table_name,
                            catalog_table.get_id(),
                        ) {
                            if is_materialized_view_engine(catalog_table.engine()) {
                                self.validate_mv_source_access(catalog_table.as_ref()).await?;
                            } else {
                                self.validate_table_access(catalog_name, database, table_name, UserPrivilegeType::Select, false, false).await?
                            }
                        }
                    }
                }
            }
            Plan::ExplainAnalyze { plan, .. } | Plan::Explain { plan, .. } => {
                self.check(ctx, plan).await?
            }
            Plan::ExplainPerf {..} => {}

            Plan::ReportIssue(_) => {}

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
            Plan::UndropDatabase(_) | Plan::DropIndex(_) => {
                // undroptable/db need convert name to id. But because of drop, can not find the id. Upgrade Object to Database.
                self.validate_access(&GrantObject::Global, UserPrivilegeType::Drop, false, false)
                    .await?;
            }
            Plan::DropTableIndex(plan) => {
                self.validate_drop_table_index_access(&plan.catalog, &plan.database, &plan.table)
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
            Plan::AlterStage(plan) => {
                match UserApiProvider::instance()
                    .get_stage(&tenant, &plan.stage_name)
                    .await
                {
                    Ok(stage) => {
                        if enable_experimental_rbac_check {
                            let privileges = vec![UserPrivilegeType::Read, UserPrivilegeType::Write];
                            for privilege in privileges {
                                self.validate_stage_access(&stage, privilege).await?;
                            }
                        } else {
                            self.validate_access(
                                &GrantObject::Global,
                                UserPrivilegeType::Super,
                                false,
                                false,
                            )
                            .await?;
                        }
                    }
                    Err(e) => {
                        return match e.code() {
                            ErrorCode::UNKNOWN_STAGE if plan.if_exists => Ok(()),
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
                if has_priv(&tenant, &plan.database, None, show_db_id, None, grant_set, true).await? {
                    return Ok(());
                }
                let (roles_name, ownerships) =
                    self.get_role_names_and_ownerships(&tenant).await?;
                check_db_tb_ownership_access(&identity, &ctl_name, &plan.database, show_db_id, &ownerships, &roles_name)?;
            }

            // Virtual Column.
            Plan::RefreshVirtualColumn(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Super, false, false).await?
            }
            Plan::VacuumVirtualColumn(plan) => {
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
                if !plan.options.contains_key(OPT_KEY_TEMP_PREFIX) {
                    self.validate_db_access(&plan.catalog, &plan.database, UserPrivilegeType::Create, false).await?;
                }
                if let Some(query) = &plan.as_select {
                    self.check(ctx, query).await?;
                }
            }
            Plan::CreateMaterializedView(plan) => {
                self.validate_db_access(
                    &plan.table_plan.catalog,
                    &plan.table_plan.database,
                    UserPrivilegeType::Create,
                    false,
                )
                .await?;
                self.check(ctx, &plan.query_plan).await?;
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
                if self.ctx.is_temp_table(&plan.catalog, &plan.database, &plan.table) {
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
            Plan::SwapTable(plan) => {
                // only the current role have OWNERSHIP privileges on the tables can execute swap.
                let session = self.ctx.get_current_session();
                let origin_table_owner = self.has_ownership(&session, &GrantObject::Table(plan.catalog.clone(), plan.database.clone(), plan.table.clone()), true, false).await?;
                let target_table_owner = self.has_ownership(&session, &GrantObject::Table(plan.catalog.clone(), plan.database.clone(), plan.target_table.clone()), true, false).await?;
                return if target_table_owner && origin_table_owner {
                    Ok(())
                } else {
                    Err(ErrorCode::PermissionDenied("Insufficient privileges: only the table owner can perform this operation"))
                }
            }
            Plan::SetOptions(plan) => {
                match &plan.target {
                    MaintenanceTarget::Table => self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Alter, false, false).await?,
                    MaintenanceTarget::MaterializedView { .. } => {
                        let table = self.ctx.get_table(&plan.catalog, &plan.database, &plan.table).await?;
                        self.validate_mv_source_access(table.as_ref()).await?
                    }
                }
            }
            Plan::UnsetOptions(plan) => {
                match &plan.target {
                    MaintenanceTarget::Table => self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Alter, false, false).await?,
                    MaintenanceTarget::MaterializedView { .. } => {
                        let table = self.ctx.get_table(&plan.catalog, &plan.database, &plan.table).await?;
                        self.validate_mv_source_access(table.as_ref()).await?
                    }
                }
            }
            Plan::AddTableColumn(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Alter, false, false).await?
            }
            Plan::RenameTableColumn(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Alter, false, false).await?
            }
            Plan::ModifyTableColumn(plan) => {
                self.validate_table_access(
                    &plan.catalog,
                    &plan.database,
                    &plan.table,
                    UserPrivilegeType::Alter,
                    false,
                    false,
                )
                .await?;

                match &plan.action {
                    ModifyColumnAction::SetMaskingPolicy(policy_name, _) => {
                        let policy_id = self
                            .resolve_masking_policy_id_by_name(policy_name)
                            .await?;
                        self.validate_masking_policy_access(policy_id, policy_name)
                            .await?;
                    }
                    ModifyColumnAction::UnsetMaskingPolicy(column) => {
                        if let Some(policy_id) = self
                            .find_masking_policy_id_for_column(
                                &plan.catalog,
                                &plan.database,
                                &plan.table,
                                column,
                            )
                            .await?
                        {
                            let policy_display = policy_id.to_string();
                            self.validate_masking_policy_access(policy_id, &policy_display)
                                .await?;
                        }
                    }
                    _ => {}
                }
            }
            Plan::ModifyTableComment(plan) => {
                match &plan.target {
                    MaintenanceTarget::Table => self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Alter, false, false).await?,
                    MaintenanceTarget::MaterializedView { .. } => {
                        let table = self.ctx.get_table(&plan.catalog, &plan.database, &plan.table).await?;
                        self.validate_mv_source_access(table.as_ref()).await?
                    }
                }
            }
            Plan::ModifyTableConnection(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Alter, false, false).await?
            }
            Plan::AddTableConstraint(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Alter, false, false).await?
            }
            Plan::DropTableConstraint(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Alter, false, false).await?
            }
            Plan::AddTableRowAccessPolicy(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Alter, false, false).await?;
                let policy_id = self
                    .resolve_row_access_policy_id_by_name(&plan.policy)
                    .await?;
                self.validate_row_access_policy_access(policy_id, &plan.policy)
                    .await?
            }
            Plan::DropTableRowAccessPolicy(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Alter, false, false).await?;
                let policy_id = self
                    .resolve_row_access_policy_id_by_name(&plan.policy)
                    .await?;
                self.validate_row_access_policy_access(policy_id, &plan.policy)
                    .await?
            }
            Plan::DropAllTableRowAccessPolicies(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Alter, false, false).await?;
                if let Some(policy_id) = self
                    .find_row_access_policy_for_table(
                        &plan.catalog,
                        &plan.database,
                        &plan.table,
                    )
                    .await?
                {
                    let policy_name = policy_id.to_string();
                    self.validate_row_access_policy_access(policy_id, &policy_name)
                        .await?;
                }
            }
            Plan::DropTableColumn(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Alter, false, false).await?
            }
            Plan::AlterTableClusterKey(plan) => {
                match &plan.target {
                    MaintenanceTarget::Table => self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Alter, false, false).await?,
                    MaintenanceTarget::MaterializedView { .. } => {
                        let table = self.ctx.get_table(&plan.catalog, &plan.database, &plan.table).await?;
                        self.validate_mv_source_access(table.as_ref()).await?
                    }
                }
            }
            Plan::AlterTablePartitionBy(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Alter, plan.if_exists, false).await?
            }
            Plan::DropTableClusterKey(plan) => {
                match &plan.target {
                    MaintenanceTarget::Table => self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Alter, false, false).await?,
                    MaintenanceTarget::MaterializedView { .. } => {
                        let table = self.ctx.get_table(&plan.catalog, &plan.database, &plan.table).await?;
                        self.validate_mv_source_access(table.as_ref()).await?
                    }
                }
            }
            Plan::CreateTableBranch(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Alter, false, false).await?
            }
            Plan::CreateTableTag(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Alter, false, false).await?
            }
            Plan::DropTableBranch(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Alter, false, false).await?
            }
            Plan::DropTableTag(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Alter, false, false).await?
            }
            Plan::RefreshTableCache(_) | Plan::RefreshDatabaseCache(_) => {
                // Only Iceberg support this plan
                return Ok(())
            }
            Plan::ReclusterTable(plan) => {
                match &plan.target {
                    MaintenanceTarget::Table => self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Alter, false, false).await?,
                    MaintenanceTarget::MaterializedView { .. } => {
                        let table = self.ctx.get_table(&plan.catalog, &plan.database, &plan.table).await?;
                        self.validate_mv_source_access(table.as_ref()).await?
                    }
                }
            }
            Plan::TruncateTable(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Delete, false, false).await?
            }
            Plan::OptimizePurge(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Super, false, false).await?
            }
            Plan::OptimizeCompactSegment(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Super, false, false).await?
            }
            Plan::OptimizeCompactBlock { s_expr, .. } => {
                let plan: OptimizeCompactBlock = s_expr.plan().clone().try_into()?;
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Super, false, false).await?
            }
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
                for target in plan.whens.iter().flat_map(|when| when.intos.iter()).chain(plan.opt_else.as_ref().into_iter().flat_map(|e| e.intos.iter())) {
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
                            let udf = condition.get_udf_names()?;
                            self.validate_udf_access(udf).await?;
                        }
                        if let Some(updates) = &matched_evaluator.update {
                            for scalar in updates.values() {
                                let udf = scalar.get_udf_names()?;
                                self.validate_udf_access(udf).await?;
                            }
                        }
                    }
                    for unmatched_evaluator in unmatched_evaluators {
                        if let Some(condition) = &unmatched_evaluator.condition {
                            let udf = condition.get_udf_names()?;
                            self.validate_udf_access(udf).await?;
                        }
                        for value in &unmatched_evaluator.values {
                            let udf = value.get_udf_names()?;
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
            Plan::RefreshLineage(_) => {
                self.validate_access(&GrantObject::Global, UserPrivilegeType::Super, false, false)
                    .await?
            }
            Plan::ShowCreateMaterializedView(plan) => {
                let table = self.ctx.get_table(&plan.catalog, &plan.database, &plan.view_name).await?;
                self.validate_mv_source_access(table.as_ref()).await?
            }
            Plan::DropMaterializedView(plan) => {
                self.validate_db_access(
                    &plan.catalog,
                    &plan.database,
                    UserPrivilegeType::Drop,
                    plan.if_exists,
                )
                .await?
            }
            Plan::RefreshMaterializedView(plan) => {
                let table = self.ctx.get_table(&plan.catalog, &plan.database, &plan.view_name).await?;
                self.validate_mv_source_access(table.as_ref()).await?
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
                    false, false,
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
                    false, false,
                )
                    .await?;
            }
            Plan::GrantRole(_)
            | Plan::GrantPriv(_)
            | Plan::RevokePriv(_)
            | Plan::RevokeRole(_) => {
                self.validate_access(&GrantObject::Global, UserPrivilegeType::Grant, false, false)
                    .await?;
            }
            Plan::Set(plan) => {
                use databend_common_ast::ast::SetType;
                if let SetType::SettingsGlobal = plan.set_type {
                    plan.idents.iter()
                        .try_for_each(|setting| {
                            if setting.eq_ignore_ascii_case("network_policy") && !self.ctx.get_current_user()?.is_account_admin() {
                                return Err(ErrorCode::PermissionDenied("Permission Denied: Setting of network_policy is restricted to account_admin role".to_string()));
                            }
                            Ok(())
                        })?;
                    self.validate_access(&GrantObject::Global, UserPrivilegeType::Super, false, false)
                        .await?;
                }
            }
            Plan::Unset(plan) => {
                use databend_common_ast::ast::SetType;
                if let SetType::SettingsGlobal = plan.unset_type {
                    plan.vars.iter()
                        .try_for_each(|setting| {
                            if setting.eq_ignore_ascii_case("network_policy") && !self.ctx.get_current_user()?.is_account_admin() {
                                    return Err(ErrorCode::PermissionDenied("Permission Denied: Setting of network_policy is restricted to account_admin role".to_string()));
                            }
                            Ok(())
                            }
                        )?;
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
            | Plan::AlterRole(_)
            | Plan::AlterUser(_) => {
                self.validate_access(&GrantObject::Global, UserPrivilegeType::Alter, false, false)
                    .await?;
            }
            Plan::RefreshTableIndex(plan) => {
                self.validate_table_index_access(&plan.catalog, &plan.database, &plan.table)
                    .await?;
            }
            Plan::CopyIntoTable(plan) => {
                self.validate_stage_access(&plan.stage_table_info.stage_info, UserPrivilegeType::Read).await?;
                self.validate_table_access(plan.catalog_info.catalog_name(), &plan.database_name, &plan.table_name, UserPrivilegeType::Insert, false, false).await?;
                if plan.enable_schema_evolution && plan.query.is_none() && !plan.no_file_to_copy {
                    self.validate_table_access(
                        plan.catalog_info.catalog_name(),
                        &plan.database_name,
                        &plan.table_name,
                        UserPrivilegeType::Alter,
                        false,
                        false,
                    )
                    .await?;
                }
                if let Some(query) = &plan.query {
                    self.check(ctx, query).await?;
                }
            }
            Plan::CopyIntoLocation(plan) => {
                self.validate_stage_access(&plan.info.stage, UserPrivilegeType::Write).await?;
                let from = plan.from.clone();
                return self.check(ctx, &from).await;
            }
            Plan::RemoveStage(plan) => {
                self.validate_stage_access(&plan.stage, UserPrivilegeType::Write).await?;
            }
            // Connection
            Plan::CreateConnection(_) => {
                let super_privilege_check_result = self
                    .validate_access(&GrantObject::Global, UserPrivilegeType::Super, false, false)
                    .await;
                if !self
                    .ctx
                    .get_settings()
                    .get_enable_experimental_connection_privilege_check()?
                {
                    return super_privilege_check_result;
                }
                if super_privilege_check_result.is_err() {
                    self.validate_access(&GrantObject::Global, UserPrivilegeType::CreateConnection, true, false)
                        .await?
                }
            }
            Plan::DescConnection(plan) => {
                self.validate_connection_access(plan.name.to_string(), UserPrivilegeType::AccessConnection)
                    .await?;
            }
            Plan::DropConnection(plan) => {
                self.validate_connection_access(plan.name.to_string(), UserPrivilegeType::AccessConnection)
                    .await?;
            }
            Plan::CreateSequence(_) => {
                let super_privilege_check_result = self
                    .validate_access(&GrantObject::Global, UserPrivilegeType::Super, false, false)
                    .await;
                if !enable_seq_rbac_check
                {
                    return super_privilege_check_result;
                }
                if super_privilege_check_result.is_err() {
                    self.validate_access(&GrantObject::Global, UserPrivilegeType::CreateSequence, true, false)
                        .await?
                }
            }
            Plan::DescSequence(plan) => {
                self.validate_seq_access(plan.ident.name().to_string())
                    .await?;
            }
            Plan::DropSequence(plan) => {
                self.validate_seq_access(plan.ident.name().to_string())
                    .await?;
            }
            Plan::SetObjectTags(plan) => {
                self.validate_tag_object_access(&plan.object, &tenant)
                    .await?;
            }
            Plan::UnsetObjectTags(plan) => {
                self.validate_tag_object_access(&plan.object, &tenant)
                    .await?;
            }
            Plan::ShowCreateCatalog(_)
            | Plan::CreateCatalog(_)
            | Plan::DropCatalog(_)
            | Plan::UseCatalog(_)
            | Plan::CreateFileFormat(_)
            | Plan::DropFileFormat(_)
            | Plan::ShowFileFormats(_)
            | Plan::CreateTag(_)
            | Plan::DropTag(_)
            | Plan::CreateNetworkPolicy(_)
            | Plan::AlterNetworkPolicy(_)
            | Plan::DropNetworkPolicy(_)
            | Plan::DescNetworkPolicy(_)
            | Plan::ShowNetworkPolicies(_)
            | Plan::CreatePasswordPolicy(_)
            | Plan::AlterPasswordPolicy(_)
            | Plan::DropPasswordPolicy(_)
            | Plan::DescPasswordPolicy(_)
            | Plan::CreateIndex(_)
            | Plan::CreateNotification(_)
            | Plan::DropNotification(_)
            | Plan::DescNotification(_)
            | Plan::AlterNotification(_)
            | Plan::DescUser(_)
            | Plan::ShowPublicKeys(_)
            | Plan::CreateTask(_)   // TODO: need to build ownership info for task
            | Plan::ShowTasks(_)    // TODO: need to build ownership info for task
            | Plan::DescribeTask(_) // TODO: need to build ownership info for task
            | Plan::ExecuteTask(_)  // TODO: need to build ownership info for task
            | Plan::DropTask(_)     // TODO: need to build ownership info for task
            | Plan::AlterTask(_)
            | Plan::CreateWorker(_)
            | Plan::AlterWorker(_)
            | Plan::DropWorker(_)
            | Plan::ShowWorkers => {
                self.validate_access(&GrantObject::Global, UserPrivilegeType::Super, false, false)
                    .await?;
            }
            Plan::CreateTableIndex(plan) => {
                self.validate_table_index_access(&plan.catalog, &plan.database, &plan.table)
                    .await?;
            }
            Plan::CreateDatamaskPolicy(_) => {
                self
                    .validate_access(
                        &GrantObject::Global,
                        UserPrivilegeType::CreateMaskingPolicy,
                        true,
                        false,
                    )
                    .await?;
            }
            Plan::CreateRowAccessPolicy(_) => {
                self.validate_access(
                    &GrantObject::Global,
                    UserPrivilegeType::CreateRowAccessPolicy,
                    false,
                    false,
                )
                .await?;
            }
            Plan::DropDatamaskPolicy(plan) => {
                if self
                    .validate_access(&GrantObject::Global, UserPrivilegeType::Super, false, false)
                    .await
                    .is_err()
                {
                    match self
                        .resolve_masking_policy_id_by_name(&plan.name)
                        .await
                    {
                        Ok(policy_id) => {
                            self.validate_masking_policy_access(policy_id, &plan.name)
                                .await?;
                        }
                        Err(err)
                            if err.code() == ErrorCode::UNKNOWN_DATAMASK && plan.if_exists => {}
                        Err(err) => return Err(err),
                    }
                }
            }
            Plan::DropRowAccessPolicy(plan) => {
                if self
                    .validate_access(&GrantObject::Global, UserPrivilegeType::Super, false, false)
                    .await
                    .is_err()
                {
                    match self
                        .resolve_row_access_policy_id_by_name(&plan.name)
                        .await
                    {
                        Ok(policy_id) => {
                            self.validate_row_access_policy_access(policy_id, &plan.name)
                                .await?;
                        }
                        Err(err)
                            if err.code() == ErrorCode::UNKNOWN_ROW_ACCESS_POLICY
                                && plan.if_exists => {}
                        Err(err) => return Err(err),
                    }
                }
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
            Plan::DescDatamaskPolicy(plan) => {
                let policy_id = self
                    .resolve_masking_policy_id_by_name(&plan.name)
                    .await?;
                self.validate_masking_policy_access(policy_id, &plan.name)
                    .await?;
            }
            Plan::DescRowAccessPolicy(plan) => {
                let policy_id = self
                    .resolve_row_access_policy_id_by_name(&plan.name)
                    .await?;
                self.validate_row_access_policy_access(policy_id, &plan.name)
                    .await?;
            }
            Plan::Begin => {}
            Plan::CreateProcedure(_) => {
                if self
                    .validate_access(&GrantObject::Global, UserPrivilegeType::Super, true, false)
                    .await.is_err() {
                    self.validate_access(&GrantObject::Global, UserPrivilegeType::CreateProcedure, true, false)
                        .await?
                }
            }
            Plan::CallProcedure(plan) => {
                if self
                    .validate_access(&GrantObject::Global, UserPrivilegeType::Super, false, false)
                    .await
                    .is_err()
                {
                    self.validate_access(
                        &GrantObject::Procedure(plan.procedure_id),
                        UserPrivilegeType::AccessProcedure,
                        false,
                        false,
                    )
                        .await?
                }
            }
            Plan::DropProcedure(plan) => {
                self.validate_procedure_access(&tenant, &plan.name).await?
            }
            Plan::DescProcedure(plan) => {
                self.validate_procedure_access(&tenant, &plan.name).await?
            }
            Plan::ExecuteImmediate(_)
            /*| Plan::ShowCreateProcedure(_)
            | Plan::RenameProcedure(_)*/ => {
                self.validate_access(&GrantObject::Global, UserPrivilegeType::Super, false, false)
                    .await?;
            }
            Plan::Commit => {}
            Plan::Abort => {}
            Plan::ShowConnections(_) => {}
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
            // TODO: rbac for workload
            Plan::ShowWorkloadGroups => {}
            Plan::CreateWorkloadGroup(_) => {}
            Plan::DropWorkloadGroup(_) => {}
            Plan::RenameWorkloadGroup(_) => {}
            Plan::SetWorkloadGroupQuotas(_) => {}
            Plan::UnsetWorkloadGroupQuotas(_) => {}
            Plan::AlterDatabase(plan) => {
                self
                    .validate_db_access(
                        &plan.catalog,
                        &plan.database,
                        UserPrivilegeType::Alter,
                        plan.if_exists,
                    )
                    .await?;
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
    if roles_name
        .iter()
        .any(|role_name| role_name == BUILTIN_ROLE_ACCOUNT_ADMIN)
    {
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
                | OwnershipObject::Warehouse { .. }
                | OwnershipObject::Connection { .. }
                | OwnershipObject::Procedure { .. }
                | OwnershipObject::Sequence { .. }
                | OwnershipObject::MaskingPolicy { .. }
                | OwnershipObject::RowAccessPolicy { .. } => {}
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
    valid_usage_priv: bool,
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

    let grant_set_roles: Vec<String> = grant_set.roles_vec();
    Ok(RoleCacheManager::instance()
        .find_related_roles(tenant, &grant_set_roles)
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
                    if valid_usage_priv {
                        e.privileges().iter().any(|privilege| {
                            UserPrivilegeSet::available_privileges_on_database(false)
                                .has_privilege(privilege)
                        })
                    } else {
                        !(e.privileges().len() == 1
                            && e.privileges().contains(UserPrivilegeType::Usage))
                            && e.privileges().iter().any(|privilege| {
                                UserPrivilegeSet::available_privileges_on_database(false)
                                    .has_privilege(privilege)
                            })
                    }
                }
                GrantObject::Database(_, ldb) => {
                    if valid_usage_priv {
                        *ldb == db_name
                    } else {
                        !(e.privileges().len() == 1
                            && e.privileges().contains(UserPrivilegeType::Usage))
                            && *ldb == db_name
                    }
                }
                GrantObject::DatabaseById(_, ldb) => {
                    if valid_usage_priv {
                        *ldb == db_id
                    } else {
                        !(e.privileges().len() == 1
                            && e.privileges().contains(UserPrivilegeType::Usage))
                            && *ldb == db_id
                    }
                }
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use databend_common_catalog::session_type::SessionType;
    use databend_common_exception::ErrorCode;
    use databend_common_exception::Result;
    use databend_common_meta_app::principal::OwnershipInfo;
    use databend_common_meta_app::principal::OwnershipObject;
    use databend_common_meta_app::tenant::Tenant;
    use parking_lot::Mutex;

    use super::MGET_OWNERSHIP_BATCH_SIZE;
    use super::OwnershipPrefetchApi;
    use super::QueryAccessCache;
    use super::mark_table_access_checked;
    use super::prefetch_ownerships_with_api;
    use super::should_prefetch_ownerships;

    #[derive(Default)]
    struct FakeOwnershipApi {
        ownerships: HashMap<OwnershipObject, Option<OwnershipInfo>>,
        existing_roles: HashMap<String, bool>,
        mget_batch_sizes: Mutex<Vec<usize>>,
        exists_role_calls: Mutex<Vec<String>>,
        mget_error: bool,
        exists_role_error: Option<String>,
        truncate_mget_result: bool,
    }

    #[async_trait::async_trait]
    impl OwnershipPrefetchApi for FakeOwnershipApi {
        async fn mget_ownerships(
            &self,
            _tenant: &Tenant,
            objects: &[OwnershipObject],
        ) -> Result<Vec<Option<OwnershipInfo>>> {
            self.mget_batch_sizes.lock().push(objects.len());
            if self.mget_error {
                return Err(ErrorCode::MetaServiceError("injected MGet failure"));
            }

            let mut ownerships = objects
                .iter()
                .map(|object| self.ownerships.get(object).cloned().unwrap_or(None))
                .collect::<Vec<_>>();
            if self.truncate_mget_result {
                ownerships.pop();
            }
            Ok(ownerships)
        }

        async fn exists_role(&self, _tenant: &Tenant, role: String) -> Result<bool> {
            self.exists_role_calls.lock().push(role.clone());
            if self.exists_role_error.as_ref() == Some(&role) {
                return Err(ErrorCode::MetaServiceError("injected role lookup failure"));
            }
            Ok(self.existing_roles.get(&role).copied().unwrap_or(false))
        }
    }

    fn database(db_id: u64) -> OwnershipObject {
        OwnershipObject::Database {
            catalog_name: "default".to_string(),
            db_id,
        }
    }

    fn table(db_id: u64, table_id: u64) -> OwnershipObject {
        OwnershipObject::Table {
            catalog_name: "default".to_string(),
            db_id,
            table_id,
        }
    }

    fn owned_by(object: &OwnershipObject, role: &str) -> Option<OwnershipInfo> {
        Some(OwnershipInfo {
            object: object.clone(),
            role: role.to_string(),
        })
    }

    fn role_names(roles: &[&str]) -> HashSet<String> {
        roles.iter().map(|role| (*role).to_string()).collect()
    }

    #[tokio::test]
    async fn test_query_access_cache_reuses_successes_and_isolates_keys() -> Result<()> {
        let cache = QueryAccessCache::default();
        let database_loads = AtomicUsize::new(0);

        let first = cache
            .get_or_load_database_id("default", "db", || async {
                database_loads.fetch_add(1, Ordering::Relaxed);
                Ok(11)
            })
            .await?;
        let repeated = cache
            .get_or_load_database_id("default", "db", || async {
                database_loads.fetch_add(1, Ordering::Relaxed);
                Ok(12)
            })
            .await?;
        let other_catalog = cache
            .get_or_load_database_id("other", "db", || async {
                database_loads.fetch_add(1, Ordering::Relaxed);
                Ok(13)
            })
            .await?;
        assert_eq!((first, repeated, other_catalog), (11, 11, 13));
        assert_eq!(database_loads.load(Ordering::Relaxed), 2);

        let object = table(11, 22);
        let ownership_loads = AtomicUsize::new(0);
        assert!(
            cache
                .get_or_load_ownership_check(&object, false, || async {
                    ownership_loads.fetch_add(1, Ordering::Relaxed);
                    Ok(true)
                })
                .await?
        );
        assert!(
            cache
                .get_or_load_ownership_check(&object, false, || async {
                    ownership_loads.fetch_add(1, Ordering::Relaxed);
                    Ok(false)
                })
                .await?
        );
        assert!(
            !cache
                .get_or_load_ownership_check(&object, true, || async {
                    ownership_loads.fetch_add(1, Ordering::Relaxed);
                    Ok(false)
                })
                .await?
        );
        assert_eq!(ownership_loads.load(Ordering::Relaxed), 2);
        assert_eq!(cache.ownership_check(&object, false), Some(true));
        assert_eq!(cache.ownership_check(&object, true), Some(false));
        Ok(())
    }

    #[tokio::test]
    async fn test_query_access_cache_does_not_cache_errors() -> Result<()> {
        let cache = QueryAccessCache::default();
        let database_loads = AtomicUsize::new(0);
        let error = cache
            .get_or_load_database_id("default", "db", || async {
                database_loads.fetch_add(1, Ordering::Relaxed);
                Err::<u64, _>(ErrorCode::MetaServiceError("injected database failure"))
            })
            .await
            .unwrap_err();
        assert_eq!(error.code(), ErrorCode::META_SERVICE_ERROR);
        assert_eq!(
            cache
                .get_or_load_database_id("default", "db", || async {
                    database_loads.fetch_add(1, Ordering::Relaxed);
                    Ok(11)
                })
                .await?,
            11
        );
        assert_eq!(database_loads.load(Ordering::Relaxed), 2);

        let object = table(11, 22);
        let ownership_loads = AtomicUsize::new(0);
        let error = cache
            .get_or_load_ownership_check(&object, false, || async {
                ownership_loads.fetch_add(1, Ordering::Relaxed);
                Err::<bool, _>(ErrorCode::MetaServiceError("injected ownership failure"))
            })
            .await
            .unwrap_err();
        assert_eq!(error.code(), ErrorCode::META_SERVICE_ERROR);
        assert!(
            !cache
                .get_or_load_ownership_check(&object, false, || async {
                    ownership_loads.fetch_add(1, Ordering::Relaxed);
                    Ok(false)
                })
                .await?
        );
        assert_eq!(ownership_loads.load(Ordering::Relaxed), 2);
        Ok(())
    }

    #[test]
    fn test_mark_table_access_checked_uses_catalog_name_and_table_id() {
        let accesses = [
            ("default", "db", "t1", 1),
            ("default", "db", "t1", 1),
            ("default", "db", "t1", 2),
            ("default", "db", "t2", 3),
            ("default", "other_db", "t1", 1),
            ("other", "db", "t1", 1),
        ];
        let mut checked_tables = HashSet::new();
        let checked_count = accesses
            .into_iter()
            .filter(|(catalog, database, table, table_id)| {
                mark_table_access_checked(&mut checked_tables, catalog, database, table, *table_id)
            })
            .count();
        assert_eq!(checked_count, 5);
    }

    #[test]
    fn test_local_sessions_skip_ownership_prefetch() {
        assert!(!should_prefetch_ownerships(&SessionType::Local));
        assert!(should_prefetch_ownerships(&SessionType::MySQL));
        assert!(should_prefetch_ownerships(&SessionType::Dummy));
    }

    #[tokio::test]
    async fn test_ownership_prefetch_batch_boundaries() -> Result<()> {
        let tenant = Tenant::new_literal("tenant");
        let cases = [
            (0, vec![]),
            (1, vec![1]),
            (256, vec![256]),
            (257, vec![256, 1]),
            (512, vec![256, 256]),
            (513, vec![256, 256, 1]),
        ];
        for (object_count, expected_batch_sizes) in cases {
            let objects = (0..object_count).map(database).collect::<Vec<_>>();
            let cache = QueryAccessCache::default();
            let api = FakeOwnershipApi::default();
            prefetch_ownerships_with_api(&cache, &tenant, &objects, &HashSet::new(), &api).await?;
            assert_eq!(*api.mget_batch_sizes.lock(), expected_batch_sizes);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_ownership_prefetch_preserves_fallback_and_role_semantics() -> Result<()> {
        let tenant = Tenant::new_literal("tenant");
        let database = database(1);
        let table_object = table(1, 2);
        let unowned = table(1, 3);
        let deleted_role_owner = table(1, 4);
        let objects = vec![
            table_object.clone(),
            database.clone(),
            unowned.clone(),
            deleted_role_owner.clone(),
            table_object.clone(),
            database.clone(),
        ];
        let ownerships = HashMap::from([
            (table_object.clone(), owned_by(&table_object, "table_owner")),
            (database.clone(), owned_by(&database, "database_owner")),
            (
                deleted_role_owner.clone(),
                owned_by(&deleted_role_owner, "deleted_role"),
            ),
        ]);
        let existing_roles = HashMap::from([
            ("table_owner".to_string(), true),
            ("database_owner".to_string(), true),
            ("deleted_role".to_string(), false),
        ]);
        let api = FakeOwnershipApi {
            ownerships: ownerships.clone(),
            existing_roles: existing_roles.clone(),
            ..Default::default()
        };
        let cache = QueryAccessCache::default();
        prefetch_ownerships_with_api(
            &cache,
            &tenant,
            &objects,
            &role_names(&["database_owner", "account_admin"]),
            &api,
        )
        .await?;

        // The table itself is not owned, but its database is. The normal Table -> Database
        // fallback therefore still grants access.
        assert_eq!(cache.ownership_check(&table_object, false), Some(false));
        assert_eq!(cache.ownership_check(&database, false), Some(true));
        assert!(
            cache.ownership_check(&table_object, false).unwrap()
                || cache.ownership_check(&database, false).unwrap()
        );
        // Missing ownership and ownership held by a deleted role both fall back to account_admin.
        assert_eq!(cache.ownership_check(&unowned, false), Some(true));
        assert_eq!(
            cache.ownership_check(&deleted_role_owner, false),
            Some(true)
        );
        // Prefetch only populates the all-effective-roles scope.
        assert_eq!(cache.ownership_check(&table_object, true), None);
        assert_eq!(*api.mget_batch_sizes.lock(), vec![4]);
        assert_eq!(api.exists_role_calls.lock().len(), 3);

        let api = FakeOwnershipApi {
            ownerships,
            existing_roles,
            ..Default::default()
        };
        let cache = QueryAccessCache::default();
        prefetch_ownerships_with_api(
            &cache,
            &tenant,
            &objects,
            &role_names(&["table_owner", "deleted_role"]),
            &api,
        )
        .await?;
        assert_eq!(cache.ownership_check(&table_object, false), Some(true));
        assert_eq!(cache.ownership_check(&database, false), Some(false));
        assert_eq!(cache.ownership_check(&unowned, false), Some(false));
        assert_eq!(
            cache.ownership_check(&deleted_role_owner, false),
            Some(false)
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_ownership_prefetch_deduplicates_owner_role_lookups_across_batches() -> Result<()>
    {
        let tenant = Tenant::new_literal("tenant");
        let objects = (0..MGET_OWNERSHIP_BATCH_SIZE + 1)
            .map(|db_id| database(db_id as u64))
            .collect::<Vec<_>>();
        let ownerships = objects
            .iter()
            .map(|object| (object.clone(), owned_by(object, "owner")))
            .collect();
        let api = FakeOwnershipApi {
            ownerships,
            existing_roles: HashMap::from([("owner".to_string(), true)]),
            ..Default::default()
        };
        let cache = QueryAccessCache::default();
        prefetch_ownerships_with_api(&cache, &tenant, &objects, &role_names(&["owner"]), &api)
            .await?;
        assert_eq!(*api.mget_batch_sizes.lock(), vec![256, 1]);
        assert_eq!(*api.exists_role_calls.lock(), vec!["owner".to_string()]);
        Ok(())
    }

    #[tokio::test]
    async fn test_ownership_prefetch_propagates_errors_without_caching_denial() -> Result<()> {
        let tenant = Tenant::new_literal("tenant");
        let object = database(1);
        let objects = vec![object.clone()];
        let cache = QueryAccessCache::default();

        let api = FakeOwnershipApi {
            mget_error: true,
            ..Default::default()
        };
        let error = prefetch_ownerships_with_api(&cache, &tenant, &objects, &HashSet::new(), &api)
            .await
            .unwrap_err();
        assert_eq!(error.code(), ErrorCode::META_SERVICE_ERROR);
        assert_eq!(cache.ownership_check(&object, false), None);

        let api = FakeOwnershipApi {
            ownerships: HashMap::from([(object.clone(), owned_by(&object, "owner"))]),
            existing_roles: HashMap::from([("owner".to_string(), true)]),
            ..Default::default()
        };
        prefetch_ownerships_with_api(&cache, &tenant, &objects, &role_names(&["owner"]), &api)
            .await?;
        assert_eq!(cache.ownership_check(&object, false), Some(true));

        let object = database(2);
        let cache = QueryAccessCache::default();
        let api = FakeOwnershipApi {
            ownerships: HashMap::from([(object.clone(), owned_by(&object, "owner"))]),
            exists_role_error: Some("owner".to_string()),
            ..Default::default()
        };
        let error =
            prefetch_ownerships_with_api(&cache, &tenant, &[object.clone()], &HashSet::new(), &api)
                .await
                .unwrap_err();
        assert_eq!(error.code(), ErrorCode::META_SERVICE_ERROR);
        assert_eq!(cache.ownership_check(&object, false), None);

        let object = database(3);
        let cache = QueryAccessCache::default();
        let api = FakeOwnershipApi {
            truncate_mget_result: true,
            ..Default::default()
        };
        let error =
            prefetch_ownerships_with_api(&cache, &tenant, &[object.clone()], &HashSet::new(), &api)
                .await
                .unwrap_err();
        assert_eq!(error.code(), ErrorCode::INTERNAL);
        assert_eq!(cache.ownership_check(&object, false), None);

        let object = database(4);
        let cache = QueryAccessCache::default();
        let api = FakeOwnershipApi {
            ownerships: HashMap::from([(object.clone(), owned_by(&database(5), "owner"))]),
            ..Default::default()
        };
        let error =
            prefetch_ownerships_with_api(&cache, &tenant, &[object.clone()], &HashSet::new(), &api)
                .await
                .unwrap_err();
        assert_eq!(error.code(), ErrorCode::INTERNAL);
        assert_eq!(cache.ownership_check(&object, false), None);
        Ok(())
    }
}
