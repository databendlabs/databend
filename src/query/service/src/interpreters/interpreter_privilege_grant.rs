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

use std::sync::Arc;

use databend_common_base::base::GlobalInstance;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::principal::PrincipalIdentity;
use databend_common_meta_app::principal::UserPrivilegeSet;
use databend_common_meta_app::principal::UserPrivilegeType::Ownership;
use databend_common_meta_app::tenant::Tenant;
use databend_common_sql::plans::GrantPrivilegePlan;
use databend_common_users::RoleCacheManager;
use databend_common_users::UserApiProvider;
use databend_enterprise_resources_management::ResourcesManagement;
use log::debug;
use log::error;
use log::info;

use crate::interpreters::common::validate_grant_object_exists;
use crate::interpreters::util::check_system_history;
use crate::interpreters::util::check_system_history_stage;
use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

#[derive(Debug)]
pub struct GrantPrivilegeInterpreter {
    ctx: Arc<QueryContext>,
    plan: GrantPrivilegePlan,
}

impl GrantPrivilegeInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: GrantPrivilegePlan) -> Result<Self> {
        Ok(GrantPrivilegeInterpreter { ctx, plan })
    }

    async fn convert_to_ownerobject(
        &self,
        tenant: &Tenant,
        object: &GrantObject,
        catalog_name: Option<String>,
    ) -> Result<OwnershipObject> {
        match object {
            GrantObject::Database(_, db_name) => {
                let catalog_name = catalog_name.unwrap();
                let catalog = self.ctx.get_catalog(&catalog_name).await?;
                check_system_history(&catalog, db_name)?;
                let db_id = catalog
                    .get_database(tenant, db_name)
                    .await?
                    .get_db_info()
                    .database_id
                    .db_id;
                Ok(OwnershipObject::Database {
                    catalog_name,
                    db_id,
                })
            }
            GrantObject::Table(_, db_name, table_name) => {
                let catalog_name = catalog_name.unwrap();
                let catalog = self.ctx.get_catalog(&catalog_name).await?;
                check_system_history(&catalog, db_name)?;
                let db_id = catalog
                    .get_database(tenant, db_name)
                    .await?
                    .get_db_info()
                    .database_id
                    .db_id;
                let table_id = catalog
                    .get_table(tenant, db_name.as_str(), table_name)
                    .await?
                    .get_id();
                Ok(OwnershipObject::Table {
                    catalog_name,
                    db_id,
                    table_id,
                })
            }
            GrantObject::TableById(_, db_id, table_id) => {
                let catalog_name = catalog_name.unwrap();
                let catalog = self.ctx.get_catalog(&catalog_name).await?;
                check_system_history(&catalog, &catalog.get_db_name_by_id(*db_id).await?)?;
                Ok(OwnershipObject::Table {
                    catalog_name,
                    db_id: *db_id,
                    table_id: *table_id,
                })
            },
            GrantObject::DatabaseById(_, db_id) => {
                let catalog_name = catalog_name.unwrap();
                let catalog = self.ctx.get_catalog(&catalog_name).await?;
                check_system_history(&catalog, &catalog.get_db_name_by_id(*db_id).await?)?;
                Ok(OwnershipObject::Database {
                    catalog_name,
                    db_id: *db_id,
                })
            },
            GrantObject::Stage(name) => {
                let config = GlobalConfig::instance();
                let sensitive_system_stage = config.log.history.stage_name.clone();
                check_system_history_stage(name, &sensitive_system_stage)?;
                Ok(OwnershipObject::Stage {
                    name: name.to_string(),
                })
            },
            GrantObject::UDF(name) => Ok(OwnershipObject::UDF {
                name: name.to_string(),
            }),
            GrantObject::Warehouse(id) => Ok(OwnershipObject::Warehouse {
                id: id.to_string(),
            }),
            GrantObject::Connection(name) => Ok(OwnershipObject::Connection {
                name: name.to_string(),
            }),
            GrantObject::Sequence(name) => Ok(OwnershipObject::Sequence {
                name: name.to_string(),
            }),
            GrantObject::Procedure(p) => Ok(OwnershipObject::Procedure {
                procedure_id: *p,
            }),
            GrantObject::Global => Err(ErrorCode::IllegalGrant(
                "Illegal GRANT/REVOKE command; please consult the manual to see which privileges can be used",
            )),
        }
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn grant_ownership(
        &self,
        ctx: &Arc<QueryContext>,
        tenant: &Tenant,
        owner_object: &OwnershipObject,
        new_role: &str,
    ) -> Result<()> {
        let user_mgr = UserApiProvider::instance();
        let session = ctx.get_current_session();
        let available_roles = session.get_all_available_roles().await?;

        // the new owner must be one of the available roles
        if !available_roles.iter().any(|r| r.name == new_role) {
            return Err(ErrorCode::IllegalGrant(
                "Illegal GRANT/REVOKE command; invalid new owner",
            ));
        }

        let mut log_msg = format!(
            "{}: grant ownership on {:?} to {}",
            ctx.get_id(),
            owner_object,
            new_role
        );

        // if the object's owner is None, it's considered as PUBLIC, everyone could access it
        let owner = user_mgr.get_ownership(tenant, owner_object).await?;
        if let Some(owner) = owner {
            let can_grant_ownership = available_roles.iter().any(|r| r.name == owner.role);
            log_msg = format!(
                "{}: grant ownership on {:?} from role {} to {}",
                ctx.get_id(),
                owner_object,
                owner.role,
                new_role
            );
            if !can_grant_ownership {
                error!("{}", log_msg);
                return Err(ErrorCode::IllegalGrant(
                    "Illegal GRANT/REVOKE command; only owner can grant ownership",
                ));
            }
        }

        info!("{}", log_msg);
        user_mgr
            .grant_ownership_to_role(tenant, owner_object, new_role)
            .await?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl Interpreter for GrantPrivilegeInterpreter {
    fn name(&self) -> &str {
        "GrantPrivilegeInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        debug!("ctx.id" = self.ctx.get_id().as_str(); "grant_privilege_execute");

        let plan = self.plan.clone();

        validate_grant_privileges(&plan.on, plan.priv_types)?;
        validate_grant_object_exists(&self.ctx, &plan.on).await?;

        // TODO: check user existence
        // TODO: check privilege on granting on the grant object

        let tenant = self.ctx.get_tenant();
        let user_mgr = UserApiProvider::instance();

        match plan.principal {
            PrincipalIdentity::User(user) => {
                user_mgr
                    .grant_privileges_to_user(&tenant, user, plan.on, plan.priv_types)
                    .await?;
            }
            PrincipalIdentity::Role(role) => {
                if plan.priv_types.has_privilege(Ownership) && plan.priv_types.len() == 1 {
                    let owner_object = self
                        .convert_to_ownerobject(&tenant, &plan.on, plan.on.catalog())
                        .await?;
                    if self.ctx.get_current_role().is_some() {
                        if let OwnershipObject::Warehouse { .. } = owner_object {
                            let warehouse_mgr =
                                GlobalInstance::get::<Arc<dyn ResourcesManagement>>();

                            // Only support grant ownership when support_forward_warehouse_request is true
                            if !warehouse_mgr.support_forward_warehouse_request() {
                                return Err(ErrorCode::IllegalGrant(
                                    "Illegal GRANT/REVOKE command; only supported for warehouses managed by the system",
                                ));
                            }
                        }
                        self.grant_ownership(&self.ctx, &tenant, &owner_object, &role)
                            .await?;
                    } else {
                        return Err(databend_common_exception::ErrorCode::UnknownRole(
                            "No current role, cannot grant ownership",
                        ));
                    }
                } else {
                    user_mgr
                        .grant_privileges_to_role(&tenant, &role, plan.on, plan.priv_types)
                        .await?;
                }
                // grant_ownership and grant_privileges_to_role will modify the kv in meta.
                // So we need invalidate the role cache.
                RoleCacheManager::instance().invalidate_cache(&tenant);
            }
        }

        Ok(PipelineBuildResult::create())
    }
}

/// Check if there's any privilege which can not be granted to this GrantObject.
/// Some global privileges can not be granted to a database or table, for example,
/// a KILL statement is meaningless for a table.
pub fn validate_grant_privileges(object: &GrantObject, privileges: UserPrivilegeSet) -> Result<()> {
    let available_privileges = object.available_privileges(true);
    let ok = privileges
        .iter()
        .all(|p| available_privileges.has_privilege(p));
    if !ok {
        return Err(ErrorCode::IllegalGrant(
            "Illegal GRANT/REVOKE command; please consult the manual to see which privileges can be used",
        ));
    }
    Ok(())
}
