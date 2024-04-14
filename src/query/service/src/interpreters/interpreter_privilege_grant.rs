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
use log::debug;
use log::error;
use log::info;

use crate::interpreters::common::validate_grant_object_exists;
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
                let db_id = catalog
                    .get_database(tenant, db_name)
                    .await?
                    .get_db_info()
                    .ident
                    .db_id;
                Ok(OwnershipObject::Database {
                    catalog_name,
                    db_id,
                })
            }
            GrantObject::Table(_, db_name, table_name) => {
                let catalog_name = catalog_name.unwrap();
                let catalog = self.ctx.get_catalog(&catalog_name).await?;
                let db_id = catalog
                    .get_database(tenant, db_name)
                    .await?
                    .get_db_info()
                    .ident
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
            GrantObject::TableById(_, db_id, table_id) => Ok(OwnershipObject::Table {
                catalog_name: catalog_name.unwrap(),
                db_id: *db_id,
                table_id: *table_id,
            }),
            GrantObject::DatabaseById(_, db_id) => Ok(OwnershipObject::Database {
                catalog_name: catalog_name.unwrap(),
                db_id: *db_id,
            }),
            GrantObject::Stage(name) => Ok(OwnershipObject::Stage {
                name: name.to_string(),
            }),
            GrantObject::UDF(name) => Ok(OwnershipObject::UDF {
                name: name.to_string(),
            }),
            GrantObject::Global => Err(ErrorCode::IllegalGrant(
                "Illegal GRANT/REVOKE command; please consult the manual to see which privileges can be used",
            )),
        }
    }

    #[minitrace::trace]
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
            "{}: grant ownership on {:?}  to {}",
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

    #[minitrace::trace]
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
        return Err(databend_common_exception::ErrorCode::IllegalGrant(
            "Illegal GRANT/REVOKE command; please consult the manual to see which privileges can be used",
        ));
    }
    Ok(())
}
