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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_management::RoleApi;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::OwnershipInfo;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::principal::RoleInfo;
use databend_common_meta_app::principal::UserPrivilegeSet;
use databend_common_meta_types::MatchSeq;
use log::info;

use crate::role_util::find_all_related_roles;
use crate::UserApiProvider;

pub const BUILTIN_ROLE_ACCOUNT_ADMIN: &str = "account_admin";
pub const BUILTIN_ROLE_PUBLIC: &str = "public";

impl UserApiProvider {
    // Get one role from by tenant.
    #[async_backtrace::framed]
    pub async fn get_role(&self, tenant: &str, role: &str) -> Result<RoleInfo> {
        let client = self.get_role_api_client(tenant)?;
        let role_data = client
            .get_role(&role.to_string(), MatchSeq::GE(0))
            .await?
            .data;
        Ok(role_data)
    }

    #[async_backtrace::framed]
    pub async fn get_role_optional(&self, tenant: &str, role: &str) -> Result<Option<RoleInfo>> {
        match self.get_role(tenant, role).await {
            Ok(r) => Ok(Some(r)),
            Err(err) => {
                if err.code() == ErrorCode::UNKNOWN_ROLE {
                    Ok(None)
                } else {
                    return Err(err);
                }
            }
        }
    }

    // Get the tenant all roles list.
    #[async_backtrace::framed]
    pub async fn get_roles(&self, tenant: &str) -> Result<Vec<RoleInfo>> {
        let seq_roles = self
            .get_role_api_client(tenant)?
            .get_roles()
            .await
            .map_err(|e| e.add_message_back("(while get roles)."))?;
        let mut roles = seq_roles.into_iter().map(|r| r.data).collect::<Vec<_>>();
        roles.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(roles)
    }

    /// Ensure the builtin roles account_admin and public exists. Please note that there's
    /// a corner case that if we added another privilege type, we should add it to the
    /// existed account_admin role.
    ///
    /// This function have two calling places:
    /// 1. when the server starts
    /// 2. when the user is authenticated
    #[async_backtrace::framed]
    pub async fn ensure_builtin_roles(&self, tenant: &str) -> Result<()> {
        // CREATE ROLE IF NOT EXISTS public;
        let public = RoleInfo::new(BUILTIN_ROLE_PUBLIC);
        self.add_role(tenant, public, true).await?;

        // if not exists, create account_admin.
        // if new privilege type on Global added, grant it to account_admin.
        let existed_account_admin = self
            .get_role_optional(tenant, BUILTIN_ROLE_ACCOUNT_ADMIN)
            .await?;
        if existed_account_admin.is_none() {
            let account_admin = {
                let mut r = RoleInfo::new(BUILTIN_ROLE_ACCOUNT_ADMIN);
                r.grants.grant_privileges(
                    &GrantObject::Global,
                    UserPrivilegeSet::available_privileges_on_global(),
                );
                r
            };
            self.add_role(tenant, account_admin, true).await?;
        } else if existed_account_admin
            .unwrap()
            .grants
            .find_object_granted_privileges(&GrantObject::Global)
            != UserPrivilegeSet::available_privileges_on_global()
        {
            info!(
                "new privilege type on GrantObject::Global detected, sync it to role account_admin"
            );
            self.grant_privileges_to_role(
                tenant,
                BUILTIN_ROLE_ACCOUNT_ADMIN,
                GrantObject::Global,
                UserPrivilegeSet::available_privileges_on_global(),
            )
            .await?;
        }

        Ok(())
    }

    #[async_backtrace::framed]
    pub async fn get_ownerships(&self, tenant: &str) -> Result<HashMap<OwnershipObject, String>> {
        let seq_owns = self
            .get_role_api_client(tenant)?
            .get_ownerships()
            .await
            .map_err(|e| e.add_message_back("(while get ownerships)."))?;

        let roles: HashMap<OwnershipObject, String> = seq_owns
            .into_iter()
            .map(|r| (r.data.object, r.data.role))
            .collect();
        Ok(roles)
    }

    #[async_backtrace::framed]
    pub async fn exists_role(&self, tenant: &str, role: &str) -> Result<bool> {
        match self.get_role(tenant, role).await {
            Ok(_) => Ok(true),
            Err(e) => {
                if e.code() == ErrorCode::UNKNOWN_ROLE {
                    Ok(false)
                } else {
                    Err(e)
                }
            }
        }
    }

    // Add a new role info.
    #[async_backtrace::framed]
    pub async fn add_role(
        &self,
        tenant: &str,
        role_info: RoleInfo,
        if_not_exists: bool,
    ) -> Result<u64> {
        if if_not_exists && self.exists_role(tenant, &role_info.name).await? {
            return Ok(0);
        }

        let client = self.get_role_api_client(tenant)?;
        let add_role = client.add_role(role_info);
        match add_role.await {
            Ok(res) => Ok(res),
            Err(e) => {
                if if_not_exists && e.code() == ErrorCode::ROLE_ALREADY_EXISTS {
                    Ok(0)
                } else {
                    Err(e.add_message_back("(while add role)"))
                }
            }
        }
    }

    #[async_backtrace::framed]
    pub async fn grant_ownership_to_role(
        &self,
        tenant: &str,
        object: &OwnershipObject,
        new_role: &str,
    ) -> Result<()> {
        // from and to role must exists
        self.get_role(tenant, new_role).await?;

        let client = self.get_role_api_client(tenant)?;
        client
            .grant_ownership(object, new_role)
            .await
            .map_err(|e| e.add_message_back("(while set role ownership)"))
    }

    #[async_backtrace::framed]
    pub async fn get_ownership(
        &self,
        tenant: &str,
        object: &OwnershipObject,
    ) -> Result<Option<OwnershipInfo>> {
        let client = self.get_role_api_client(tenant)?;
        let ownership = client
            .get_ownership(object)
            .await
            .map_err(|e| e.add_message_back("(while get ownership)"))?;
        Ok(ownership)
    }

    #[async_backtrace::framed]
    pub async fn grant_privileges_to_role(
        &self,
        tenant: &str,
        role: &str,
        object: GrantObject,
        privileges: UserPrivilegeSet,
    ) -> Result<Option<u64>> {
        let client = self.get_role_api_client(tenant)?;
        client
            .update_role_with(role, MatchSeq::GE(1), |ri: &mut RoleInfo| {
                ri.grants.grant_privileges(&object, privileges)
            })
            .await
            .map_err(|e| e.add_message_back("(while set role privileges)"))
    }

    #[async_backtrace::framed]
    pub async fn revoke_privileges_from_role(
        &self,
        tenant: &str,
        role: &str,
        object: GrantObject,
        privileges: UserPrivilegeSet,
    ) -> Result<Option<u64>> {
        let client = self.get_role_api_client(tenant)?;
        client
            .update_role_with(role, MatchSeq::GE(1), |ri: &mut RoleInfo| {
                ri.grants.revoke_privileges(&object, privileges)
            })
            .await
            .map_err(|e| e.add_message_back("(while revoke role privileges)"))
    }

    // the grant_role can not have cycle with target_role.
    #[async_backtrace::framed]
    pub async fn grant_role_to_role(
        &self,
        tenant: &str,
        target_role: &String,
        grant_role: String,
    ) -> Result<Option<u64>> {
        let related_roles = self
            .find_related_roles(tenant, &[grant_role.clone()])
            .await?;
        let have_cycle = related_roles.iter().any(|r| r.identity() == target_role);
        if have_cycle {
            return Err(ErrorCode::InvalidRole(format!(
                "{} contains {}, can not be grant to {}",
                &grant_role, &target_role, &target_role
            )));
        }

        let client = self.get_role_api_client(tenant)?;
        client
            .update_role_with(target_role, MatchSeq::GE(1), |ri: &mut RoleInfo| {
                ri.grants.grant_role(grant_role)
            })
            .await
            .map_err(|e| e.add_message_back("(while grant role to role)"))
    }

    #[async_backtrace::framed]
    pub async fn revoke_role_from_role(
        &self,
        tenant: &str,
        role: &str,
        revoke_role: &String,
    ) -> Result<Option<u64>> {
        let client = self.get_role_api_client(tenant)?;
        client
            .update_role_with(role, MatchSeq::GE(1), |ri: &mut RoleInfo| {
                ri.grants.revoke_role(revoke_role)
            })
            .await
            .map_err(|e| e.add_message_back("(while revoke role from role)"))
    }

    // Drop a role by name
    #[async_backtrace::framed]
    pub async fn drop_role(&self, tenant: &str, role: String, if_exists: bool) -> Result<()> {
        let client = self.get_role_api_client(tenant)?;
        let drop_role = client.drop_role(role, MatchSeq::GE(1));
        match drop_role.await {
            Ok(res) => Ok(res),
            Err(e) => {
                if if_exists && e.code() == ErrorCode::UNKNOWN_ROLE {
                    Ok(())
                } else {
                    Err(e.add_message_back("(while set drop role)"))
                }
            }
        }
    }

    // Find all related roles by role names. Every role have a PUBLIC role, and ACCOUNT_ADMIN
    // default contains every role.
    #[async_backtrace::framed]
    async fn find_related_roles(
        &self,
        tenant: &str,
        role_identities: &[String],
    ) -> Result<Vec<RoleInfo>> {
        let tenant_roles_map = self
            .get_roles(tenant)
            .await?
            .into_iter()
            .map(|r| (r.identity().to_string(), r))
            .collect::<HashMap<_, _>>();
        Ok(find_all_related_roles(&tenant_roles_map, role_identities))
    }
}
