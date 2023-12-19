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
use databend_common_meta_app::principal::GrantObjectByID;
use databend_common_meta_app::principal::OwnershipInfo;
use databend_common_meta_app::principal::RoleInfo;
use databend_common_meta_app::principal::UserPrivilegeSet;
use databend_common_meta_types::MatchSeq;

use crate::role_util::find_all_related_roles;
use crate::UserApiProvider;

pub const BUILTIN_ROLE_ACCOUNT_ADMIN: &str = "account_admin";
pub const BUILTIN_ROLE_PUBLIC: &str = "public";

impl UserApiProvider {
    // Get one role from by tenant.
    #[async_backtrace::framed]
    pub async fn get_role(&self, tenant: &str, role: String) -> Result<RoleInfo> {
        let builtin_roles = self.builtin_roles();
        if let Some(role_info) = builtin_roles.get(&role) {
            return Ok(role_info.clone());
        }

        let client = self.get_role_api_client(tenant)?;
        let role_data = client.get_role(&role, MatchSeq::GE(0)).await?.data;
        Ok(role_data)
    }

    // Get the tenant all roles list.
    #[async_backtrace::framed]
    pub async fn get_roles(&self, tenant: &str) -> Result<Vec<RoleInfo>> {
        let builtin_roles = self.builtin_roles();
        let seq_roles = self
            .get_role_api_client(tenant)?
            .get_roles()
            .await
            .map_err(|e| e.add_message_back("(while get roles)."))?;
        // overwrite the builtin roles.
        let mut roles = seq_roles
            .into_iter()
            .map(|r| r.data)
            .filter(|r| !builtin_roles.contains_key(&r.name))
            .collect::<Vec<_>>();
        roles.extend(builtin_roles.values().cloned());
        roles.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(roles)
    }

    #[async_backtrace::framed]
    pub async fn exists_role(&self, tenant: &str, role: String) -> Result<bool> {
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
        if if_not_exists && self.exists_role(tenant, role_info.name.clone()).await? {
            return Ok(0);
        }

        let client = self.get_role_api_client(tenant)?;
        let add_role = client.add_role(role_info);
        match add_role.await {
            Ok(res) => Ok(res),
            Err(e) => {
                if if_not_exists && e.code() == ErrorCode::USER_ALREADY_EXISTS {
                    Ok(0)
                } else {
                    Err(e.add_message_back("(while add role)"))
                }
            }
        }
    }

    // Currently we have two builtin roles:
    // 1. ACCOUNT_ADMIN, which has the equivalent privileges of `GRANT ALL ON *.* TO ROLE account_admin`,
    //    it also contains all roles. ACCOUNT_ADMIN can access the data objects which owned by any role.
    // 2. PUBLIC, on the other side only includes the public accessible privileges, but every role
    //    contains the PUBLIC role. The data objects which owned by PUBLIC can be accessed by any role.
    fn builtin_roles(&self) -> HashMap<String, RoleInfo> {
        let mut account_admin = RoleInfo::new(BUILTIN_ROLE_ACCOUNT_ADMIN);
        account_admin.grants.grant_privileges(
            &GrantObject::Global,
            UserPrivilegeSet::available_privileges_on_global(),
        );
        account_admin.grants.grant_privileges(
            &GrantObject::Global,
            UserPrivilegeSet::available_privileges_on_stage(),
        );
        account_admin.grants.grant_privileges(
            &GrantObject::Global,
            UserPrivilegeSet::available_privileges_on_udf(),
        );
        let public = RoleInfo::new(BUILTIN_ROLE_PUBLIC);
        // // Can not get table id and db id in here, so contain use name.
        // public.grants.grant_privileges(
        // &GrantObject::Table(
        // "default".to_string(),
        // "system".to_string(),
        // "one".to_string(),
        // ),
        // UserPrivilegeType::Select.into(),
        // );
        //
        // MySQL all user has this priv.
        // public.grants.grant_privileges(
        // &GrantObject::Database("default".to_string(), "information_schema".to_string()),
        // UserPrivilegeType::Select.into(),
        // );

        let mut result = HashMap::new();
        result.insert(BUILTIN_ROLE_ACCOUNT_ADMIN.into(), account_admin);
        result.insert(BUILTIN_ROLE_PUBLIC.into(), public);
        result
    }

    #[async_backtrace::framed]
    pub async fn grant_ownership_to_role(
        &self,
        tenant: &str,
        object: &GrantObjectByID,
        new_role: &str,
    ) -> Result<()> {
        // from and to role must exists
        self.get_role(tenant, new_role.to_string()).await?;

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
        object: &GrantObjectByID,
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
        role: &String,
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
        role: &String,
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
        role: &String,
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
