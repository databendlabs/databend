// Copyright 2022 Datafuse Labs.
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

use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::GrantObject;
use common_meta_types::RoleInfo;
use common_meta_types::UserPrivilegeSet;

use crate::role_util::find_all_related_roles;
use crate::UserApiProvider;

pub const BUILTIN_ROLE_ACCOUNT_ADMIN: &str = "account_admin";
pub const BUILTIN_ROLE_PUBLIC: &str = "public";

impl UserApiProvider {
    // Get one role from by tenant.
    pub async fn get_role(&self, tenant: &str, role: String) -> Result<RoleInfo> {
        let client = self.get_role_api_client(tenant)?;
        let role_data = client.get_role(role, None).await?.data;
        Ok(role_data)
    }

    // Get the tenant all roles list.
    pub async fn get_roles(&self, tenant: &str) -> Result<Vec<RoleInfo>> {
        let client = self.get_role_api_client(tenant)?;
        let get_roles = client.get_roles();

        let mut res = vec![];
        match get_roles.await {
            Err(e) => Err(e.add_message_back("(while get roles).")),
            Ok(seq_roles_info) => {
                for seq_role_info in seq_roles_info {
                    res.push(seq_role_info.data);
                }

                Ok(res)
            }
        }
    }

    // Add a new role info.
    pub async fn add_role(
        &self,
        tenant: &str,
        role_info: RoleInfo,
        if_not_exists: bool,
    ) -> Result<u64> {
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

    // Ensure the builtin roles inside a tenant. Currently we have two builtin roles:
    // 1. ACCOUNT_ADMIN, which has the equivalent privileges of `GRANT ALL ON *.* TO ROLE account_admin`,
    //    it also contains all roles. ACCOUNT_ADMIN can access the data objects which owned by any role.
    // 2. PUBLIC, which have no any privilege by default, but every role contains the PUBLIC role.
    //    The data objects which owned by PUBLIC can be accessed by any role.
    pub async fn ensure_builtin_roles(&self, tenant: &str) -> Result<u64> {
        let mut role_info = RoleInfo::new(BUILTIN_ROLE_ACCOUNT_ADMIN);
        role_info.grants.grant_privileges(
            &GrantObject::Global,
            UserPrivilegeSet::available_privileges_on_global(),
        );
        self.add_role(tenant, role_info, true).await?;
        let role_info = RoleInfo::new(BUILTIN_ROLE_PUBLIC);
        self.add_role(tenant, role_info, true).await
    }

    pub async fn grant_privileges_to_role(
        &self,
        tenant: &str,
        role: String,
        object: GrantObject,
        privileges: UserPrivilegeSet,
    ) -> Result<Option<u64>> {
        let client = self.get_role_api_client(tenant)?;
        client
            .grant_privileges(role, object, privileges, None)
            .await
            .map_err(|e| e.add_message_back("(while set role privileges)"))
    }

    pub async fn revoke_privileges_from_role(
        &self,
        tenant: &str,
        role: String,
        object: GrantObject,
        privileges: UserPrivilegeSet,
    ) -> Result<Option<u64>> {
        let client = self.get_role_api_client(tenant)?;
        client
            .revoke_privileges(role, object, privileges, None)
            .await
            .map_err(|e| e.add_message_back("(while revoke role privileges)"))
    }

    // the grant_role can not have cycle with target_role.
    pub async fn grant_role_to_role(
        &self,
        tenant: &str,
        target_role: String,
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
            .grant_role(target_role, grant_role, None)
            .await
            .map_err(|e| e.add_message_back("(while grant role to role)"))
    }

    pub async fn revoke_role_from_role(
        &self,
        tenant: &str,
        role: String,
        revoke_role: String,
    ) -> Result<Option<u64>> {
        let client = self.get_role_api_client(tenant)?;
        client
            .revoke_role(role, revoke_role, None)
            .await
            .map_err(|e| e.add_message_back("(while revoke role from role)"))
    }

    // Drop a role by name
    pub async fn drop_role(&self, tenant: &str, role: String, if_exists: bool) -> Result<()> {
        let client = self.get_role_api_client(tenant)?;
        let drop_role = client.drop_role(role, None);
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
