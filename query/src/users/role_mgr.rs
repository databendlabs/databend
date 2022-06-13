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

use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::GrantObject;
use common_meta_types::RoleInfo;
use common_meta_types::UserPrivilegeSet;

use crate::users::UserApiProvider;

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
                if if_not_exists && e.code() == ErrorCode::user_already_exists_code() {
                    Ok(0)
                } else {
                    Err(e.add_message_back("(while add role)"))
                }
            }
        }
    }

    // Ensure the builtin roles inside a tenant. Currently the only builtin role is account_admin,
    // which has the equivalent privileges of `GRANT ALL ON *.* TO ROLE account_admin`.
    pub async fn ensure_builtin_roles(&self, tenant: &str) -> Result<u64> {
        let mut role_info = RoleInfo::new("account_admin");
        role_info.grants.grant_privileges(
            &GrantObject::Global,
            UserPrivilegeSet::available_privileges_on_global(),
        );
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

    pub async fn grant_role_to_role(
        &self,
        tenant: &str,
        role: String,
        grant_role: String,
    ) -> Result<Option<u64>> {
        let client = self.get_role_api_client(tenant)?;
        client
            .grant_role(role, grant_role, None)
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
                if if_exists && e.code() == ErrorCode::unknown_role_code() {
                    Ok(())
                } else {
                    Err(e.add_message_back("(while set drop role)"))
                }
            }
        }
    }
}
