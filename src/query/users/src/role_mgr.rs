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
use databend_common_meta_app::principal::BUILTIN_ROLE_ACCOUNT_ADMIN;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::OwnershipInfo;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::principal::RoleInfo;
use databend_common_meta_app::principal::UserPrivilegeSet;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::tenant::Tenant;

use crate::UserApiProvider;
use crate::meta_service_error;
use crate::role_util::find_all_related_roles;

impl UserApiProvider {
    // Get one role from by tenant.
    #[async_backtrace::framed]
    pub async fn get_role(&self, tenant: &Tenant, role: String) -> Result<RoleInfo> {
        let builtin_roles = self.builtin_roles();
        if let Some(role_info) = builtin_roles.get(&role) {
            return Ok(role_info.clone());
        }

        let client = self.role_api(tenant);
        let role_data = client
            .get_role(&role)
            .await
            .map_err(meta_service_error)?
            .ok_or_else(|| ErrorCode::UnknownRole(format!("Role '{}' does not exist.", role)))?
            .data;
        Ok(role_data)
    }

    // Get the tenant all roles list.
    #[async_backtrace::framed]
    pub async fn get_roles(&self, tenant: &Tenant) -> Result<Vec<RoleInfo>> {
        let builtin_roles = self.builtin_roles();
        let seq_roles = self
            .role_api(tenant)
            .get_meta_roles()
            .await
            .map_err(meta_service_error)?;
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

    // Currently we have to built account_admin role in query:
    // 1. ACCOUNT_ADMIN, which has the equivalent privileges of `GRANT ALL ON *.* TO ROLE account_admin`,
    //    it also contains all roles. ACCOUNT_ADMIN can access the data objects which owned by any role.
    // 2. Because the previous deserialization using from_bits caused forward compatibility issues after adding new privilege type
    //    Until we can confirm all product use https://github.com/datafuselabs/databend/releases/tag/v1.2.321-nightly or later,
    //    We can add account_admin into meta.
    fn builtin_roles(&self) -> HashMap<String, RoleInfo> {
        let mut account_admin = RoleInfo::new(BUILTIN_ROLE_ACCOUNT_ADMIN, None);
        account_admin.grants.grant_privileges(
            &GrantObject::Global,
            UserPrivilegeSet::available_privileges_on_global(),
        );

        let mut result = HashMap::new();
        result.insert(BUILTIN_ROLE_ACCOUNT_ADMIN.into(), account_admin);
        result
    }

    #[async_backtrace::framed]
    pub async fn list_ownerships(
        &self,
        tenant: &Tenant,
    ) -> Result<HashMap<OwnershipObject, String>> {
        let seq_owns = self
            .role_api(tenant)
            .list_ownerships()
            .await
            .map_err(meta_service_error)?;

        let roles: HashMap<OwnershipObject, String> = seq_owns
            .into_iter()
            .map(|r| (r.data.object, r.data.role))
            .collect();
        Ok(roles)
    }

    #[async_backtrace::framed]
    pub async fn exists_role(&self, tenant: &Tenant, role: String) -> Result<bool> {
        if self.builtin_roles().contains_key(&role) {
            return Ok(true);
        }
        Ok(self
            .role_api(tenant)
            .get_role(&role)
            .await
            .map_err(meta_service_error)?
            .is_some())
    }

    // Add a new role info.
    #[async_backtrace::framed]
    pub async fn create_role(
        &self,
        tenant: &Tenant,
        role_info: RoleInfo,
        create_option: &CreateOption,
    ) -> Result<()> {
        let can_replace = create_option.is_overriding();
        let client = self.role_api(tenant);
        client
            .create_role(role_info, can_replace)
            .await
            .map_err(meta_service_error)?
            .or_else(|e| {
                if create_option.if_not_exist() {
                    Ok(())
                } else {
                    Err(ErrorCode::from(e))
                }
            })
    }

    // Update role comment.
    #[async_backtrace::framed]
    pub async fn update_role_comment(
        &self,
        tenant: &Tenant,
        role_name: &str,
        comment: Option<String>,
    ) -> Result<()> {
        let client = self.role_api(tenant);

        client
            .update_role_with(role_name, |role_info| {
                role_info.comment = comment.clone();
            })
            .await
            .map_err(meta_service_error)?
            .map_err(|e| ErrorCode::from(e).add_message_back("(while updating role comment)"))?;
        Ok(())
    }

    #[async_backtrace::framed]
    pub async fn grant_ownership_to_role(
        &self,
        tenant: &Tenant,
        object: &OwnershipObject,
        new_role: &str,
    ) -> Result<()> {
        // from and to role must exists
        self.get_role(tenant, new_role.to_string()).await?;

        let client = self.role_api(tenant);
        client
            .grant_ownership(object, new_role)
            .await
            .map_err(|e| ErrorCode::from(e).add_message_back("(while set role ownership)"))
    }

    #[async_backtrace::framed]
    pub async fn get_ownership(
        &self,
        tenant: &Tenant,
        object: &OwnershipObject,
    ) -> Result<Option<OwnershipInfo>> {
        let client = self.role_api(tenant);
        let ownership = client
            .get_ownership(object)
            .await
            .map_err(meta_service_error)?;
        if let Some(owner) = ownership {
            // if object has ownership, but the owner role is not exists, set owner role to ACCOUNT_ADMIN,
            // only account_admin can access this object.
            // Note: list_ownerships no need to do this check.
            // Because this can cause system.table queries to slow down
            // The intention is that the account admin can grant ownership.
            // So system.tables will display dropped role. It's by design.
            if !self.exists_role(tenant, owner.role.clone()).await? {
                Ok(Some(OwnershipInfo {
                    role: BUILTIN_ROLE_ACCOUNT_ADMIN.to_string(),
                    object: object.clone(),
                }))
            } else {
                Ok(Some(owner))
            }
        } else {
            Ok(None)
        }
    }

    /// Get multiple ownerships in batch.
    /// Note: Unlike get_ownership, this does NOT check if the owner role exists.
    /// This is by design for performance - callers who need role existence check
    /// should use get_ownership for individual objects.
    #[async_backtrace::framed]
    pub async fn mget_ownerships(
        &self,
        tenant: &Tenant,
        objects: &[OwnershipObject],
    ) -> Result<Vec<Option<OwnershipInfo>>> {
        let client = self.role_api(tenant);
        client
            .mget_ownerships(objects)
            .await
            .map_err(meta_service_error)
    }

    #[async_backtrace::framed]
    pub async fn grant_privileges_to_role(
        &self,
        tenant: &Tenant,
        role: &str,
        object: GrantObject,
        privileges: UserPrivilegeSet,
    ) -> Result<()> {
        let client = self.role_api(tenant);
        client
            .update_role_with(role, |ri: &mut RoleInfo| {
                ri.update_role_time();
                ri.grants.grant_privileges(&object, privileges)
            })
            .await
            .map_err(meta_service_error)?
            .map_err(|e| ErrorCode::from(e).add_message_back("(while set role privileges)"))?;
        Ok(())
    }

    #[async_backtrace::framed]
    pub async fn revoke_privileges_from_role(
        &self,
        tenant: &Tenant,
        role: &str,
        object: GrantObject,
        privileges: UserPrivilegeSet,
    ) -> Result<()> {
        let client = self.role_api(tenant);
        client
            .update_role_with(role, |ri: &mut RoleInfo| {
                ri.update_role_time();
                ri.grants.revoke_privileges(&object, privileges)
            })
            .await
            .map_err(meta_service_error)?
            .map_err(|e| ErrorCode::from(e).add_message_back("(while revoke role privileges)"))?;
        Ok(())
    }

    // the grant_role can not have cycle with target_role.
    #[async_backtrace::framed]
    pub async fn grant_role_to_role(
        &self,
        tenant: &Tenant,
        target_role: &String,
        grant_role: String,
    ) -> Result<()> {
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

        let client = self.role_api(tenant);
        client
            .update_role_with(target_role, |ri: &mut RoleInfo| {
                ri.update_role_time();
                ri.grants.grant_role(grant_role)
            })
            .await
            .map_err(meta_service_error)?
            .map_err(|e| ErrorCode::from(e).add_message_back("(while grant role to role)"))?;
        Ok(())
    }

    #[async_backtrace::framed]
    pub async fn revoke_role_from_role(
        &self,
        tenant: &Tenant,
        role: &str,
        revoke_role: &String,
    ) -> Result<()> {
        let client = self.role_api(tenant);
        client
            .update_role_with(role, |ri: &mut RoleInfo| {
                ri.update_role_time();
                ri.grants.revoke_role(revoke_role)
            })
            .await
            .map_err(meta_service_error)?
            .map_err(|e| ErrorCode::from(e).add_message_back("(while revoke role from role)"))?;
        Ok(())
    }

    // Drop a role by name
    #[async_backtrace::framed]
    pub async fn drop_role(&self, tenant: &Tenant, role: String, if_exists: bool) -> Result<()> {
        let client = self.role_api(tenant);
        // If the dropped role owns objects, transfer objects owner to account_admin role.
        client
            .transfer_ownership_to_admin(&role)
            .await
            .map_err(|e| {
                ErrorCode::from(e).add_message_back("(while transfer_ownership_to_admin)")
            })?;

        match client.drop_role(&role).await.map_err(meta_service_error)? {
            Ok(()) => Ok(()),
            Err(e) => {
                if if_exists {
                    Ok(())
                } else {
                    Err(ErrorCode::from(e).add_message_back("(while drop role)"))
                }
            }
        }
    }

    // Find all related roles by role names. Every role have a PUBLIC role, and ACCOUNT_ADMIN
    // default contains every role.
    #[async_backtrace::framed]
    async fn find_related_roles(
        &self,
        tenant: &Tenant,
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
