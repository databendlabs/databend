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

use databend_common_meta_api::meta_txn_error::MetaTxnError;
use databend_common_meta_app::principal::OwnershipInfo;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::principal::RoleInfo;
use databend_common_meta_app::principal::role_ident;
use databend_common_meta_app::tenant_key::errors::UnknownError;
use databend_meta_client::kvapi::ListKVReply;
use databend_meta_client::types::MetaError;
use databend_meta_client::types::SeqV;

#[async_trait::async_trait]
pub trait RoleApi: Sync + Send {
    /// Create a role.
    ///
    /// Returns `Ok(true)` if the role was created successfully.
    /// When `can_replace` is true, an existing role is overwritten and still returns `Ok(true)`.
    /// Returns `Ok(false)` only when `can_replace` is false and the role already exists.
    async fn create_role(&self, role_info: RoleInfo, can_replace: bool) -> Result<bool, MetaError>;

    async fn get_role(&self, role: &str) -> Result<Option<SeqV<RoleInfo>>, MetaError>;

    /// get all roles that store in meta
    async fn get_meta_roles(&self) -> Result<Vec<SeqV<RoleInfo>>, MetaError>;

    async fn get_raw_meta_roles(&self) -> Result<ListKVReply, MetaError>;

    async fn list_ownerships(&self) -> Result<Vec<SeqV<OwnershipInfo>>, MetaError>;

    /// List ownerships filtered by owner object type.
    async fn list_ownerships_by_owner_object(
        &self,
        owner_object: OwnershipObject,
    ) -> Result<Vec<OwnershipInfo>, MetaError>;

    /// General role update.
    ///
    /// It fetches the role, updates it in place, then writes it back with the seq it sees.
    ///
    /// Seq number ensures there is no other write happens between get and set.
    async fn update_role_with<F>(
        &self,
        role: &str,
        f: F,
    ) -> Result<Result<u64, UnknownError<role_ident::Resource>>, MetaError>
    where
        F: FnOnce(&mut RoleInfo) + Send;

    /// Only drop role will call transfer.
    ///
    /// If a role is dropped, but the owner object is exists,
    ///
    /// The owner role need to update to account_admin.
    async fn transfer_ownership_to_admin(&self, role: &str) -> Result<(), MetaTxnError>;

    /// Set `role` as the owner of `object`, replacing any previous owner.
    #[allow(clippy::ptr_arg)]
    async fn grant_ownership(
        &self,
        object: &OwnershipObject,
        role: &str,
    ) -> Result<(), MetaTxnError>;

    /// Delete the ownership of `object`. Call it after the object itself is dropped.
    async fn revoke_ownership(&self, object: &OwnershipObject) -> Result<(), MetaTxnError>;

    /// Get the ownership info by object. If it's not granted to any role, return PUBLIC
    async fn get_ownership(
        &self,
        object: &OwnershipObject,
    ) -> Result<Option<OwnershipInfo>, MetaError>;

    /// Get multiple ownership info by objects in batch.
    async fn mget_ownerships(
        &self,
        objects: &[OwnershipObject],
    ) -> Result<Vec<Option<OwnershipInfo>>, MetaError>;

    /// Drop a role by name.
    ///
    /// Returns `Ok(true)` if the role was dropped, `Ok(false)` if it did not exist.
    async fn drop_role(&self, role: &str) -> Result<bool, MetaError>;
}
