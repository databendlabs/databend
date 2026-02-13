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

use databend_common_exception::Result;
use databend_common_meta_app::principal::OwnershipInfo;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::principal::RoleInfo;
use databend_common_meta_app::principal::role_ident;
use databend_common_meta_app::tenant_key::errors::ExistError;
use databend_meta_kvapi::kvapi::ListKVReply;
use databend_meta_types::MatchSeq;
use databend_meta_types::MetaError;
use databend_meta_types::SeqV;

#[async_trait::async_trait]
pub trait RoleApi: Sync + Send {
    async fn add_role(
        &self,
        role_info: RoleInfo,
        can_replace: bool,
    ) -> std::result::Result<std::result::Result<(), ExistError<role_ident::Resource>>, MetaError>;

    #[allow(clippy::ptr_arg)]
    async fn get_role(&self, role: &str, seq: MatchSeq) -> Result<SeqV<RoleInfo>>;

    /// get all roles that store in meta
    async fn get_meta_roles(&self) -> Result<Vec<SeqV<RoleInfo>>>;

    async fn get_raw_meta_roles(&self) -> Result<ListKVReply>;

    async fn list_ownerships(&self) -> Result<Vec<SeqV<OwnershipInfo>>>;

    async fn list_udf_ownerships(&self) -> Result<Vec<OwnershipInfo>>;
    async fn list_stage_ownerships(&self) -> Result<Vec<OwnershipInfo>>;
    async fn list_seq_ownerships(&self) -> Result<Vec<OwnershipInfo>>;
    async fn list_procedure_ownerships(&self) -> Result<Vec<OwnershipInfo>>;
    async fn list_connection_ownerships(&self) -> Result<Vec<OwnershipInfo>>;
    async fn list_warehouse_ownerships(&self) -> Result<Vec<OwnershipInfo>>;

    /// General role update.
    ///
    /// It fetches the role that matches the specified seq number, update it in place, then write it back with the seq it sees.
    ///
    /// Seq number ensures there is no other write happens between get and set.
    #[allow(clippy::ptr_arg)]
    async fn update_role_with<F>(&self, role: &str, seq: MatchSeq, f: F) -> Result<Option<u64>>
    where F: FnOnce(&mut RoleInfo) + Send;

    /// Only drop role will call transfer.
    ///
    /// If a role is dropped, but the owner object is exists,
    ///
    /// The owner role need to update to account_admin.
    async fn transfer_ownership_to_admin(&self, role: &str) -> Result<()>;

    /// Grant ownership would transfer ownership of a object from one role to another role
    ///
    ///
    /// Seq number ensures there is no other write happens between get and set.
    /// from: the role that currently owns the object, it could be empty on the first time
    /// to: the role that will own the object, to.0 is the owner role name, to.1 is the role details
    /// None RoleInfo means the role is built-in role, could only update grant object metadata
    ///
    /// Grant ownership used when create new object, contains two step:
    /// 1. grant ownership privilege obj to role,
    /// 2. kv api upsert new owner object key.
    ///
    /// or:
    ///
    /// 1. revoke ownership on obj from old role,
    /// 2. grant ownership on obj to new role,
    /// 3. kv api upsert new owner object key.
    /// Note: if role/old_role is `account_admin` or `public` no need to revoke/grant ownership privilege
    #[allow(clippy::ptr_arg)]
    async fn grant_ownership(&self, object: &OwnershipObject, role: &str) -> Result<()>;

    /// Remember to call this method when you dropped a OwnerObject like table/database/stage/udf.
    /// Revoke ownership used when drop old object, contains two step:
    /// 1. revoke ownership privilege obj to new role,
    /// 2. kv api delete old owner object key.
    ///
    /// Note: if role is `account_admin` or None no need to revoke
    async fn revoke_ownership(&self, object: &OwnershipObject) -> Result<()>;

    /// Get the ownership info by object. If it's not granted to any role, return PUBLIC
    async fn get_ownership(&self, object: &OwnershipObject) -> Result<Option<OwnershipInfo>>;

    /// Get multiple ownership info by objects in batch.
    async fn mget_ownerships(
        &self,
        objects: &[OwnershipObject],
    ) -> Result<Vec<Option<OwnershipInfo>>>;

    async fn drop_role(&self, role: String, seq: MatchSeq) -> Result<()>;
}
