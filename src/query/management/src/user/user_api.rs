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

use databend_common_meta_app::principal::UserIdentity;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::principal::tenant_user_ident::Resource as UserIdentResource;
use databend_common_meta_app::tenant_key::errors::ExistError;
use databend_common_meta_app::tenant_key::errors::UnknownError;
use databend_meta_kvapi::kvapi::ListKVReply;
use databend_meta_types::MetaError;
use databend_meta_types::SeqV;

#[async_trait::async_trait]
pub trait UserApi: Sync + Send {
    /// Create a user.
    /// Returns `Ok(Ok(()))` on success.
    /// Returns `Ok(Err(ExistError))` if the user already exists and `overriding` is false.
    async fn create_user(
        &self,
        user_info: UserInfo,
        overriding: bool,
    ) -> Result<Result<(), ExistError<UserIdentResource, UserIdentity>>, MetaError>;

    /// Get a user by identity.
    /// Returns `Some(SeqV)` if the user exists, `None` otherwise.
    async fn get_user(&self, user: &UserIdentity) -> Result<Option<SeqV<UserInfo>>, MetaError>;

    async fn get_users(&self) -> Result<Vec<SeqV<UserInfo>>, MetaError>;

    /// Just get user count in meta
    async fn get_raw_users(&self) -> Result<ListKVReply, MetaError>;

    /// Update a user in place.
    ///
    /// This method internally calls `get_user` to fetch the current user state,
    /// applies the update function, then writes back with optimistic concurrency control.
    /// The seq from the internal `get_user` is used for CAS (compare-and-swap).
    ///
    /// Returns `Ok(Ok(new_seq))` on success.
    /// Returns `Ok(Err(UnknownError))` if the user does not exist or seq mismatch.
    async fn update_user_with<F>(
        &self,
        user: &UserIdentity,
        f: F,
    ) -> Result<Result<u64, UnknownError<UserIdentResource, UserIdentity>>, MetaError>
    where
        F: FnOnce(&mut UserInfo) + Send;

    /// Upsert user info with exact seq matching.
    /// Returns `Ok(Ok(new_seq))` on success.
    /// Returns `Ok(Err(UnknownError))` if seq mismatch.
    async fn upsert_user_info(
        &self,
        user: &UserInfo,
        seq: u64,
    ) -> Result<Result<u64, UnknownError<UserIdentResource, UserIdentity>>, MetaError>;

    /// Drop a user.
    /// Returns `Ok(Ok(()))` if the user was dropped.
    /// Returns `Ok(Err(UnknownError))` if the user did not exist.
    async fn drop_user(
        &self,
        user: &UserIdentity,
    ) -> Result<Result<(), UnknownError<UserIdentResource, UserIdentity>>, MetaError>;
}
