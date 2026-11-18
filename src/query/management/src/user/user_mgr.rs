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

use databend_common_meta_api::kv_pb_api::KVPbApi;
use databend_common_meta_api::kv_pb_api::UpsertPB;
use databend_common_meta_api::kv_pb_api::decode_seqv;
use databend_common_meta_app::KeyExistsBuilder;
use databend_common_meta_app::KeyUnknownBuilder;
use databend_common_meta_app::KeyWithTenant;
use databend_common_meta_app::principal::TenantUserIdent;
use databend_common_meta_app::principal::UserIdentity;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::principal::tenant_user_ident::Resource as UserIdentResource;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_app::tenant_key::errors::ExistError;
use databend_common_meta_app::tenant_key::errors::UnknownError;
use databend_meta_client::ClientHandle;
use databend_meta_client::kvapi;
use databend_meta_client::kvapi::DirName;
use databend_meta_client::kvapi::KvApiExt;
use databend_meta_client::kvapi::ListKVReply;
use databend_meta_client::kvapi::ListOptions;
use databend_meta_client::kvapi::StructKey;
use databend_meta_client::types::MatchSeq;
use databend_meta_client::types::MetaError;
use databend_meta_client::types::SeqV;
use databend_meta_client::types::With;
use databend_meta_plugin_cache::Cache;
use databend_meta_runtime::DatabendRuntime;
use fastrace::func_name;
use futures::TryStreamExt;

use crate::user::user_api::UserApi;

pub struct UserMgr {
    kv_api: Arc<dyn kvapi::KVApi<Error = MetaError>>,
    tenant: Tenant,
    /// The users of the tenant, if watched: see `get_watched_user`.
    watched: Option<Arc<Cache>>,
}

impl UserMgr {
    pub fn create(kv_api: Arc<dyn kvapi::KVApi<Error = MetaError>>, tenant: &Tenant) -> Self {
        UserMgr {
            kv_api,
            tenant: tenant.clone(),
            watched: None,
        }
    }

    /// Watch the users of `tenant`, for `with_watched`.
    pub async fn watch(client: Arc<ClientHandle<DatabendRuntime>>, tenant: &Tenant) -> Arc<Cache> {
        let cache = Cache::new(client, Self::users_dir(tenant), "users").await;
        Arc::new(cache)
    }

    /// Read the users by `get_watched_user` from `watched`, a `watch` of the tenant.
    pub fn with_watched(mut self, watched: Arc<Cache>) -> Self {
        self.watched = Some(watched);
        self
    }

    /// A user, from the watched copy if there is one and it holds the user, or else from
    /// meta-service, see `Cache::try_get_ready`. A user from the copy may lag a change by the
    /// watch delay. Use `get_user` where a change must be seen at once, or to update the user.
    #[async_backtrace::framed]
    pub async fn get_watched_user(
        &self,
        user: &UserIdentity,
    ) -> Result<Option<SeqV<UserInfo>>, MetaError> {
        let key = self.user_ident(user).to_string_key();
        let local = self.watched.as_ref().and_then(|w| w.try_get_ready(&key));
        let Some(value) = local else {
            return self.get_user(user).await;
        };
        let user = decode_seqv::<UserInfo>(value, || format!("decode value of {key}"))?;
        Ok(Some(user))
    }

    fn user_ident(&self, user: &UserIdentity) -> TenantUserIdent {
        TenantUserIdent::new_user_host(self.tenant.clone(), &user.username, &user.hostname)
    }

    fn user_prefix(&self) -> String {
        Self::user_dummy_ident(&self.tenant).tenant_prefix()
    }

    /// The directory of the users of `tenant`, which a watch of them takes.
    fn users_dir(tenant: &Tenant) -> String {
        Self::user_dummy_ident(tenant).tenant_dir()
    }

    fn user_dummy_ident(tenant: &Tenant) -> TenantUserIdent {
        TenantUserIdent::new_user_host(tenant.clone(), "dummy", "dummy")
    }
}

#[async_trait::async_trait]
impl UserApi for UserMgr {
    #[async_backtrace::framed]
    #[fastrace::trace]
    async fn create_user(
        &self,
        user_info: UserInfo,
        overriding: bool,
    ) -> Result<Result<(), ExistError<UserIdentResource, UserIdentity>>, MetaError> {
        let ident = TenantUserIdent::new_user_host(
            self.tenant.clone(),
            &user_info.name,
            &user_info.hostname,
        );

        let seq = if overriding {
            MatchSeq::GE(0)
        } else {
            MatchSeq::Exact(0)
        };

        let req = UpsertPB::insert(ident.clone(), user_info).with(seq);
        let res = self.kv_api.upsert_pb(&req).await?;

        if !overriding && res.prev.is_some() {
            return Ok(Err(ident.exist_error(func_name!())));
        }

        Ok(Ok(()))
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    async fn get_user(&self, user: &UserIdentity) -> Result<Option<SeqV<UserInfo>>, MetaError> {
        let ident = self.user_ident(user);
        self.kv_api.get_pb(&ident).await
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    async fn get_users(&self) -> Result<Vec<SeqV<UserInfo>>, MetaError> {
        let ident = TenantUserIdent::new_user_host(self.tenant.clone(), "dummy", "dummy");
        let dir_name = DirName::new(ident);

        self.kv_api
            .list_pb(ListOptions::unlimited(&dir_name))
            .await?
            .map_ok(|item| item.seqv)
            .try_collect()
            .await
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    async fn get_raw_users(&self) -> Result<ListKVReply, MetaError> {
        let user_prefix = self.user_prefix();
        self.kv_api
            .list_kv_collect(ListOptions::unlimited(user_prefix.as_str()))
            .await
    }

    /// Update a user in place.
    ///
    /// This method internally calls `get_user` to fetch the current user state,
    /// applies the update function, then writes back with optimistic concurrency control.
    #[async_backtrace::framed]
    async fn update_user_with<F>(
        &self,
        user: &UserIdentity,
        f: F,
    ) -> Result<Result<u64, UnknownError<UserIdentResource, UserIdentity>>, MetaError>
    where
        F: FnOnce(&mut UserInfo) + Send,
    {
        let ident = self.user_ident(user);

        let Some(seqv) = self.get_user(user).await? else {
            return Ok(Err(ident.unknown_error(func_name!())));
        };

        let seq = seqv.seq;
        let mut user_info = seqv.data;
        f(&mut user_info);

        self.upsert_user_info(&user_info, seq).await
    }

    #[async_backtrace::framed]
    async fn upsert_user_info(
        &self,
        user: &UserInfo,
        seq: u64,
    ) -> Result<Result<u64, UnknownError<UserIdentResource, UserIdentity>>, MetaError> {
        let ident = TenantUserIdent::new_user_host(self.tenant.clone(), &user.name, &user.hostname);

        let req = UpsertPB::update(ident.clone(), user.clone()).with(MatchSeq::Exact(seq));
        let res = self.kv_api.upsert_pb(&req).await?;

        match res.result {
            Some(SeqV { seq, .. }) => Ok(Ok(seq)),
            None => Ok(Err(ident.unknown_error(func_name!()))),
        }
    }

    #[async_backtrace::framed]
    async fn drop_user(
        &self,
        user: &UserIdentity,
    ) -> Result<Result<(), UnknownError<UserIdentResource, UserIdentity>>, MetaError> {
        let ident = self.user_ident(user);
        let req = UpsertPB::delete(ident.clone());
        let res = self.kv_api.upsert_pb(&req).await?;

        match res.prev {
            Some(_) => Ok(Ok(())),
            None => Ok(Err(ident.unknown_error(func_name!()))),
        }
    }
}
