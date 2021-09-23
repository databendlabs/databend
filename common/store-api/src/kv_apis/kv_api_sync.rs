//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use std::sync::Arc;

use async_trait::async_trait;
use common_metatypes::KVMeta;
use common_metatypes::MatchSeq;

use crate::kv_apis::kv_api::MGetKVActionResult;
use crate::util::STORE_RUNTIME;
use crate::GetKVActionResult;
use crate::KVApi;
use crate::PrefixListReply;
use crate::UpsertKVActionResult;

pub trait SyncKVApi: KVApi
where Self: Clone + 'static
{
    fn sync_upsert_kv(
        &self,
        key: &str,
        seq: MatchSeq,
        value: Option<Vec<u8>>,
        value_meta: Option<KVMeta>,
    ) -> common_exception::Result<UpsertKVActionResult> {
        let me = self.clone();
        let key = key.to_owned();
        STORE_RUNTIME.block_on(
            async move { me.upsert_kv(&key, seq, value, value_meta).await },
            None,
        )?
    }

    fn sync_update_kv_meta(
        &self,
        key: &str,
        seq: MatchSeq,
        value_meta: Option<KVMeta>,
    ) -> common_exception::Result<UpsertKVActionResult> {
        let me = self.clone();
        let key = key.to_owned();
        STORE_RUNTIME.block_on(
            async move { me.update_kv_meta(&key, seq, value_meta).await },
            None,
        )?
    }

    fn sync_get_kv(&self, key: &str) -> common_exception::Result<GetKVActionResult> {
        let me = self.clone();
        let key = key.to_owned();
        STORE_RUNTIME.block_on(async move { me.get_kv(&key).await }, None)?
    }

    fn sync_mget_kv(&self, keys: &[String]) -> common_exception::Result<MGetKVActionResult> {
        let me = self.clone();
        let keys = keys.to_owned();
        STORE_RUNTIME.block_on(async move { me.mget_kv(&keys).await }, None)?
    }

    fn sync_prefix_list_kv(&self, prefix: &str) -> common_exception::Result<PrefixListReply> {
        let me = self.clone();
        let prefix = prefix.to_owned();
        STORE_RUNTIME.block_on(async move { me.prefix_list_kv(&prefix).await }, None)?
    }
}

impl<T> SyncKVApi for T where T: KVApi + Clone + 'static {}

#[async_trait]
impl KVApi for Arc<dyn KVApi> {
    async fn upsert_kv(
        &self,
        key: &str,
        seq: MatchSeq,
        value: Option<Vec<u8>>,
        value_meta: Option<KVMeta>,
    ) -> common_exception::Result<UpsertKVActionResult> {
        self.as_ref().upsert_kv(key, seq, value, value_meta).await
    }

    async fn update_kv_meta(
        &self,
        key: &str,
        seq: MatchSeq,
        value_meta: Option<KVMeta>,
    ) -> common_exception::Result<UpsertKVActionResult> {
        self.as_ref().update_kv_meta(key, seq, value_meta).await
    }

    async fn get_kv(&self, key: &str) -> common_exception::Result<GetKVActionResult> {
        self.as_ref().get_kv(key).await
    }

    async fn mget_kv(&self, key: &[String]) -> common_exception::Result<MGetKVActionResult> {
        self.as_ref().mget_kv(key).await
    }

    async fn prefix_list_kv(&self, prefix: &str) -> common_exception::Result<PrefixListReply> {
        self.as_ref().prefix_list_kv(prefix).await
    }
}
