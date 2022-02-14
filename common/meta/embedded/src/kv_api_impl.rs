// Copyright 2021 Datafuse Labs.
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

use async_trait::async_trait;
use common_meta_api::KVApi;
pub use common_meta_sled_store::init_temp_sled_db;
use common_meta_types::GetKVActionReply;
use common_meta_types::MGetKVActionReply;
use common_meta_types::MetaError;
use common_meta_types::PrefixListReply;
use common_meta_types::UpsertKVAction;
use common_meta_types::UpsertKVActionReply;

use crate::MetaEmbedded;

#[async_trait]
impl KVApi for MetaEmbedded {
    async fn upsert_kv(&self, act: UpsertKVAction) -> Result<UpsertKVActionReply, MetaError> {
        let sm = self.inner.lock().await;
        sm.upsert_kv(act).await
    }

    async fn get_kv(&self, key: &str) -> Result<GetKVActionReply, MetaError> {
        let sm = self.inner.lock().await;
        sm.get_kv(key).await
    }

    async fn mget_kv(&self, key: &[String]) -> Result<MGetKVActionReply, MetaError> {
        let sm = self.inner.lock().await;
        sm.mget_kv(key).await
    }

    async fn prefix_list_kv(&self, prefix: &str) -> Result<PrefixListReply, MetaError> {
        let sm = self.inner.lock().await;
        sm.prefix_list_kv(prefix).await
    }
}
