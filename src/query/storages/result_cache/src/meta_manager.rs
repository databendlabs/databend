// Copyright 2023 Datafuse Labs.
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

use common_exception::Result;
use common_meta_kvapi::kvapi::KVApi;
use common_meta_store::MetaStore;
use common_meta_types::KVMeta;
use common_meta_types::MatchSeq;
use common_meta_types::Operation;
use common_meta_types::UpsertKV;
use common_users::UserApiProvider;

use crate::common::ResultCacheValue;

pub(super) struct ResultCacheMetaManager {
    key: String,
    ttl: u64,
    inner: Arc<MetaStore>,
}

impl ResultCacheMetaManager {
    pub fn create(key: String, ttl: u64) -> Self {
        let inner = UserApiProvider::instance().get_meta_store_client();
        Self { key, ttl, inner }
    }

    pub async fn set(&self, value: ResultCacheValue, expire_at: u64) -> Result<()> {
        let value = serde_json::to_vec(&value)?;

        let _ = self
            .inner
            .upsert_kv(UpsertKV {
                key: self.key.clone(),
                seq: MatchSeq::GE(0),
                value: Operation::Update(value),
                value_meta: Some(KVMeta {
                    expire_at: Some(expire_at),
                }),
            })
            .await?;
        Ok(())
    }

    pub fn get_ttl(&self) -> u64 {
        self.ttl
    }
}
