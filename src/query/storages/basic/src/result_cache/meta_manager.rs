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
use std::time::Duration;

use databend_common_exception::Result;
use databend_common_meta_store::MetaStore;
use databend_meta_kvapi::kvapi::KVApi;
use databend_meta_kvapi::kvapi::KvApiExt;
use databend_meta_kvapi::kvapi::ListOptions;
use databend_meta_types::MatchSeq;
use databend_meta_types::MetaSpec;
use databend_meta_types::Operation;
use databend_meta_types::SeqV;
use databend_meta_types::UpsertKV;

use crate::meta_service_error;
use crate::result_cache::common::ResultCacheValue;

pub struct ResultCacheMetaManager {
    ttl: u64,
    inner: Arc<MetaStore>,
}

impl ResultCacheMetaManager {
    pub fn create(inner: Arc<MetaStore>, ttl: u64) -> Self {
        Self { ttl, inner }
    }

    #[async_backtrace::framed]
    pub async fn set(
        &self,
        key: String,
        value: ResultCacheValue,
        seq: MatchSeq,
        ttl: Duration,
    ) -> Result<()> {
        let value = serde_json::to_vec(&value)?;
        let _ = self
            .inner
            .upsert_kv(UpsertKV {
                key,
                seq,
                value: Operation::Update(value),
                value_meta: Some(MetaSpec::new_ttl(ttl)),
            })
            .await
            .map_err(meta_service_error)?;
        Ok(())
    }

    #[async_backtrace::framed]
    pub async fn get(&self, key: String) -> Result<Option<ResultCacheValue>> {
        let raw = self.inner.get_kv(&key).await.map_err(meta_service_error)?;
        match raw {
            None => Ok(None),
            Some(SeqV { data, .. }) => {
                let value = serde_json::from_slice(&data)?;
                Ok(Some(value))
            }
        }
    }

    #[async_backtrace::framed]
    pub async fn list(&self, prefix: &str) -> Result<Vec<ResultCacheValue>> {
        let result = self
            .inner
            .list_kv_collect(ListOptions::unlimited(prefix))
            .await
            .map_err(meta_service_error)?;

        let mut r = vec![];
        for (_key, val) in result {
            let u = serde_json::from_slice::<ResultCacheValue>(&val.data)?;

            r.push(u);
        }

        Ok(r)
    }

    pub fn get_ttl(&self) -> u64 {
        self.ttl
    }
}
