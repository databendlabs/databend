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

use std::collections::BTreeMap;

use async_trait::async_trait;
use common_exception::Result;
use common_infallible::RwLock;
use common_metatypes::KVMeta;
use common_metatypes::KVValue;
use common_metatypes::MatchSeq;
use common_metatypes::MatchSeqExt;
use common_metatypes::SeqValue;
use common_store_api::kv_apis::kv_api::MGetKVActionResult;
use common_store_api::GetKVActionResult;
use common_store_api::PrefixListReply;
use common_store_api::UpsertKVActionResult;

use crate::KVApi;

/// this mod is supposed to be used as a MOCK of LocalKVStoreApi
/// crates depend on it, should explicitly enable the "mocks" feature of this crate
/// like this:
///
/// [dev-dependencies]
/// common-store-api-sdk= {path = "../store-api-sdk", features= ["mocks"]}
///
///
type Key = String;
type Value = SeqValue<KVValue>;

#[derive(Default)]
struct Inner {
    // Store State machine uses one sequence for all keys, we simulated this
    seq: u64,
    kv: BTreeMap<Key, Value>,
}

impl Inner {
    fn next_seq(&mut self) -> u64 {
        self.seq += 1;
        self.seq
    }
}

/// A Simple in memory kv store
pub struct LocalKVStore {
    inner: RwLock<Inner>,
}

impl LocalKVStore {
    #[allow(dead_code)]
    pub fn new() -> common_exception::Result<LocalKVStore> {
        let res = LocalKVStore {
            inner: Default::default(),
        };
        Ok(res)
    }
}

#[async_trait]
impl KVApi for LocalKVStore {
    async fn upsert_kv(
        &self,
        key: &str,
        matcher: MatchSeq,
        value: Option<Vec<u8>>,
        value_meta: Option<KVMeta>,
    ) -> Result<UpsertKVActionResult> {
        let (prev, result) = {
            let mut inner = self.inner.write();

            if let Some((seq, v)) = inner.kv.get(key) {
                let r = matcher.match_seq(*seq);
                let prev = (*seq, v.clone());
                if r.is_ok() {
                    // seq match
                    let mut new_v = v.clone();
                    new_v.meta = value_meta;
                    new_v.value = value.unwrap_or_default();
                    let new_seq = inner.next_seq();
                    let result = (new_seq, new_v);
                    inner.kv.insert(key.to_owned(), result.clone());
                    (Some(prev), Some(result))
                } else {
                    // seq not match,
                    // store state machine use a pair of identical values to indicates
                    // that nothing changed, we simulate it.
                    let c = prev.clone();
                    (Some(prev), Some(c))
                }
            } else {
                // key not found, let's insert
                let new_v = KVValue {
                    meta: value_meta,
                    value: value.unwrap_or_default(),
                };
                let new_seq = inner.next_seq();
                let result = (new_seq, new_v);
                inner.kv.insert(key.to_owned(), result.clone());
                (None, Some(result))
            }
        };
        Ok(UpsertKVActionResult { prev, result })
    }

    async fn update_kv_meta(
        &self,
        key: &str,
        matcher: MatchSeq,
        value_meta: Option<KVMeta>,
    ) -> Result<UpsertKVActionResult> {
        let (prev, result) = {
            let mut inner = self.inner.write();
            if let Some((seq, v)) = inner.kv.get(key) {
                let r = matcher.match_seq(*seq);
                let prev = (*seq, v.clone());
                if r.is_ok() {
                    // seq match
                    let mut new_v = v.clone();
                    new_v.meta = value_meta;
                    let new_seq = inner.next_seq();
                    let result = (new_seq, new_v);
                    inner.kv.insert(key.to_owned(), result.clone());
                    (Some(prev), Some(result))
                } else {
                    // seq not match,
                    let c = prev.clone();
                    (Some(prev), Some(c))
                }
            } else {
                // key not found
                (None, None)
            }
        };
        Ok(UpsertKVActionResult { prev, result })
    }

    async fn get_kv(&self, key: &str) -> Result<GetKVActionResult> {
        let inner = self.inner.read();
        let kv = &inner.kv;
        let v = kv.get(key);
        let res = GetKVActionResult { result: v.cloned() };
        Ok(res)
    }

    async fn mget_kv(&self, keys: &[String]) -> Result<MGetKVActionResult> {
        let inner = self.inner.read();
        let kv = &inner.kv;
        let res = MGetKVActionResult {
            result: keys.iter().map(|k| kv.get(k).cloned()).collect::<Vec<_>>(),
        };
        Ok(res)
    }

    async fn prefix_list_kv(&self, prefix: &str) -> Result<PrefixListReply> {
        let inner = self.inner.read();
        let kv = &inner.kv;
        Ok(kv
            .range(prefix.to_owned()..)
            .map(|(k, v)| (k.to_owned(), v.clone()))
            .collect::<Vec<_>>())
    }
}
