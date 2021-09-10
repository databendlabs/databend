// Copyright 2020 Datafuse Labs.
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

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use async_trait::async_trait;
use common_exception::Result;
use common_metatypes::KVMeta;
use common_metatypes::MatchSeq;
use common_metatypes::Operation;
use common_store_api::kv_api::MGetKVActionResult;
use common_store_api::GetKVActionResult;
use common_store_api::KVApi;
use common_store_api::PrefixListReply;
use common_store_api::UpsertKVActionResult;
use common_tracing::tracing;
use datafuse_store::configs;
use datafuse_store::meta_service::AppliedState;
use datafuse_store::meta_service::Cmd;
use datafuse_store::meta_service::LogEntry;
use datafuse_store::meta_service::StateMachine;

/// Local storage that provides the API defined by `KVApi`.
/// It is just a wrapped `StateMachine`, which is the same one used by raft driven meta-store service.
/// For a local store, there is no distributed WAL involved,
/// thus it just bypasses the raft log and operate directly on the `StateMachine`.
///
/// Since `StateMachine` is backed with sled::Tree, this impl has the same limitation as meta-store:
/// - A sled::Db has to be a singleton, according to sled doc.
/// - Every unit test has to generate a unique sled::Tree name to create a `LocalKVStore`.
pub struct LocalKVStore {
    inner: StateMachine,
}

impl LocalKVStore {
    /// Creates a KVApi impl backed with a `StateMachine`.
    ///
    /// A LocalKVStore is identified by the `name`.
    /// Caveat: Two instances with the same `name` reference to the same underlying sled::Tree.
    ///
    /// One of the following has to be called to initialize a process-wise sled::Db,
    /// before using `LocalKVStore`:
    /// - `datafuse_store::meta_service::raft_db::init_sled_db`
    /// - `datafuse_store::meta_service::raft_db::init_temp_sled_db`
    #[allow(dead_code)]
    pub async fn new(name: &str) -> common_exception::Result<LocalKVStore> {
        let mut config = configs::Config::empty();

        config.sled_tree_prefix = format!("{}-local-kv-store", name);

        if cfg!(target_os = "macos") {
            tracing::warn!("Disabled fsync for meta data tests. fsync on mac is quite slow");
            config.meta_no_sync = true;
        }

        Ok(LocalKVStore {
            // StateMachine does not need to be replaced, thus we always use id=0
            inner: StateMachine::open(&config, 0).await?,
        })
    }

    /// Creates a KVApi impl with a random and unique name.
    /// For use in testing, one should:
    /// - call `datafuse_store::meta_service::raft_db::init_temp_sled_db()` to
    ///   initialize sled::Db so that persistent data is cleaned up when process exits.
    /// - create a unique LocalKVStore with this function.
    ///
    #[allow(dead_code)]
    pub async fn new_temp() -> common_exception::Result<LocalKVStore> {
        // generate a unique id as part of the name of sled::Tree

        static GLOBAL_SEQ: AtomicUsize = AtomicUsize::new(0);
        let x = GLOBAL_SEQ.fetch_add(1, Ordering::SeqCst);
        let id = 29000_u64 + (x as u64);

        let name = format!("temp-{}", id);

        Self::new(&name).await
    }
}

#[async_trait]
impl KVApi for LocalKVStore {
    async fn upsert_kv(
        &mut self,
        key: &str,
        seq: MatchSeq,
        value: Option<Vec<u8>>,
        value_meta: Option<KVMeta>,
    ) -> Result<UpsertKVActionResult> {
        let cmd = Cmd::UpsertKV {
            key: key.to_string(),
            seq,
            value: value.into(),
            value_meta,
        };

        let res = self
            .inner
            .apply_non_dup(&LogEntry { txid: None, cmd })
            .await?;

        match res {
            AppliedState::KV { prev, result } => Ok(UpsertKVActionResult { prev, result }),
            _ => {
                panic!("expect AppliedState::KV");
            }
        }
    }

    async fn update_kv_meta(
        &mut self,
        key: &str,
        seq: MatchSeq,
        value_meta: Option<KVMeta>,
    ) -> Result<UpsertKVActionResult> {
        let cmd = Cmd::UpsertKV {
            key: key.to_string(),
            seq,
            value: Operation::AsIs,
            value_meta,
        };

        let res = self
            .inner
            .apply_non_dup(&LogEntry { txid: None, cmd })
            .await?;

        match res {
            AppliedState::KV { prev, result } => Ok(UpsertKVActionResult { prev, result }),
            _ => {
                panic!("expect AppliedState::KV");
            }
        }
    }

    async fn get_kv(&mut self, key: &str) -> Result<GetKVActionResult> {
        let res = self.inner.get_kv(key)?;
        Ok(GetKVActionResult { result: res })
    }

    async fn mget_kv(&mut self, key: &[String]) -> Result<MGetKVActionResult> {
        let res = self.inner.mget_kv(key)?;
        Ok(MGetKVActionResult { result: res })
    }

    async fn prefix_list_kv(&mut self, prefix: &str) -> Result<PrefixListReply> {
        let res = self.inner.prefix_list_kv(prefix)?;
        Ok(res)
    }
}
