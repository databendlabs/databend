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

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use async_trait::async_trait;
use common_base::tokio::sync::Mutex;
use common_exception::Result;
use common_meta_api::KVApi;
use common_meta_raft_store::config::RaftConfig;
use common_meta_raft_store::state_machine::AppliedState;
use common_meta_raft_store::state_machine::StateMachine;
pub use common_meta_sled_store::init_temp_sled_db;
use common_meta_types::Cmd;
use common_meta_types::GetKVActionReply;
use common_meta_types::KVMeta;
use common_meta_types::MGetKVActionReply;
use common_meta_types::MatchSeq;
use common_meta_types::Operation;
use common_meta_types::PrefixListReply;
use common_meta_types::UpsertKVActionReply;
use common_tracing::tracing;

/// Local storage that provides the API defined by `KVApi+MetaApi`.
///
/// It is just a wrapped `StateMachine`, which is the same one used by raft driven metasrv.
/// For a local kv, there is no distributed WAL involved,
/// thus it just bypasses the raft log and operate directly on the `StateMachine`.
///
/// Since `StateMachine` is backed with sled::Tree, this impl has the same limitation as metasrv:
/// - A sled::Db has to be a singleton, according to sled doc.
/// - Every unit test has to generate a unique sled::Tree name to create a `MetaEmbedded`.
#[derive(Clone)]
pub struct MetaEmbedded {
    inner: Arc<Mutex<StateMachine>>,
}

impl MetaEmbedded {
    /// Creates a KVApi impl backed with a `StateMachine`.
    ///
    /// A LocalKVStore is identified by the `name`.
    /// Caveat: Two instances with the same `name` reference to the same underlying sled::Tree.
    ///
    /// One of the following has to be called to initialize a process-wise sled::Db,
    /// before using `LocalKVStore`:
    /// - `common_meta_sled_store::init_sled_db`
    /// - `common_meta_sled_store::init_temp_sled_db`
    #[allow(dead_code)]
    pub async fn new(name: &str) -> common_exception::Result<MetaEmbedded> {
        let mut config = RaftConfig::empty();

        config.sled_tree_prefix = format!("{}-local-kv", name);

        if cfg!(target_os = "macos") {
            tracing::warn!("Disabled fsync for meta data tests. fsync on mac is quite slow");
            config.no_sync = true;
        }

        Ok(MetaEmbedded {
            // StateMachine does not need to be replaced, thus we always use id=0
            inner: Arc::new(Mutex::new(StateMachine::open(&config, 0).await?)),
        })
    }

    /// Creates a KVApi impl with a random and unique name.
    #[allow(dead_code)]
    pub async fn new_temp() -> common_exception::Result<MetaEmbedded> {
        let temp_dir = tempfile::tempdir()?;
        common_meta_sled_store::init_temp_sled_db(temp_dir);

        // generate a unique id as part of the name of sled::Tree

        static GLOBAL_SEQ: AtomicUsize = AtomicUsize::new(0);
        let x = GLOBAL_SEQ.fetch_add(1, Ordering::SeqCst);
        let id = 29000_u64 + (x as u64);

        let name = format!("temp-{}", id);

        Self::new(&name).await
    }

    pub fn sync_new_temp() -> common_exception::Result<MetaEmbedded> {
        futures::executor::block_on(MetaEmbedded::new_temp())
    }
}

#[async_trait]
impl KVApi for MetaEmbedded {
    async fn upsert_kv(
        &self,
        key: &str,
        seq: MatchSeq,
        value: Option<Vec<u8>>,
        value_meta: Option<KVMeta>,
    ) -> Result<UpsertKVActionReply> {
        let cmd = Cmd::UpsertKV {
            key: key.to_string(),
            seq,
            value: value.into(),
            value_meta,
        };

        let mut sm = self.inner.lock().await;
        let res = sm.apply_cmd(&cmd).await?;

        match res {
            AppliedState::KV { prev, result } => Ok(UpsertKVActionReply { prev, result }),
            _ => {
                panic!("expect AppliedState::KV");
            }
        }
    }

    async fn update_kv_meta(
        &self,
        key: &str,
        seq: MatchSeq,
        value_meta: Option<KVMeta>,
    ) -> Result<UpsertKVActionReply> {
        let cmd = Cmd::UpsertKV {
            key: key.to_string(),
            seq,
            value: Operation::AsIs,
            value_meta,
        };

        let mut sm = self.inner.lock().await;
        let res = sm.apply_cmd(&cmd).await?;

        match res {
            AppliedState::KV { prev, result } => Ok(UpsertKVActionReply { prev, result }),
            _ => {
                panic!("expect AppliedState::KV");
            }
        }
    }

    async fn get_kv(&self, key: &str) -> Result<GetKVActionReply> {
        let sm = self.inner.lock().await;
        let res = sm.get_kv(key)?;
        Ok(GetKVActionReply { result: res })
    }

    async fn mget_kv(&self, key: &[String]) -> Result<MGetKVActionReply> {
        let sm = self.inner.lock().await;
        let res = sm.mget_kv(key)?;
        Ok(MGetKVActionReply { result: res })
    }

    async fn prefix_list_kv(&self, prefix: &str) -> Result<PrefixListReply> {
        let sm = self.inner.lock().await;
        let res = sm.prefix_list_kv(prefix)?;
        Ok(res)
    }
}
