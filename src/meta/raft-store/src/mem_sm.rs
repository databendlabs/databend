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

use std::future;
use std::sync::Arc;

use databend_common_meta_kvapi::kvapi::KVApi;
use databend_common_meta_kvapi::kvapi::KVStream;
use databend_common_meta_types::protobuf::StreamItem;
use databend_common_meta_types::sys_data::SysData;
use databend_common_meta_types::AppliedState;
use databend_common_meta_types::Change;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::SeqV;
use databend_common_meta_types::SeqValue;
use databend_common_meta_types::TxnReply;
use databend_common_meta_types::TxnRequest;
use databend_common_meta_types::UpsertKV;
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use log::debug;
use tokio::sync::Mutex;

use crate::applier::Applier;
use crate::leveled_store::level::Level;
use crate::state_machine::ExpireKey;
use crate::state_machine::StateMachineSubscriber;
use crate::state_machine_api::StateMachineApi;
use crate::state_machine_api_ext::StateMachineApiExt;

/// A pure in-memory state machine as mock for testing.
#[derive(Debug, Default)]
pub struct InMemoryStateMachine {
    level: Level,
    expire_cursor: ExpireKey,
}

impl StateMachineApi for InMemoryStateMachine {
    type Map = Level;

    fn get_expire_cursor(&self) -> ExpireKey {
        self.expire_cursor
    }

    fn set_expire_cursor(&mut self, cursor: ExpireKey) {
        self.expire_cursor = cursor;
    }

    fn map_ref(&self) -> &Self::Map {
        &self.level
    }

    fn map_mut(&mut self) -> &mut Self::Map {
        &mut self.level
    }

    fn sys_data_mut(&mut self) -> &mut SysData {
        &mut self.level.sys_data
    }

    fn get_subscriber(&self) -> Option<&dyn StateMachineSubscriber> {
        None
    }
}

#[derive(Clone)]
pub struct InMemoryMeta {
    sm: Arc<Mutex<InMemoryStateMachine>>,
}

impl InMemoryMeta {
    pub fn new() -> Self {
        InMemoryMeta {
            sm: Arc::new(Mutex::new(InMemoryStateMachine::default())),
        }
    }
    fn non_expired<V>(seq_value: Option<SeqV<V>>, now_ms: u64) -> Option<SeqV<V>> {
        if seq_value.is_expired(now_ms) {
            None
        } else {
            seq_value
        }
    }
}

#[async_trait::async_trait]
impl KVApi for InMemoryMeta {
    type Error = MetaError;

    async fn upsert_kv(&self, upsert_kv: UpsertKV) -> Result<Change<Vec<u8>>, Self::Error> {
        debug!("InMemoryStateMachine::upsert_kv({})", upsert_kv);

        let mut sm = self.sm.lock().await;
        let mut applier = Applier::new(&mut *sm);

        let (prev, result) = applier.upsert_kv(&upsert_kv).await?;

        let st = Change::new(prev, result).into();
        Ok(st)
    }

    async fn get_kv_stream(&self, keys: &[String]) -> Result<KVStream<Self::Error>, Self::Error> {
        debug!("InMemoryStateMachine::get_kv_stream({:?})", keys);

        let local_now_ms = SeqV::<()>::now_ms();

        let mut items = Vec::with_capacity(keys.len());

        let sm = self.sm.lock().await;

        for k in keys {
            let got = sm.get_maybe_expired_kv(k.as_str()).await?;
            let v = Self::non_expired(got, local_now_ms);
            items.push(Ok(StreamItem::from((k.clone(), v))));
        }

        Ok(futures::stream::iter(items).boxed())
    }

    async fn list_kv(&self, prefix: &str) -> Result<KVStream<Self::Error>, Self::Error> {
        debug!("InMemoryStateMachine::list_kv({})", prefix);

        let local_now_ms = SeqV::<()>::now_ms();

        let sm = self.sm.lock().await;

        let strm = sm
            .list_kv(prefix)
            .await?
            .try_filter(move |(_k, v)| future::ready(!v.is_expired(local_now_ms)))
            .map_ok(StreamItem::from)
            .map_err(|e| e.into());

        Ok(strm.boxed())
    }

    async fn transaction(&self, txn: TxnRequest) -> Result<TxnReply, Self::Error> {
        debug!("InMemoryStateMachine::transaction({})", txn);

        let mut sm = self.sm.lock().await;
        let mut applier = Applier::new(&mut *sm);

        let applied_state = applier.apply_txn(&txn).await?;
        match applied_state {
            AppliedState::TxnReply(txn_reply) => Ok(txn_reply),
            _ => unreachable!("expect TxnReply, got {:?}", applied_state),
        }
    }
}
