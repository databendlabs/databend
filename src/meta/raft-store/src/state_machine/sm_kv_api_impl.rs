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

use databend_common_meta_kvapi::kvapi;
use databend_common_meta_kvapi::kvapi::GetKVReply;
use databend_common_meta_kvapi::kvapi::KVStream;
use databend_common_meta_kvapi::kvapi::MGetKVReply;
use databend_common_meta_kvapi::kvapi::UpsertKVReply;
use databend_common_meta_kvapi::kvapi::UpsertKVReq;
use databend_common_meta_types::protobuf::StreamItem;
use databend_common_meta_types::AppliedState;
use databend_common_meta_types::Cmd;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::SeqV;
use databend_common_meta_types::TxnReply;
use databend_common_meta_types::TxnRequest;
use databend_common_meta_types::UpsertKV;
use futures_util::StreamExt;
use log::debug;

use crate::state_machine::StateMachine;

#[async_trait::async_trait]
impl kvapi::KVApi for StateMachine {
    type Error = MetaError;

    async fn upsert_kv(&self, act: UpsertKVReq) -> Result<UpsertKVReply, Self::Error> {
        let cmd = Cmd::UpsertKV(UpsertKV {
            key: act.key,
            seq: act.seq,
            value: act.value,
            value_meta: act.value_meta,
        });

        let res = self.sm_tree.txn(true, |mut txn_sled_tree| {
            let r = self
                .apply_cmd(&cmd, &mut txn_sled_tree, None, SeqV::<()>::now_ms())
                .unwrap();
            Ok(r)
        })?;

        match res {
            AppliedState::KV(x) => Ok(x),
            _ => {
                panic!("expect AppliedState::KV");
            }
        }
    }

    async fn transaction(&self, txn: TxnRequest) -> Result<TxnReply, Self::Error> {
        let cmd = Cmd::Transaction(txn);

        let res = self.sm_tree.txn(true, |mut txn_sled_tree| {
            let r = self.apply_cmd(&cmd, &mut txn_sled_tree, None, SeqV::<()>::now_ms())?;
            Ok(r)
        })?;

        match res {
            AppliedState::TxnReply(x) => Ok(x),
            _ => {
                unreachable!("expect AppliedState::TxnReply");
            }
        }
    }

    async fn get_kv(&self, key: &str) -> Result<GetKVReply, Self::Error> {
        let sv = self.kvs().get(&key.to_string())?;
        debug!("get_kv sv:{:?}", sv);

        let local_now_ms = SeqV::<()>::now_ms();
        let (_expired, res) = Self::expire_seq_v(sv, local_now_ms);
        Ok(res)
    }

    async fn mget_kv(&self, keys: &[String]) -> Result<MGetKVReply, Self::Error> {
        let kvs = self.kvs();
        let mut res = vec![];

        let local_now_ms = SeqV::<()>::now_ms();

        for x in keys.iter() {
            let v = kvs.get(x)?;
            let (_, v) = Self::expire_seq_v(v, local_now_ms);
            res.push(v)
        }

        Ok(res)
    }

    async fn list_kv(&self, prefix: &str) -> Result<KVStream<Self::Error>, Self::Error> {
        let kvs = self.kvs();
        let kv_pairs = kvs.scan_prefix(&prefix.to_string())?;

        let x = kv_pairs.into_iter();

        let local_now_ms = SeqV::<()>::now_ms();

        // Convert expired to None
        let x = x.map(move |(k, v)| (k, Self::expire_seq_v(Some(v), local_now_ms).1));
        // Remove None
        let x = x.filter(|(_k, v)| v.is_some());

        let x = x.map(|kv: (String, Option<SeqV>)| Ok(StreamItem::from(kv)));

        Ok(futures::stream::iter(x).boxed())
    }
}
