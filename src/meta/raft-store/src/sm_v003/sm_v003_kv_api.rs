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
use std::io;

use databend_common_meta_kvapi::kvapi;
use databend_common_meta_kvapi::kvapi::KVStream;
use databend_common_meta_types::protobuf::StreamItem;
use databend_common_meta_types::Change;
use databend_common_meta_types::SeqV;
use databend_common_meta_types::SeqValue;
use databend_common_meta_types::TxnReply;
use databend_common_meta_types::TxnRequest;
use databend_common_meta_types::UpsertKV;
use futures_util::StreamExt;
use futures_util::TryStreamExt;

use crate::sm_v003::SMV003;
use crate::state_machine_api_ext::StateMachineApiExt;

/// A wrapper that implements KVApi **readonly** methods for the state machine.
pub struct SMV003KVApi<'a> {
    pub(crate) sm: &'a SMV003,
}

#[async_trait::async_trait]
impl kvapi::KVApi for SMV003KVApi<'_> {
    type Error = io::Error;

    async fn upsert_kv(&self, _req: UpsertKV) -> Result<Change<Vec<u8>>, Self::Error> {
        unreachable!("write operation SM2KVApi::upsert_kv is disabled")
    }

    async fn get_kv_stream(&self, keys: &[String]) -> Result<KVStream<Self::Error>, Self::Error> {
        let local_now_ms = SeqV::<()>::now_ms();

        let mut items = Vec::with_capacity(keys.len());

        for k in keys {
            let got = self.sm.get_maybe_expired_kv(k).await?;
            let v = Self::non_expired(got, local_now_ms);
            items.push(Ok(StreamItem::from((k.clone(), v))));
        }

        Ok(futures::stream::iter(items).boxed())
    }

    async fn list_kv(&self, prefix: &str) -> Result<KVStream<Self::Error>, Self::Error> {
        let local_now_ms = SeqV::<()>::now_ms();

        let strm = self
            .sm
            .list_kv(prefix)
            .await?
            .try_filter(move |(_k, v)| future::ready(!v.is_expired(local_now_ms)))
            .map_ok(StreamItem::from);

        Ok(strm.boxed())
    }

    async fn transaction(&self, _txn: TxnRequest) -> Result<TxnReply, Self::Error> {
        unreachable!("write operation SM2KVApi::transaction is disabled")
    }
}

impl SMV003KVApi<'_> {
    fn non_expired<V>(seq_value: Option<SeqV<V>>, now_ms: u64) -> Option<SeqV<V>> {
        if seq_value.is_expired(now_ms) {
            None
        } else {
            seq_value
        }
    }
}
