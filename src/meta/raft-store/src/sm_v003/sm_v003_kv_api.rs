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
use databend_common_meta_types::Change;
use databend_common_meta_types::SeqV;
use databend_common_meta_types::TxnReply;
use databend_common_meta_types::TxnRequest;
use databend_common_meta_types::UpsertKV;
use databend_common_meta_types::protobuf::StreamItem;
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use map_api::mvcc::ScopedGet;
use map_api::mvcc::ScopedRange;
use seq_marked::SeqValue;
use state_machine_api::UserKey;

use crate::leveled_store::snapshot::StateMachineSnapshot;
use crate::sm_v003::SMV003;
use crate::testing::since_epoch_millis;
use crate::utils::add_cooperative_yielding;
use crate::utils::prefix_right_bound;
use crate::utils::seq_marked_to_seqv;

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
        let local_now_ms = since_epoch_millis();
        let strm = state_machine_snapshot_get_kv_stream(
            self.sm.to_state_machine_snapshot(),
            keys.to_vec(),
            local_now_ms,
        );

        Ok(strm)
    }

    async fn list_kv(&self, prefix: &str) -> Result<KVStream<Self::Error>, Self::Error> {
        let local_now_ms = since_epoch_millis();

        // get an unchanging readonly view
        let snapshot_view = self.sm.data().to_state_machine_snapshot();

        let p = prefix.to_string();

        let strm = if let Some(right) = prefix_right_bound(&p) {
            snapshot_view
                .range(UserKey::new(&p)..UserKey::new(right))
                .await?
        } else {
            snapshot_view.range(UserKey::new(&p)..).await?
        };

        let strm = add_cooperative_yielding(strm, format!("SMV003KVApi::list_kv: {prefix}"))
            // Skip tombstone
            .try_filter_map(|(k, marked)| future::ready(Ok(seq_marked_to_seqv(k, marked))))
            // Skip expired
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

/// A helper function that get many keys in stream.
#[futures_async_stream::try_stream(boxed, ok = StreamItem, error = io::Error)]
async fn state_machine_snapshot_get_kv_stream(
    state_machine_snapshot: StateMachineSnapshot,
    keys: Vec<String>,
    local_now_ms: u64,
) {
    for key in keys {
        let got = state_machine_snapshot
            .get(UserKey::new(key.clone()))
            .await?;

        let seqv = Into::<Option<SeqV>>::into(got);

        let non_expired = SMV003KVApi::non_expired(seqv, local_now_ms);

        yield StreamItem::from((key, non_expired));
    }
}
