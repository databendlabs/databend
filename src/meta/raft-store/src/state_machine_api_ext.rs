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

use databend_common_meta_types::CmdContext;
use databend_common_meta_types::EvalExpireTime;
use databend_common_meta_types::MatchSeqExt;
use databend_common_meta_types::Operation;
use databend_common_meta_types::SeqV;
use databend_common_meta_types::SeqValue;
use databend_common_meta_types::UpsertKV;
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use log::debug;
use log::warn;

use crate::leveled_store::map_api::AsMap;
use crate::leveled_store::map_api::IOResultStream;
use crate::leveled_store::map_api::MapApi;
use crate::leveled_store::map_api::MapApiExt;
use crate::leveled_store::map_api::MapApiRO;
use crate::marked::Marked;
use crate::state_machine::ExpireKey;
use crate::state_machine_api::StateMachineApi;
use crate::utils::prefix_right_bound;

#[async_trait::async_trait]
pub trait StateMachineApiExt: StateMachineApi {
    /// It returns 2 entries: the previous one and the new one after upsert.
    async fn upsert_kv_primary_index(
        &mut self,
        upsert_kv: &UpsertKV,
        cmd_ctx: &CmdContext,
    ) -> Result<(Marked<Vec<u8>>, Marked<Vec<u8>>), io::Error> {
        let kv_meta = upsert_kv.value_meta.as_ref().map(|m| m.to_kv_meta(cmd_ctx));

        let prev = self.map_ref().str_map().get(&upsert_kv.key).await?.clone();

        if upsert_kv.seq.match_seq(prev.seq()).is_err() {
            return Ok((prev.clone(), prev));
        }

        let (prev, mut result) = match &upsert_kv.value {
            Operation::Update(v) => {
                self.map_mut()
                    .set(upsert_kv.key.clone(), Some((v.clone(), kv_meta.clone())))
                    .await?
            }
            Operation::Delete => self.map_mut().set(upsert_kv.key.clone(), None).await?,
            Operation::AsIs => {
                MapApiExt::update_meta(self.map_mut(), upsert_kv.key.clone(), kv_meta.clone())
                    .await?
            }
        };

        let expire_ms = kv_meta.eval_expire_at_ms();
        if expire_ms < self.get_expire_cursor().time_ms {
            // The record has expired, delete it at once.
            //
            // Note that it must update first then delete,
            // in order to keep compatibility with the old state machine.
            // Old SM will just insert an expired record, and that causes the system seq increase by 1.
            let (_p, r) = self.map_mut().set(upsert_kv.key.clone(), None).await?;
            result = r;
        };

        debug!(
            "applied upsert: {:?}; prev: {:?}; res: {:?}",
            upsert_kv, prev, result
        );

        Ok((prev, result))
    }

    /// List kv entries by prefix.
    ///
    /// If a value is expired, it is not returned.
    async fn list_kv(&self, prefix: &str) -> Result<IOResultStream<(String, SeqV)>, io::Error> {
        let p = prefix.to_string();

        let strm = if let Some(right) = prefix_right_bound(&p) {
            self.map_ref().str_map().range(p.clone()..right).await?
        } else {
            self.map_ref().str_map().range(p.clone()..).await?
        };

        let strm = strm
            // Return only keys with the expected prefix
            .try_take_while(move |(k, _)| future::ready(Ok(k.starts_with(&p))))
            // Skip tombstone
            .try_filter_map(|(k, marked)| {
                let seqv = Into::<Option<SeqV>>::into(marked);
                let res = seqv.map(|x| (k, x));
                future::ready(Ok(res))
            });

        Ok(strm.boxed())
    }

    /// Update the secondary index for speeding up expiration operation.
    ///
    /// Remove the expiration index for the removed record, and add a new one for the new record.
    async fn update_expire_index(
        &mut self,
        key: impl ToString + Send,
        removed: &Marked<Vec<u8>>,
        added: &Marked<Vec<u8>>,
    ) -> Result<(), io::Error> {
        // No change, no need to update expiration index
        if removed == added {
            return Ok(());
        }

        // Remove previous expiration index, add a new one.

        if let Some(exp_ms) = removed.get_expire_at_ms() {
            self.map_mut()
                .set(ExpireKey::new(exp_ms, removed.order_key().seq()), None)
                .await?;
        }

        if let Some(exp_ms) = added.get_expire_at_ms() {
            let k = ExpireKey::new(exp_ms, added.order_key().seq());
            let v = key.to_string();
            self.map_mut().set(k, Some((v, None))).await?;
        }

        Ok(())
    }

    /// List expiration index by expiration time,
    /// upto current time(exclusive) in milliseconds.
    ///
    /// Only records with expire time less than current time will be returned.
    /// Expire time that equals to current time is not considered expired.
    async fn list_expire_index(
        &self,
        curr_time_ms: u64,
    ) -> Result<IOResultStream<(ExpireKey, String)>, io::Error> {
        let start = self.get_expire_cursor();

        // curr_time > expire_at => expired
        let end = ExpireKey::new(curr_time_ms, 0);

        // There is chance the raft leader produce smaller timestamp for later logs.
        if start >= end {
            return Ok(futures::stream::empty().boxed());
        }

        let strm = self.map_ref().expire_map().range(start..end).await?;

        let strm = strm
            // Return only non-deleted records
            .try_filter_map(|(k, marked)| {
                let expire_entry = marked.unpack().map(|(v, _v_meta)| (k, v));
                future::ready(Ok(expire_entry))
            });

        Ok(strm.boxed())
    }

    fn update_expire_cursor(&mut self, log_time_ms: u64) {
        if log_time_ms < self.get_expire_cursor().time_ms {
            warn!(
                "update_last_cleaned: log_time_ms {} < last_cleaned_expire.time_ms {}",
                log_time_ms,
                self.get_expire_cursor().time_ms
            );
            return;
        }

        self.set_expire_cursor(ExpireKey::new(log_time_ms, 0));
    }

    /// Get a cloned value by key.
    ///
    /// It does not check expiration of the returned entry.
    async fn get_maybe_expired_kv(&self, key: &str) -> Result<Option<SeqV>, io::Error> {
        let got = self.map_ref().str_map().get(key).await?;
        let seqv = Into::<Option<SeqV>>::into(got);
        Ok(seqv)
    }
}

impl<T> StateMachineApiExt for T where T: StateMachineApi {}
