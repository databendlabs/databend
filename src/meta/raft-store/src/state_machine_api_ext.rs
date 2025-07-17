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
use std::ops::RangeBounds;
use std::time::Duration;

use databend_common_meta_types::CmdContext;
use databend_common_meta_types::Expirable;
use databend_common_meta_types::MatchSeqExt;
use databend_common_meta_types::Operation;
use databend_common_meta_types::SeqV;
use databend_common_meta_types::SeqValue;
use databend_common_meta_types::UpsertKV;
use display_more::DisplayUnixTimeStampExt;
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use log::debug;
use log::warn;
use map_api::map_api::MapApi;
use map_api::map_api_ro::MapApiRO;
use map_api::IOResultStream;
use seq_marked::SeqMarked;

use crate::leveled_store::map_api::AsMap;
use crate::leveled_store::map_api::MapApiHelper;
use crate::marked::MetaValue;
use crate::state_machine::ExpireKey;
use crate::state_machine::UserKey;
use crate::state_machine_api::StateMachineApi;
use crate::utils::add_cooperative_yielding;
use crate::utils::prefix_right_bound;

#[async_trait::async_trait]
pub trait StateMachineApiExt: StateMachineApi {
    /// It returns 2 entries: the previous one and the new one after upsert.
    async fn upsert_kv_primary_index(
        &mut self,
        upsert_kv: &UpsertKV,
        cmd_ctx: &CmdContext,
    ) -> Result<(SeqMarked<MetaValue>, SeqMarked<MetaValue>), io::Error> {
        let kv_meta = upsert_kv.value_meta.as_ref().map(|m| m.to_kv_meta(cmd_ctx));

        let prev = self.user_map().get(upsert_kv.key.as_ref()).await?.clone();

        if upsert_kv.seq.match_seq(&prev.seq()).is_err() {
            return Ok((prev.clone(), prev));
        }

        let (prev, mut result) = match &upsert_kv.value {
            Operation::Update(v) => {
                self.user_map_mut()
                    .set(
                        UserKey::new(&upsert_kv.key),
                        Some((kv_meta.clone(), v.clone())),
                    )
                    .await?
            }
            Operation::Delete => {
                self.user_map_mut()
                    .set(UserKey::new(&upsert_kv.key), None)
                    .await?
            }
            #[allow(deprecated)]
            Operation::AsIs => {
                MapApiHelper::update_meta(
                    self.user_map_mut(),
                    UserKey::new(&upsert_kv.key),
                    kv_meta.clone(),
                )
                .await?
            }
        };

        let expire_ms = kv_meta.expires_at_ms();
        let curr_time_ms = cmd_ctx.time().millis();
        if expire_ms < curr_time_ms {
            warn!(
                "upsert_kv_primary_index: expired key inserted: {} < timestamp in log entry: {}; key: {}",
                Duration::from_millis(expire_ms).display_unix_timestamp_short(),
                Duration::from_millis(curr_time_ms)
                    .display_unix_timestamp_short(),
                upsert_kv.key
            );
            // The record has expired, delete it at once.
            //
            // Note that it must update first then delete,
            // in order to keep compatibility with the old state machine.
            // Old SM will just insert an expired record, and that causes the system seq increase by 1.
            let (_p, r) = self
                .user_map_mut()
                .set(UserKey::new(&upsert_kv.key), None)
                .await?;
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
            self.user_map()
                .range(UserKey::new(&p)..UserKey::new(right))
                .await?
        } else {
            self.user_map().range(UserKey::new(&p)..).await?
        };

        let strm = add_cooperative_yielding(strm, format!("list_kv: {prefix}"))
            // Skip tombstone
            .try_filter_map(|(k, marked)| future::ready(Ok(seq_marked_to_seqv(k, marked))));

        Ok(strm.boxed())
    }

    /// Return a range of kv entries.
    async fn range_kv<R>(&self, rng: R) -> Result<IOResultStream<(String, SeqV)>, io::Error>
    where R: RangeBounds<UserKey> + Send + Sync + Clone + 'static {
        let left = rng.start_bound().cloned();
        let right = rng.end_bound().cloned();

        let leveled_map = self.user_map();
        let strm = leveled_map.as_user_map().range(rng).await?;

        let strm = add_cooperative_yielding(strm, format!("range_kv: {left:?} to {right:?}"))
            // Skip tombstone
            .try_filter_map(|(k, marked)| future::ready(Ok(seq_marked_to_seqv(k, marked))));

        Ok(strm.boxed())
    }

    /// Update the secondary index for speeding up expiration operation.
    ///
    /// Remove the expiration index for the removed record, and add a new one for the new record.
    async fn update_expire_index(
        &mut self,
        key: impl ToString + Send,
        removed: &SeqMarked<MetaValue>,
        added: &SeqMarked<MetaValue>,
    ) -> Result<(), io::Error> {
        // No change, no need to update expiration index
        if removed == added {
            return Ok(());
        }

        // Remove previous expiration index, add a new one.

        if let Some(exp_ms) = removed.expires_at_ms_opt() {
            self.expire_map_mut()
                .set(ExpireKey::new(exp_ms, *removed.internal_seq()), None)
                .await?;
        }

        if let Some(exp_ms) = added.expires_at_ms_opt() {
            let k = ExpireKey::new(exp_ms, *added.internal_seq());
            let v = key.to_string();
            self.expire_map_mut().set(k, Some(v)).await?;
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
        // curr_time > expire_at => expired
        let end = ExpireKey::new(curr_time_ms, 0);

        let strm = self.expire_map().range(..end).await?;

        let strm = add_cooperative_yielding(strm, format!("list_expire_index up to {end}"))
            // Return only non-deleted records
            .try_filter_map(|(k, seq_marked)| {
                let expire_entry = seq_marked.into_data().map(|v| (k, v));
                future::ready(Ok(expire_entry))
            });

        Ok(strm.boxed())
    }

    /// Get a cloned value by key.
    ///
    /// It does not check expiration of the returned entry.
    async fn get_maybe_expired_kv(&self, key: &String) -> Result<Option<SeqV>, io::Error> {
        let got = self.user_map().get(key.as_ref()).await?;
        let seqv = Into::<Option<SeqV>>::into(got);
        Ok(seqv)
    }
}

impl<T> StateMachineApiExt for T where T: StateMachineApi {}

/// Convert internal data format [`SeqMarked<T>`] containing tombstone to a public API format [`SeqV`] without tombstone.
///
/// A tombstone is converted to None.
fn seq_marked_to_seqv(k: UserKey, marked: SeqMarked<MetaValue>) -> Option<(String, SeqV)> {
    let seqv = Into::<Option<SeqV>>::into(marked);
    seqv.map(|x| (k.to_string(), x))
}
