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

use std::fmt::Debug;
use std::future;
use std::io;

use databend_common_meta_kvapi::kvapi;
use databend_common_meta_kvapi::kvapi::KVStream;
use databend_common_meta_kvapi::kvapi::UpsertKVReply;
use databend_common_meta_kvapi::kvapi::UpsertKVReq;
use databend_common_meta_types::protobuf::StreamItem;
use databend_common_meta_types::snapshot_db::DB;
use databend_common_meta_types::sys_data::SysData;
use databend_common_meta_types::AppliedState;
use databend_common_meta_types::CmdContext;
use databend_common_meta_types::Entry;
use databend_common_meta_types::EvalExpireTime;
use databend_common_meta_types::MatchSeqExt;
use databend_common_meta_types::Operation;
use databend_common_meta_types::SeqV;
use databend_common_meta_types::SeqValue;
use databend_common_meta_types::StorageError;
use databend_common_meta_types::TxnReply;
use databend_common_meta_types::TxnRequest;
use databend_common_meta_types::UpsertKV;
use futures::Stream;
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use log::debug;
use log::info;
use log::warn;
use openraft::RaftLogId;

use crate::applier::Applier;
use crate::leveled_store::leveled_map::compactor::Compactor;
use crate::leveled_store::leveled_map::LeveledMap;
use crate::leveled_store::map_api::AsMap;
use crate::leveled_store::map_api::IOResultStream;
use crate::leveled_store::map_api::MapApi;
use crate::leveled_store::map_api::MapApiExt;
use crate::leveled_store::map_api::MapApiRO;
use crate::leveled_store::sys_data_api::SysDataApiRO;
use crate::marked::Marked;
use crate::state_machine::ExpireKey;
use crate::state_machine::StateMachineSubscriber;
use crate::utils::prefix_right_bound;

/// A wrapper that implements KVApi **readonly** methods for the state machine.
pub struct SMV003KVApi<'a> {
    sm: &'a SMV003,
}

#[async_trait::async_trait]
impl<'a> kvapi::KVApi for SMV003KVApi<'a> {
    type Error = io::Error;

    async fn upsert_kv(&self, _req: UpsertKVReq) -> Result<UpsertKVReply, Self::Error> {
        unreachable!("write operation SM2KVApi::upsert_kv is disabled")
    }

    async fn get_kv_stream(&self, keys: &[String]) -> Result<KVStream<Self::Error>, Self::Error> {
        let local_now_ms = SeqV::<()>::now_ms();

        let mut items = Vec::with_capacity(keys.len());

        for k in keys {
            let got = self.sm.get_maybe_expired_kv(k.as_str()).await?;
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

impl<'a> SMV003KVApi<'a> {
    fn non_expired<V>(seq_value: Option<SeqV<V>>, now_ms: u64) -> Option<SeqV<V>> {
        if seq_value.is_expired(now_ms) {
            None
        } else {
            seq_value
        }
    }
}

#[derive(Debug, Default)]
pub struct SMV003 {
    pub(in crate::sm_v003) levels: LeveledMap,

    /// The expiration key since which for next clean.
    pub(in crate::sm_v003) expire_cursor: ExpireKey,

    /// subscriber of state machine data
    pub(crate) subscriber: Option<Box<dyn StateMachineSubscriber>>,
}

impl SMV003 {
    pub fn kv_api(&self) -> SMV003KVApi {
        SMV003KVApi { sm: self }
    }

    /// Install and replace state machine with the content of a snapshot.
    pub async fn install_snapshot_v003(&mut self, db: DB) -> Result<(), io::Error> {
        let data_size = db.inner().file_size();
        let sys_data = db.sys_data().clone();

        info!(
            "SMV003::install_snapshot: data_size: {}; sys_data: {:?}",
            data_size, sys_data
        );

        // Do not skip install if both self.last_applied and db.last_applied are None.
        //
        // The snapshot may contain data when its last_applied is None,
        // when importing data with metactl:
        // The snapshot is empty but contains Nodes data that are manually added.
        //
        // See: `databend_metactl::import`
        let my_last_applied = *self.sys_data_ref().last_applied_ref();
        #[allow(clippy::collapsible_if)]
        if my_last_applied.is_some() {
            if &my_last_applied >= sys_data.last_applied_ref() {
                info!(
                    "SMV003 try to install a smaller snapshot({:?}), ignored, my last applied: {:?}",
                    sys_data.last_applied_ref(),
                    self.sys_data_ref().last_applied_ref()
                );
                return Ok(());
            }
        }

        self.levels.clear();
        let levels = self.levels_mut();
        *levels.sys_data_mut() = sys_data;
        *levels.persisted_mut() = Some(db);
        Ok(())
    }

    pub fn get_snapshot(&self) -> Option<DB> {
        self.levels.persisted().cloned()
    }

    #[allow(dead_code)]
    pub(crate) fn new_applier(&mut self) -> Applier {
        Applier::new(self)
    }

    pub async fn apply_entries(
        &mut self,
        entries: impl IntoIterator<Item = Entry>,
    ) -> Result<Vec<AppliedState>, StorageError> {
        let mut applier = Applier::new(self);

        let mut res = vec![];

        for ent in entries.into_iter() {
            let log_id = *ent.get_log_id();
            let r = applier
                .apply(&ent)
                .await
                .map_err(|e| StorageError::apply(log_id, &e))?;
            res.push(r);
        }
        Ok(res)
    }

    /// Get a cloned value by key.
    ///
    /// It does not check expiration of the returned entry.
    pub async fn get_maybe_expired_kv(&self, key: &str) -> Result<Option<SeqV>, io::Error> {
        let got = self.levels.str_map().get(key).await?;
        let seqv = Into::<Option<SeqV>>::into(got);
        Ok(seqv)
    }

    /// List kv entries by prefix.
    ///
    /// If a value is expired, it is not returned.
    pub async fn list_kv(&self, prefix: &str) -> Result<IOResultStream<(String, SeqV)>, io::Error> {
        let p = prefix.to_string();

        let strm = if let Some(right) = prefix_right_bound(&p) {
            self.levels.str_map().range(p.clone()..right).await?
        } else {
            self.levels.str_map().range(p.clone()..).await?
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

        // Make it static

        let vs = strm.collect::<Vec<_>>().await;
        let strm = futures::stream::iter(vs);

        Ok(strm.boxed())
    }

    pub(crate) fn update_expire_cursor(&mut self, log_time_ms: u64) {
        if log_time_ms < self.expire_cursor.time_ms {
            warn!(
                "update_last_cleaned: log_time_ms {} < last_cleaned_expire.time_ms {}",
                log_time_ms, self.expire_cursor.time_ms
            );
            return;
        }

        self.expire_cursor = ExpireKey::new(log_time_ms, 0);
    }

    /// List expiration index by expiration time,
    /// upto current time(exclusive) in milliseconds.
    ///
    /// Only records with expire time less than current time will be returned.
    /// Expire time that equals to current time is not considered expired.
    pub(crate) async fn list_expire_index(
        &self,
        curr_time_ms: u64,
    ) -> Result<impl Stream<Item = Result<(ExpireKey, String), io::Error>> + '_, io::Error> {
        let start = self.expire_cursor;

        // curr_time > expire_at => expired
        let end = ExpireKey::new(curr_time_ms, 0);

        // There is chance the raft leader produce smaller timestamp for later logs.
        if start >= end {
            return Ok(futures::stream::empty().boxed());
        }

        let strm = self.levels.expire_map().range(start..end).await?;

        let strm = strm
            // Return only non-deleted records
            .try_filter_map(|(k, marked)| {
                let expire_entry = marked.unpack().map(|(v, _v_meta)| (k, v));
                future::ready(Ok(expire_entry))
            });

        Ok(strm.boxed())
    }

    pub fn sys_data_ref(&self) -> &SysData {
        self.levels.writable_ref().sys_data_ref()
    }

    pub fn sys_data_mut(&mut self) -> &mut SysData {
        self.levels.writable_mut().sys_data_mut()
    }

    pub fn into_levels(self) -> LeveledMap {
        self.levels
    }

    pub fn levels(&self) -> &LeveledMap {
        &self.levels
    }

    pub fn levels_mut(&mut self) -> &mut LeveledMap {
        &mut self.levels
    }

    pub fn set_subscriber(&mut self, subscriber: Box<dyn StateMachineSubscriber>) {
        self.subscriber = Some(subscriber);
    }

    pub fn freeze_writable(&mut self) {
        self.levels.freeze_writable();
    }

    pub fn try_acquire_compactor(&mut self) -> Option<Compactor> {
        self.levels.try_acquire_compactor()
    }

    pub async fn acquire_compactor(&mut self) -> Compactor {
        self.levels.acquire_compactor().await
    }

    /// Replace all the state machine data with the given one.
    /// The input is a multi-level data.
    pub fn replace(&mut self, level: LeveledMap) {
        let applied = self.levels.writable_ref().last_applied_ref();
        let new_applied = level.writable_ref().last_applied_ref();

        assert!(
            new_applied >= applied,
            "the state machine({:?}) can not be replaced with an older one({:?})",
            applied,
            new_applied
        );

        self.levels = level;

        // The installed data may not clean up all expired keys, if it is built with an older state machine.
        // So we need to reset the cursor then the next time applying a log it will clean up all expired.
        self.expire_cursor = ExpireKey::new(0, 0);
    }

    /// It returns 2 entries: the previous one and the new one after upsert.
    pub(crate) async fn upsert_kv_primary_index(
        &mut self,
        upsert_kv: &UpsertKV,
        cmd_ctx: &CmdContext,
    ) -> Result<(Marked<Vec<u8>>, Marked<Vec<u8>>), io::Error> {
        let kv_meta = upsert_kv.value_meta.as_ref().map(|m| m.to_kv_meta(cmd_ctx));

        let prev = self.levels.str_map().get(&upsert_kv.key).await?.clone();

        if upsert_kv.seq.match_seq(prev.seq()).is_err() {
            return Ok((prev.clone(), prev));
        }

        let (prev, mut result) = match &upsert_kv.value {
            Operation::Update(v) => {
                self.levels
                    .set(upsert_kv.key.clone(), Some((v.clone(), kv_meta.clone())))
                    .await?
            }
            Operation::Delete => self.levels.set(upsert_kv.key.clone(), None).await?,
            Operation::AsIs => {
                MapApiExt::update_meta(&mut self.levels, upsert_kv.key.clone(), kv_meta.clone())
                    .await?
            }
        };

        let expire_ms = kv_meta.eval_expire_at_ms();
        if expire_ms < self.expire_cursor.time_ms {
            // The record has expired, delete it at once.
            //
            // Note that it must update first then delete,
            // in order to keep compatibility with the old state machine.
            // Old SM will just insert an expired record, and that causes the system seq increase by 1.
            let (_p, r) = self.levels.set(upsert_kv.key.clone(), None).await?;
            result = r;
        };

        debug!(
            "applied upsert: {:?}; prev: {:?}; res: {:?}",
            upsert_kv, prev, result
        );

        Ok((prev, result))
    }

    /// Update the secondary index for speeding up expiration operation.
    ///
    /// Remove the expiration index for the removed record, and add a new one for the new record.
    pub(crate) async fn update_expire_index(
        &mut self,
        key: impl ToString,
        removed: &Marked<Vec<u8>>,
        added: &Marked<Vec<u8>>,
    ) -> Result<(), io::Error> {
        // No change, no need to update expiration index
        if removed == added {
            return Ok(());
        }

        // Remove previous expiration index, add a new one.

        if let Some(exp_ms) = removed.get_expire_at_ms() {
            self.levels
                .set(ExpireKey::new(exp_ms, removed.order_key().seq()), None)
                .await?;
        }

        if let Some(exp_ms) = added.get_expire_at_ms() {
            let k = ExpireKey::new(exp_ms, added.order_key().seq());
            let v = key.to_string();
            self.levels.set(k, Some((v, None))).await?;
        }

        Ok(())
    }
}
