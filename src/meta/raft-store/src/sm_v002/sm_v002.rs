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
use std::iter::repeat_with;
use std::sync::Arc;

use databend_common_meta_kvapi::kvapi;
use databend_common_meta_kvapi::kvapi::KVStream;
use databend_common_meta_kvapi::kvapi::UpsertKVReply;
use databend_common_meta_kvapi::kvapi::UpsertKVReq;
use databend_common_meta_types::protobuf::StreamItem;
use databend_common_meta_types::AppliedState;
use databend_common_meta_types::CmdContext;
use databend_common_meta_types::Entry;
use databend_common_meta_types::EvalExpireTime;
use databend_common_meta_types::MatchSeqExt;
use databend_common_meta_types::Operation;
use databend_common_meta_types::SeqV;
use databend_common_meta_types::SeqValue;
use databend_common_meta_types::SnapshotData;
use databend_common_meta_types::StorageIOError;
use databend_common_meta_types::TxnReply;
use databend_common_meta_types::TxnRequest;
use databend_common_meta_types::UpsertKV;
use futures::Stream;
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use itertools::Itertools;
use log::debug;
use log::info;
use log::warn;
use openraft::RaftLogId;
use tokio::sync::RwLock;

use crate::applier::Applier;
use crate::key_spaces::SMEntry;
use crate::leveled_store::leveled_map::LeveledMap;
use crate::leveled_store::map_api::AsMap;
use crate::leveled_store::map_api::MapApi;
use crate::leveled_store::map_api::MapApiExt;
use crate::leveled_store::map_api::MapApiRO;
use crate::leveled_store::map_api::ResultStream;
use crate::leveled_store::sys_data::SysData;
use crate::leveled_store::sys_data_api::SysDataApiRO;
use crate::marked::Marked;
use crate::sm_v002::sm_v002;
use crate::sm_v002::Importer;
use crate::sm_v002::SnapshotViewV002;
use crate::state_machine::sm::BlockingConfig;
use crate::state_machine::ExpireKey;
use crate::state_machine::StateMachineSubscriber;
use crate::utils::prefix_right_bound;

/// A wrapper that implements KVApi **readonly** methods for the state machine.
pub struct SMV002KVApi<'a> {
    sm: &'a SMV002,
}

#[async_trait::async_trait]
impl<'a> kvapi::KVApi for SMV002KVApi<'a> {
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

impl<'a> SMV002KVApi<'a> {
    fn non_expired<V>(seq_value: Option<SeqV<V>>, now_ms: u64) -> Option<SeqV<V>> {
        if seq_value.is_expired(now_ms) {
            None
        } else {
            seq_value
        }
    }
}

#[derive(Debug, Default)]
pub struct SMV002 {
    pub(in crate::sm_v002) levels: LeveledMap,

    blocking_config: BlockingConfig,

    /// The expiration key since which for next clean.
    pub(in crate::sm_v002) expire_cursor: ExpireKey,

    /// subscriber of state machine data
    pub(crate) subscriber: Option<Box<dyn StateMachineSubscriber>>,
}

impl SMV002 {
    pub fn kv_api(&self) -> SMV002KVApi {
        SMV002KVApi { sm: self }
    }

    /// Install and replace state machine with the content of a snapshot
    ///
    /// After install, the state machine has only one level of data.
    pub async fn install_snapshot(
        state_machine: Arc<RwLock<Self>>,
        data: Box<SnapshotData>,
    ) -> Result<(), io::Error> {
        //
        let data_size = data.data_size().await?;
        info!("snapshot data len: {}", data_size);

        let mut importer = sm_v002::SMV002::new_importer();

        let f = data.into_std().await;

        let h = databend_common_base::runtime::spawn_blocking(move || {
            // Create a worker pool to deserialize the entries.

            let queue_depth = 1024;
            let n_workers = 16;
            let (tx, rx) = ordq::new(queue_depth, repeat_with(|| Deserializer).take(n_workers));

            // Spawn a thread to import the deserialized entries.

            let import_th = databend_common_base::runtime::Thread::spawn(move || {
                while let Some(res) = rx.recv() {
                    let entries: Result<Vec<SMEntry>, io::Error> =
                        res.map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

                    let entries = entries?;

                    for ent in entries {
                        importer.import(ent)?;
                    }
                }

                let level_data = importer.commit();
                Ok::<_, io::Error>(level_data)
            });

            // Feed input strings to the worker pool.
            {
                let mut br = io::BufReader::with_capacity(16 * 1024 * 1024, f);
                let lines = io::BufRead::lines(&mut br);
                for c in &lines.into_iter().chunks(1024) {
                    let chunk = c.collect::<Result<Vec<_>, _>>()?;
                    tx.send(chunk)
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
                }

                // drop `tx` to notify the worker threads to exit.
                tx.close()
            }

            let level_data = import_th
                .join()
                .map_err(|_e| io::Error::new(io::ErrorKind::Other, "import thread failure"))??;

            Ok::<_, io::Error>(level_data)
        });

        let level_data = h
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))??;

        let new_last_applied = *level_data.last_applied_ref();

        {
            let mut sm = state_machine.write().await;

            // When rebuilding the state machine, last_applied is empty,
            // and it should always install the snapshot.
            //
            // And the snapshot may contain data when its last_applied is None,
            // when importing data with metactl:
            // The snapshot is empty but contains Nodes data that are manually added.
            //
            // See: `databend_metactl::snapshot`
            if &new_last_applied <= sm.sys_data_ref().last_applied_ref()
                && sm.sys_data_ref().last_applied_ref().is_some()
            {
                info!(
                    "no need to install: snapshot({:?}) <= sm({:?})",
                    new_last_applied,
                    sm.sys_data_ref().last_applied_ref()
                );
                return Ok(());
            }

            let mut levels = LeveledMap::new(level_data);
            // Push all data down to frozen level, create a new empty writable level.
            // So that the top writable level is small enough.
            // Writable level can not return a static stream for `range()`. It has to copy all data in the range.
            // See the MapApiRO::range() implementation for Level.
            levels.freeze_writable();

            sm.replace(levels);
        }

        info!(
            "installed state machine from snapshot, last_applied: {:?}",
            new_last_applied,
        );

        Ok(())
    }

    pub fn new_importer() -> Importer {
        Importer::default()
    }

    /// Return a Arc of the blocking config. It is only used for testing.
    pub fn blocking_config_mut(&mut self) -> &mut BlockingConfig {
        &mut self.blocking_config
    }

    pub fn blocking_config(&self) -> &BlockingConfig {
        &self.blocking_config
    }

    #[allow(dead_code)]
    pub(crate) fn new_applier(&mut self) -> Applier {
        Applier::new(self)
    }

    pub async fn apply_entries(
        &mut self,
        entries: impl IntoIterator<Item = Entry>,
    ) -> Result<Vec<AppliedState>, StorageIOError> {
        let mut applier = Applier::new(self);

        let mut res = vec![];

        for ent in entries.into_iter() {
            let log_id = *ent.get_log_id();
            let r = applier
                .apply(&ent)
                .await
                .map_err(|e| StorageIOError::apply(log_id, &e))?;
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
    pub async fn list_kv(&self, prefix: &str) -> Result<ResultStream<(String, SeqV)>, io::Error> {
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
    /// upto current time(exclusive) in milli seconds.
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

    pub fn set_subscriber(&mut self, subscriber: Box<dyn StateMachineSubscriber>) {
        self.subscriber = Some(subscriber);
    }

    /// Creates a snapshot view that contains the latest state.
    ///
    /// Internally, the state machine creates a new empty writable level and makes all current states immutable.
    ///
    /// This operation is fast because it does not copy any data.
    pub fn full_snapshot_view(&mut self) -> SnapshotViewV002 {
        let frozen = self.levels.freeze_writable();

        SnapshotViewV002::new(frozen.clone())
    }

    /// Replace all of the state machine data with the given one.
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

        // The installed data may not cleaned up all expired keys, if it is built with an older state machine.
        // So we need to reset the cursor then the next time applying a log it will cleanup all expired.
        self.expire_cursor = ExpireKey::new(0, 0);
    }

    /// Keep the top(writable) level, replace all the frozen levels.
    ///
    /// This is called after compacting some of the frozen levels.
    pub fn replace_frozen(&mut self, snapshot: &SnapshotViewV002) {
        assert!(
            Arc::ptr_eq(
                self.levels.immutable_levels_ref().newest().unwrap().inner(),
                snapshot.original_ref().newest().unwrap().inner()
            ),
            "the frozen must not change"
        );

        self.levels.replace_immutable_levels(snapshot.compacted());
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
                .set(ExpireKey::new(exp_ms, removed.internal_seq().seq()), None)
                .await?;
        }

        if let Some(exp_ms) = added.get_expire_at_ms() {
            let k = ExpireKey::new(exp_ms, added.internal_seq().seq());
            let v = key.to_string();
            self.levels.set(k, Some((v, None))).await?;
        }

        Ok(())
    }
}

struct Deserializer;

impl ordq::Work for Deserializer {
    type I = Vec<String>;
    type O = Result<Vec<SMEntry>, io::Error>;

    fn run(&mut self, strings: Self::I) -> Self::O {
        let mut res = Vec::with_capacity(strings.len());
        for s in strings {
            let ent: SMEntry = serde_json::from_str(&s)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            res.push(ent);
        }
        Ok(res)
    }
}
