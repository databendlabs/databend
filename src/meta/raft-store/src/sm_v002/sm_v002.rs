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

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::fmt::Debug;
use std::io;
use std::sync::Arc;

use common_meta_kvapi::kvapi;
use common_meta_kvapi::kvapi::GetKVReply;
use common_meta_kvapi::kvapi::ListKVReply;
use common_meta_kvapi::kvapi::MGetKVReply;
use common_meta_kvapi::kvapi::UpsertKVReply;
use common_meta_kvapi::kvapi::UpsertKVReq;
use common_meta_types::AppliedState;
use common_meta_types::Entry;
use common_meta_types::LogId;
use common_meta_types::MatchSeqExt;
use common_meta_types::Node;
use common_meta_types::NodeId;
use common_meta_types::Operation;
use common_meta_types::SeqV;
use common_meta_types::SeqValue;
use common_meta_types::SnapshotData;
use common_meta_types::StoredMembership;
use common_meta_types::TxnReply;
use common_meta_types::TxnRequest;
use common_meta_types::UpsertKV;
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;
use tokio::sync::RwLock;
use tracing::debug;
use tracing::info;
use tracing::warn;

use crate::applier::Applier;
use crate::key_spaces::RaftStoreEntry;
use crate::sm_v002::leveled_store::level::Level;
use crate::sm_v002::leveled_store::map_api::MapApi;
use crate::sm_v002::marked::Marked;
use crate::sm_v002::sm_v002;
pub use crate::sm_v002::snapshot_view_v002::SnapshotViewV002;
use crate::state_machine::sm::BlockingConfig;
use crate::state_machine::ExpireKey;
use crate::state_machine::StateMachineSubscriber;

/// A wrapper that implements KVApi **readonly** methods for the state machine.
pub struct SMV002KVApi<'a> {
    sm: &'a SMV002,
}

#[async_trait::async_trait]
impl<'a> kvapi::KVApi for SMV002KVApi<'a> {
    type Error = Infallible;

    async fn upsert_kv(&self, _req: UpsertKVReq) -> Result<UpsertKVReply, Self::Error> {
        unreachable!("write operation SM2KVApi::upsert_kv is disabled")
    }

    async fn get_kv(&self, key: &str) -> Result<GetKVReply, Self::Error> {
        let got = self.sm.get_kv(key);

        let local_now_ms = SeqV::<()>::now_ms();
        let got = Self::non_expired(got, local_now_ms);
        Ok(got)
    }

    async fn mget_kv(&self, keys: &[String]) -> Result<MGetKVReply, Self::Error> {
        let local_now_ms = SeqV::<()>::now_ms();

        let values = keys
            .iter()
            .map(|k| {
                let got = self.sm.get_kv(k.as_str());
                Self::non_expired(got, local_now_ms)
            })
            .collect();
        Ok(values)
    }

    async fn prefix_list_kv(&self, prefix: &str) -> Result<ListKVReply, Self::Error> {
        let local_now_ms = SeqV::<()>::now_ms();

        let kvs = self
            .sm
            .prefix_list_kv(prefix)
            .into_iter()
            .filter(|(_k, v)| !v.is_expired(local_now_ms));

        Ok(kvs.collect())
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
    pub(in crate::sm_v002) top: Level,

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

        let mut importer = sm_v002::SnapshotViewV002::new_importer();

        let br = BufReader::new(data);
        let mut lines = AsyncBufReadExt::lines(br);

        while let Some(l) = lines.next_line().await? {
            let ent: RaftStoreEntry = serde_json::from_str(&l)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            importer
                .import(ent)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        }

        let level_data = importer.commit();
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
            if &new_last_applied <= sm.last_applied_ref() && sm.last_applied_ref().is_some() {
                info!(
                    "no need to install: snapshot({:?}) <= sm({:?})",
                    new_last_applied,
                    sm.last_applied_ref()
                );
                return Ok(());
            }

            sm.replace_data(Level::new(level_data, None));
        }

        info!(
            "installed state machine from snapshot, last_applied: {:?}",
            new_last_applied,
        );

        Ok(())
    }

    /// Return a Arc of the blocking config. It is only used for testing.
    pub fn blocking_config_mut(&mut self) -> &mut BlockingConfig {
        &mut self.blocking_config
    }

    pub fn blocking_config(&self) -> &BlockingConfig {
        &self.blocking_config
    }

    pub fn apply_entries<'a>(
        &mut self,
        entries: impl IntoIterator<Item = &'a Entry>,
    ) -> Vec<AppliedState> {
        let mut applier = Applier::new(self);

        let mut res = vec![];

        for l in entries.into_iter() {
            let r = applier.apply(l);
            res.push(r);
        }
        res
    }

    /// Get a cloned value by key.
    ///
    /// It does not check expiration of the returned entry.
    pub fn get_kv(&self, key: &str) -> Option<SeqV> {
        let got = MapApi::<String>::get(&self.top, key).clone();
        Into::<Option<SeqV>>::into(got)
    }

    /// Get a value by key and return a reference to the internal entry.
    ///
    /// It is an internal API and does not examine the expiration time.
    pub(crate) fn get_kv_ref(&self, key: &str) -> &dyn SeqValue {
        let got = MapApi::<String>::get(&self.top, key);
        got
    }

    // TODO(1): when get an applier, pass in a now_ms to ensure all expired are cleaned.
    /// Update or insert a kv entry.
    ///
    /// If the input entry has expired, it performs a delete operation.
    pub(crate) fn upsert_kv(&mut self, upsert_kv: UpsertKV) -> (Option<SeqV>, Option<SeqV>) {
        let (prev, result) = self.upsert_kv_primary_index(&upsert_kv);

        self.update_expire_index(&upsert_kv.key, &prev, &result);

        let prev = Into::<Option<SeqV>>::into(prev);
        let result = Into::<Option<SeqV>>::into(result);

        (prev, result)
    }

    /// List kv entries by prefix.
    ///
    /// If a value is expired, it is not returned.
    pub fn prefix_list_kv(&self, prefix: &str) -> Vec<(String, SeqV)> {
        let p = prefix.to_string();
        let mut res = Vec::new();
        let it = MapApi::<String>::range(&self.top, p..);

        for (k, marked) in it {
            if k.starts_with(prefix) {
                let seqv = Into::<Option<SeqV>>::into(marked.clone());

                if let Some(x) = seqv {
                    res.push((k.clone(), x));
                }
            } else {
                break;
            }
        }

        res
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

    /// List expiration index by expiration time.
    pub(crate) fn list_expire_index(&self) -> impl Iterator<Item = (&ExpireKey, &String)> {
        self.top
            .range::<ExpireKey, _>(&self.expire_cursor..)
            // Return only non-deleted records
            .filter_map(|(k, v)| v.unpack().map(|(v, _)| (k, v)))
    }

    pub fn curr_seq(&self) -> u64 {
        self.top.data_ref().curr_seq()
    }

    pub fn last_applied_ref(&self) -> &Option<LogId> {
        self.top.data_ref().last_applied_ref()
    }

    pub fn last_membership_ref(&self) -> &StoredMembership {
        self.top.data_ref().last_membership_ref()
    }

    pub fn nodes_ref(&self) -> &BTreeMap<NodeId, Node> {
        self.top.data_ref().nodes_ref()
    }

    pub fn last_applied_mut(&mut self) -> &mut Option<LogId> {
        self.top.data_mut().last_applied_mut()
    }

    pub fn last_membership_mut(&mut self) -> &mut StoredMembership {
        self.top.data_mut().last_membership_mut()
    }

    pub fn nodes_mut(&mut self) -> &mut BTreeMap<NodeId, Node> {
        self.top.data_mut().nodes_mut()
    }

    pub fn set_subscriber(&mut self, subscriber: Box<dyn StateMachineSubscriber>) {
        self.subscriber = Some(subscriber);
    }

    /// Creates a snapshot that contains the latest state.
    ///
    /// Internally, the state machine creates a new empty writable level and makes all current states immutable.
    pub fn full_snapshot(&mut self) -> crate::sm_v002::snapshot_view_v002::SnapshotViewV002 {
        self.top.new_level();

        // Safe unwrap: just created new level and it must have a base level.
        let base = self.top.get_base().unwrap();

        crate::sm_v002::snapshot_view_v002::SnapshotViewV002::new(base)
    }

    /// Replace all of the state machine data with the given one.
    pub fn replace_data(&mut self, level: Level) {
        let applied = self.top.data_ref().last_applied_ref();
        let new_applied = level.data_ref().last_applied_ref();

        assert!(
            new_applied >= applied,
            "the state machine({:?}) can not be replaced with an older one({:?})",
            applied,
            new_applied
        );

        self.top = level;

        // The installed data may not cleaned up all expired keys, if it is built with an older state machine.
        // So we need to reset the cursor then the next time applying a log it will cleanup all expired.
        self.expire_cursor = ExpireKey::new(0, 0);
    }

    pub fn replace_base(&mut self, snapshot: &SnapshotViewV002) {
        assert!(
            Arc::ptr_eq(&self.top.get_base().unwrap(), &snapshot.original()),
            "the base must not be changed"
        );

        self.top.replace_base(Some(snapshot.top()));
    }

    /// It returns 2 entries: the previous one and the new one after upsert.
    fn upsert_kv_primary_index(
        &mut self,
        upsert_kv: &UpsertKV,
    ) -> (Marked<Vec<u8>>, Marked<Vec<u8>>) {
        let prev = MapApi::<String>::get(&self.top, &upsert_kv.key).clone();

        if upsert_kv.seq.match_seq(prev.seq()).is_err() {
            return (prev.clone(), prev);
        }

        let (prev, mut result) = match &upsert_kv.value {
            Operation::Update(v) => self.top.set(
                upsert_kv.key.clone(),
                Some((v.clone(), upsert_kv.value_meta.clone())),
            ),
            Operation::Delete => self.top.set(upsert_kv.key.clone(), None),
            Operation::AsIs => self
                .top
                .update_meta(upsert_kv.key.clone(), upsert_kv.value_meta.clone()),
        };

        let expire_ms = upsert_kv.get_expire_at_ms().unwrap_or(u64::MAX);
        if expire_ms < self.expire_cursor.time_ms {
            // The record has expired, delete it at once.
            //
            // Note that it must update first then delete,
            // in order to keep compatibility with the old state machine.
            // Old SM will just insert an expired record, and that causes the system seq increase by 1.
            let (_p, r) = self.top.set(upsert_kv.key.clone(), None);
            result = r;
        };

        debug!(
            "applied upsert: {:?}; prev: {:?}; res: {:?}",
            upsert_kv, prev, result
        );

        (prev, result)
    }

    /// Update the secondary index for speeding up expiration operation.
    ///
    /// Remove the expiration index for the removed record, and add a new one for the new record.
    fn update_expire_index(
        &mut self,
        key: impl ToString,
        removed: &Marked<Vec<u8>>,
        added: &Marked<Vec<u8>>,
    ) {
        // No change, no need to update expiration index
        if removed == added {
            return;
        }

        // Remove previous expiration index, add a new one.

        if let Some(exp_ms) = removed.expire_at_ms() {
            self.top
                .set(ExpireKey::new(exp_ms, removed.internal_seq().seq()), None);
        }

        if let Some(exp_ms) = added.expire_at_ms() {
            let k = ExpireKey::new(exp_ms, added.internal_seq().seq());
            let v = key.to_string();
            self.top.set(k, Some((v, None)));
        }
    }
}
