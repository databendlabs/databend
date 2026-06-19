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
use std::sync::Arc;
use std::sync::Mutex;

use databend_common_proto_conv::FromToProto;
use databend_meta_client::kvapi;
use databend_meta_client::kvapi::KVApi;
use databend_meta_client::kvapi::KvApiExt;
use databend_meta_client::types::MetaError;
use databend_meta_client::types::SeqV;
use databend_meta_client::types::TxnCondition;
use databend_meta_client::types::TxnOp;
use databend_meta_client::types::TxnRequest;
use seq_marked::SeqValue;

use super::core::send_txn;
use super::for_update::ForUpdate;
use super::op_builder::txn_del;
use super::op_builder::txn_put_pb_with_ttl;
use super::read_entry::ReadEntry;
use crate::kv_pb_api::decode_seqv;

/// The reads and writes staged by one transaction attempt.
///
/// Held behind an [`Arc`] so the user-facing transaction handle and the private
/// committer share one set.
#[derive(Default)]
struct TxnState {
    reads: BTreeMap<String, ReadEntry>,
    operations: BTreeMap<String, TxnOp>,
}

/// A single transaction attempt against a KV backend.
///
/// Reads and writes are kept in maps keyed by the key string. The read set
/// records every retrieved key (see [`ReadEntry`]); at commit, the keys read
/// [`for update`](Self::get_for_update) become `eq_seq` guards while the rest
/// only serve as a snapshot cache, so re-reading a key returns the cached value
/// instead of hitting the backend. Writes keep the operation last staged.
///
/// A guard asserting a key's version is installed by marking the read for
/// update, so a read that a write depends on can never be left unguarded.
///
/// The read and write sets sit behind an `Arc<Mutex<…>>`, so reads and writes
/// take `&self`. A [`ForUpdate`] handle can borrow the transaction and write
/// straight back to it, and several handles can be held at once without passing
/// the transaction around.
///
/// Usually obtained from [`MetaTxnManager::run`](crate::MetaTxnManager::run).
pub struct MetaTxn<'a, KV: ?Sized> {
    kv: &'a KV,
    state: Arc<Mutex<TxnState>>,
}

/// Commit authority for a [`MetaTxn`].
///
/// This is intentionally not exposed to transaction-building closures: staging
/// code can read and write, but only the manager can drain and commit state.
pub(crate) struct MetaTxnCommit<'a, KV: ?Sized> {
    kv: &'a KV,
    state: Arc<Mutex<TxnState>>,
}

impl<'a, KV> MetaTxn<'a, KV>
where KV: KVApi<Error = MetaError> + ?Sized
{
    pub(crate) fn new(kv: &'a KV) -> (Self, MetaTxnCommit<'a, KV>) {
        let state = Arc::new(Mutex::new(TxnState::default()));
        (
            Self {
                kv,
                state: state.clone(),
            },
            MetaTxnCommit { kv, state },
        )
    }

    /// Read a key without arming a guard.
    pub async fn get<K>(&self, key: &K) -> Result<Option<SeqV<K::ValueType>>, MetaError>
    where
        K: kvapi::Key,
        K::ValueType: FromToProto,
    {
        self.read(key, false).await
    }

    /// Read a key and mark it for update, so it becomes an `eq_seq` guard at
    /// commit: the commit fails unless `key` is still at the version read.
    /// Reading is what installs the guard, so it cannot be forgotten.
    /// Re-reading the same key keeps the version first observed.
    ///
    /// The returned [`ForUpdate`] borrows the transaction and writes back to the
    /// same key via [`put`](ForUpdate::put) / [`delete`](ForUpdate::delete).
    pub async fn get_for_update<K>(&self, key: &K) -> Result<ForUpdate<'_, 'a, KV, K>, MetaError>
    where
        K: kvapi::Key + Clone,
        K::ValueType: FromToProto,
    {
        let got = self.read(key, true).await?;
        Ok(ForUpdate::new(self, key.clone(), got))
    }

    /// Stage a put. Replaces any operation previously staged for `key`.
    pub fn put<K>(&self, key: &K, value: &K::ValueType) -> Result<(), MetaError>
    where
        K: kvapi::Key,
        K::ValueType: FromToProto + 'static,
    {
        let op = txn_put_pb_with_ttl(key, value, None)?;
        self.state
            .lock()
            .unwrap()
            .operations
            .insert(key.to_string_key(), op);
        Ok(())
    }

    /// Stage a delete. Replaces any operation previously staged for `key`.
    pub fn delete<K>(&self, key: &K)
    where K: kvapi::Key {
        self.state
            .lock()
            .unwrap()
            .operations
            .insert(key.to_string_key(), txn_del(key));
    }

    /// Read a key through the snapshot cache, recording it in the read set.
    ///
    /// On a cache hit the backend is not touched; the version first observed and
    /// its value are reused. `for_update` is OR-ed in, so a plain read followed
    /// by a for-update read upgrades the key to a guard.
    async fn read<K>(
        &self,
        key: &K,
        for_update: bool,
    ) -> Result<Option<SeqV<K::ValueType>>, MetaError>
    where
        K: kvapi::Key,
        K::ValueType: FromToProto,
    {
        let key_str = key.to_string_key();

        let cached = {
            let mut state = self.state.lock().unwrap();
            state.reads.get_mut(&key_str).map(|entry| {
                entry.for_update |= for_update;
                entry.seqv.clone()
            })
        };

        let raw = match cached {
            Some(raw) => raw,
            None => {
                let raw = self.kv.get_kv(&key_str).await?;
                self.state
                    .lock()
                    .unwrap()
                    .reads
                    .insert(key_str.clone(), ReadEntry {
                        for_update,
                        seqv: raw.clone(),
                    });
                raw
            }
        };

        decode_raw::<K::ValueType>(raw, &key_str)
    }
}

impl<KV> MetaTxnCommit<'_, KV>
where KV: KVApi<Error = MetaError> + ?Sized
{
    /// Whether no write has been staged: an empty transaction has nothing to
    /// commit.
    pub(crate) fn is_empty(&self) -> bool {
        self.state.lock().unwrap().operations.is_empty()
    }

    /// Consume the transaction committer, sending its staged reads and writes as
    /// one commit and reporting whether its guards held.
    pub(crate) async fn execute(self) -> Result<bool, MetaError> {
        let (reads, operations) = {
            let mut state = self.state.lock().unwrap();
            (
                std::mem::take(&mut state.reads),
                std::mem::take(&mut state.operations),
            )
        };
        let req = build_request(reads, operations);
        let (succ, _responses) = send_txn(self.kv, req).await?;
        Ok(succ)
    }
}

/// Build a [`TxnRequest`] from a read set and staged operations: each
/// for-update read becomes an `eq_seq` guard.
fn build_request(
    reads: BTreeMap<String, ReadEntry>,
    operations: BTreeMap<String, TxnOp>,
) -> TxnRequest {
    let conditions = reads
        .into_iter()
        .filter(|(_, r)| r.for_update)
        .map(|(key, r)| TxnCondition::eq_seq(key, r.seqv.seq()))
        .collect();
    let operations = operations.into_values().collect();
    TxnRequest::new(conditions, operations)
}

/// Decode a raw record (seq, meta, encoded value) into its typed value,
/// preserving seq and meta. The read set stores the raw form so it stays
/// type-independent; the concrete value is produced here, per read.
fn decode_raw<T>(raw: Option<SeqV<Vec<u8>>>, key: &str) -> Result<Option<SeqV<T>>, MetaError>
where T: FromToProto {
    raw.map(|s| decode_seqv::<T>(s, || format!("decode value of {key}")))
        .transpose()
        .map_err(MetaError::from)
}
