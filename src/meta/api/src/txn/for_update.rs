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

use std::fmt::Display;

use databend_common_meta_app::MetaServiceKeyErrorBuilder;
use databend_common_proto_conv::FromToProto;
use databend_meta_client::kvapi;
use databend_meta_client::kvapi::KVApi;
use databend_meta_client::types::InvalidArgument;
use databend_meta_client::types::SeqV;

use super::meta_txn::MetaTxn;

/// A key read for update.
///
/// It borrows the transaction and holds the value and key read. The `eq_seq`
/// guard was installed in the transaction's read set when the key was read.
/// Writing through it ([`put`](Self::put) / [`delete`](Self::delete)) goes back
/// to that transaction and reuses that key, so a write cannot target a different
/// key than the one guarded, and replaces any operation previously staged for it.
///
/// The handle borrows the transaction shared-ly (the transaction keeps its read
/// and write sets behind a lock), so several handles may be held at once: read a
/// batch of keys for update, inspect them, then stage the writes — without
/// passing the transaction around.
pub struct ForUpdate<'t, 'a, KV: ?Sized, K: kvapi::Key> {
    txn: &'t MetaTxn<'a, KV>,
    key: K,
    seq_v: Option<SeqV<K::ValueType>>,
}

impl<'t, 'a, KV, K> ForUpdate<'t, 'a, KV, K>
where
    KV: ?Sized,
    K: kvapi::Key,
{
    pub(crate) fn new(txn: &'t MetaTxn<'a, KV>, key: K, seq_v: Option<SeqV<K::ValueType>>) -> Self {
        Self { txn, key, seq_v }
    }

    /// The version read, `0` if the key was absent.
    pub fn seq(&self) -> u64 {
        self.seq_v.as_ref().map(|s| s.seq).unwrap_or(0)
    }

    /// The full record read — seq, meta, and value — `None` if the key was
    /// absent. Use this to reach the value's `meta`.
    pub fn seq_v(&self) -> Option<&SeqV<K::ValueType>> {
        self.seq_v.as_ref()
    }

    /// The value read, `None` if the key was absent.
    pub fn value(&self) -> Option<&K::ValueType> {
        self.seq_v.as_ref().map(|s| &s.data)
    }

    /// The value read, or the key's [`unknown_error`] if the key was absent.
    ///
    /// The [`ok_or`](Option::ok_or)-style "must exist" form of
    /// [`value`](Self::value), for a key a write depends on. `ctx` labels the
    /// error with the calling context. The error is exactly what the key
    /// declares, leaving conversion to the caller.
    ///
    /// [`unknown_error`]: MetaServiceKeyErrorBuilder::unknown_error
    pub fn some_or_unknown(&self, ctx: impl Display) -> Result<&K::ValueType, K::UnknownError>
    where K: MetaServiceKeyErrorBuilder {
        self.value().ok_or_else(|| self.key.unknown_error(ctx))
    }

    /// `()` if the key was absent, or the key's [`exist_error`] if it was
    /// present.
    ///
    /// The complement of [`some_or_unknown`](Self::some_or_unknown): the guard for a
    /// key that must be free before a write creates it. `ctx` labels the error
    /// with the calling context. The error is exactly what the key declares,
    /// leaving conversion to the caller.
    ///
    /// [`exist_error`]: MetaServiceKeyErrorBuilder::exist_error
    pub fn none_or_exist(&self, ctx: impl Display) -> Result<(), K::ExistError>
    where K: MetaServiceKeyErrorBuilder {
        match self.value() {
            Some(_) => Err(self.key.exist_error(ctx)),
            None => Ok(()),
        }
    }

    /// Consume the handle, yielding the value read.
    pub fn into_value(self) -> Option<K::ValueType> {
        self.seq_v.map(|s| s.data)
    }

    /// Stage a delete of the read key.
    pub fn delete(self) {
        self.txn.delete(&self.key);
    }
}

impl<'t, 'a, KV, K> ForUpdate<'t, 'a, KV, K>
where
    KV: KVApi + ?Sized,
    KV::Error: From<InvalidArgument>,
    K: kvapi::Key,
    K::ValueType: FromToProto + 'static,
{
    /// Stage a put to the read key.
    pub fn put(self, value: &K::ValueType) -> Result<(), KV::Error> {
        self.txn.put(&self.key, value)
    }
}
