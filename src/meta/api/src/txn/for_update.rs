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
use crate::txn::absent_for_update::AbsentForUpdate;
use crate::txn::for_update_target::ForUpdateTarget;
use crate::txn::present_for_update::PresentForUpdate;

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
    target: ForUpdateTarget<'t, 'a, KV, K>,
    seq_v: Option<SeqV<K::ValueType>>,
}

impl<'t, 'a, KV, K> ForUpdate<'t, 'a, KV, K>
where
    KV: ?Sized,
    K: kvapi::Key,
{
    pub(crate) fn new(txn: &'t MetaTxn<'a, KV>, key: K, seq_v: Option<SeqV<K::ValueType>>) -> Self {
        Self {
            target: ForUpdateTarget { txn, key },
            seq_v,
        }
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

    /// Assert the value exists, or return the key's [`unknown_error`].
    ///
    /// This consumes the optional handle and returns a present-only handle, so
    /// follow-up code can use the seq/value without re-checking the option.
    ///
    /// [`unknown_error`]: MetaServiceKeyErrorBuilder::unknown_error
    pub fn some_or_unknown_error(
        self,
        ctx: impl Display,
    ) -> Result<PresentForUpdate<'t, 'a, KV, K>, K::UnknownError>
    where
        K: MetaServiceKeyErrorBuilder,
    {
        let Self { target, seq_v } = self;

        match seq_v {
            Some(seq_v) => Ok(PresentForUpdate { target, seq_v }),
            None => Err(target.key.unknown_error(ctx)),
        }
    }

    /// Assert the value does not exist, or return the key's [`exist_error`].
    ///
    /// This consumes the optional handle and returns an absent-only handle, so
    /// follow-up code can create the value without re-checking the option.
    ///
    /// [`exist_error`]: MetaServiceKeyErrorBuilder::exist_error
    pub fn none_or_exist_error(
        self,
        ctx: impl Display,
    ) -> Result<AbsentForUpdate<'t, 'a, KV, K>, K::ExistError>
    where
        K: MetaServiceKeyErrorBuilder,
    {
        let Self { target, seq_v } = self;

        match seq_v {
            Some(_) => Err(target.key.exist_error(ctx)),
            None => Ok(AbsentForUpdate { target }),
        }
    }

    /// Consume the handle, yielding the value read.
    pub fn into_value(self) -> Option<K::ValueType> {
        let Self { target, seq_v } = self;
        seq_v.map(|seq_v| PresentForUpdate { target, seq_v }.into_value())
    }

    /// Stage a delete of the read key.
    pub fn delete(self) {
        self.target.delete();
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
        self.target.put(value)
    }
}
