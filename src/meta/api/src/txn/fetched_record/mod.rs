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

use databend_common_meta_app::KeyExistsBuilder;
use databend_common_meta_app::KeyUnknownBuilder;
use databend_common_proto_conv::FromToProto;
use databend_meta_client::kvapi;
use databend_meta_client::types::SeqV;

use super::meta_txn::MetaTxn;

mod absent;
mod present;
mod target;

pub use absent::AbsentRecord;
pub use present::PresentRecord;
use target::FetchedRecordTarget;

/// A record fetched by a transaction for-update read.
///
/// It borrows the transaction and holds the key and value read. If it came from
/// [`MetaTxn::get_for_update`], the `eq_seq` guard was installed in the
/// transaction's read set when the key was read. Staging a write through it
/// ([`stage_put`](Self::stage_put) / [`stage_delete`](Self::stage_delete)) goes
/// back to that transaction and reuses that key, so a write cannot target a
/// different key than the one guarded, and replaces any operation previously
/// staged for it.
///
/// The handle borrows the transaction shared-ly (the transaction keeps its read
/// and write sets behind a lock), so several handles may be held at once: read a
/// batch of keys for update, inspect them, then stage the writes - without
/// passing the transaction around.
pub struct FetchedRecord<'t, 'a, KV: ?Sized, K: kvapi::Key> {
    target: FetchedRecordTarget<'t, 'a, KV, K>,
    seq_v: Option<SeqV<K::ValueType>>,
}

impl<'t, 'a, KV, K> FetchedRecord<'t, 'a, KV, K>
where
    KV: ?Sized,
    K: kvapi::Key,
{
    pub(crate) fn new(txn: &'t MetaTxn<'a, KV>, key: K, seq_v: Option<SeqV<K::ValueType>>) -> Self {
        Self {
            target: FetchedRecordTarget { txn, key },
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
    /// [`unknown_error`]: KeyUnknownBuilder::unknown_error
    pub fn some_or_unknown(
        self,
        ctx: impl Display,
    ) -> Result<PresentRecord<'t, 'a, KV, K>, K::UnknownError>
    where
        K: KeyUnknownBuilder,
    {
        let Self { target, seq_v } = self;

        match seq_v {
            Some(seq_v) => Ok(PresentRecord { target, seq_v }),
            None => Err(target.key.unknown_error(ctx)),
        }
    }

    /// Assert the value does not exist, or return the key's [`exist_error`].
    ///
    /// This consumes the optional handle and returns an absent-only handle, so
    /// follow-up code can create the value without re-checking the option.
    ///
    /// [`exist_error`]: KeyExistsBuilder::exist_error
    pub fn none_or_exists(
        self,
        ctx: impl Display,
    ) -> Result<AbsentRecord<'t, 'a, KV, K>, K::ExistError>
    where
        K: KeyExistsBuilder,
    {
        let Self { target, seq_v } = self;

        match seq_v {
            Some(_) => Err(target.key.exist_error(ctx)),
            None => Ok(AbsentRecord { target }),
        }
    }

    /// Consume the handle, yielding the value read.
    pub fn into_value(self) -> Option<K::ValueType> {
        let Self { target, seq_v } = self;
        seq_v.map(|seq_v| PresentRecord { target, seq_v }.into_value())
    }

    /// Stage a delete of the read key.
    pub fn stage_delete(self) {
        self.target.stage_delete();
    }
}

impl<'t, 'a, KV, K> FetchedRecord<'t, 'a, KV, K>
where
    KV: ?Sized,
    K: kvapi::Key,
    K::ValueType: FromToProto + 'static,
{
    /// Stage a put to the read key.
    pub fn stage_put(self, value: &K::ValueType) {
        self.target.stage_put(value)
    }
}
