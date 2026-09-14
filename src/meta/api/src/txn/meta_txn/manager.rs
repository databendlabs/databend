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
use std::future::Future;

use databend_common_meta_app::KeyExistsBuilder;
use databend_common_meta_app::KeyUnknownBuilder;
use databend_common_proto_conv::FromToProto;
use databend_meta_client::kvapi;
use databend_meta_client::kvapi::KVApi;

use super::KvApiOrUserError;
use super::MetaTxn;
use super::MoveKeyError;
use super::RunError;
use crate::kv_pb_api::errors::PbDecodeError;
use crate::txn_backoff::txn_backoff;

/// The retry driver for [`MetaTxn`].
///
/// It binds a KV backend and a retry policy and drives a closure as a CAS retry
/// loop via [`run`](Self::run). Each attempt runs against a *fresh* transaction
/// state, so a failed commit simply discards it and the closure runs again on a
/// clean one — no read or write state leaks between attempts.
pub struct MetaTxnManager<'a, KV: ?Sized> {
    kv: &'a KV,
    ctx: String,
    retries: Option<u32>,
}

impl<'a, KV> MetaTxnManager<'a, KV>
where KV: KVApi + ?Sized
{
    pub fn new(kv: &'a KV, ctx: impl Display) -> Self {
        Self {
            kv,
            ctx: ctx.to_string(),
            retries: None,
        }
    }

    /// Cap the retries [`run`](Self::run) performs. `None` uses the default
    /// backoff length.
    pub fn retries(mut self, n: Option<u32>) -> Self {
        self.retries = n;
        self
    }

    /// Move `old_key` to `new_key`, preserving its value.
    ///
    /// The source must exist and the target must be absent. Both keys are read
    /// for update, so the commit is guarded by the versions observed in the
    /// same transaction attempt.
    pub async fn move_key<K>(
        &self,
        old_key: K,
        new_key: K,
    ) -> Result<(), MoveKeyError<KV::Error, K::UnknownError, K::ExistError>>
    where
        K: kvapi::Key + KeyUnknownBuilder + KeyExistsBuilder + Clone,
        K::ValueType: FromToProto + 'static,
        KV::Error: From<PbDecodeError>,
    {
        let ctx = self.ctx.clone();

        self.run(|txn| {
            let old_key = old_key.clone();
            let new_key = new_key.clone();
            let ctx = ctx.clone();
            async move {
                let old = txn.get_for_update(old_key).await?;
                let old = old
                    .some_or_unknown(&ctx)
                    .map_err(KvApiOrUserError::unknown)?;

                let new = txn.get_for_update(new_key).await?;
                let new = new.none_or_exists(&ctx).map_err(KvApiOrUserError::exists)?;

                new.stage_put(old.value());
                old.stage_delete();

                Ok(())
            }
        })
        .await?;

        Ok(())
    }

    /// Remove `key` if it exists and its sequence matches `seq`.
    ///
    /// On sequence mismatch, the key's own unknown error is built from the
    /// manager context plus expected/current seq details.
    pub async fn remove_key<K>(
        &self,
        key: K,
        seq: Option<u64>,
    ) -> Result<(), RunError<KV::Error, K::UnknownError>>
    where
        K: kvapi::Key + KeyUnknownBuilder + Clone,
        K::ValueType: FromToProto,
        KV::Error: From<PbDecodeError>,
    {
        let ctx = self.ctx.clone();

        self.run(|txn| {
            let key = key.clone();
            let ctx = ctx.clone();
            async move {
                let record = txn.get_for_update(key.clone()).await?;
                let record = record
                    .some_or_unknown(&ctx)
                    .map_err(KvApiOrUserError::user)?;

                if let Some(seq) = seq {
                    if record.seq() != seq {
                        return Err(KvApiOrUserError::user(key.unknown_error(
                            remove_key_seq_mismatch_context(&ctx, seq, record.seq()),
                        )));
                    }
                }

                record.stage_delete();
                Ok(())
            }
        })
        .await
    }

    /// Drive `build` as a CAS retry loop, handing it a fresh [`MetaTxn`] each
    /// attempt and committing what it stages.
    ///
    /// `build` takes the transaction by value — it reads keys with
    /// [`get_for_update`](MetaTxn::get_for_update), stages writes, and returns
    /// the business outcome `TxnOutput`. A failed commit means a value the
    /// closure read has changed, so the transaction is dropped and `build` runs
    /// again on a fresh one after backoff. Terminal outcomes must therefore be
    /// derived from the reads, not from whether the commit applied.
    ///
    /// Write the closure as `|txn| async move { … }` — an async closure
    /// `async |txn| …` would force a higher-ranked `Send` bound that does not
    /// hold inside `#[async_trait]`:
    ///
    /// ```ignore
    /// MetaTxnManager::new(kv, ctx)
    ///     .run(|txn| async move {
    ///         let cur = txn.get_for_update(key.clone()).await?;
    ///         // ... inspect `cur`, then stage writes ...
    ///         cur.stage_put(&value);
    ///         Ok::<_, KvApiOrUserError<_, MyError>>(())
    ///     })
    ///     .await
    /// ```
    pub async fn run<TxnOutput, UserError, BuildFn, BuildFuture>(
        &self,
        mut build: BuildFn,
    ) -> Result<TxnOutput, RunError<KV::Error, UserError>>
    where
        BuildFn: FnMut(MetaTxn<'a, KV>) -> BuildFuture,
        BuildFuture: Future<Output = Result<TxnOutput, KvApiOrUserError<KV::Error, UserError>>>,
    {
        let mut trials = txn_backoff(self.retries, self.ctx.clone());
        loop {
            let trial = trials.next().unwrap();
            trial?.await;

            // The closure gets a cloned handle. The manager keeps one so user
            // code cannot drain state or bypass retries.
            let txn = MetaTxn::new(self.kv);
            let commit = txn.clone();
            let out = build(txn).await?;

            // A closure that stages no write produced its result purely from the
            // reads; there is nothing to commit, so return it directly.
            if commit.is_empty() {
                return Ok(out);
            }

            let (succ, _responses) = commit.execute().await.map_err(RunError::KvApi)?;
            if succ {
                return Ok(out);
            }
            // Commit failed: a read changed under us. Retry on a fresh txn.
        }
    }
}

fn remove_key_seq_mismatch_context(ctx: &str, expected: u64, current: u64) -> String {
    format!("{ctx}; seq mismatched: expect {expected}, current {current}")
}
