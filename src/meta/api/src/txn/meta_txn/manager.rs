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

use databend_common_meta_app::app_error::TxnRetryMaxTimes;
use databend_meta_client::kvapi::KVApi;

use super::MetaTxn;
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

    /// Drive `build` as a CAS retry loop, handing it a fresh [`MetaTxn`] each
    /// attempt and committing what it stages.
    ///
    /// `build` takes the transaction by value — it reads keys with
    /// [`get_for_update`](MetaTxn::get_for_update), stages writes, and returns
    /// the business outcome `T`. A failed commit means a value the closure read
    /// has changed, so the transaction is dropped and `build` runs again on a
    /// fresh one after backoff. Terminal outcomes must therefore be derived from
    /// the reads, not from whether the commit applied.
    ///
    /// Write the closure as `|txn| async move { … }` — an async closure
    /// `async |txn| …` would force a higher-ranked `Send` bound that does not
    /// hold inside `#[async_trait]`:
    ///
    /// ```ignore
    /// MetaTxnManager::new(kv, ctx)
    ///     .run(|txn| async move {
    ///         let cur = txn.get_for_update(&key).await?;
    ///         // ... inspect `cur`, then stage writes ...
    ///         txn.put(&key, &value)?;
    ///         Ok(())
    ///     })
    ///     .await
    /// ```
    pub async fn run<T, E, F, Fut>(&self, mut build: F) -> Result<T, E>
    where
        F: FnMut(MetaTxn<'a, KV>) -> Fut,
        Fut: Future<Output = Result<T, E>>,
        E: From<KV::Error> + From<TxnRetryMaxTimes>,
    {
        let mut trials = txn_backoff(self.retries, self.ctx.clone());
        loop {
            trials.next().unwrap()?.await;

            // The closure gets only the staging handle. The manager keeps the
            // commit handle so user code cannot drain state or bypass retries.
            let (txn, commit) = MetaTxn::new(self.kv);
            let out = build(txn).await?;

            // A closure that stages no write produced its result purely from the
            // reads; there is nothing to commit, so return it directly.
            if commit.is_empty() {
                return Ok(out);
            }

            if commit.execute().await? {
                return Ok(out);
            }
            // Commit failed: a read changed under us. Retry on a fresh txn.
        }
    }
}
