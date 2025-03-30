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

use std::future::Future;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use codeq::Encode;
use databend_common_base::runtime::spawn_named;
use databend_common_meta_client::ClientHandle;
use databend_common_meta_kvapi::kvapi::KVApi;
use databend_common_meta_types::protobuf as pb;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::SeqValue;
use databend_common_meta_types::UpsertKV;
use databend_common_meta_types::With;
use futures::FutureExt;
use log::debug;
use log::info;
use log::warn;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::acquirer::Permit;
use crate::errors::AcquireError;
use crate::errors::ConnectionClosed;
use crate::errors::EarlyRemoved;
use crate::queue::PermitEvent;
use crate::PermitEntry;
use crate::PermitKey;

/// The acquirer is responsible for acquiring a semaphore permit.
///
/// It is used to acquire a semaphore by creating a new [`PermitEntry`] in the meta-service.
///
/// The acquirer will keep the semaphore entry alive by extending the lease periodically,
/// until it is acquired or removed.
pub(crate) struct Acquirer {
    pub(crate) prefix: String,

    /// The ID of this acquirer.
    ///
    /// Different ID represent different trail to acquire a semaphore.
    /// The ID is used as part of the [`PermitEntry`].
    pub(crate) acquirer_id: String,

    /// The time to live if the [`Acquirer`] does not extend the lease.
    ///
    /// For example, the [`Acquirer`] crashed abnormally.
    pub(crate) lease: Duration,

    /// The key of the permit sequence number generator.
    ///
    /// It is used to generate a globally unique sequence number for each [`Permit`].
    /// [`Permit`] will be sorted in meta-service by this sequence number
    /// and to determine the order to be acquired.
    pub(crate) seq_generator_key: String,

    /// The meta client to interact with the meta-service.
    pub(crate) meta_client: Arc<ClientHandle>,

    /// The sender to cancel the subscriber task.
    pub(crate) subscriber_cancel_tx: oneshot::Sender<()>,

    /// The receiver to receive semaphore state change events from the internal subscriber task.
    /// This task subscribes to the watch stream and forwards relevant state changes through this channel.
    pub(crate) permit_event_rx: mpsc::Receiver<PermitEvent>,

    /// The context information of this acquirer instance, used for logging.
    pub(crate) ctx: String,
}

impl Acquirer {
    /// Acquires a new semaphore permit and returns a [`Permit`] handle.
    pub async fn acquire(mut self) -> Result<Permit, AcquireError> {
        let mut sleep_time = Duration::from_millis(10);
        let max_sleep_time = Duration::from_secs(1);

        let permit_entry = PermitEntry {
            id: self.acquirer_id.clone(),
            permits: 1,
        };
        let val_bytes = permit_entry
            .encode_to_vec()
            .map_err(|e| conn_io_error(e, "encode semaphore entry").context(&self.ctx))?;

        // The sem_key is the key of the semaphore entry.
        let permit_key = loop {
            // Step 1: Get a new globally unique sequence number.
            let sem_seq = self.next_global_unique_seq().await?;

            // Step 2: Create a new semaphore entry with the key format `{prefix}/queue/{seq:020}`.
            //         We use a transaction to ensure the entry is only inserted if the sequence number
            //         hasn't changed since we obtained it. This guarantees that semaphore entries are
            //         inserted in the same order as their sequence numbers, which is critical for
            //         consistent acquisition order across distributed processes.
            //         If the transaction fails, we retry with a new sequence number.
            //
            //         See the lib doc for more details.
            let sem_key = PermitKey::new(self.prefix.clone(), sem_seq);
            let sem_key_str = sem_key.format_key();

            let txn = pb::TxnRequest::default();

            let cond = pb::TxnCondition::eq_seq(&self.seq_generator_key, sem_seq);
            let op = pb::TxnOp::put_with_ttl(&sem_key_str, val_bytes.clone(), Some(self.lease));
            let txn = txn.push_if_then([cond], [op]);

            let txn_reply = self.meta_client.transaction(txn).await.map_err(|e| {
                conn_io_error(
                    e,
                    format!(
                        "insert semaphore (seq={} entry={}) in transaction",
                        &sem_key, &permit_entry
                    ),
                )
                .context(&self.ctx)
            })?;

            if txn_reply.success {
                info!("acquire semaphore: {} -> {}", self.acquirer_id, sem_seq);
                break sem_key;
            } else {
                info!(
                    "acquire semaphore failed: {} -> {}; sleep {:?} and retry",
                    self.acquirer_id, sem_seq, sleep_time
                );

                tokio::time::sleep(sleep_time).await;
                sleep_time = std::cmp::min(sleep_time * 3 / 2, max_sleep_time);
            }
        };

        // Step 3: The sem entry is inserted, keep it alive by extending the lease.

        let leaser_cancel_tx = self.spawn_extend_lease_task(permit_key.clone(), val_bytes);

        // Step 4: Wait for the semaphore to be acquired or removed.

        while let Some(sem_event) = self.permit_event_rx.recv().await {
            debug!("semaphore event: {:?}", sem_event);
            match sem_event {
                PermitEvent::Acquired((seq, _)) => {
                    if seq == permit_key.seq {
                        debug!(
                            "{} acquired: {}->{}",
                            self.ctx, permit_key, self.acquirer_id
                        );
                        break;
                    }
                }
                PermitEvent::Removed((seq, _)) => {
                    if seq == permit_key.seq {
                        warn!(
                            "semaphore removed before acquired: {}->{}",
                            permit_key, self.acquirer_id
                        );
                        return Err(AcquireError::EarlyRemoved(EarlyRemoved::new(
                            permit_key.clone(),
                            permit_entry.clone(),
                        )));
                    }
                }
            }
        }

        let guard = Permit::new(
            self.permit_event_rx,
            permit_key,
            permit_entry,
            self.subscriber_cancel_tx,
            leaser_cancel_tx,
        );

        Ok(guard)
    }

    /// Gets a new globally unique sequence number by updating a key in the meta-service.
    ///
    /// This method uses the meta client to perform an upsert operation on the sequence
    /// generator key, which atomically increments the sequence number and returns the
    /// new value.
    async fn next_global_unique_seq(&self) -> Result<u64, ConnectionClosed> {
        let upsert = UpsertKV::update(&self.seq_generator_key, b"");

        let res = self.meta_client.upsert_kv(upsert).await;
        let resp = res.map_err(|e| {
            conn_io_error(e, "upsert seq_generator_key to get a new seq").context(&self.ctx)
        })?;

        debug_assert!(
            resp.result.is_some(),
            "upsert to get semaphore seq, the result must not be None"
        );

        let seq = resp.result.seq();
        Ok(seq)
    }

    /// Spawns a background task that periodically extends the lease of a [`PermitEntry`].
    ///
    /// This task ensures the [`PermitEntry`] remains valid in the meta-service by refreshing
    /// its TTL before expiration. The task continues until explicitly canceled
    /// by dropping the returned cancel sender, at which point it will remove the [`PermitEntry`].
    fn spawn_extend_lease_task(
        &self,
        sem_key: PermitKey,
        val_bytes: Vec<u8>,
    ) -> oneshot::Sender<()> {
        let (cancel_tx, cancel_rx) = oneshot::channel::<()>();

        let fu = Self::extend_lease_loop(
            self.meta_client.clone(),
            sem_key.clone(),
            val_bytes,
            self.lease,
            cancel_rx.map(|_| ()),
        );

        let task_name = format!("{}/(seq={})", self.ctx, sem_key.seq);

        spawn_named(
            async move {
                let res = fu.await;
                if let Err(e) = res {
                    warn!(
                        "semaphore lease extended task quit with error: {}; semaphore key: {}",
                        e, sem_key
                    );
                }
            },
            task_name,
        );

        cancel_tx
    }

    /// A background task to extend the lease of the [`PermitEntry`] periodically
    ///
    /// If it receives the cancel signal, it will remove the [`PermitEntry`].
    async fn extend_lease_loop(
        meta_client: Arc<ClientHandle>,
        sem_key: PermitKey,
        val_bytes: Vec<u8>,
        ttl: Duration,
        cancel: impl Future<Output = ()> + Send + 'static,
    ) -> Result<(), ConnectionClosed> {
        let sleep_time = ttl / 3;
        let sleep_time = std::cmp::min(sleep_time, Duration::from_millis(2_000));

        let key_str = sem_key.format_key();

        let mut c = std::pin::pin!(cancel);

        loop {
            // Sleep for a while before the next extend.
            tokio::time::sleep(sleep_time).await;

            // Check if the cancel signal is received.
            if futures::poll!(c.as_mut()).is_ready() {
                info!(
                    "leaser task canceled by user: {}, about to remove the semaphore permit entry",
                    sem_key
                );

                let upsert = UpsertKV::delete(&key_str);

                let res = meta_client.upsert_kv(upsert).await;
                res.map_err(|e| conn_io_error(e, "remove semaphore permit entry"))?;

                return Ok(());
            }

            // Extend the lease only if the entry still exists. If the entry has been removed,
            // it means the semaphore has been released. Re-inserting it would cause confusion
            // in the semaphore state and potentially lead to inconsistent behavior.

            let upsert = UpsertKV::update(&key_str, &val_bytes)
                .with(MatchSeq::GE(1))
                .with_ttl(ttl);

            let res = meta_client.upsert_kv(upsert).await;
            res.map_err(|e| conn_io_error(e, "extend semaphore permit lease"))?;
        }
    }
}

/// Create a [`ConnectionClosed`] error from io error with context.
fn conn_io_error(e: impl Into<io::Error>, ctx: impl ToString) -> ConnectionClosed {
    ConnectionClosed::new_io_error(e).context(ctx)
}
