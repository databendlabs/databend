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

use core::fmt;
use std::future::Future;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use codeq::Encode;
use databend_meta_client::ClientHandle;
use databend_meta_runtime::DatabendRuntime;
use databend_meta_runtime_api::SpawnApi;
use databend_meta_types::MatchSeq;
use databend_meta_types::UpsertKV;
use databend_meta_types::With;
use databend_meta_types::protobuf as pb;
use futures::FutureExt;
use log::info;
use log::warn;
use seq_marked::SeqValue;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::PermitEntry;
use crate::PermitKey;
use crate::acquirer::Permit;
use crate::acquirer::SharedAcquirerStat;
use crate::errors::AcquireError;
use crate::errors::ConnectionClosed;
use crate::errors::EarlyRemoved;
use crate::queue::PermitEvent;

#[derive(Clone, Debug)]
pub(crate) enum SeqPolicy {
    /// Use a sequence number generator to ensure globally unique sequence numbers.
    ///
    /// Retry if the seq generator is updated by other process.
    GeneratorKey { generator_key: String },

    /// Use timestamp-based sequence numbering for permit ordering.
    ///
    /// More efficient than generator-based approach but may allow permits with
    /// duplicate timestamps. Retries automatically increment the timestamp by 1µs.
    ///
    /// The timestamp parameter allows applications to specify a particular time point,
    /// useful for scenarios like retry operations where consistency is needed.
    TimeBased { timestamp: Duration },
}

/// The acquirer is responsible for acquiring a semaphore permit.
///
/// It is used to acquire a semaphore by creating a new [`PermitEntry`] in the meta-service.
///
/// The acquirer will keep the semaphore entry alive by extending the lease periodically,
/// until it is acquired or removed.
pub struct Acquirer {
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

    /// The way to generate sequence number for a [`PermitKey`].
    ///
    /// It is used to generate a globally unique sequence number for each [`Permit`].
    /// [`Permit`] will be sorted in meta-service by this sequence number
    /// and to determine the order to be acquired.
    pub(crate) seq_policy: SeqPolicy,

    /// The meta client to interact with the meta-service.
    pub(crate) meta_client: Arc<ClientHandle<DatabendRuntime>>,

    /// The sender to cancel the subscriber task.
    pub(crate) subscriber_cancel_tx: oneshot::Sender<()>,

    /// The receiver to receive semaphore state change events from the internal subscriber task.
    /// This task subscribes to the watch stream and forwards relevant state changes through this channel.
    pub(crate) permit_event_rx: mpsc::Receiver<Result<PermitEvent, ConnectionClosed>>,

    /// The stat about the process of acquiring the semaphore.
    pub(crate) stat: SharedAcquirerStat,

    pub(crate) acquire_in_progress: Option<DropHandle>,

    /// The context information of this acquirer instance, used for logging.
    pub(crate) name: String,
}

pub(crate) struct DropHandle {
    pub(crate) name: String,
    pub(crate) finished: bool,
}

impl Drop for DropHandle {
    fn drop(&mut self) {
        if !self.finished {
            warn!(
                "{} dropped before finishing acquiring the permit",
                self.name
            )
        }
    }
}

pub struct QueuedPermit {
    pub(crate) permit_key: PermitKey,
    pub(crate) permit_entry: PermitEntry,
    pub(crate) leaser_cancel_tx: oneshot::Sender<()>,
}

impl fmt::Debug for QueuedPermit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "QueuedPermit{{ permit_key: {}, permit_entry: {} }}",
            self.permit_key, self.permit_entry
        )
    }
}

impl fmt::Display for QueuedPermit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "QueuedPermit{{ permit_key: {}, permit_entry: {} }}",
            self.permit_key, self.permit_entry
        )
    }
}

impl Acquirer {
    /// Acquires a semaphore permit by enqueuing a request and waiting for it to be granted.
    ///
    /// Returns a [`Permit`] handle that must be explicitly released when no longer needed.
    ///
    /// This is a convenience method that combines [`Self::enqueue_permit`] and [`Self::await_permit`].
    pub async fn acquire(mut self) -> Result<Permit, AcquireError> {
        let queued = self.enqueue_permit().await?;
        self.await_permit(queued).await
    }

    /// Enqueues a permit request to the semaphore queue in meta-service.
    ///
    /// Creates a new permit entry and adds it to the `<prefix>/queue` with a unique sequence number.
    /// The permit will be granted when it reaches the front of the queue based on available capacity.
    ///
    /// Returns a [`QueuedPermit`] containing the permit key and lease management handle for permit acquisition.
    pub async fn enqueue_permit(&mut self) -> Result<QueuedPermit, ConnectionClosed> {
        self.stat.start();

        let permit_entry = PermitEntry {
            id: self.acquirer_id.clone(),
            permits: 1,
        };
        let val_bytes = permit_entry
            .encode_to_vec()
            .map_err(|e| conn_io_error(e, "encode semaphore entry").context(&self.name))?;

        // Step 1: Create a new semaphore entry with the key format `{prefix}/queue/{seq:020}`.

        let permit_key = match self.seq_policy.clone() {
            SeqPolicy::GeneratorKey { generator_key } => {
                self.enqueue_permit_entry(&permit_entry, &generator_key)
                    .await?
            }
            SeqPolicy::TimeBased { timestamp } => {
                self.enqueue_time_based_permit_entry(&permit_entry, timestamp)
                    .await?
            }
        };

        info!("{} created: {}", self.name, permit_key);

        // Step 2: The sem entry is inserted, keep it alive by extending the lease.

        let leaser_cancel_tx = self.spawn_extend_lease_task(permit_key.clone(), val_bytes);

        Ok(QueuedPermit {
            permit_key,
            permit_entry,
            leaser_cancel_tx,
        })
    }

    /// Waits for the permit to be granted by the semaphore queue.
    ///
    /// It will wait until the permit is acquired or removed(failed to acquire).
    pub async fn await_permit(mut self, enqueued: QueuedPermit) -> Result<Permit, AcquireError> {
        // Step 1 assign the enqueued values to the local variables.
        let QueuedPermit {
            permit_key,
            permit_entry,
            leaser_cancel_tx,
        } = enqueued;

        // Step 3: Wait for the semaphore to be acquired or removed.

        let mut acquired = false;

        while let Some(sem_event_res) = self.permit_event_rx.recv().await {
            info!(
                "Acquirer({}): received semaphore event: {:?}",
                self.name, sem_event_res
            );

            let sem_event = sem_event_res?;

            self.stat.on_receive_event(&sem_event);

            match sem_event {
                PermitEvent::Acquired((seq, _)) => {
                    if seq == permit_key.seq {
                        self.stat.on_acquire();

                        info!(
                            "{} acquired: {}->{}",
                            self.name, permit_key, self.acquirer_id
                        );

                        acquired = true;
                        break;
                    }
                }
                PermitEvent::Removed((seq, _)) => {
                    if seq == permit_key.seq {
                        self.stat.on_remove();

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

        if acquired {
            let mut in_proc = self.acquire_in_progress.take().unwrap();
            in_proc.finished = true;

            let permit = Permit::new(self, permit_key, permit_entry, leaser_cancel_tx);

            Ok(permit)
        } else {
            Err(ConnectionClosed::new_str("event channel closed").into())
        }
    }

    /// Add a new semaphore entry to the `<prefix>/queue` in the meta-service.
    ///
    /// In this method, the sequence number is generated in meta-service, by updating a generator key.
    /// The semaphore entry is inserted with this seq number as key.
    async fn enqueue_permit_entry(
        &mut self,
        permit_entry: &PermitEntry,
        seq_generator_key: &str,
    ) -> Result<PermitKey, ConnectionClosed> {
        let mut sleep_time = Duration::from_millis(50);
        let max_sleep_time = Duration::from_secs(2);

        let val_bytes = permit_entry
            .encode_to_vec()
            .map_err(|e| conn_io_error(e, "encode semaphore entry").context(&self.name))?;

        loop {
            // Step 1: Get a new globally unique sequence number.
            let sem_seq = self.next_global_unique_seq(seq_generator_key).await?;

            self.stat.on_finish_get_seq();

            // Step 2: Create a new semaphore entry with the key format `{prefix}/queue/{seq:020}`.
            //         We use a transaction to ensure the entry is only inserted if the sequence number
            //         hasn't changed since we obtained it. This guarantees that semaphore entries are
            //         inserted in the same order as their sequence numbers, which is critical for
            //         consistent acquisition order across distributed processes.
            //         If the transaction fails, we retry with a new sequence number.
            //
            //         See the lib doc for more details.
            let permit_key = PermitKey::new(self.prefix.clone(), sem_seq);
            let sem_key_str = permit_key.format_key();

            let txn = pb::TxnRequest::default();

            let cond = pb::TxnCondition::eq_seq(seq_generator_key, sem_seq);
            let op = pb::TxnOp::put_with_ttl(&sem_key_str, val_bytes.clone(), Some(self.lease));
            let txn = txn.push_if_then([cond], [op]);

            let txn_reply = self.meta_client.transaction(txn).await.map_err(|e| {
                conn_io_error(
                    e,
                    format!(
                        "insert semaphore (seq={} entry={}) in transaction",
                        &permit_key, &permit_entry
                    ),
                )
                .context(&self.name)
            })?;

            self.stat.on_finish_try_insert_seq();

            if txn_reply.success {
                info!(
                    "acquire semaphore: enqueue done: acquirer_id: {}, sem_seq: {}",
                    self.acquirer_id, sem_seq
                );
                self.stat.on_insert_seq(permit_key.seq);
                return Ok(permit_key);
            } else {
                info!(
                    "acquire semaphore: enqueue conflict: acquirer: {}, sem_seq: {}; sleep {:?} and retry",
                    self.acquirer_id, sem_seq, sleep_time
                );

                tokio::time::sleep(sleep_time).await;
                sleep_time = std::cmp::min(sleep_time * 3 / 2, max_sleep_time);
            }
        }
    }

    /// Enqueues a permit entry using timestamp-based sequence numbering.
    ///
    /// Uses the provided timestamp as the sequence number for ordering permits in the queue.
    /// This approach is more efficient but may allow non-strict ordering since timestamps
    /// may not be globally unique across distributed processes.
    ///
    /// # Conflict Resolution
    /// When timestamp conflicts occur, the method automatically increments the timestamp
    /// by 1 microsecond and retries. This means the final sequence number may differ
    /// from the initially provided timestamp.
    ///
    /// # Ordering Guarantees
    /// Unlike generator-based sequencing, timestamp-based sequencing may allow more
    /// permits than the semaphore capacity to be acquired due to potential race conditions.
    async fn enqueue_time_based_permit_entry(
        &mut self,
        permit_entry: &PermitEntry,
        mut timestamp: Duration,
    ) -> Result<PermitKey, ConnectionClosed> {
        let val_bytes = permit_entry
            .encode_to_vec()
            .map_err(|e| conn_io_error(e, "encode semaphore entry").context(&self.name))?;

        loop {
            // Use provided timestamp.
            // On conflict, increment timestamp by 1 microsecond for retry.
            let sem_seq = {
                let micros = timestamp.as_micros() as u64;
                timestamp += Duration::from_micros(1);
                micros
            };

            self.stat.on_finish_get_seq();

            // Create a new semaphore entry with the key format `{prefix}/queue/{seq:020}`.
            let permit_key = PermitKey::new(self.prefix.clone(), sem_seq);
            let sem_key_str = permit_key.format_key();

            let upsert = UpsertKV::insert(&sem_key_str, &val_bytes).with_ttl(self.lease);

            let res = self.meta_client.upsert_kv(upsert).await;
            let resp = res.map_err(|e| {
                conn_io_error(
                    e,
                    format!(
                        "insert semaphore (seq={} entry={}) in transaction",
                        &permit_key, &permit_entry
                    ),
                )
                .context(&self.name)
            })?;

            self.stat.on_finish_try_insert_seq();

            if resp.result.is_some() {
                info!(
                    "acquire semaphore: enqueue done: acquirer_id: {}, sem_seq: {}",
                    self.acquirer_id, sem_seq
                );
                self.stat.on_insert_seq(permit_key.seq);
                return Ok(permit_key);
            } else {
                info!(
                    "acquire semaphore: enqueue failed(duplicated key): acquirer: {}, sem_seq: {}; retry at once",
                    self.acquirer_id, sem_seq
                );
            }
        }
    }

    /// Gets a new globally unique sequence number by updating a key in the meta-service.
    ///
    /// This method uses the meta client to perform an upsert operation on the sequence
    /// generator key, which atomically increments the sequence number and returns the
    /// new value.
    async fn next_global_unique_seq(
        &self,
        seq_generator_key: &str,
    ) -> Result<u64, ConnectionClosed> {
        let upsert = UpsertKV::update(seq_generator_key, b"");

        let res = self.meta_client.upsert_kv(upsert).await;
        let resp = res.map_err(|e| {
            conn_io_error(e, "upsert seq_generator_key to get a new seq").context(&self.name)
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
        permit_key: PermitKey,
        val_bytes: Vec<u8>,
    ) -> oneshot::Sender<()> {
        let (cancel_tx, cancel_rx) = oneshot::channel::<()>();

        let fu = Self::extend_lease_loop(
            self.meta_client.clone(),
            permit_key.clone(),
            val_bytes,
            self.lease,
            cancel_rx.map(|_| ()),
            self.name.clone(),
        );

        let task_name = format!("{}/(seq={})", self.name, permit_key.seq);

        DatabendRuntime::spawn(
            async move {
                let res = fu.await;
                if let Err(e) = res {
                    warn!(
                        "semaphore lease extended task quit with error: {}; semaphore key: {}",
                        e, permit_key
                    );
                }
            },
            Some(task_name),
        );

        cancel_tx
    }

    /// A background task to extend the lease of the [`PermitEntry`] periodically
    ///
    /// If it receives the cancel signal, it will remove the [`PermitEntry`].
    async fn extend_lease_loop(
        meta_client: Arc<ClientHandle<DatabendRuntime>>,
        permit_key: PermitKey,
        val_bytes: Vec<u8>,
        ttl: Duration,
        cancel: impl Future<Output = ()> + Send + 'static,
        acquirer_name: impl ToString,
    ) -> Result<(), ConnectionClosed> {
        let acquirer_name = acquirer_name.to_string();

        let sleep_time = ttl / 3;
        let sleep_time = std::cmp::min(sleep_time, Duration::from_millis(2_000));

        let key_str = permit_key.format_key();

        let mut c = std::pin::pin!(cancel);
        let mut failure_count = 0;

        loop {
            tokio::select! {
                // Sleep for a while before the next extend.
                _ = tokio::time::sleep(sleep_time) => {
                    // Extend the lease only if the entry still exists. If the entry has been removed,
                    // it means the semaphore has been released. Re-inserting it would cause confusion
                    // in the semaphore state and potentially lead to inconsistent behavior.
                    log::debug!(
                        "{}: About to extend semaphore permit lease: {} ttl: {:?}",
                        acquirer_name, key_str, ttl
                    );

                    let upsert = UpsertKV::update(&key_str, &val_bytes)
                        .with(MatchSeq::GE(1))
                        .with_ttl(ttl);

                    let res = meta_client.upsert_kv(upsert).await;
                    if let Err(e) = res {
                        failure_count += 1;
                        warn!("{}: {}-th {} when extending lease for {}, retry after {:?}", acquirer_name, failure_count, e, permit_key,  sleep_time);
                    } else {
                        failure_count = 0;
                    }
                }
                // Wait if the cancel signal is received.
                _ = &mut c => {
                    // The permit is dropped by the user, we should remove the semaphore permit entry.
                    info!(
                        "leaser task canceled by user: {}, about to remove the semaphore permit entry",
                        permit_key
                    );

                    let upsert = UpsertKV::delete(&key_str);

                    let res = meta_client.upsert_kv(upsert).await;
                    if let Err(e) = res {
                        warn!("{}: {} when removing {} after release, leave it until ttl expires", acquirer_name, e, permit_key);
                    }

                    return Ok(());
                }
            }
        }
    }
}

/// Create a [`ConnectionClosed`] error from io error with context.
fn conn_io_error(e: impl Into<io::Error>, ctx: impl ToString) -> ConnectionClosed {
    ConnectionClosed::new_io_error(e).context(ctx)
}
