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
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use codeq::Encode;
use databend_common_base::runtime::spawn_named;
use databend_common_meta_client::ClientHandle;
use databend_common_meta_kvapi::kvapi::KVApi;
use databend_common_meta_types::protobuf as pb;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::UpsertKV;
use databend_common_meta_types::With;
use futures::FutureExt;
use log::info;
use log::warn;
use seq_marked::SeqValue;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::acquirer::Permit;
use crate::acquirer::SharedAcquirerStat;
use crate::errors::AcquireError;
use crate::errors::ConnectionClosed;
use crate::errors::EarlyRemoved;
use crate::queue::PermitEvent;
use crate::PermitEntry;
use crate::PermitKey;

#[derive(Clone, Debug)]
pub(crate) enum SeqPolicy {
    /// Use a sequence number generator to ensure globally unique sequence numbers.
    ///
    /// Retry if the seq generator is updated by other process.
    GeneratorKey { generator_key: String },

    /// Use the current timestamp as the sequence number.
    ///
    /// Retry if duplicated
    TimeBased,
}

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

    /// The way to generate sequence number for a [`PermitKey`].
    ///
    /// It is used to generate a globally unique sequence number for each [`Permit`].
    /// [`Permit`] will be sorted in meta-service by this sequence number
    /// and to determine the order to be acquired.
    pub(crate) seq_policy: SeqPolicy,

    /// The meta client to interact with the meta-service.
    pub(crate) meta_client: Arc<ClientHandle>,

    /// The sender to cancel the subscriber task.
    pub(crate) subscriber_cancel_tx: oneshot::Sender<()>,

    /// The receiver to receive semaphore state change events from the internal subscriber task.
    /// This task subscribes to the watch stream and forwards relevant state changes through this channel.
    pub(crate) permit_event_rx: mpsc::Receiver<PermitEvent>,

    /// The stat about the process of acquiring the semaphore.
    pub(crate) stat: SharedAcquirerStat,

    /// The context information of this acquirer instance, used for logging.
    pub(crate) name: String,
}

pub struct QueuedPermit {
    pub(crate) permit_key: PermitKey,
    pub(crate) permit_entry: PermitEntry,
    pub(crate) leaser_cancel_tx: oneshot::Sender<()>,
}

impl Acquirer {
    /// Acquires a semaphore permit by enqueuing a request and waiting for it to be granted.
    ///
    /// Returns a [`Permit`] handle that must be explicitly released when no longer needed.
    ///
    /// It is a combination of [`Self::request_permit`] and [`Self::await_permit`].
    pub async fn acquire(mut self) -> Result<Permit, AcquireError> {
        let enqueued = self.request_permit().await?;
        self.await_permit(enqueued).await
    }

    /// Enqueues a permit request to the semaphore queue in meta-service.
    ///
    /// Creates a new permit entry and adds it to the `<prefix>/queue` with a unique sequence number.
    /// The permit will be granted when it reaches the front of the queue based on available capacity.
    ///
    /// Returns a [`QueuedPermit`] containing the permit key and lease management handle for permit acquisition.
    pub async fn request_permit(&mut self) -> Result<QueuedPermit, ConnectionClosed> {
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
            SeqPolicy::TimeBased => self.enqueue_time_based_permit_entry(&permit_entry).await?,
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

        while let Some(sem_event) = self.permit_event_rx.recv().await {
            info!(
                "Acquirer({}): received semaphore event: {:?}",
                self.name, sem_event
            );

            self.stat.on_receive_event(&sem_event);

            match sem_event {
                PermitEvent::Acquired((seq, _)) => {
                    if seq == permit_key.seq {
                        self.stat.on_acquire();

                        info!(
                            "{} acquired: {}->{}",
                            self.name, permit_key, self.acquirer_id
                        );
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

        let permit = Permit::new(self, permit_key, permit_entry, leaser_cancel_tx);

        Ok(permit)
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
            let sem_key = PermitKey::new(self.prefix.clone(), sem_seq);
            let sem_key_str = sem_key.format_key();

            let txn = pb::TxnRequest::default();

            let cond = pb::TxnCondition::eq_seq(seq_generator_key, sem_seq);
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
                .context(&self.name)
            })?;

            self.stat.on_finish_try_insert_seq();

            if txn_reply.success {
                info!(
                    "acquire semaphore: enqueue done: acquirer_id: {}, sem_seq: {}",
                    self.acquirer_id, sem_seq
                );
                self.stat.on_insert_seq(sem_key.seq);
                return Ok(sem_key);
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

    /// Add a new semaphore entry to the `<prefix>/queue` in the meta-service.
    ///
    /// In this method, the sequence number is generated with current timestamp,
    /// which means it may not be globally unique, on which case it will be retried,
    /// and it may not be in strict order, meaning more semaphore entries than the `capacity` may be acquired.
    async fn enqueue_time_based_permit_entry(
        &mut self,
        permit_entry: &PermitEntry,
    ) -> Result<PermitKey, ConnectionClosed> {
        let val_bytes = permit_entry
            .encode_to_vec()
            .map_err(|e| conn_io_error(e, "encode semaphore entry").context(&self.name))?;

        loop {
            let sem_seq = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64;

            self.stat.on_finish_get_seq();

            // Create a new semaphore entry with the key format `{prefix}/queue/{seq:020}`.
            let sem_key = PermitKey::new(self.prefix.clone(), sem_seq);
            let sem_key_str = sem_key.format_key();

            let upsert = UpsertKV::insert(&sem_key_str, &val_bytes).with_ttl(self.lease);

            let res = self.meta_client.upsert_kv(upsert).await;
            let resp = res.map_err(|e| {
                conn_io_error(
                    e,
                    format!(
                        "insert semaphore (seq={} entry={}) in transaction",
                        &sem_key, &permit_entry
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
                self.stat.on_insert_seq(sem_key.seq);
                return Ok(sem_key);
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
            self.name.clone(),
        );

        let task_name = format!("{}/(seq={})", self.name, sem_key.seq);

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
        ctx: impl ToString,
    ) -> Result<(), ConnectionClosed> {
        let ctx = ctx.to_string();

        let sleep_time = ttl / 3;
        let sleep_time = std::cmp::min(sleep_time, Duration::from_millis(2_000));

        let key_str = sem_key.format_key();

        let mut c = std::pin::pin!(cancel);

        loop {
            tokio::select! {
                // Sleep for a while before the next extend.
                _ = tokio::time::sleep(sleep_time) => {
                    // Extend the lease only if the entry still exists. If the entry has been removed,
                    // it means the semaphore has been released. Re-inserting it would cause confusion
                    // in the semaphore state and potentially lead to inconsistent behavior.
                    log::debug!(
                        "{}: About to extend semaphore permit lease: {} ttl: {:?}",
                        ctx, key_str, ttl
                    );

                    let upsert = UpsertKV::update(&key_str, &val_bytes)
                        .with(MatchSeq::GE(1))
                        .with_ttl(ttl);

                    let res = meta_client.upsert_kv(upsert).await;
                    res.map_err(|e| conn_io_error(e, "extend semaphore permit lease"))?;
                }
                // Wait if the cancel signal is received.
                _ = &mut c => {
                    // The permit is dropped by the user, we should remove the semaphore permit entry.
                    info!(
                        "leaser task canceled by user: {}, about to remove the semaphore permit entry",
                        sem_key
                    );

                    let upsert = UpsertKV::delete(&key_str);

                    let res = meta_client.upsert_kv(upsert).await;
                    res.map_err(|e| conn_io_error(e, "remove semaphore permit entry"))?;

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
