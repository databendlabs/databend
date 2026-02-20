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

use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use databend_meta_client::ClientHandle;
use databend_meta_runtime::DatabendRuntime;
use databend_meta_runtime_api::SpawnApi;
use futures::FutureExt;
use log::info;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::acquirer::Acquirer;
use crate::acquirer::DropHandle;
use crate::acquirer::Permit;
use crate::acquirer::SeqPolicy;
use crate::acquirer::SharedAcquirerStat;
use crate::errors::AcquireError;
use crate::errors::ConnectionClosed;
use crate::meta_event_subscriber::MetaEventSubscriber;
use crate::meta_event_subscriber::Processor;
use crate::queue::PermitEvent;

/// Semaphore implemented on top of the distributed meta-service.
pub struct Semaphore {
    /// The dir path to store the semaphore ids, without trailing slash.
    ///
    /// Such as `foo`, not `foo/`
    prefix: String,

    /// The sequence number generation policy for permit ordering.
    ///
    /// Determines how sequence numbers are assigned to semaphore permits:
    /// - `GeneratorKey`: Uses a centralized generator in meta-service for strict ordering
    /// - `TimeBased`: Uses timestamps for faster but potentially non-strict ordering
    seq_policy: SeqPolicy,

    /// The metadata client to interact with the remote meta-service.
    meta_client: Arc<ClientHandle<DatabendRuntime>>,

    /// Duration for automatic permit lease expiration.
    ///
    /// A background task renews the lease every `lease/3` interval.
    /// The permit is automatically released when its entry is removed from meta-service,
    /// either through explicit deletion or lease expiration.
    permit_ttl: Duration,

    /// The background subscriber task handle.
    subscriber_task_handle: Option<JoinHandle<()>>,

    /// The sender to cancel the background subscriber task.
    ///
    /// When this sender is dropped, the corresponding receiver becomes ready,
    /// which signals the background task to terminate gracefully.
    subscriber_cancel_tx: oneshot::Sender<()>,

    /// The receiver to receive semaphore state change event from the subscriber.
    sem_event_rx: Option<mpsc::Receiver<Result<PermitEvent, ConnectionClosed>>>,

    /// A process-wide unique identifier for the semaphore. Used for debugging purposes.
    uniq: u64,
}

impl fmt::Display for Semaphore {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Semaphore[uniq={}]({})", self.uniq, self.prefix)
    }
}

impl Semaphore {
    /// Acquires a new semaphore and returns an [`Permit`] handle.
    ///
    /// # Parameters
    ///
    /// * `meta_client` - The metadata client to interact with the remote meta-service.
    /// * `prefix` - The name of the semaphore and also the directory name to store in meta-service.
    /// * `capacity` - The capacity of the semaphore.
    /// * `id` - The unique identifier for the acquirer. Not used yet in this implementation. In future it is used to force release a semaphore by name.
    /// * `lease` - The time-to-live duration for the acquired semaphore to be released automatically if the lease is not extended.
    ///
    /// # Returns
    ///
    /// Returns an [`Permit`] handle that represents the acquired semaphore. When this handle
    /// is dropped, the semaphore will be released.
    /// It is also a [`Future`] that will be resolved when the semaphore is removed from meta-service.
    pub async fn new_acquired(
        meta_client: Arc<ClientHandle<DatabendRuntime>>,
        prefix: impl ToString,
        capacity: u64,
        id: impl ToString,
        lease: Duration,
    ) -> Result<Permit, AcquireError> {
        let sem = Self::new(meta_client, prefix, capacity, lease).await;
        sem.acquire(id).await
    }

    /// Acquires a new semaphore using timestamp-based sequence numbering.
    ///
    /// This method sets the semaphore to use timestamp-based ordering instead of
    /// the default generator-based ordering, which can be more efficient but less strict.
    ///
    /// # Parameters
    /// * `seq_timestamp` - Optional timestamp to use as sequence number. If `None`, uses current time.
    pub async fn new_acquired_by_time(
        meta_client: Arc<ClientHandle<DatabendRuntime>>,
        prefix: impl ToString,
        capacity: u64,
        id: impl ToString,
        seq_timestamp: Option<Duration>,
        lease: Duration,
    ) -> Result<Permit, AcquireError> {
        let mut sem = Self::new(meta_client, prefix, capacity, lease).await;
        sem.set_time_based_seq(seq_timestamp);
        sem.acquire(id).await
    }

    /// Create a new semaphore.
    ///
    /// The created semaphore starts to subscribe key-value change event but does not acquire any permit yet.
    /// Use [`acquire`](Self::acquire) to acquire a permit.
    ///
    /// # Parameters
    ///
    /// * `meta_client` - The metadata client to interact with the remote meta-service.
    /// * `prefix` - The prefix of the semaphore name and also the directory name to store in meta-service.
    /// * `capacity` - The capacity of the semaphore.
    ///
    /// This method spawns a background task to subscribe to the meta-service key value change events.
    /// The task will be notified to quit when this instance is dropped.
    pub async fn new(
        meta_client: Arc<ClientHandle<DatabendRuntime>>,
        prefix: impl ToString,
        capacity: u64,
        permit_ttl: Duration,
    ) -> Self {
        let mut prefix = prefix.to_string();

        // strip the trailing '/'
        if prefix.ends_with('/') {
            prefix.pop();
        }

        let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
        let (tx, rx) = mpsc::channel(64);

        static UNIQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let uniq = UNIQ.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        let mut sem = Semaphore {
            seq_policy: SeqPolicy::GeneratorKey {
                generator_key: Self::seq_generator_key_for(&prefix),
            },
            prefix,
            meta_client,
            permit_ttl,
            subscriber_task_handle: None,
            subscriber_cancel_tx: cancel_tx,
            sem_event_rx: Some(rx),
            uniq,
        };

        sem.spawn_meta_event_subscriber(tx, capacity, cancel_rx)
            .await;

        sem
    }

    /// Configure semaphore to use timestamp-based sequence numbering.
    ///
    /// Switches from generator-based to timestamp-based sequencing for better performance
    /// at the cost of potentially non-strict ordering.
    ///
    /// # Parameters
    /// * `timestamp` - Optional timestamp to use. If `None`, uses current system time.
    pub fn set_time_based_seq(&mut self, timestamp: Option<Duration>) {
        let timestamp =
            timestamp.unwrap_or_else(|| SystemTime::now().duration_since(UNIX_EPOCH).unwrap());

        self.seq_policy = SeqPolicy::TimeBased { timestamp };
    }

    /// Configure semaphore to use generator-based sequence numbering.
    ///
    /// Switches to centralized sequence generation in meta-service for strict ordering.
    /// This is the default policy but can be explicitly set if previously changed.
    pub fn set_storage_based_seq(&mut self) {
        self.seq_policy = SeqPolicy::GeneratorKey {
            generator_key: self.seq_generator_key(),
        };
    }

    /// Acquires a semaphore with a given id and ttl.
    ///
    /// This function will block until the semaphore is acquired or an error occurs.
    ///
    /// # Parameters
    ///
    /// * `id` - A unique identifier to differentiate between different acquirers.
    ///   The content of the ID does not affect the semaphore behavior.
    ///   It is only kept for other process to identify an acquirer.
    /// * `lease` - The time-to-live duration after which the semaphore will be automatically released.
    ///   Note that this is not a timeout for the acquisition process itself.
    ///
    /// # Lease Extension
    ///
    /// When attempting to acquire a semaphore, after the semaphore permit entry is inserted into
    /// the queue in meta-service, a background task is spawned to periodically extend the lease to
    /// prevent it from expiring. The extension interval is calculated as `lease / 3`,
    /// but capped at 2000ms maximum. This lease extender task will be automatically cancelled when
    /// either:
    ///
    /// * The semaphore is explicitly released, the returned value from this method is dropped.
    /// * The [`Acquirer`] is dropped, i.e., before the semaphore is acquired.
    ///
    /// # Returns
    ///
    /// Returns an [`Permit`] handle that represents the acquired semaphore. When this handle
    /// is dropped or its future is awaited, the semaphore will be released.
    pub async fn acquire(self, id: impl ToString) -> Result<Permit, AcquireError> {
        let permit_ttl = self.permit_ttl;
        let mut acquirer = self.new_acquirer(id, permit_ttl);

        let queued_res = acquirer.enqueue_permit().await;

        info!("{}: enqueue-result: {:?}", acquirer.name, queued_res);

        let queued = queued_res?;

        let name = acquirer.name.clone();
        let stat = acquirer.stat.clone();

        let res = acquirer.await_permit(queued).await;

        info!("{}: acquire-result: {:?}; stat: {}", name, res, stat);

        res
    }

    /// Creates a new acquirer for this semaphore without immediately acquiring.
    ///
    /// This allows for more granular control over the acquisition process by
    /// separating creation from the actual acquire operation.
    pub fn new_acquirer(mut self, id: impl ToString, lease: Duration) -> Acquirer {
        let id = id.to_string();

        let stat = SharedAcquirerStat::new();

        let name = format!("{}-Acquirer(id={})", self, id);
        Acquirer {
            prefix: self.prefix.clone(),
            acquirer_id: id.to_string(),
            lease,
            seq_policy: self.seq_policy(),
            meta_client: self.meta_client.clone(),
            subscriber_cancel_tx: self.subscriber_cancel_tx,
            permit_event_rx: self.sem_event_rx.take().unwrap(),
            stat: stat.clone(),
            acquire_in_progress: Some(DropHandle {
                name: name.clone(),
                finished: false,
            }),
            name: name.clone(),
        }
    }

    /// Spawns a background task to subscribe to the meta-service key value change events.
    ///
    /// This task monitors changes to semaphore entries in the meta-service and converts
    /// them into [`PermitEvent`] instances. These events are then sent through a channel
    /// to notify the semaphore instance about acquisitions and releases.
    ///
    /// The subscriber watches a specific key range determined by the semaphore's prefix.
    /// When semaphore permit are created or deleted, appropriate events are generated.
    async fn spawn_meta_event_subscriber(
        &mut self,
        tx: mpsc::Sender<Result<PermitEvent, ConnectionClosed>>,
        capacity: u64,
        cancel_rx: oneshot::Receiver<()>,
    ) {
        let (left, right) = self.queue_key_range();

        let watcher_name = format!("{}-watcher", self);
        let subscriber = MetaEventSubscriber {
            left,
            right,
            meta_client: self.meta_client.clone(),
            permit_ttl: self.permit_ttl,
            processor: Processor::new(capacity, tx).with_watcher_name(&watcher_name),
            watcher_name,
        };

        let task_name = self.to_string();
        let fu = subscriber.subscribe_kv_changes(cancel_rx.map(|_| ()));

        let handle = DatabendRuntime::spawn(fu, Some(task_name));
        self.subscriber_task_handle = Some(handle);
    }

    fn seq_policy(&self) -> SeqPolicy {
        self.seq_policy.clone()
    }

    /// The key to store the permit sequence number generator.
    ///
    /// Update this key in the meta-service to obtain a new sequence number.
    fn seq_generator_key(&self) -> String {
        Self::seq_generator_key_for(&self.prefix)
    }

    fn seq_generator_key_for(prefix: &str) -> String {
        format!("{}/seq_generator", prefix)
    }

    /// The left-close right-open range for the semaphore ids.
    ///
    /// Since `'0'` is the next char of `'/'`.
    /// `[prefix + "/queue/", prefix + "/queue0")` is the range of the semaphore ids.
    fn queue_key_range(&self) -> (String, String) {
        let p = format!("{}/queue", self.prefix);
        let left = p.clone() + "/";
        let right = p.clone() + "0";

        (left, right)
    }
}
