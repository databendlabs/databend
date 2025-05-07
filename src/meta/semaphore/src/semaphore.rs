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

use databend_common_base::runtime::spawn_named;
use databend_common_meta_client::ClientHandle;
use futures::FutureExt;
use log::info;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::acquirer::Acquirer;
use crate::acquirer::Permit;
use crate::acquirer::SeqPolicy;
use crate::acquirer::SharedAcquirerStat;
use crate::errors::AcquireError;
use crate::meta_event_subscriber::MetaEventSubscriber;
use crate::meta_event_subscriber::Processor;
use crate::queue::PermitEvent;

/// Semaphore implemented on top of the distributed meta-service.
pub struct Semaphore {
    /// The dir path to store the semaphore ids, without trailing slash.
    ///
    /// Such as `foo`, not `foo/`
    prefix: String,

    /// Whether to generate seq number based on time.
    ///
    /// Which is less accurate but more efficient,
    /// because it does lead to transaction conflict when CAS the seq_generator key.
    ///
    /// By default, it is `false`,
    time_based_seq: bool,

    /// The metadata client to interact with the remote meta-service.
    meta_client: Arc<ClientHandle>,

    /// The background subscriber task handle.
    subscriber_task_handle: Option<JoinHandle<()>>,

    /// The sender to cancel the background subscriber task.
    ///
    /// When this sender is dropped, the corresponding receiver becomes ready,
    /// which signals the background task to terminate gracefully.
    subscriber_cancel_tx: oneshot::Sender<()>,

    /// The receiver to receive semaphore state change event from the subscriber.
    sem_event_rx: Option<mpsc::Receiver<PermitEvent>>,

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
        meta_client: Arc<ClientHandle>,
        prefix: impl ToString,
        capacity: u64,
        id: impl ToString,
        lease: Duration,
    ) -> Result<Permit, AcquireError> {
        let sem = Self::new(meta_client, prefix, capacity).await;
        sem.acquire(id, lease).await
    }

    pub async fn new_acquired_by_time(
        meta_client: Arc<ClientHandle>,
        prefix: impl ToString,
        capacity: u64,
        id: impl ToString,
        lease: Duration,
    ) -> Result<Permit, AcquireError> {
        let mut sem = Self::new(meta_client, prefix, capacity).await;
        sem.set_time_based_seq(true);
        sem.acquire(id, lease).await
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
    pub async fn new(meta_client: Arc<ClientHandle>, prefix: impl ToString, capacity: u64) -> Self {
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
            prefix,
            time_based_seq: false,
            meta_client,
            subscriber_task_handle: None,
            subscriber_cancel_tx: cancel_tx,
            sem_event_rx: Some(rx),
            uniq,
        };

        sem.spawn_meta_event_subscriber(tx, capacity, cancel_rx)
            .await;

        sem
    }

    /// Set the time-based sequence number generation policy.
    pub fn set_time_based_seq(&mut self, time_based_seq: bool) {
        self.time_based_seq = time_based_seq;
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
    pub async fn acquire(
        mut self,
        id: impl ToString,
        lease: Duration,
    ) -> Result<Permit, AcquireError> {
        let id = id.to_string();

        let stat = SharedAcquirerStat::new();

        let ctx = format!("{}-Acquirer(id={})", self, id);
        let acquirer = Acquirer {
            prefix: self.prefix.clone(),
            acquirer_id: id.to_string(),
            lease,
            seq_policy: self.seq_policy(),
            meta_client: self.meta_client.clone(),
            subscriber_cancel_tx: self.subscriber_cancel_tx,
            permit_event_rx: self.sem_event_rx.take().unwrap(),
            stat: stat.clone(),
            ctx: ctx.clone(),
        };

        let res = acquirer.acquire().await;

        info!("{}: acquire-result: {:?}; stat: {}", ctx, res, stat);

        res
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
        tx: mpsc::Sender<PermitEvent>,
        capacity: u64,
        cancel_rx: oneshot::Receiver<()>,
    ) {
        let (left, right) = self.queue_key_range();

        let ctx = format!("{}-watcher", self);
        let subscriber = MetaEventSubscriber {
            left,
            right,
            meta_client: self.meta_client.clone(),
            processor: Processor::new(capacity, tx).with_context(&ctx),
            ctx,
        };

        let task_name = self.to_string();
        let fu = subscriber.subscribe_kv_changes(cancel_rx.map(|_| ()));

        let handle = spawn_named(fu, task_name);
        self.subscriber_task_handle = Some(handle);
    }

    fn seq_policy(&self) -> SeqPolicy {
        if self.time_based_seq {
            SeqPolicy::TimeBased
        } else {
            SeqPolicy::GeneratorKey {
                generator_key: self.seq_generator_key(),
            }
        }
    }

    /// The key to store the permit sequence number generator.
    ///
    /// Update this key in the meta-service to obtain a new sequence number.
    fn seq_generator_key(&self) -> String {
        format!("{}/seq_generator", self.prefix)
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
