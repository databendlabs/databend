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

use futures::future::BoxFuture;
use futures::FutureExt;
use log::debug;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::errors::ConnectionClosed;
use crate::queue::SemaphoreEvent;
use crate::storage::PermitEntry;
use crate::storage::PermitKey;

/// An instance represents a semaphore that has been acquired.
///
/// This instance is a [`Future`] that will be ready when the semaphore is lost.
/// For example, the connection to meta-service is lost and failed to extend the lease.
///
/// The semaphore will be released (and the lease extending task will try to delete
/// the semaphore entry from meta-service) when this instance is dropped intentionally.
///
/// Internally, it contains a `BoxFuture` that holds a oneshot sender.
/// When the future completes or the [`AcquiredGuard`] instance is dropped,
/// the oneshot sender signals the lease extending task to stop,
/// allowing for proper cleanup of the semaphore entry in the meta-service.
pub struct AcquiredGuard {
    pub(crate) fu: BoxFuture<'static, Result<(), ConnectionClosed>>,
}

impl std::fmt::Debug for AcquiredGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AcquiredGuard")
    }
}

impl Future for AcquiredGuard {
    type Output = Result<(), ConnectionClosed>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.fu.poll_unpin(cx)
    }
}

impl AcquiredGuard {
    pub(crate) fn new(
        subscriber_cancel_tx: oneshot::Sender<()>,
        sem_event_rx: mpsc::Receiver<SemaphoreEvent>,
        sem_key: PermitKey,
        sem_entry: PermitEntry,
        leaser_cancel_tx: oneshot::Sender<()>,
    ) -> Self {
        let fu = Self::watch_for_remove(
            sem_event_rx,
            sem_key,
            sem_entry,
            subscriber_cancel_tx,
            leaser_cancel_tx,
        );

        let acquired = AcquiredGuard { fu: Box::pin(fu) };

        acquired
    }

    /// Waits for the semaphore entry to be removed.
    ///
    /// This function blocks until the semaphore key is either:
    /// - Deleted intentionally by another process
    /// - Expired due to TTL timeout on the meta-service
    ///
    /// It can be used to detect when a lock is no longer held and take appropriate action.
    ///
    /// If it returns `Err(ConnectionClosed)`, it does not mean the semaphore is lost,
    /// in such case, the semaphore may still be valid.
    pub(crate) async fn watch_for_remove(
        mut sem_event_rx: mpsc::Receiver<SemaphoreEvent>,
        sem_key: PermitKey,
        sem_entry: PermitEntry,
        _subscriber_cancel_tx: oneshot::Sender<()>,
        _leaser_cancel_tx: oneshot::Sender<()>,
    ) -> Result<(), ConnectionClosed> {
        let ctx = format!("Semaphore-Acquired: {}->{}", sem_key, sem_entry);

        while let Some(sem_event) = sem_event_rx.recv().await {
            debug!("semaphore event: {} received by: {}", sem_event, ctx);

            match sem_event {
                SemaphoreEvent::Acquired((_seq, _entry)) => {}
                SemaphoreEvent::Removed((seq, _)) => {
                    if seq == sem_key.seq {
                        debug!("semaphore entry is removed: {}->{}", sem_key, sem_entry.id);
                        return Ok(());
                    }
                }
            }
        }

        Err(ConnectionClosed::new_str("semaphore event stream closed").context(&ctx))
    }
}
