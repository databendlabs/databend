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

use crate::acquirer;
use crate::acquirer::SharedAcquirerStat;
use crate::errors::ConnectionClosed;
use crate::queue::PermitEvent;
use crate::storage::PermitEntry;
use crate::storage::PermitKey;

/// An instance represents an acquired semaphore permit.
///
/// This instance is a [`Future`] that will be ready when the semaphore permit is removed from meta-service.
/// For example, the connection to meta-service is lost and failed to extend the lease.
///
/// The permit will be released (and the lease extending task will try to delete
/// the [`PermitEntry`] from meta-service) when this instance is dropped intentionally.
///
/// Internally, it contains a `BoxFuture` that holds an oneshot sender.
/// When the [`Permit`] instance is dropped,
/// the oneshot sender signals the lease extending task to stop,
/// allowing for proper cleanup of the [`PermitEntry`] in the meta-service.
pub struct Permit {
    pub acquirer_name: String,
    pub stat: SharedAcquirerStat,
    pub fu: BoxFuture<'static, Result<(), ConnectionClosed>>,
}

impl std::fmt::Debug for Permit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let stat = self.stat.to_string();
        write!(f, "{}-Permit: {}", self.acquirer_name, stat)
    }
}

impl std::fmt::Display for Permit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let stat = self.stat.to_string();
        write!(f, "{}-Permit: {}", self.acquirer_name, stat)
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        let stat = self.stat.to_string();
        info!("{}-Permit Released(dropped): {}", self.acquirer_name, stat);
    }
}

impl Future for Permit {
    type Output = Result<(), ConnectionClosed>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.fu.poll_unpin(cx)
    }
}

impl Permit {
    /// Create a new acquired permit instance.
    ///
    /// It keeps watching the permit event stream and will be notified when the [`PermitEntry`] is removed.
    /// And it also keeps two channels to cancel the subscriber task and the lease extending task.
    pub(crate) fn new(
        acquirer: Acquirer,
        permit_key: PermitKey,
        permit_entry: PermitEntry,
        leaser_cancel_tx: oneshot::Sender<()>,
    ) -> Self {
        let fu = Self::watch_for_remove(
            acquirer.permit_event_rx,
            permit_key,
            permit_entry,
            acquirer.subscriber_cancel_tx,
            leaser_cancel_tx,
        );

        Permit {
            acquirer_name: acquirer.name,
            stat: acquirer.stat,
            fu: Box::pin(fu),
        }
    }

    pub fn stat(&self) -> &SharedAcquirerStat {
        &self.stat
    }

    /// Waits for the [`PermitEntry`] to be removed.
    ///
    /// This function blocks until the [`PermitEntry`] is either:
    /// - Deleted intentionally by another process
    /// - Expired due to TTL timeout on the meta-service
    ///
    /// It can be used to detect when a [`Permit`] is no longer held and take appropriate action.
    ///
    /// Note that if it returns `Err(ConnectionClosed)`, it does not mean the permit is lost,
    /// in such case, the permit may still be valid.
    pub(crate) async fn watch_for_remove(
        mut permit_event_rx: mpsc::Receiver<PermitEvent>,
        permit_key: PermitKey,
        permit_entry: PermitEntry,
        _subscriber_cancel_tx: oneshot::Sender<()>,
        _leaser_cancel_tx: oneshot::Sender<()>,
    ) -> Result<(), ConnectionClosed> {
        let ctx = format!("Semaphore-Acquired: {}->{}", permit_key, permit_entry);

        while let Some(sem_event) = permit_event_rx.recv().await {
            debug!("semaphore event: {} received by: {}", sem_event, ctx);

            match sem_event {
                PermitEvent::Acquired((_seq, _entry)) => {}
                PermitEvent::Removed((seq, _)) => {
                    if seq == permit_key.seq {
                        debug!(
                            "semaphore PermitEntry is removed: {}->{}",
                            permit_key, permit_entry.id
                        );
                        return Ok(());
                    }
                }
            }
        }

        Err(ConnectionClosed::new_str("semaphore event stream closed").context(&ctx))
    }
}
