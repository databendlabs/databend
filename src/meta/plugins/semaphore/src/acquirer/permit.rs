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

use databend_meta_runtime::DatabendRuntime;
use databend_meta_runtime_api::SpawnApi;
use futures::FutureExt;
use log::debug;
use log::info;
use log::warn;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::acquirer::Acquirer;
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

    // Hold it so that the subscriber keep running.
    pub(crate) _subscriber_cancel_tx: oneshot::Sender<()>,

    // Hold it so that the lease extending task keep running.
    pub(crate) _leaser_cancel_tx: oneshot::Sender<()>,

    /// Gets ready if the [`PermitEntry`] is removed from meta-service.
    pub(crate) is_removed_rx: oneshot::Receiver<()>,
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
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match self.is_removed_rx.poll_unpin(cx) {
            std::task::Poll::Ready(_) => std::task::Poll::Ready(()),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
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
        let (is_removed_tx, is_removed_rx) = oneshot::channel::<()>();

        // There must be a standalone task that consumes incoming events so that it won't block the permit_event_rx sender
        let acquirer_name = acquirer.name.clone();
        let fu = Self::watch_for_remove(
            acquirer.permit_event_rx,
            acquirer_name.clone(),
            permit_key,
            permit_entry,
            is_removed_tx,
        );

        DatabendRuntime::spawn(fu, Some(format!("{}-WatchRemove", acquirer_name.clone())));

        Permit {
            acquirer_name,
            stat: acquirer.stat,
            _subscriber_cancel_tx: acquirer.subscriber_cancel_tx,
            _leaser_cancel_tx: leaser_cancel_tx,
            is_removed_rx,
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
        mut permit_event_rx: mpsc::Receiver<Result<PermitEvent, ConnectionClosed>>,
        acquirer_name: String,
        permit_key: PermitKey,
        permit_entry: PermitEntry,
        is_removed_tx: oneshot::Sender<()>,
    ) {
        let ctx = format!("{}: {}->{}", acquirer_name, permit_key, permit_entry);

        while let Some(sem_event_res) = permit_event_rx.recv().await {
            let sem_event = match sem_event_res {
                Ok(x) => x,
                Err(e) => {
                    warn!("{}: watch_for_remove recv error: {}", ctx, e);
                    return;
                }
            };

            debug!("{}: received semaphore event: {}", ctx, sem_event);

            match sem_event {
                PermitEvent::Acquired((_seq, _entry)) => {}
                PermitEvent::Removed((seq, _)) => {
                    if seq == permit_key.seq {
                        warn!("{}: PermitEntry is removed", ctx);
                        is_removed_tx.send(()).ok();
                        return;
                    }
                }
            }
        }
    }
}
