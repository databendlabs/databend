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

use tokio::sync::watch;

/// A Notify based on tokio::sync::watch,
/// which allows `notify_waiters` to be called before `notified` was called,
/// without losing notification.
pub struct WatchNotify {
    rx: watch::Receiver<bool>,
    tx: watch::Sender<bool>,
}

impl Default for WatchNotify {
    fn default() -> Self {
        Self::new()
    }
}

impl WatchNotify {
    pub fn new() -> Self {
        let (tx, rx) = watch::channel(false);
        Self { rx, tx }
    }

    pub async fn notified(&self) {
        let mut rx = self.rx.clone();
        // we do care about the result,
        // any change or error should wake up the waiting task
        let _ = rx.changed().await;
    }

    pub fn notify_waiters(&self) {
        let _ = self.tx.send_replace(true);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_notify() {
        let notify = WatchNotify::new();
        let notified = notify.notified();
        notify.notify_waiters();
        notified.await;
    }

    #[tokio::test]
    async fn test_notify_waiters_ahead() {
        let notify = WatchNotify::new();
        // notify_waiters ahead of notified being instantiated and awaited
        notify.notify_waiters();

        // this should not await indefinitely
        let notified = notify.notified();
        notified.await;
    }
}
