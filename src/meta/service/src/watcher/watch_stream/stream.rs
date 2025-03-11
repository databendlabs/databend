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

use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use futures::Stream;
use tokio::sync::mpsc::Receiver;

use crate::watcher::dispatch::DispatcherHandle;
use crate::watcher::watch_stream::sender::WatchStreamSender;

/// A wrapper around [`tokio::sync::mpsc::Receiver`] that implements [`Stream`].
#[derive(Debug)]
pub struct WatchStream<T> {
    rx: Receiver<T>,
    // TODO: use a Box<dyn Fn> to replace these two fields
    /// Hold a clone of the sender to remove itself from the dispatcher when dropped.
    sender: Arc<WatchStreamSender>,
    dispatcher_handle: DispatcherHandle,
}

impl<T> Drop for WatchStream<T> {
    fn drop(&mut self) {
        let sender = self.sender.clone();
        self.dispatcher_handle.request(move |d| {
            d.remove_watcher(sender);
        })
    }
}

impl<T> WatchStream<T> {
    /// Create a new `WatcherStream`.
    pub fn new(
        rx: Receiver<T>,
        sender: Arc<WatchStreamSender>,
        dispatcher: DispatcherHandle,
    ) -> Self {
        Self {
            rx,
            sender,
            dispatcher_handle: dispatcher,
        }
    }
}

impl<T> Stream for WatchStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}
