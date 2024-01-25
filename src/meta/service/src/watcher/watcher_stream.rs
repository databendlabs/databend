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

use std::ops::Range;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use databend_common_base::base::tokio::sync::mpsc::error::SendError;
use databend_common_base::base::tokio::sync::mpsc::Receiver;
use databend_common_base::rangemap::RangeMapKey;
use databend_common_meta_types::protobuf::watch_request::FilterType;
use databend_common_meta_types::protobuf::WatchResponse;
use futures::Stream;
use tonic::Status;

use super::WatcherId;
use super::WatcherSender;
use crate::watcher::EventDispatcherHandle;

/// Attributes of a watcher that is interested in kv change events.
#[derive(Clone, Debug)]
pub struct Watcher {
    pub id: WatcherId,

    /// Defines how to filter keys with `key_range`.
    pub filter_type: FilterType,

    /// The range of key this watcher is interested in.
    pub key_range: Range<String>,
}

impl Watcher {
    pub fn new(id: WatcherId, filter_type: FilterType, key_range: Range<String>) -> Self {
        Self {
            id,
            filter_type,
            key_range,
        }
    }
}

/// A handle of a watching stream, for feeding messages to the stream.
pub struct WatchStreamHandle {
    pub watcher: Watcher,
    tx: WatcherSender,
}

impl WatchStreamHandle {
    pub fn new(watcher: Watcher, tx: WatcherSender) -> Self {
        WatchStreamHandle { watcher, tx }
    }

    pub async fn send(
        &self,
        resp: WatchResponse,
    ) -> Result<(), SendError<Result<WatchResponse, Status>>> {
        self.tx.send(Ok(resp)).await
    }
}

/// A wrapper around [`tokio::sync::mpsc::Receiver`] that implements [`Stream`].
#[derive(Debug)]
pub struct WatchStream<T> {
    inner: Receiver<T>,
    watcher: Watcher,
    dispatcher: EventDispatcherHandle,
}

impl<T> Drop for WatchStream<T> {
    fn drop(&mut self) {
        let rng = self.watcher.key_range.clone();
        let id = self.watcher.id;

        self.dispatcher.request(move |d| {
            let key = RangeMapKey::new(rng, id);
            d.remove_watcher(&key)
        })
    }
}

impl<T> WatchStream<T> {
    /// Create a new `WatcherStream`.
    pub fn new(rx: Receiver<T>, watcher: Watcher, dispatcher: EventDispatcherHandle) -> Self {
        Self {
            inner: rx,
            watcher,
            dispatcher,
        }
    }

    /// Closes the receiving half of a channel without dropping it.
    pub fn close(&mut self) {
        self.inner.close()
    }
}

impl<T> Stream for WatchStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.poll_recv(cx)
    }
}

impl<T> AsRef<Receiver<T>> for WatchStream<T> {
    fn as_ref(&self) -> &Receiver<T> {
        &self.inner
    }
}

impl<T> AsMut<Receiver<T>> for WatchStream<T> {
    fn as_mut(&mut self) -> &mut Receiver<T> {
        &mut self.inner
    }
}
