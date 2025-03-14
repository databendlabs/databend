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
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use futures::Stream;
use tokio::sync::mpsc::Receiver;

/// A wrapper around [`tokio::sync::mpsc::Receiver`] that implements [`Stream`].
pub struct WatchStream<T> {
    rx: Receiver<T>,
    /// cleanup when this stream is dropped.
    on_drop: Option<Box<dyn FnOnce() + Send + 'static>>,
}

impl<T> fmt::Debug for WatchStream<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "WatchStream")
    }
}

impl<T> Drop for WatchStream<T> {
    fn drop(&mut self) {
        let Some(on_drop) = self.on_drop.take() else {
            return;
        };
        on_drop();
    }
}

impl<T> WatchStream<T> {
    /// Create a new `WatcherStream`.
    pub fn new(rx: Receiver<T>, on_drop: Box<dyn FnOnce() + Send + 'static>) -> Self {
        Self {
            rx,
            on_drop: Some(on_drop),
        }
    }
}

impl<T> Stream for WatchStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}
