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

pub mod grpc_service;

use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use futures::Stream;
use pin_project::pin_project;

#[pin_project]
pub(crate) struct OnCompleteStream<S> {
    #[pin]
    stream: S,
    callback: Option<Box<dyn FnOnce() + Send + 'static>>,
}

impl<S> OnCompleteStream<S> {
    pub fn new(stream: S, callback: impl FnOnce() + Send + 'static) -> Self {
        Self {
            stream,
            callback: Some(Box::new(callback)),
        }
    }
}

impl<S: Stream> Stream for OnCompleteStream<S> {
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        let res = this.stream.poll_next(cx);

        if let Poll::Ready(None) = &res {
            if let Some(callback) = this.callback.take() {
                callback();
            }
        }

        res
    }
}
