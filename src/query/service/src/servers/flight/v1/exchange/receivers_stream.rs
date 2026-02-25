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

//! Shared `ReceiversStream` — merges multiple `InboundChannel`s into a single
//! `Stream<Item = Result<DataBlock>>`, used by both `ThreadChannelReader` and
//! `BroadcastRecvTransform`.

use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use futures::Stream;
use futures_util::future::BoxFuture;

use crate::servers::flight::v1::network::InboundChannel;

pub(super) struct ReceiversStream {
    finished: Vec<bool>,
    receivers: Vec<Arc<dyn InboundChannel>>,
    futures: Vec<BoxFuture<'static, std::result::Result<Option<DataBlock>, ErrorCode>>>,
}

impl ReceiversStream {
    pub fn new(receivers: Vec<Arc<dyn InboundChannel>>) -> Self {
        Self {
            futures: vec![],
            finished: vec![false; receivers.len()],
            receivers,
        }
    }

    pub fn close(&mut self) {
        for (idx, receiver) in self.receivers.iter().enumerate() {
            self.finished[idx] = true;
            receiver.close();
        }
    }
}

impl Drop for ReceiversStream {
    fn drop(&mut self) {
        self.close();
    }
}

impl Stream for ReceiversStream {
    type Item = Result<DataBlock>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if this.futures.is_empty() {
            for receiver in this.receivers.iter() {
                this.futures.push(unsafe {
                    std::mem::transmute::<
                        Pin<Box<dyn Future<Output = Result<Option<DataBlock>>> + Send + '_>>,
                        Pin<Box<dyn Future<Output = Result<Option<DataBlock>>> + Send + 'static>>,
                    >(receiver.recv())
                });
            }
        }

        let mut all_finished = true;
        for idx in 0..this.receivers.len() {
            if this.finished[idx] {
                continue;
            }

            match this.futures[idx].as_mut().poll(cx) {
                Poll::Ready(Ok(Some(block))) => {
                    this.futures[idx] = unsafe {
                        std::mem::transmute::<
                            Pin<Box<dyn Future<Output = Result<Option<DataBlock>>> + Send + '_>>,
                            Pin<
                                Box<
                                    dyn Future<Output = Result<Option<DataBlock>>> + Send + 'static,
                                >,
                            >,
                        >(this.receivers[idx].recv())
                    };
                    return Poll::Ready(Some(Ok(block)));
                }
                Poll::Ready(Ok(None)) => {
                    this.finished[idx] = true;
                    continue;
                }
                Poll::Ready(Err(e)) => {
                    for idx in 0..this.receivers.len() {
                        this.finished[idx] = true;
                        this.receivers[idx].close();
                    }

                    return Poll::Ready(Some(Err(e)));
                }
                Poll::Pending => {
                    all_finished = false;
                    continue;
                }
            }
        }

        match all_finished {
            true => Poll::Ready(None),
            false => Poll::Pending,
        }
    }
}
