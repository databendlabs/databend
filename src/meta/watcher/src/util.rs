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

//! Utilities for the watcher stream.
//!
//! Helper functions for creating and managing watch streams in the metadata store.

use std::future::Future;

use futures::Sink;
use futures::SinkExt;
use futures::Stream;
use futures::StreamExt;
use log::warn;
use tokio::sync::mpsc;
use tokio_util::sync::PollSendError;
use tokio_util::sync::PollSender;

use crate::type_config::TypeConfig;
use crate::KVResult;
use crate::WatchResult;

/// Creates a Sink that converts key-seqv results to watch results.
///
/// Wraps a tokio mpsc Sender in a Sink that can receive `KeySeqVResult` items.
/// Each item is converted using `key_seqv_result_to_watch_result` before being sent.
pub fn new_watch_sink<C>(
    tx: mpsc::Sender<WatchResult<C>>,
    ctx: &'static str,
) -> impl Sink<KVResult<C>, Error = PollSendError<WatchResult<C>>> + Send + 'static
where
    C: TypeConfig,
{
    let snk = PollSender::new(tx);

    let snk = assert_sink::<WatchResult<C>, _, _>(snk);

    let snk = snk.with(move |res: KVResult<C>| {
        let watch_result = key_seqv_result_to_watch_result::<C>(res, ctx);
        futures::future::ready(Ok::<_, PollSendError<_>>(watch_result))
    });

    assert_sink::<KVResult<C>, _, _>(snk)
}

/// Creates a Future that forwards items from a Stream to a Sink.
///
/// Continuously pulls items from the Stream and feeds them to the Sink until
/// either the Stream is exhausted or an error occurs. Errors are logged but not propagated.
#[allow(clippy::manual_async_fn)]
pub fn try_forward<T, S, O>(
    mut strm: S,
    mut snk: O,
    ctx: &'static str,
) -> impl Future<Output = ()> + Send + 'static
where
    T: Send,
    S: Stream<Item = T> + Send + Unpin + 'static,
    O: Sink<T> + Send + Unpin + 'static,
{
    async move {
        while let Some(res) = strm.next().await {
            if let Err(_sink_error) = snk.feed(res).await {
                warn!("fail to send to Sink; close input stream; when({})", ctx);
                break;
            }
        }

        if let Err(_sink_error) = snk.flush().await {
            warn!("fail to flush to Sink; nothing to do; when({})", ctx);
        }
    }
}

/// Converts a key-seqv result to a watch result.
///
/// Transforms a [`KVResult`] into a [`WatchResult`].
/// Errors are logged with the provided context and converted to a gRPC Status.
fn key_seqv_result_to_watch_result<C>(input: KVResult<C>, ctx: &'static str) -> WatchResult<C>
where C: TypeConfig {
    match input {
        Ok((key, seq_v)) => {
            let resp = C::new_response((key, None, Some(seq_v)));
            Ok(resp)
        }
        Err(err) => {
            warn!("{}; when(converting to watch result: {})", err, ctx);
            Err(C::data_error(err))
        }
    }
}
/// Type checking helper for Sink implementations.
///
/// Used for compile-time verification with no runtime effect.
fn assert_sink<T, E, S>(s: S) -> S
where S: Sink<T, Error = E> + Send + 'static {
    s
}
