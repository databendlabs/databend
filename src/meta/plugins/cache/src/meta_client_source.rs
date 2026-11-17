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

//! This mod provide cache with meta-client as data source

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use databend_meta_client::ClientHandle;
use databend_meta_client::types::MetaClientError;
use databend_meta_client::types::SeqV;
use databend_meta_client::types::protobuf::Empty;
use databend_meta_client::types::protobuf::WatchRequest;
use databend_meta_client::types::protobuf::WatchResponse;
use databend_meta_runtime::DatabendRuntime;
use futures::Stream;
use futures::StreamExt;
use futures::future;
use futures::future::Either;
use futures::stream;
use log::debug;
use log::info;
use log::warn;
use sub_cache::Source;
use sub_cache::errors::ConnectionClosed;
use sub_cache::errors::SubscribeError;
use sub_cache::errors::Unsupported;
use sub_cache::event_stream::Change;
use sub_cache::event_stream::Event;
use sub_cache::event_stream::EventStream;
use tonic::Status;

/// How often the meta node serving a watch is asked whether it still leads, see
/// [`MetaClientSource`].
const LEADER_CHECK_INTERVAL: Duration = Duration::from_secs(5);

/// How many meta nodes one `subscribe` asks whether they lead, following the leader each names.
const LEADER_LOOKUPS: usize = 3;

/// The source of a [`Cache`](crate::Cache): a watch served by the meta-service leader.
///
/// A follower that installs a snapshot sends no events for the changes the snapshot contains, and
/// keeps its watch streams open (databend-meta#193). A leader never installs a snapshot, so the
/// watch is opened only on the leader. The node serving it is asked every
/// [`LEADER_CHECK_INTERVAL`] whether it still leads the term it led when the watch was opened. If
/// it does not, or does not answer, the stream ends with an error, and the cache rebuilds from a
/// new watch on the current leader. The changes in a snapshot that a node installs right after it
/// steps down can still be missing from the copy for up to one check interval.
///
/// A leader cut off from the other meta nodes still leads in its own view: its watch stays open,
/// without changes, until the cut heals. If the client cannot connect to the leader, e.g. its
/// endpoints lack the leader's gRPC address, the cache does not become ready.
pub struct MetaClientSource {
    pub(crate) client: Arc<ClientHandle<DatabendRuntime>>,

    /// The name of the watcher owning this source. For debugging purposes.
    pub(crate) name: String,
}

#[async_trait::async_trait]
impl Source<SeqV> for MetaClientSource {
    async fn subscribe(
        &self,
        left: &str,
        right: &str,
    ) -> Result<EventStream<SeqV>, SubscribeError> {
        let watch =
            WatchRequest::new(left.to_string(), Some(right.to_string())).with_initial_flush(true);

        self.watch_on_leader(watch).await.map_err(|err| {
            warn!(
                "{}: error when establishing watch stream: {}",
                self.name, err
            );
            err.context("MetaClientSource::subscribe")
                .context(&self.name)
        })
    }
}

impl MetaClientSource {
    /// Open `watch` on the leader. The stream ends with an error once its node no longer leads,
    /// see [`MetaClientSource`].
    async fn watch_on_leader(
        &self,
        watch: WatchRequest,
    ) -> Result<EventStream<SeqV>, SubscribeError> {
        // The meta client does not export the type of an established client, so its values here
        // are only inferred, never named.
        let mut lookups = 0;
        let (mut leader, term) = loop {
            let mut client = self
                .client
                .make_established_client()
                .await
                .map_err(subscribe_error)?;
            let status = client.get_cluster_status(Empty {}).await;
            let status = status.map_err(status_error)?.into_inner();
            if status.is_leader {
                break (client, status.current_term);
            }

            let not_leader = format!(
                "{} is {}, not the leader; leader: {}",
                client.target_endpoint(),
                status.state,
                status.leader.as_deref().unwrap_or("unknown")
            );
            lookups += 1;
            if lookups == LEADER_LOOKUPS {
                let err = ConnectionClosed::new_str(not_leader).context("find the leader to watch");
                return Err(SubscribeError::Connection(err));
            }
            info!("{}: {}; switch to the leader", self.name, not_leader);

            // A follower forwards a read to the leader, and its reply names the leader: the
            // client connects to the leader from then on.
            if let Err(err) = self.client.get_kv(&watch.key).await {
                warn!("{}: a read to find the leader failed: {}", self.name, err);
            }
        };

        let target = leader.target_endpoint().to_string();
        let strm = leader.watch(watch).await.map_err(status_error)?;
        debug!(
            "{}: watch stream established on {}, the leader of term {}",
            self.name, target, term
        );

        let name = self.name.clone();
        let events = strm
            .into_inner()
            .map(move |x| watch_result_to_cache_event(x, &name));

        let stop = async move {
            loop {
                tokio::time::sleep(LEADER_CHECK_INTERVAL).await;

                let ask = leader.get_cluster_status(Empty {});
                let status = match tokio::time::timeout(LEADER_CHECK_INTERVAL, ask).await {
                    Ok(Ok(resp)) => resp.into_inner(),
                    Ok(Err(err)) => return format!("cannot ask {target} whether it leads: {err}"),
                    Err(_) => {
                        return format!(
                            "{target} did not answer whether it leads in {LEADER_CHECK_INTERVAL:?}"
                        );
                    }
                };
                if !status.is_leader || status.current_term != term {
                    return format!(
                        "{target} no longer leads term {term}: it is {} in term {}",
                        status.state, status.current_term
                    );
                }
            }
        };

        Ok(until_stopped(events, stop))
    }
}

/// `name` is the watcher name for debugging purposes.
fn watch_result_to_cache_event(
    watch_result: Result<WatchResponse, Status>,
    name: &str,
) -> Result<Event<SeqV>, ConnectionClosed> {
    let watch_response = watch_result.map_err(|status| {
        warn!("{}: watch stream error: {}", name, status);
        ConnectionClosed::new_str(status.to_string())
    })?;

    let event = if let Some(ev) = watch_response.event {
        let change = Change::new(ev.key, ev.prev.map(SeqV::from), ev.current.map(SeqV::from));

        if watch_response.is_initialization {
            Event::Initialization(change)
        } else {
            Event::Change(change)
        }
    } else {
        if !watch_response.is_initialization {
            warn!(
                "{}: watch stream received an empty event, but not in initialization mode",
                name
            );
        }
        Event::InitializationComplete
    };

    Ok(event)
}

fn subscribe_error(client_err: MetaClientError) -> SubscribeError {
    match client_err {
        MetaClientError::HandshakeError(hs_err) => {
            SubscribeError::Unsupported(Unsupported::new(hs_err))
        }
        MetaClientError::NetworkError(net_err) => {
            SubscribeError::Connection(ConnectionClosed::new_str(net_err.to_string()))
        }
    }
}

/// Like [`subscribe_error`], for the error status of a call on an established client.
fn status_error(status: Status) -> SubscribeError {
    subscribe_error(MetaClientError::from(status))
}

/// The items of `events` until it ends, or until `stop` resolves: then an error with the reason
/// `stop` gives, and the end.
fn until_stopped(
    events: impl Stream<Item = Result<Event<SeqV>, ConnectionClosed>> + Send + 'static,
    stop: impl Future<Output = String> + Send + 'static,
) -> EventStream<SeqV> {
    let state = Some((events.boxed(), Box::pin(stop)));
    stream::unfold(state, |state| async move {
        let (mut events, mut stop) = state?;
        // `stop` first, so that a stream with an event always ready does not starve the check.
        let next = match future::select(stop.as_mut(), events.next()).await {
            Either::Left((reason, _)) => Err(reason),
            Either::Right((item, _)) => Ok(item),
        };
        match next {
            Ok(Some(item)) => Some((item, Some((events, stop)))),
            Ok(None) => None,
            Err(reason) => Some((Err(ConnectionClosed::new_str(reason)), None)),
        }
    })
    .boxed()
}

#[cfg(test)]
mod tests {
    use std::task::Poll;

    use futures::executor::block_on;

    use super::*;

    fn complete() -> Result<Event<SeqV>, ConnectionClosed> {
        Ok(Event::InitializationComplete)
    }

    #[test]
    fn test_until_stopped_ends_with_the_events() {
        let events = stream::iter([complete(), complete()]);
        let got = block_on(until_stopped(events, future::pending()).collect::<Vec<_>>());
        assert_eq!(got.len(), 2);
        assert!(got.iter().all(Result::is_ok));
    }

    /// Once `stop` resolves, an error with its reason ends the stream, though more events are
    /// ready.
    #[test]
    fn test_until_stopped_ends_with_an_error_when_stopped() {
        let events = stream::iter([complete(), complete(), complete()]);
        let mut polled = false;
        let stop = future::poll_fn(move |cx| {
            if polled {
                return Poll::Ready("stopped".to_string());
            }
            polled = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        });

        let got = block_on(until_stopped(events, stop).collect::<Vec<_>>());
        assert_eq!(got.len(), 2);
        assert!(got[0].is_ok());
        let err = got[1].as_ref().unwrap_err();
        assert!(err.to_string().contains("stopped"), "{err}");
    }
}
