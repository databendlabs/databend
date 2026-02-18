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

use std::sync::Arc;

use databend_meta_client::ClientHandle;
use databend_meta_runtime::DatabendRuntime;
use databend_meta_types::MetaClientError;
use databend_meta_types::SeqV;
use databend_meta_types::protobuf::WatchRequest;
use databend_meta_types::protobuf::WatchResponse;
use futures::StreamExt;
use log::debug;
use log::warn;
use sub_cache::Source;
use sub_cache::errors::ConnectionClosed;
use sub_cache::errors::SubscribeError;
use sub_cache::errors::Unsupported;
use sub_cache::event_stream::Change;
use sub_cache::event_stream::Event;
use sub_cache::event_stream::EventStream;
use tonic::Status;

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

        let res = self.client.watch_with_initialization(watch).await;

        let client_err = match res {
            Ok(strm) => {
                debug!("{}: watch stream established", self.name);

                let strm = strm.map({
                    let name = self.name.clone();
                    move |x| watch_result_to_cache_event(x, &name)
                });

                let strm = Box::pin(strm);

                return Ok(strm);
            }

            Err(client_err) => client_err,
        };

        warn!(
            "{}: error when establishing watch stream: {}",
            self.name, client_err
        );

        let err = match client_err {
            MetaClientError::HandshakeError(hs_err) => {
                let unsupported = Unsupported::new(hs_err);
                SubscribeError::Unsupported(unsupported)
            }

            MetaClientError::NetworkError(net_err) => {
                let conn_err = ConnectionClosed::new_str(net_err.to_string());
                SubscribeError::Connection(conn_err)
            }
        };

        let err = err
            .context("MetaClientSource::subscribe")
            .context(&self.name);

        Err(err)
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
