// Copyright 2022 Datafuse Labs.
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

use common_meta_types::protobuf::raft_service_client::RaftServiceClient;
use common_meta_types::Endpoint;
use common_meta_types::NodeId;
use common_metrics::counter;
use tonic::transport::channel::Channel;
use tracing::debug;

use crate::metrics::incr_meta_metrics_active_peers;

/// A metrics reporter of active raft peers.
pub struct PeerCounter {
    target: NodeId,
    endpoint: Endpoint,
    endpoint_str: String,
}

impl counter::Count for PeerCounter {
    fn incr_count(&mut self, n: i64) {
        incr_meta_metrics_active_peers(&self.target, &self.endpoint_str, n)
    }
}

/// RaftClient is a grpc client bound with a metrics reporter..
pub type RaftClient = counter::WithCount<PeerCounter, RaftServiceClient<Channel>>;

/// Defines the API of the client to a raft node.
pub trait RaftClientApi {
    fn new(target: NodeId, endpoint: Endpoint, channel: Channel) -> Self;
    fn endpoint(&self) -> &Endpoint;
}

impl RaftClientApi for RaftClient {
    fn new(target: NodeId, endpoint: Endpoint, channel: Channel) -> Self {
        let endpoint_str = endpoint.to_string();

        debug!(
            "RaftClient::new: target: {} endpoint: {}",
            target, endpoint_str
        );

        counter::WithCount::new(RaftServiceClient::new(channel), PeerCounter {
            target,
            endpoint,
            endpoint_str,
        })
    }

    fn endpoint(&self) -> &Endpoint {
        &self.counter().endpoint
    }
}
