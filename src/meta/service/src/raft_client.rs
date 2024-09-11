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

use databend_common_meta_types::protobuf::raft_service_client::RaftServiceClient;
use databend_common_meta_types::Endpoint;
use databend_common_meta_types::GrpcConfig;
use databend_common_meta_types::NodeId;
use databend_common_metrics::count;
use log::debug;
use tonic::transport::channel::Channel;

use crate::metrics::raft_metrics;

/// A metrics reporter of active raft peers.
#[derive(Debug)]
pub struct PeerCounter {
    target: NodeId,
    endpoint: Endpoint,
    endpoint_str: String,
}

impl count::Count for PeerCounter {
    fn incr_count(&mut self, n: i64) {
        raft_metrics::network::incr_active_peers(&self.target, &self.endpoint_str, n)
    }
}

/// RaftClient is a grpc client bound with a metrics reporter..
pub type RaftClient = count::WithCount<PeerCounter, RaftServiceClient<Channel>>;

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

        let cli = RaftServiceClient::new(channel)
            .max_decoding_message_size(GrpcConfig::MAX_DECODING_SIZE)
            .max_encoding_message_size(GrpcConfig::MAX_ENCODING_SIZE);

        count::WithCount::new(cli, PeerCounter {
            target,
            endpoint,
            endpoint_str,
        })
    }

    fn endpoint(&self) -> &Endpoint {
        &self.counter().endpoint
    }
}
