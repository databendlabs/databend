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

use databend_common_meta_client::RequestFor;
use databend_common_meta_types::Endpoint;
use databend_common_meta_types::ForwardRPCError;
use databend_common_meta_types::MetaOperationError;
use databend_common_meta_types::NodeId;

use crate::message::ForwardRequest;
use crate::message::ForwardRequestBody;
use crate::message::ForwardResponse;

/// A handler that handles meta node request locally
#[async_trait::async_trait]
pub trait Handler<Req: RequestFor> {
    async fn handle(&self, req: ForwardRequest<Req>) -> Result<Req::Reply, MetaOperationError>;
}

/// A handler that forward meta node request locally
#[async_trait::async_trait]
pub trait Forwarder<Req: RequestFor> {
    async fn forward(
        &self,
        target: NodeId,
        req: ForwardRequest<Req>,
    ) -> Result<(Endpoint, Req::Reply), ForwardRPCError>;
}

impl RequestFor for ForwardRequestBody {
    type Reply = ForwardResponse;
}
