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

use std::sync::Arc;

use databend_common_exception::Result;
use tonic::Request;
use tonic::transport::Channel;

use crate::pb::ApplyResourceRequest;
use crate::pb::ApplyResourceResponse;
use crate::pb::resource_service_client::ResourceServiceClient;

pub(crate) const RESOURCE_CLIENT_VERSION: &str = "v1";
pub(crate) const RESOURCE_CLIENT_VERSION_NAME: &str = "RESOURCE_CLIENT_VERSION";

pub struct ResourceClient {
    pub resource_client: ResourceServiceClient<Channel>,
}

impl ResourceClient {
    pub async fn new(channel: Channel) -> Result<Arc<ResourceClient>> {
        let resource_client = ResourceServiceClient::new(channel);
        Ok(Arc::new(ResourceClient { resource_client }))
    }

    pub async fn apply_resource(
        &self,
        req: Request<ApplyResourceRequest>,
    ) -> Result<ApplyResourceResponse> {
        let mut client = self.resource_client.clone();
        let resp = client.apply_resource(req).await?;
        Ok(resp.into_inner())
    }
}
